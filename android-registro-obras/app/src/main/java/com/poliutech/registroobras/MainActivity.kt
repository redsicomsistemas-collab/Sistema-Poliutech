package com.poliutech.marstatuscotizacion

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.speech.RecognitionListener
import android.speech.RecognizerIntent
import android.speech.SpeechRecognizer
import android.text.InputType
import android.widget.ArrayAdapter
import android.widget.EditText
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.core.view.isVisible
import androidx.recyclerview.widget.LinearLayoutManager
import com.google.android.material.snackbar.Snackbar
import com.poliutech.marstatuscotizacion.databinding.ActivityMainBinding
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.text.NumberFormat
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone

class MainActivity : AppCompatActivity() {
    private enum class VoiceTarget {
        COMMAND,
        CONDITIONS,
    }

    private lateinit var binding: ActivityMainBinding
    private lateinit var prefs: android.content.SharedPreferences
    private val client = OkHttpClient()
    private val adapter = QuoteAdapter(::promptStatusChange, ::openQuotePdf)
    private var currentRol: String = ""
    private var validStatuses: List<String> = listOf("TODOS")
    private var currentVoicePreview: JSONObject? = null
    private var speechRecognizer: SpeechRecognizer? = null
    private var isListening = false
    private var shouldRestartListening = false
    private var lastFinalTranscript: String = ""
    private var activeVoiceTarget: VoiceTarget = VoiceTarget.COMMAND
    private var lastDashboardTotalQuotes = 0
    private var isVoicePanelExpanded = false
    private val speechHandler = Handler(Looper.getMainLooper())
    private val restartListeningRunnable = Runnable {
        if (shouldRestartListening && isListening) {
            try {
                speechRecognizer?.cancel()
                speechRecognizer?.startListening(buildSpeechIntent())
            } catch (_: Exception) {
                binding.txtVoicePreview.text = "No se pudo reanudar el micrófono. Intenta de nuevo."
                isListening = false
                shouldRestartListening = false
                setVoiceButtonIdle()
            }
        }
    }
    private val requestNotificationPermission = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (granted) {
            registerPushTokenIfPossible()
        }
    }
    private val requestAudioPermission = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (granted) {
            startVoiceRecognition()
        } else {
            showMessage("Debes permitir el micrófono para dictar.")
        }
    }
    private val voiceQuoteLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) {
        if (it.resultCode == RESULT_OK && getToken().isNotBlank()) {
            loadAllData()
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)
        setSupportActionBar(binding.toolbar)
        setupSpeechRecognizer()

        prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE)
        if (prefs.getString(KEY_BASE_URL, "").isNullOrBlank()) {
            prefs.edit().putString(KEY_BASE_URL, DEFAULT_BASE_URL).apply()
        }

        currentRol = prefs.getString(KEY_USER_ROLE, "").orEmpty()
        binding.recyclerQuotes.layoutManager = LinearLayoutManager(this)
        binding.recyclerQuotes.adapter = adapter

        binding.btnLogin.setOnClickListener { login() }
        binding.btnRecargar.setOnClickListener { loadAllData() }
        binding.btnVoiceDictate.setOnClickListener {
            activeVoiceTarget = VoiceTarget.COMMAND
            startVoiceRecognition()
        }
        binding.btnVoiceConditionsDictate.setOnClickListener {
            activeVoiceTarget = VoiceTarget.CONDITIONS
            startVoiceRecognition()
        }
        binding.btnVoicePreview.setOnClickListener { requestVoiceQuotePreview(confirm = false) }
        binding.btnVoiceSave.setOnClickListener { requestVoiceQuotePreview(confirm = true) }
        binding.btnVoiceClear.setOnClickListener { clearVoiceInputs() }
        binding.btnToggleVoicePanel.setOnClickListener { openVoiceQuoteScreen() }
        binding.btnCambiarServidor.setOnClickListener { showServerDialog(force = false) }
        binding.checkMostrarPassword.setOnCheckedChangeListener { _, isChecked ->
            binding.inputPassword.inputType = if (isChecked) {
                InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_VISIBLE_PASSWORD
            } else {
                InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD
            }
            binding.inputPassword.setSelection(binding.inputPassword.text?.length ?: 0)
        }
        binding.spinnerStatus.onItemSelectedListener = SimpleItemSelectedListener {
            if (getToken().isNotBlank()) {
                loadQuotes()
            }
        }

        updateServerLabel()
        refreshUiState()
        ensureNotificationPermission()
        setupStatusSpinner(listOf("TODOS"))
        if (getToken().isNotBlank()) {
            registerPushTokenIfPossible()
            loadAllData()
            handlePdfIntent(intent)
            handleFollowupIntent(intent)
        }
    }

    override fun onResume() {
        super.onResume()
        registerPushTokenIfPossible()
    }

    override fun onDestroy() {
        super.onDestroy()
        speechHandler.removeCallbacks(restartListeningRunnable)
        speechRecognizer?.cancel()
        speechRecognizer?.destroy()
        speechRecognizer = null
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handlePdfIntent(intent)
        handleFollowupIntent(intent)
    }

    override fun onCreateOptionsMenu(menu: android.view.Menu): Boolean {
        menuInflater.inflate(R.menu.main_menu, menu)
        return true
    }

    override fun onOptionsItemSelected(item: android.view.MenuItem): Boolean {
        return when (item.itemId) {
            R.id.action_change_server -> {
                showServerDialog(force = false)
                true
            }
            R.id.action_reload -> {
                loadAllData()
                true
            }
            R.id.action_logout -> {
                prefs.edit().remove(KEY_TOKEN).remove(KEY_USER_NAME).remove(KEY_USER_ROLE).apply()
                currentRol = ""
                refreshUiState()
                true
            }
            else -> super.onOptionsItemSelected(item)
        }
    }

    private fun refreshUiState() {
        val loggedIn = getToken().isNotBlank()
        binding.loginCard.isVisible = !loggedIn
        binding.contentGroup.isVisible = true
        binding.txtCurrentUser.text = prefs.getString(KEY_USER_NAME, "").orEmpty()
        binding.txtCurrentUser.isVisible = loggedIn
        binding.spinnerStatus.isEnabled = loggedIn
        binding.btnRecargar.isEnabled = loggedIn
        binding.recyclerQuotes.isEnabled = loggedIn
        binding.btnVoicePreview.isEnabled = loggedIn
        binding.btnVoiceSave.isEnabled = loggedIn
        binding.btnVoiceClear.isEnabled = loggedIn
        binding.btnVoiceConditionsDictate.isEnabled = loggedIn
        binding.btnToggleVoicePanel.isEnabled = loggedIn
        binding.inputVoiceCommand.isEnabled = loggedIn
        binding.inputVoiceClient.isEnabled = loggedIn
        binding.inputVoiceNotes.isEnabled = loggedIn
        binding.inputVoiceConditions.isEnabled = loggedIn
        binding.txtEmpty.isVisible = loggedIn && adapter.itemCount == 0
        if (!loggedIn) {
            lastDashboardTotalQuotes = 0
            isVoicePanelExpanded = false
            currentVoicePreview = null
            adapter.submitList(emptyList())
            binding.txtVoicePreview.text = "Inicia sesión para ver cotizaciones y usar el dictado por voz."
            renderVoicePanelState()
            setVoiceButtonIdle()
        } else {
            renderVoicePanelState()
            if (currentVoicePreview == null && binding.txtVoicePreview.text.isBlank()) {
                binding.txtVoicePreview.text = getString(R.string.voice_preview_empty)
            }
            if (!isListening) setVoiceButtonIdle()
        }
    }

    private fun forceLogout(message: String) {
        prefs.edit().remove(KEY_TOKEN).remove(KEY_USER_NAME).remove(KEY_USER_ROLE).apply()
        currentRol = ""
        adapter.submitList(emptyList())
        refreshUiState()
        showMessage(message)
    }

    private fun toggleVoicePanel() {
        if (getToken().isBlank()) {
            showMessage("Inicia sesión para usar la cotización por voz.")
            return
        }
        openVoiceQuoteScreen()
    }

    private fun renderVoicePanelState() {
        binding.voicePanelCard.isVisible = false
        binding.btnToggleVoicePanel.text = getString(R.string.voice_open_panel)
    }

    private fun openVoiceQuoteScreen() {
        if (getToken().isBlank()) {
            showMessage("Inicia sesión para usar la cotización por voz.")
            return
        }
        voiceQuoteLauncher.launch(Intent(this, VoiceQuoteActivity::class.java))
    }

    private fun clearVoiceInputs() {
        stopVoiceRecognition(submitPreview = false)
        currentVoicePreview = null
        binding.inputVoiceCommand.text?.clear()
        binding.inputVoiceClient.text?.clear()
        binding.inputVoiceNotes.text?.clear()
        binding.inputVoiceConditions.text?.clear()
        binding.txtVoicePreview.text = getString(R.string.voice_preview_empty)
        showMessage("Se limpió la captura anterior.")
    }

    private fun handleAuthError(responseCode: Int, body: String): Boolean {
        if (responseCode == 401 || responseCode == 403) {
            forceLogout(parseError(body, "Tu sesión venció. Vuelve a iniciar sesión."))
            return true
        }
        return false
    }

    private fun getBaseUrl(): String = prefs.getString(KEY_BASE_URL, "")?.trim()?.trimEnd('/').orEmpty()
    private fun getToken(): String = prefs.getString(KEY_TOKEN, "")?.trim().orEmpty()

    private fun updateServerLabel() {
        binding.txtServerUrl.text = getBaseUrl().ifBlank { getString(R.string.server_not_configured) }
    }

    private fun setupStatusSpinner(statuses: List<String>) {
        validStatuses = statuses
        val adapter = ArrayAdapter(this, android.R.layout.simple_spinner_item, statuses)
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item)
        binding.spinnerStatus.adapter = adapter
    }

    private fun showServerDialog(force: Boolean) {
        val input = EditText(this).apply {
            hint = "https://tu-app.onrender.com"
            setText(getBaseUrl())
            setSelection(text.length)
        }
        val dialog = AlertDialog.Builder(this)
            .setTitle(if (force) "Configura servidor" else "Cambiar servidor")
            .setView(input)
            .setCancelable(!force)
            .setPositiveButton("Guardar") { _, _ ->
                val url = input.text?.toString()?.trim().orEmpty()
                if (!url.startsWith("http://") && !url.startsWith("https://")) {
                    showMessage("La URL debe iniciar con http:// o https://")
                    if (force) showServerDialog(force = true)
                    return@setPositiveButton
                }
                prefs.edit().putString(KEY_BASE_URL, url.trimEnd('/')).apply()
                updateServerLabel()
            }
        if (!force) {
            dialog.setNegativeButton("Cancelar", null)
        }
        dialog.show()
    }

    private fun login() {
        val baseUrl = getBaseUrl()
        if (baseUrl.isBlank()) {
            showServerDialog(force = true)
            return
        }
        val nombre = binding.inputUsuario.text?.toString()?.trim().orEmpty()
        val password = binding.inputPassword.text?.toString()?.trim().orEmpty()
        if (nombre.isBlank() || password.isBlank()) {
            showMessage("Captura usuario y contraseña.")
            return
        }

        val payload = JSONObject()
            .put("nombre", nombre)
            .put("password", password)

        val request = Request.Builder()
            .url("$baseUrl/api/mobile/login")
            .post(payload.toString().toRequestBody(JSON))
            .build()

        binding.progressBar.isVisible = true
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showMessage("No se pudo conectar al servidor.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    if (!response.isSuccessful) {
                        showMessage(parseError(body, "No se pudo iniciar sesión."))
                        return@runOnUiThread
                    }
                    val json = JSONObject(body)
                    val user = json.getJSONObject("user")
                    prefs.edit()
                        .putString(KEY_TOKEN, json.getString("token"))
                        .putString(KEY_USER_NAME, user.getString("nombre"))
                        .putString(KEY_USER_ROLE, user.getString("rol"))
                        .apply()
                    currentRol = user.getString("rol")
                    refreshUiState()
                    registerPushTokenIfPossible()
                    loadAllData()
                    handleFollowupIntent(intent)
                }
            }
        })
    }

    private fun loadAllData() {
        val baseUrl = getBaseUrl()
        val token = getToken()
        if (baseUrl.isBlank() || token.isBlank()) {
            refreshUiState()
            return
        }
        loadDashboardSummary()
        loadQuotes()
    }

    private fun loadDashboardSummary() {
        val request = Request.Builder()
            .url("${getBaseUrl()}/api/mobile/dashboard/summary")
            .header("Authorization", "Bearer ${getToken()}")
            .get()
            .build()

        binding.progressBar.isVisible = true
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showMessage("No se pudo cargar el dashboard.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    if (!response.isSuccessful) {
                        if (handleAuthError(response.code, body)) return@runOnUiThread
                        showMessage(parseError(body, "No se pudo cargar el dashboard."))
                        return@runOnUiThread
                    }
                    val json = JSONObject(body)
                    val kpis = json.getJSONObject("kpis")
                    val breakdown = json.getJSONObject("status_breakdown")
                    val statusList = mutableListOf("TODOS")
                    val valid = json.optJSONArray("valid_estatus") ?: JSONArray()
                    for (i in 0 until valid.length()) {
                        statusList.add(valid.getString(i))
                    }
                    val currentSelection = binding.spinnerStatus.selectedItem?.toString().orEmpty().ifBlank { "TODOS" }
                    setupStatusSpinner(statusList)
                    val selectedIndex = statusList.indexOf(currentSelection).takeIf { it >= 0 } ?: 0
                    binding.spinnerStatus.setSelection(selectedIndex, false)

                    lastDashboardTotalQuotes = kpis.optInt("total_cotizaciones")
                    binding.txtTotalQuotes.text = lastDashboardTotalQuotes.toString()
                    binding.txtTotalAmount.text = formatMoney(kpis.optDouble("total_importe"))
                    binding.txtPendingCount.text = breakdown.optInt("PENDIENTE").toString()
                    binding.txtWonCount.text = breakdown.optInt("GANADA").toString()
                }
            }
        })
    }

    private fun loadQuotes() {
        val selectedStatus = binding.spinnerStatus.selectedItem?.toString().orEmpty()
        val url = buildString {
            append(getBaseUrl())
            append("/api/mobile/cotizaciones")
            if (selectedStatus.isNotBlank() && selectedStatus != "TODOS") {
                append("?estatus=")
                append(selectedStatus)
            }
        }

        val request = Request.Builder()
            .url(url)
            .header("Authorization", "Bearer ${getToken()}")
            .get()
            .build()

        binding.progressBar.isVisible = true
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showMessage("No se pudieron cargar las cotizaciones.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    if (!response.isSuccessful) {
                        if (handleAuthError(response.code, body)) return@runOnUiThread
                        showMessage(parseError(body, "No se pudieron cargar las cotizaciones."))
                        return@runOnUiThread
                    }
                    val itemsJson = JSONObject(body).optJSONArray("items") ?: JSONArray()
                    val items = mutableListOf<QuoteItem>()
                    for (i in 0 until itemsJson.length()) {
                        val row = itemsJson.getJSONObject(i)
                        items.add(
                            QuoteItem(
                                id = row.optInt("id"),
                                folio = row.optString("folio"),
                                cliente = row.optString("cliente"),
                                fecha = formatDate(row.optString("fecha")),
                                estatus = row.optString("estatus"),
                                total = formatMoney(row.optDouble("total")),
                                responsable = row.optString("responsable"),
                                pdfUrl = row.optString("pdf_url"),
                            )
                        )
                    }
                    adapter.submitList(items)
                    val selectedFilter = binding.spinnerStatus.selectedItem?.toString().orEmpty().ifBlank { "TODOS" }
                    binding.txtEmpty.text = when {
                        items.isNotEmpty() -> getString(R.string.no_quotes)
                        currentRol.equals("ADMIN", ignoreCase = true) &&
                            selectedFilter == "TODOS" &&
                            lastDashboardTotalQuotes > 0 ->
                            "La sesión está activa, pero el listado llegó vacío. Cierra sesión y vuelve a entrar para refrescar permisos."
                        else -> getString(R.string.no_quotes)
                    }
                    binding.txtEmpty.isVisible = items.isEmpty()
                }
            }
        })
    }

    private fun promptStatusChange(item: QuoteItem) {
        val choices = validStatuses.filter { it != "TODOS" }.toTypedArray()
        val currentIndex = choices.indexOf(item.estatus).takeIf { it >= 0 } ?: 0
        AlertDialog.Builder(this)
            .setTitle("Cambiar estatus")
            .setSingleChoiceItems(choices, currentIndex, null)
            .setPositiveButton("Guardar") { dialog, _ ->
                val selected = (dialog as AlertDialog).listView.checkedItemPosition
                if (selected >= 0) {
                    updateQuoteStatus(item, choices[selected])
                }
            }
            .setNegativeButton("Cancelar", null)
            .show()
    }

    private fun updateQuoteStatus(item: QuoteItem, nuevoStatus: String) {
        val payload = JSONObject().put("estatus", nuevoStatus)
        val request = Request.Builder()
            .url("${getBaseUrl()}/api/mobile/cotizaciones/${item.id}/estatus")
            .header("Authorization", "Bearer ${getToken()}")
            .post(payload.toString().toRequestBody(JSON))
            .build()

        binding.progressBar.isVisible = true
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showMessage("No se pudo actualizar el estatus.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    if (!response.isSuccessful) {
                        if (handleAuthError(response.code, body)) return@runOnUiThread
                        showMessage(parseError(body, "No se pudo actualizar el estatus."))
                        return@runOnUiThread
                    }
                    showMessage("Estatus actualizado a $nuevoStatus.")
                    loadAllData()
                }
            }
        })
    }

    private fun startVoiceRecognition() {
        if (getToken().isBlank()) {
            showMessage("Inicia sesión para crear cotizaciones por voz.")
            return
        }
        if (isListening) {
            stopVoiceRecognition(submitPreview = true)
            return
        }
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            requestAudioPermission.launch(Manifest.permission.RECORD_AUDIO)
            return
        }
        val recognizer = speechRecognizer
        if (recognizer == null || !SpeechRecognizer.isRecognitionAvailable(this)) {
            showMessage("El reconocimiento de voz no está disponible en este equipo.")
            return
        }
        shouldRestartListening = true
        isListening = true
        lastFinalTranscript = currentVoiceTargetText()
        binding.txtVoicePreview.text = if (activeVoiceTarget == VoiceTarget.CONDITIONS) {
            "Grabando condiciones... di 'otra condicion es que' para otro renglón. Pulsa Terminar cuando acabes."
        } else {
            "Escuchando... habla con calma. Solo se detiene cuando pulses Terminar."
        }
        setVoiceButtonsListening()
        speechHandler.removeCallbacks(restartListeningRunnable)
        recognizer.cancel()
        recognizer.startListening(buildSpeechIntent())
    }

    private fun stopVoiceRecognition(submitPreview: Boolean) {
        shouldRestartListening = false
        speechHandler.removeCallbacks(restartListeningRunnable)
        if (isListening) {
            speechRecognizer?.stopListening()
        }
        isListening = false
        setVoiceButtonIdle()
        if (submitPreview) {
            val transcript = currentVoiceTargetText()
            if (transcript.isNotBlank() && activeVoiceTarget == VoiceTarget.COMMAND) {
                requestVoiceQuotePreview(confirm = false)
            } else if (transcript.isBlank()) {
                binding.txtVoicePreview.text = "No se detectó texto en el dictado."
            }
        }
    }

    private fun setupSpeechRecognizer() {
        if (!SpeechRecognizer.isRecognitionAvailable(this)) return
        speechRecognizer = SpeechRecognizer.createSpeechRecognizer(this).apply {
            setRecognitionListener(object : RecognitionListener {
                override fun onReadyForSpeech(params: Bundle?) {
                    binding.txtVoicePreview.text = if (activeVoiceTarget == VoiceTarget.CONDITIONS) {
                        "Escuchando condiciones... toca Terminar cuando acabes."
                    } else {
                        "Escuchando... habla con calma y toca Terminar cuando acabes."
                    }
                }

                override fun onBeginningOfSpeech() {
                    binding.txtVoicePreview.text = if (activeVoiceTarget == VoiceTarget.CONDITIONS) {
                        "Grabando condiciones comerciales..."
                    } else {
                        "Grabando comando..."
                    }
                }

                override fun onRmsChanged(rmsdB: Float) = Unit

                override fun onBufferReceived(buffer: ByteArray?) = Unit

                override fun onEndOfSpeech() {
                    if (isListening && shouldRestartListening) {
                        binding.txtVoicePreview.text = "Pausa detectada... seguiré escuchando hasta que pulses Terminar."
                        scheduleRecognizerRestart(120)
                    }
                }

                override fun onError(error: Int) {
                    if (!isListening && !shouldRestartListening) return
                    if (shouldRestartListening) {
                        binding.txtVoicePreview.text = "Reanudando micrófono..."
                        scheduleRecognizerRestart(120)
                        return
                    }
                    isListening = false
                    shouldRestartListening = false
                    setVoiceButtonIdle()
                    binding.txtVoicePreview.text = "No se pudo continuar el dictado. Intenta de nuevo."
                }

                override fun onResults(results: Bundle?) {
                    val matches = results
                        ?.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION)
                        .orEmpty()
                    val transcript = matches.firstOrNull().orEmpty().trim()
                    if (transcript.isNotBlank()) {
                        lastFinalTranscript = appendTranscriptChunk(lastFinalTranscript, transcript)
                        updateVoiceTargetText(lastFinalTranscript)
                    }
                    if (shouldRestartListening && isListening) {
                        binding.txtVoicePreview.text = if (activeVoiceTarget == VoiceTarget.CONDITIONS) {
                            "Sigue grabando condiciones o toca Terminar para procesar."
                        } else {
                            "Sigue hablando o toca Terminar para procesar."
                        }
                        scheduleRecognizerRestart(120)
                    } else {
                        stopVoiceRecognition(submitPreview = lastFinalTranscript.isNotBlank())
                    }
                }

                override fun onPartialResults(partialResults: Bundle?) {
                    val partial = partialResults
                        ?.getStringArrayList(SpeechRecognizer.RESULTS_RECOGNITION)
                        ?.firstOrNull()
                        .orEmpty()
                        .trim()
                    if (partial.isNotBlank()) {
                        val merged = appendTranscriptChunk(lastFinalTranscript, partial)
                        updateVoiceTargetText(merged)
                    }
                }

                override fun onEvent(eventType: Int, params: Bundle?) = Unit
            })
        }
    }

    private fun buildSpeechIntent(): Intent {
        return Intent(RecognizerIntent.ACTION_RECOGNIZE_SPEECH).apply {
            putExtra(RecognizerIntent.EXTRA_LANGUAGE_MODEL, RecognizerIntent.LANGUAGE_MODEL_FREE_FORM)
            putExtra(RecognizerIntent.EXTRA_LANGUAGE, "es-MX")
            putExtra(RecognizerIntent.EXTRA_PREFER_OFFLINE, false)
            putExtra(RecognizerIntent.EXTRA_PARTIAL_RESULTS, true)
            putExtra(RecognizerIntent.EXTRA_MAX_RESULTS, 5)
            putExtra(RecognizerIntent.EXTRA_SPEECH_INPUT_MINIMUM_LENGTH_MILLIS, 600000)
            putExtra(RecognizerIntent.EXTRA_SPEECH_INPUT_COMPLETE_SILENCE_LENGTH_MILLIS, 60000)
            putExtra(RecognizerIntent.EXTRA_SPEECH_INPUT_POSSIBLY_COMPLETE_SILENCE_LENGTH_MILLIS, 60000)
        }
    }

    private fun setVoiceButtonIdle() {
        binding.btnVoiceDictate.text = "Dictar"
        binding.btnVoiceConditionsDictate.text = getString(R.string.voice_dictate_conditions)
    }

    private fun setVoiceButtonsListening() {
        when (activeVoiceTarget) {
            VoiceTarget.COMMAND -> {
                binding.btnVoiceDictate.text = "Terminar"
                binding.btnVoiceConditionsDictate.text = getString(R.string.voice_dictate_conditions)
            }
            VoiceTarget.CONDITIONS -> {
                binding.btnVoiceDictate.text = "Dictar"
                binding.btnVoiceConditionsDictate.text = "Terminar condiciones"
            }
        }
    }

    private fun appendTranscriptChunk(base: String, addition: String): String {
        val left = base.trim()
        val right = addition.trim()
        if (right.isBlank()) return left
        if (left.isBlank()) return right
        if (left.equals(right, ignoreCase = true)) return left
        if (right.startsWith(left, ignoreCase = true)) return right
        if (left.startsWith(right, ignoreCase = true)) return left
        if (left.endsWith(right, ignoreCase = true)) return left
        return "$left $right".replace(Regex("\\s+"), " ").trim()
    }

    private fun currentVoiceTargetText(): String {
        return when (activeVoiceTarget) {
            VoiceTarget.COMMAND -> binding.inputVoiceCommand.text?.toString()?.trim().orEmpty()
            VoiceTarget.CONDITIONS -> binding.inputVoiceConditions.text?.toString()?.trim().orEmpty()
        }
    }

    private fun updateVoiceTargetText(value: String) {
        when (activeVoiceTarget) {
            VoiceTarget.COMMAND -> {
                binding.inputVoiceCommand.setText(value)
                binding.inputVoiceCommand.setSelection(value.length)
            }
            VoiceTarget.CONDITIONS -> {
                binding.inputVoiceConditions.setText(value)
                binding.inputVoiceConditions.setSelection(value.length)
            }
        }
    }

    private fun scheduleRecognizerRestart(delayMs: Long) {
        speechHandler.removeCallbacks(restartListeningRunnable)
        speechHandler.postDelayed(restartListeningRunnable, delayMs)
    }

    private fun requestVoiceQuotePreview(confirm: Boolean) {
        val command = binding.inputVoiceCommand.text?.toString()?.trim().orEmpty()
        if (command.isBlank()) {
            showMessage("Dicta o escribe el comando de cotización.")
            return
        }

        val payload = JSONObject()
            .put("comando", command)
            .put("cliente", binding.inputVoiceClient.text?.toString()?.trim().orEmpty())
            .put("notas", binding.inputVoiceNotes.text?.toString()?.trim().orEmpty())
            .put("condiciones", binding.inputVoiceConditions.text?.toString()?.trim().orEmpty())
            .put("confirmar", confirm)

        val request = Request.Builder()
            .url("${getBaseUrl()}/api/mobile/cotizaciones/voz")
            .header("Authorization", "Bearer ${getToken()}")
            .post(payload.toString().toRequestBody(JSON))
            .build()

        binding.progressBar.isVisible = true
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showMessage(if (confirm) "No se pudo guardar la cotización." else "No se pudo interpretar el comando.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    if (!response.isSuccessful) {
                        if (handleAuthError(response.code, body)) return@runOnUiThread
                        showMessage(parseError(body, if (confirm) "No se pudo guardar la cotización." else "No se pudo interpretar el comando."))
                        return@runOnUiThread
                    }
                    val json = JSONObject(body)
                    val preview = json.getJSONObject("preview")
                    currentVoicePreview = preview
                    val clientName = preview.optString("cliente")
                    if (clientName.isNotBlank()) {
                        binding.inputVoiceClient.setText(clientName)
                        binding.inputVoiceClient.setSelection(clientName.length)
                    }
                    renderVoicePreview(preview)

                    if (confirm) {
                        val cotizacion = json.optJSONObject("cotizacion")
                        val folio = cotizacion?.optString("folio").orEmpty()
                        val pdfUrl = cotizacion?.optString("pdf_url").orEmpty()
                        showMessage(if (folio.isNotBlank()) "Cotización $folio creada." else "Cotización creada.")
                        binding.inputVoiceCommand.text?.clear()
                        binding.inputVoiceNotes.text?.clear()
                        binding.inputVoiceConditions.text?.clear()
                        currentVoicePreview = null
                        loadAllData()
                        if (pdfUrl.isNotBlank()) {
                            downloadAndOpenPdf(pdfUrl)
                        }
                    }
                }
            }
        })
    }

    private fun renderVoicePreview(preview: JSONObject) {
        val items = preview.optJSONArray("items") ?: JSONArray()
        val summary = preview.optJSONObject("resumen") ?: JSONObject()
        val headerData = preview.optJSONObject("datos_encabezado") ?: JSONObject()
        val warnings = preview.optJSONArray("warnings") ?: JSONArray()
        val warningLines = mutableListOf<String>()
        for (i in 0 until warnings.length()) {
            warningLines.add("- ${warnings.optString(i)}")
        }

        val itemLines = mutableListOf<String>()
        for (i in 0 until items.length()) {
            val item = items.optJSONObject(i) ?: continue
            val unit = item.optString("unidad").ifBlank { "En blanco" }
            val quantity = item.opt("cantidad")?.toString()?.takeIf { it.isNotBlank() } ?: "En blanco"
            val priceValue = item.opt("precio_unitario")?.toString()?.takeIf { it.isNotBlank() } ?: ""
            val subtotalValue = item.opt("subtotal")?.toString()?.takeIf { it.isNotBlank() } ?: ""
            val system = item.optString("sistema").ifBlank { "En blanco" }
            val description = item.optString("descripcion").ifBlank { "En blanco" }
            val block = buildString {
                append("${i + 1}. ")
                append(item.optString("nombre"))
                append("\n   Unidad: ")
                append(unit)
                append(" | Cantidad: ")
                append(quantity)
                append(" | PU: ")
                append(if (priceValue.isBlank()) "En blanco" else formatMoney(priceValue.toDouble()))
                append(" | Subtotal: ")
                append(if (subtotalValue.isBlank()) "En blanco" else formatMoney(subtotalValue.toDouble()))
                append("\n   Sistema: ")
                append(system)
                append("\n   Descripción: ")
                append(description)
                val area = item.optDouble("area_por_pieza")
                if (area > 0) {
                    append("\n   Area por pieza: ")
                    append(area)
                    append(" m2")
                }
                val finish = item.optString("acabado")
                if (finish.isNotBlank()) {
                    append("\n   Acabado: ")
                    append(finish)
                }
            }
            itemLines.add(block)
        }

        val text = buildString {
            append("Cliente: ")
            append(preview.optString("cliente").ifBlank { "Sin detectar" })
            append("\nResponsable: ")
            append(preview.optString("responsable"))
            append("\nEmpresa: ")
            append(headerData.optString("empresa").ifBlank { "En blanco" })
            append("\nCorreo: ")
            append(headerData.optString("correo").ifBlank { "En blanco" })
            append("\nTeléfono: ")
            append(headerData.optString("telefono").ifBlank { "En blanco" })
            append("\nResponsable de contacto: ")
            append(headerData.optString("responsable_contacto").ifBlank { "En blanco" })
            append("\nDirección: ")
            append(headerData.optString("direccion").ifBlank { "En blanco" })
            append("\nCiudad: ")
            append(headerData.optString("ciudad").ifBlank { "En blanco" })
            append("\nPartidas detectadas: ")
            append(summary.optInt("partidas", items.length()))
            append("\nSubtotal: ")
            append(formatMoney(summary.optDouble("subtotal")))
            append("\nIVA: ")
            append(formatMoney(summary.optDouble("iva")))
            append("\nTotal: ")
            append(formatMoney(summary.optDouble("total")))

            if (itemLines.isNotEmpty()) {
                append("\n\nDetalle:\n")
                append(itemLines.joinToString("\n\n"))
            }

            val notes = preview.optString("notas")
            if (notes.isNotBlank()) {
                append("\nNotas: ")
                append(notes)
            }

            val conditions = preview.optJSONArray("condiciones") ?: JSONArray()
            if (conditions.length() > 0) {
                append("\n\nCondiciones comerciales:\n")
                for (i in 0 until conditions.length()) {
                    append("- ")
                    append(conditions.optString(i))
                    if (i < conditions.length() - 1) append("\n")
                }
            }

            if (warningLines.isNotEmpty()) {
                append("\n\nAvisos:\n")
                append(warningLines.joinToString("\n"))
            }
        }
        binding.txtVoicePreview.text = text
    }

    private fun openQuotePdf(item: QuoteItem) {
        val pdfUrl = item.pdfUrl.trim()
        if (pdfUrl.isBlank()) {
            showMessage("Esta cotización no tiene PDF disponible.")
            return
        }
        downloadAndOpenPdf(pdfUrl)
    }

    private fun handlePdfIntent(intent: Intent?) {
        val pdfUrl = intent?.getStringExtra(EXTRA_OPEN_PDF_URL).orEmpty()
        if (pdfUrl.isBlank()) return
        intent?.removeExtra(EXTRA_OPEN_PDF_URL)
        if (getToken().isBlank()) {
            showMessage("Inicia sesión para abrir el PDF.")
            return
        }
        downloadAndOpenPdf(pdfUrl)
    }

    private fun handleFollowupIntent(intent: Intent?) {
        val cotizacionId = intent?.getStringExtra(EXTRA_OPEN_FOLLOWUP_COTIZACION_ID).orEmpty()
        val seguimientoId = intent?.getStringExtra(EXTRA_OPEN_FOLLOWUP_ID).orEmpty()
        if (cotizacionId.isBlank() || seguimientoId.isBlank()) return
        if (getToken().isBlank()) {
            showMessage("Inicia sesión para abrir el seguimiento.")
            return
        }
        intent?.removeExtra(EXTRA_OPEN_FOLLOWUP_COTIZACION_ID)
        intent?.removeExtra(EXTRA_OPEN_FOLLOWUP_ID)
        openFollowupDetail(cotizacionId, seguimientoId)
    }

    private fun openFollowupDetail(cotizacionId: String, seguimientoId: String) {
        binding.progressBar.isVisible = true
        val request = Request.Builder()
            .url("${getBaseUrl()}/api/mobile/cotizaciones/$cotizacionId/seguimiento/$seguimientoId")
            .header("Authorization", "Bearer ${getToken()}")
            .get()
            .build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showMessage("No se pudo abrir el seguimiento.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    if (!response.isSuccessful) {
                        if (handleAuthError(response.code, body)) return@runOnUiThread
                        showMessage(parseError(body, "No se pudo abrir el seguimiento."))
                        return@runOnUiThread
                    }
                    val json = JSONObject(body)
                    val cotizacion = json.getJSONObject("cotizacion")
                    val seguimiento = json.getJSONObject("seguimiento")
                    val fecha = formatDate(seguimiento.optString("fecha"))
                    val actualizado = formatDate(seguimiento.optString("actualizado_en"))
                    val mensaje = buildString {
                        append("Folio: ")
                        append(cotizacion.optString("folio"))
                        append("\nCliente: ")
                        append(cotizacion.optString("cliente"))
                        append("\nEstatus: ")
                        append(cotizacion.optString("estatus"))
                        append("\nResponsable: ")
                        append(cotizacion.optString("responsable"))
                        append("\nAutor: ")
                        append(seguimiento.optString("autor"))
                        append("\nFecha: ")
                        append(fecha)
                        if (actualizado.isNotBlank() && actualizado != fecha) {
                            append("\nEditado: ")
                            append(actualizado)
                        }
                        append("\n\n")
                        append(seguimiento.optString("comentario"))
                    }
                    AlertDialog.Builder(this@MainActivity)
                        .setTitle("Seguimiento")
                        .setMessage(mensaje)
                        .setPositiveButton("Cerrar", null)
                        .show()
                }
            }
        })
    }

    private fun downloadAndOpenPdf(pdfUrl: String) {
        val token = getToken()
        if (token.isBlank()) {
            showMessage("Inicia sesión para abrir el PDF.")
            return
        }
        binding.progressBar.isVisible = true
        val request = Request.Builder()
            .url(pdfUrl)
            .header("Authorization", "Bearer $token")
            .get()
            .build()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                binding.progressBar.isVisible = false
                showBlockingMessage("No se pudo descargar el PDF.\n${e.message ?: "Error de red."}")
            }

            override fun onResponse(call: Call, response: Response) {
                val statusCode = response.code
                val responseBody = response.body
                val contentType = responseBody?.contentType()?.toString().orEmpty()
                val bytes = responseBody?.bytes()
                if (!response.isSuccessful || bytes == null || bytes.isEmpty()) {
                    runOnUiThread {
                        binding.progressBar.isVisible = false
                        val preview = bytes?.toString(Charsets.UTF_8)?.take(120)?.replace('\n', ' ')?.trim().orEmpty()
                        val detail = if (preview.isNotBlank()) " $preview" else ""
                        showBlockingMessage("No se pudo descargar el PDF. HTTP $statusCode.$detail")
                    }
                    response.close()
                    return
                }
                if (!contentType.contains("pdf", ignoreCase = true) && !bytes.take(4).toByteArray().contentEquals(byteArrayOf(0x25, 0x50, 0x44, 0x46))) {
                    runOnUiThread {
                        binding.progressBar.isVisible = false
                        val preview = bytes.toString(Charsets.UTF_8).take(120).replace('\n', ' ').trim()
                        showBlockingMessage("La respuesta no fue PDF. HTTP $statusCode. $preview")
                    }
                    response.close()
                    return
                }
                val file = File(cacheDir, "cotizacion_${System.currentTimeMillis()}.pdf")
                file.writeBytes(bytes)
                response.close()
                runOnUiThread {
                    binding.progressBar.isVisible = false
                    openLocalPdf(file)
                }
            }
        })
    }

    private fun openLocalPdf(file: File) {
        try {
            val uri = FileProvider.getUriForFile(this, "${packageName}.fileprovider", file)
            val intent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri, "application/pdf")
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            }
            startActivity(intent)
        } catch (_: Exception) {
            showBlockingMessage("No se pudo abrir el PDF descargado.")
        }
    }

    private fun parseError(body: String, fallback: String): String {
        return try {
            JSONObject(body).optString("error", fallback)
        } catch (_: Exception) {
            fallback
        }
    }

    private fun showMessage(message: String) {
        Snackbar.make(binding.root, message, Snackbar.LENGTH_LONG).show()
    }

    private fun showBlockingMessage(message: String) {
        AlertDialog.Builder(this)
            .setTitle("PDF")
            .setMessage(message)
            .setPositiveButton("OK", null)
            .show()
    }

    private fun ensureNotificationPermission() {
        if (android.os.Build.VERSION.SDK_INT < android.os.Build.VERSION_CODES.TIRAMISU) return
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED) {
            registerPushTokenIfPossible()
            return
        }
        requestNotificationPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
    }

    private fun registerPushTokenIfPossible() {
        val baseUrl = getBaseUrl()
        val token = getToken()
        if (baseUrl.isBlank() || token.isBlank()) return
        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.TIRAMISU &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            return
        }
        PushRegistration.registerCurrentToken(this, client, baseUrl, token)
    }

    private fun formatMoney(value: Double): String {
        return NumberFormat.getCurrencyInstance(Locale("es", "MX")).format(value)
    }

    private fun formatDate(raw: String): String {
        return try {
            val normalized = raw.replace("Z", "+00:00")
            val date = java.time.OffsetDateTime.parse(normalized).toInstant()
            val sdf = SimpleDateFormat("dd/MM/yyyy HH:mm", Locale("es", "MX"))
            sdf.timeZone = TimeZone.getTimeZone("America/Mexico_City")
            sdf.format(Date.from(date))
        } catch (_: Exception) {
            raw
        }
    }

    companion object {
        private const val PREFS_NAME = "registro_obras_prefs"
        private const val KEY_BASE_URL = "base_url"
        private const val KEY_TOKEN = "token"
        private const val KEY_USER_NAME = "user_name"
        private const val KEY_USER_ROLE = "user_role"
        private const val DEFAULT_BASE_URL = "https://sistema-poliutech.onrender.com"
        const val EXTRA_OPEN_PDF_URL = "open_pdf_url"
        const val EXTRA_OPEN_FOLLOWUP_COTIZACION_ID = "open_followup_cotizacion_id"
        const val EXTRA_OPEN_FOLLOWUP_ID = "open_followup_id"
        private val JSON = "application/json; charset=utf-8".toMediaType()
    }
}
