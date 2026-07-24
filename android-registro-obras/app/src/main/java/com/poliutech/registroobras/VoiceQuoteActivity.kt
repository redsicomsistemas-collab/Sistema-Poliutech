package com.poliutech.marstatuscotizacion

import android.Manifest
import android.animation.ObjectAnimator
import android.animation.PropertyValuesHolder
import android.animation.ValueAnimator
import android.content.Intent
import android.content.pm.PackageManager
import android.media.MediaRecorder
import android.os.Build
import android.os.Bundle
import android.view.WindowManager
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.core.view.isVisible
import com.google.android.material.snackbar.Snackbar
import com.poliutech.marstatuscotizacion.databinding.ActivityVoiceQuoteBinding
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.text.NumberFormat
import java.util.Locale

class VoiceQuoteActivity : AppCompatActivity() {
    private enum class VoiceTarget {
        COMMAND,
        CONDITIONS,
    }

    private lateinit var binding: ActivityVoiceQuoteBinding
    private lateinit var prefs: android.content.SharedPreferences
    private val client = OkHttpClient()
    private var mediaRecorder: MediaRecorder? = null
    private var activeAudioFile: File? = null
    private var isListening = false
    private var activeVoiceTarget: VoiceTarget = VoiceTarget.COMMAND
    private var currentVoicePreview: JSONObject? = null
    private var commandTranscriptBuffer: String = ""
    private var conditionsTranscriptBuffer: String = ""
    private var recordingAnimator: ObjectAnimator? = null
    private var busyAnimator: ObjectAnimator? = null
    private val requestAudioPermission = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (granted) {
            startVoiceRecognition()
        } else {
            showMessage("Debes permitir el micrófono para dictar.")
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityVoiceQuoteBinding.inflate(layoutInflater)
        setContentView(binding.root)
        setSupportActionBar(binding.toolbar)
        supportActionBar?.setDisplayHomeAsUpEnabled(true)
        supportActionBar?.setDisplayShowHomeEnabled(true)

        prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE)

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
        binding.txtVoicePreview.text = getString(R.string.voice_preview_empty)
    }

    override fun onOptionsItemSelected(item: android.view.MenuItem): Boolean {
        return when (item.itemId) {
            android.R.id.home -> {
                finish()
                true
            }
            else -> super.onOptionsItemSelected(item)
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        stopRecorderQuietly()
        stopRecordingAnimation()
        stopBusyState()
    }

    private fun getBaseUrl(): String = prefs.getString(KEY_BASE_URL, "")?.trim()?.trimEnd('/').orEmpty()
    private fun getToken(): String = prefs.getString(KEY_TOKEN, "")?.trim().orEmpty()

    private fun clearVoiceInputs() {
        stopVoiceRecognition(submitPreview = false)
        currentVoicePreview = null
        commandTranscriptBuffer = ""
        conditionsTranscriptBuffer = ""
        activeAudioFile?.delete()
        activeAudioFile = null
        binding.inputVoiceCommand.text?.clear()
        binding.inputVoiceClient.text?.clear()
        binding.inputVoiceNotes.text?.clear()
        binding.inputVoiceConditions.text?.clear()
        binding.txtVoicePreview.text = getString(R.string.voice_preview_empty)
        stopBusyState()
        hideVoiceStatus()
        showMessage("Se limpió la captura anterior.")
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
        startAudioRecording()
    }

    private fun stopVoiceRecognition(submitPreview: Boolean) {
        val recordedFile = activeAudioFile
        stopRecorderQuietly()
        window.clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        isListening = false
        setVoiceButtonIdle()
        stopRecordingAnimation()
        if (!submitPreview) {
            hideVoiceStatus()
            return
        }
        if (recordedFile == null || !recordedFile.exists() || recordedFile.length() <= 0L) {
            binding.txtVoicePreview.text = "No se capturó audio. Intenta de nuevo."
            hideVoiceStatus()
            return
        }
        uploadRecordedAudio(recordedFile, activeVoiceTarget)
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

    private fun startAudioRecording() {
        try {
            stopRecorderQuietly()
            activeAudioFile = File.createTempFile(
                if (activeVoiceTarget == VoiceTarget.CONDITIONS) "voice_conditions_" else "voice_command_",
                ".m4a",
                cacheDir
            )
            val recorder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                MediaRecorder(this)
            } else {
                @Suppress("DEPRECATION")
                MediaRecorder()
            }
            recorder.apply {
                setAudioSource(MediaRecorder.AudioSource.MIC)
                setOutputFormat(MediaRecorder.OutputFormat.MPEG_4)
                setAudioEncoder(MediaRecorder.AudioEncoder.AAC)
                setAudioEncodingBitRate(128000)
                setAudioSamplingRate(44100)
                setOutputFile(activeAudioFile!!.absolutePath)
                prepare()
                start()
            }
            mediaRecorder = recorder
            window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
            isListening = true
            setVoiceButtonsListening()
            startRecordingAnimation()
            showRecordingStatus(
                if (activeVoiceTarget == VoiceTarget.CONDITIONS) {
                    "Grabando condiciones..."
                } else {
                    "Grabando audio..."
                }
            )
            binding.txtVoicePreview.text = if (activeVoiceTarget == VoiceTarget.CONDITIONS) {
                "Grabando condiciones en audio... pulsa Terminar condiciones cuando acabes."
            } else {
                "Grabando audio real... pulsa Terminar cuando acabes."
            }
        } catch (_: Exception) {
            stopRecorderQuietly()
            window.clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
            isListening = false
            setVoiceButtonIdle()
            hideVoiceStatus()
            showMessage("No se pudo iniciar la grabación de audio.")
        }
    }

    private fun stopRecorderQuietly() {
        try {
            mediaRecorder?.stop()
        } catch (_: Exception) {
        }
        try {
            mediaRecorder?.reset()
        } catch (_: Exception) {
        }
        try {
            mediaRecorder?.release()
        } catch (_: Exception) {
        }
        mediaRecorder = null
    }

    private fun uploadRecordedAudio(file: File, target: VoiceTarget) {
        if (getToken().isBlank()) {
            showMessage("Inicia sesión para transcribir audio.")
            return
        }
        startBusyState("Transcribiendo audio en servidor...")
        val multipartBody = MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart("target", if (target == VoiceTarget.CONDITIONS) "condiciones" else "comando")
            .addFormDataPart(
                "audio",
                file.name,
                file.asRequestBody("audio/mp4".toMediaTypeOrNull())
            )
            .build()
        val request = Request.Builder()
            .url("${getBaseUrl()}/api/mobile/cotizaciones/voz/transcribir")
            .header("Authorization", "Bearer ${getToken()}")
            .post(multipartBody)
            .build()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                stopBusyState()
                binding.txtVoicePreview.text = "No se pudo transcribir el audio."
                showMessage("No se pudo subir el audio para transcribir.")
            }

            override fun onResponse(call: Call, response: Response) {
                val body = response.body?.string().orEmpty()
                runOnUiThread {
                    stopBusyState()
                    if (!response.isSuccessful) {
                        binding.txtVoicePreview.text = parseError(body, "No se pudo transcribir el audio.")
                        showMessage(parseError(body, "No se pudo transcribir el audio."))
                        return@runOnUiThread
                    }
                    val json = JSONObject(body)
                    val transcript = json.optString("transcript").trim()
                    if (transcript.isBlank()) {
                        binding.txtVoicePreview.text = "La transcripción llegó vacía."
                        hideVoiceStatus()
                        return@runOnUiThread
                    }
                    val merged = appendTranscriptChunk(currentVoiceBuffer(), transcript)
                    updateVoiceTargetText(merged)
                    binding.txtVoicePreview.text = if (target == VoiceTarget.CONDITIONS) {
                        "Condiciones transcritas. Puedes grabar otra tanda o guardar."
                    } else {
                        "Audio transcrito. Puedes revisar, previsualizar o seguir grabando."
                    }
                    if (target == VoiceTarget.COMMAND) {
                        requestVoiceQuotePreview(confirm = false)
                    }
                }
            }
        })
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
        val buffered = currentVoiceBuffer()
        if (buffered.isNotBlank()) return buffered
        return when (activeVoiceTarget) {
            VoiceTarget.COMMAND -> binding.inputVoiceCommand.text?.toString()?.trim().orEmpty()
            VoiceTarget.CONDITIONS -> binding.inputVoiceConditions.text?.toString()?.trim().orEmpty()
        }
    }

    private fun startRecordingAnimation() {
        stopRecordingAnimation()
        recordingAnimator = ObjectAnimator.ofPropertyValuesHolder(
            binding.voiceRecordingDot,
            PropertyValuesHolder.ofFloat("scaleX", 1f, 1.05f),
            PropertyValuesHolder.ofFloat("scaleY", 1f, 1.05f),
            PropertyValuesHolder.ofFloat("alpha", 1f, 0.72f)
        ).apply {
            duration = 650L
            repeatCount = ValueAnimator.INFINITE
            repeatMode = ValueAnimator.REVERSE
            start()
        }
    }

    private fun stopRecordingAnimation() {
        recordingAnimator?.cancel()
        recordingAnimator = null
        binding.voiceRecordingDot.alpha = 1f
        binding.voiceRecordingDot.scaleX = 1f
        binding.voiceRecordingDot.scaleY = 1f
    }

    private fun startBusyState(message: String) {
        binding.progressBar.isIndeterminate = true
        binding.progressBar.isVisible = true
        binding.voiceStatusContainer.isVisible = true
        binding.voiceBusySpinner.isVisible = true
        binding.voiceRecordingDot.isVisible = false
        binding.voiceStatusText.text = message
        binding.txtVoicePreview.text = message
        busyAnimator?.cancel()
        busyAnimator = ObjectAnimator.ofFloat(binding.voiceBusySpinner, "alpha", 0.35f, 1f).apply {
            duration = 550L
            repeatCount = ValueAnimator.INFINITE
            repeatMode = ValueAnimator.REVERSE
            start()
        }
    }

    private fun stopBusyState() {
        busyAnimator?.cancel()
        busyAnimator = null
        binding.progressBar.alpha = 1f
        binding.progressBar.isVisible = false
        binding.voiceBusySpinner.alpha = 1f
        binding.voiceBusySpinner.isVisible = false
        if (!isListening) {
            hideVoiceStatus()
        }
    }

    private fun showRecordingStatus(message: String) {
        binding.voiceStatusContainer.isVisible = true
        binding.voiceRecordingDot.isVisible = true
        binding.voiceBusySpinner.isVisible = false
        binding.voiceStatusText.text = message
    }

    private fun hideVoiceStatus() {
        binding.voiceStatusContainer.isVisible = false
        binding.voiceBusySpinner.isVisible = false
        binding.voiceRecordingDot.isVisible = true
    }

    private fun currentVoiceBuffer(): String {
        return when (activeVoiceTarget) {
            VoiceTarget.COMMAND -> commandTranscriptBuffer.trim()
            VoiceTarget.CONDITIONS -> conditionsTranscriptBuffer.trim()
        }
    }

    private fun updateVoiceTargetText(value: String) {
        when (activeVoiceTarget) {
            VoiceTarget.COMMAND -> commandTranscriptBuffer = value
            VoiceTarget.CONDITIONS -> conditionsTranscriptBuffer = value
        }
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
                        setResult(RESULT_OK)
                        binding.inputVoiceCommand.text?.clear()
                        binding.inputVoiceNotes.text?.clear()
                        binding.inputVoiceConditions.text?.clear()
                        currentVoicePreview = null
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
            itemLines.add(
                buildString {
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
                }
            )
        }
        binding.txtVoicePreview.text = buildString {
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
                append("\n\nPartidas:\n")
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
    }

    private fun downloadAndOpenPdf(pdfUrl: String) {
        val token = getToken()
        if (token.isBlank()) {
            showMessage("Inicia sesión para abrir el PDF.")
            return
        }
        binding.progressBar.isVisible = true
        startBusyState("Descargando PDF...")
        val request = Request.Builder()
            .url(pdfUrl)
            .header("Authorization", "Bearer $token")
            .get()
            .build()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = runOnUiThread {
                stopBusyState()
                showBlockingMessage("No se pudo descargar el PDF.\n${e.message ?: "Error de red."}")
            }

            override fun onResponse(call: Call, response: Response) {
                val statusCode = response.code
                val responseBody = response.body
                val contentType = responseBody?.contentType()?.toString().orEmpty()
                val bytes = responseBody?.bytes()
                if (!response.isSuccessful || bytes == null || bytes.isEmpty()) {
                    runOnUiThread {
                        stopBusyState()
                        val preview = bytes?.toString(Charsets.UTF_8)?.take(120)?.replace('\n', ' ')?.trim().orEmpty()
                        val detail = if (preview.isNotBlank()) " $preview" else ""
                        showBlockingMessage("No se pudo descargar el PDF. HTTP $statusCode.$detail")
                    }
                    response.close()
                    return
                }
                if (!contentType.contains("pdf", ignoreCase = true) &&
                    !bytes.take(4).toByteArray().contentEquals(byteArrayOf(0x25, 0x50, 0x44, 0x46))
                ) {
                    runOnUiThread {
                        stopBusyState()
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
                    stopBusyState()
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

    private fun formatMoney(value: Double): String {
        return NumberFormat.getCurrencyInstance(Locale("es", "MX")).format(value)
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

    companion object {
        private const val PREFS_NAME = "registro_obras_prefs"
        private const val KEY_BASE_URL = "base_url"
        private const val KEY_TOKEN = "token"
        private val JSON = "application/json; charset=utf-8".toMediaType()
    }
}
