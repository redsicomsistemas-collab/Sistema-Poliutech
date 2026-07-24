package com.poliutech.marstatuscotizacion

import android.app.AlertDialog
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.FileProvider
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.io.File
import java.io.IOException

class PdfOpenActivity : AppCompatActivity() {

    private val client = OkHttpClient()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE)
        val token = prefs.getString(KEY_TOKEN, "")?.trim().orEmpty()
        val pdfUrl = intent.getStringExtra(EXTRA_PDF_URL).orEmpty()

        if (token.isBlank()) {
            showAndFinish("Inicia sesión para abrir el PDF.")
            return
        }
        if (pdfUrl.isBlank()) {
            showAndFinish("No llegó la URL del PDF.")
            return
        }

        val request = Request.Builder()
            .url(pdfUrl)
            .header("Authorization", "Bearer $token")
            .get()
            .build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                runOnUiThread { showAndFinish("No se pudo descargar el PDF.\n${e.message ?: "Error de red."}") }
            }

            override fun onResponse(call: Call, response: Response) {
                val statusCode = response.code
                val contentType = response.body?.contentType()?.toString().orEmpty()
                val bytes = response.body?.bytes()
                response.close()

                if (!response.isSuccessful || bytes == null || bytes.isEmpty()) {
                    val preview = bytes?.toString(Charsets.UTF_8)?.take(120)?.replace('\n', ' ')?.trim().orEmpty()
                    runOnUiThread { showAndFinish("No se pudo descargar el PDF. HTTP $statusCode. $preview") }
                    return
                }

                if (!contentType.contains("pdf", ignoreCase = true) &&
                    !bytes.take(4).toByteArray().contentEquals(byteArrayOf(0x25, 0x50, 0x44, 0x46))
                ) {
                    val preview = bytes.toString(Charsets.UTF_8).take(120).replace('\n', ' ').trim()
                    runOnUiThread { showAndFinish("La respuesta no fue PDF. HTTP $statusCode. $preview") }
                    return
                }

                val file = File(cacheDir, "cotizacion_${System.currentTimeMillis()}.pdf")
                file.writeBytes(bytes)
                runOnUiThread { openLocalPdf(file) }
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
            finish()
        } catch (_: Exception) {
            showAndFinish("No se pudo abrir el PDF descargado.")
        }
    }

    private fun showAndFinish(message: String) {
        AlertDialog.Builder(this)
            .setTitle("PDF")
            .setMessage(message)
            .setPositiveButton("OK") { _, _ -> finish() }
            .setOnCancelListener { finish() }
            .show()
    }

    companion object {
        private const val PREFS_NAME = "registro_obras_prefs"
        private const val KEY_TOKEN = "token"
        const val EXTRA_PDF_URL = "extra_pdf_url"
    }
}
