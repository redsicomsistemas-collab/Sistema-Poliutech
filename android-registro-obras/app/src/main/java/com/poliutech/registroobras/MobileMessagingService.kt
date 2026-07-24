package com.poliutech.marstatuscotizacion

import android.Manifest
import android.app.PendingIntent
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage

class MobileMessagingService : FirebaseMessagingService() {

    override fun onNewToken(token: String) {
        super.onNewToken(token)
        val prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE)
        val baseUrl = prefs.getString(KEY_BASE_URL, "").orEmpty()
        val authToken = prefs.getString(KEY_TOKEN, "").orEmpty()
        PushRegistration.registerCurrentToken(this, okhttp3.OkHttpClient(), baseUrl, authToken)
    }

    override fun onMessageReceived(message: RemoteMessage) {
        super.onMessageReceived(message)
        ensureChannel()
        val title = message.data["title"] ?: message.notification?.title ?: "Pendiente por revisar"
        val body = message.data["body"] ?: message.notification?.body ?: "Tienes un pendiente nuevo."
        val pdfUrl = message.data["pdf_url"].orEmpty()
        val targetUrl = message.data["url"].orEmpty()
        val approveUrl = message.data["approve_url"].orEmpty()
        val rejectUrl = message.data["reject_url"].orEmpty()
        val cotizacionId = message.data["cotizacion_id"].orEmpty()
        val seguimientoId = message.data["seguimiento_id"].orEmpty()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            return
        }
        val pendingIntent = if (cotizacionId.isNotBlank() && seguimientoId.isNotBlank()) {
            PendingIntent.getActivity(
                this,
                ("$cotizacionId-$seguimientoId").hashCode(),
                Intent(this, MainActivity::class.java).apply {
                    putExtra(MainActivity.EXTRA_OPEN_FOLLOWUP_COTIZACION_ID, cotizacionId)
                    putExtra(MainActivity.EXTRA_OPEN_FOLLOWUP_ID, seguimientoId)
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP)
                },
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )
        } else if (targetUrl.isNotBlank()) {
            browserPendingIntent(targetUrl, "open-url-${targetUrl.hashCode()}")
        } else if (pdfUrl.isNotBlank()) {
            PendingIntent.getActivity(
                this,
                pdfUrl.hashCode(),
                Intent(this, PdfOpenActivity::class.java).apply {
                    putExtra(PdfOpenActivity.EXTRA_PDF_URL, pdfUrl)
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP)
                },
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )
        } else {
            null
        }
        val builder = NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.logo_app)
            .setContentTitle(title)
            .setContentText(body)
            .setStyle(NotificationCompat.BigTextStyle().bigText(body))
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)

        if (approveUrl.isNotBlank()) {
            builder.addAction(
                R.drawable.logo_app,
                "Aprobar",
                browserPendingIntent(approveUrl, "approve-$cotizacionId")
            )
        }
        if (rejectUrl.isNotBlank()) {
            builder.addAction(
                R.drawable.logo_app,
                "Rechazar",
                browserPendingIntent(rejectUrl, "reject-$cotizacionId")
            )
        }

        val notification = builder.build()
        NotificationManagerCompat.from(this).notify((System.currentTimeMillis() % Int.MAX_VALUE).toInt(), notification)
    }

    private fun browserPendingIntent(url: String, requestKey: String): PendingIntent {
        return PendingIntent.getActivity(
            this,
            requestKey.hashCode(),
            Intent(Intent.ACTION_VIEW, Uri.parse(url)).apply {
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP)
            },
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
    }

    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        val channel = NotificationChannel(
            CHANNEL_ID,
            "Pendientes",
            NotificationManager.IMPORTANCE_HIGH
        ).apply {
            description = "Alertas de pendientes y cotizaciones."
        }
        manager.createNotificationChannel(channel)
    }

    companion object {
        private const val CHANNEL_ID = "pending_quotes"
        private const val PREFS_NAME = "registro_obras_prefs"
        private const val KEY_BASE_URL = "base_url"
        private const val KEY_TOKEN = "token"
    }
}
