package com.poliutech.marstatuscotizacion

import android.content.Context
import android.os.Build
import android.util.Log
import com.google.firebase.FirebaseApp
import com.google.firebase.messaging.FirebaseMessaging
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject

object PushRegistration {

    private const val TAG = "PushRegistration"
    private val jsonType = "application/json; charset=utf-8".toMediaType()

    fun registerCurrentToken(context: Context, client: OkHttpClient, baseUrl: String, authToken: String) {
        if (baseUrl.isBlank() || authToken.isBlank()) return
        if (FirebaseApp.initializeApp(context) == null) {
            Log.w(TAG, "Firebase no esta configurado todavia en la app.")
            return
        }

        FirebaseMessaging.getInstance().token
            .addOnSuccessListener { token ->
                if (token.isNullOrBlank()) return@addOnSuccessListener
                val payload = JSONObject()
                    .put("token", token)
                    .put("platform", "android")
                    .put("device_name", "${Build.MANUFACTURER} ${Build.MODEL}".trim())
                    .put("app_version", appVersion(context))
                val request = Request.Builder()
                    .url("${baseUrl.trimEnd('/')}/api/mobile/push-token")
                    .header("Authorization", "Bearer $authToken")
                    .post(payload.toString().toRequestBody(jsonType))
                    .build()
                client.newCall(request).enqueue(SimpleCallback(
                    onFailure = { error -> Log.w(TAG, "No se pudo registrar token push: ${error.message}") },
                    onSuccess = { _, _ -> Log.d(TAG, "Token push registrado.") }
                ))
            }
            .addOnFailureListener { error ->
                Log.w(TAG, "No se pudo obtener token FCM", error)
            }
    }

    private fun appVersion(context: Context): String {
        return try {
            val pkgInfo = context.packageManager.getPackageInfo(context.packageName, 0)
            pkgInfo.versionName ?: "1.0"
        } catch (_: Exception) {
            "1.0"
        }
    }
}
