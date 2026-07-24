package com.poliutech.marstatuscotizacion

import okhttp3.Call
import okhttp3.Callback
import okhttp3.Response
import java.io.IOException

class SimpleCallback(
    private val onFailure: (IOException) -> Unit,
    private val onSuccess: (Call, Response) -> Unit,
) : Callback {
    override fun onFailure(call: Call, e: IOException) = onFailure(e)

    override fun onResponse(call: Call, response: Response) {
        onSuccess(call, response)
        response.close()
    }
}
