package com.poliutech.marstatuscotizacion

data class QuoteItem(
    val id: Int,
    val folio: String,
    val cliente: String,
    val fecha: String,
    val estatus: String,
    val total: String,
    val responsable: String,
    val pdfUrl: String,
)
