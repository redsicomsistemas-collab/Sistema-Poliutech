package com.poliutech.marstatuscotizacion

data class RegistroObraItem(
    val id: Int,
    val numero: String,
    val obra: String,
    val ubicacion: String,
    val encargado: String,
    val puesto: String,
    val telefono: String,
    val correo: String,
    val responsable: String,
    var selected: Boolean = false,
)
