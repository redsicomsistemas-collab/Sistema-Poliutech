def _num(value, default=0.0):
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def round4(value):
    return round(_num(value), 4)


def _detail_quantity(apu, detalle):
    base_qty = _num(detalle.cantidad)
    factor = _num(getattr(detalle, "factor", 1.0), 1.0) or 1.0
    desperdicio = _num(getattr(detalle, "desperdicio_pct", 0.0)) / 100.0
    cantidad = base_qty * factor * (1.0 + desperdicio)

    if detalle.tipo_insumo in {"mano_obra", "maquinaria"}:
        rendimiento = _num(getattr(detalle, "rendimiento", 0.0))
        cuadrilla = _num(getattr(detalle, "cuadrilla", 1.0), 1.0) or 1.0
        jornada_horas = _num(getattr(apu, "jornada_horas", 8.0), 8.0) or 8.0
        if rendimiento > 0:
            cantidad = ((base_qty * cuadrilla) / rendimiento) * jornada_horas * factor * (1.0 + desperdicio)

    return round4(cantidad)


def recalcular_apu(apu):
    costo_materiales = 0.0
    costo_mano_obra = 0.0
    costo_maquinaria = 0.0

    for detalle in apu.detalles:
        detalle.cantidad_calculada = _detail_quantity(apu, detalle)
        detalle.factor_total = round4(
            (_num(getattr(detalle, "factor", 1.0), 1.0) or 1.0)
            * (1.0 + _num(getattr(detalle, "desperdicio_pct", 0.0)) / 100.0)
        )
        detalle.subtotal = round4(detalle.cantidad_calculada * _num(detalle.precio_unitario))

        if detalle.tipo_insumo == "material":
            costo_materiales += detalle.subtotal
        elif detalle.tipo_insumo == "mano_obra":
            costo_mano_obra += detalle.subtotal
        elif detalle.tipo_insumo == "maquinaria":
            costo_maquinaria += detalle.subtotal

    herramienta_pct = _num(getattr(apu, "herramienta_menor_pct", 0.0))
    costo_herramienta = costo_mano_obra * (herramienta_pct / 100.0)

    apu.costo_materiales = round4(costo_materiales)
    apu.costo_mano_obra = round4(costo_mano_obra)
    apu.costo_maquinaria = round4(costo_maquinaria)
    apu.costo_herramienta = round4(costo_herramienta)

    costo_directo = costo_materiales + costo_mano_obra + costo_maquinaria + costo_herramienta
    apu.costo_directo = round4(costo_directo)

    indirecto_pct = _num(getattr(apu, "indirecto_pct", 0.0))
    indirecto_monto = costo_directo * (indirecto_pct / 100.0)
    base_fin = costo_directo + indirecto_monto

    financiamiento_pct = _num(getattr(apu, "financiamiento_pct", 0.0))
    financiamiento_monto = base_fin * (financiamiento_pct / 100.0)
    base_utilidad = base_fin + financiamiento_monto

    utilidad_pct = _num(getattr(apu, "utilidad_pct", 0.0))
    utilidad_monto = base_utilidad * (utilidad_pct / 100.0)
    base_cargos = base_utilidad + utilidad_monto

    cargos_pct = _num(getattr(apu, "cargos_adicionales_pct", 0.0))
    cargos_monto = base_cargos * (cargos_pct / 100.0)

    apu.indirecto_monto = round4(indirecto_monto)
    apu.financiamiento_monto = round4(financiamiento_monto)
    apu.utilidad_monto = round4(utilidad_monto)
    apu.cargos_adicionales_monto = round4(cargos_monto)
    apu.precio_unitario = round4(base_cargos + cargos_monto)

    cantidad_objetivo = _num(getattr(apu, "cantidad_objetivo", 1.0), 1.0) or 1.0
    apu.importe_partida = round4(apu.precio_unitario * cantidad_objetivo)

    rendimiento_base = _num(getattr(apu, "rendimiento_base", 0.0))
    apu.jornadas_estimadas = round4(cantidad_objetivo / rendimiento_base) if rendimiento_base > 0 else 0.0
    apu.factor_sobrecosto = round4((apu.precio_unitario / costo_directo)) if costo_directo else 0.0

    for detalle in apu.detalles:
        detalle.participacion_directo = round4((detalle.subtotal / costo_directo) * 100.0) if costo_directo else 0.0

    return apu
