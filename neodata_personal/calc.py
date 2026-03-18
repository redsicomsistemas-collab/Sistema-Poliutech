from datetime import datetime, timedelta
import math

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
        if getattr(detalle, "tipo_insumo", "") == "auxiliar" and getattr(detalle, "auxiliar", None):
            detalle.descripcion = detalle.auxiliar.concepto
            detalle.codigo = getattr(detalle.auxiliar, "clave", None)
            detalle.categoria = getattr(detalle.auxiliar, "categoria", None)
            detalle.unidad = getattr(detalle.auxiliar, "unidad", detalle.unidad)
            detalle.precio_unitario = _num(getattr(detalle.auxiliar, "precio_unitario", 0.0))

        if detalle.tipo_insumo == "material" and _num(getattr(detalle, "cantidad_presentacion", 0.0)) > 0:
            piezas = max(
                _num(getattr(detalle, "compra_minima", 0.0), 0.0),
                math.ceil(detalle.cantidad_calculada / _num(getattr(detalle, "cantidad_presentacion", 1.0), 1.0)),
            )
            detalle.piezas_compra = piezas
            detalle.cantidad_comprada = round4(piezas * _num(getattr(detalle, "cantidad_presentacion", 0.0)))
            if _num(getattr(detalle, "precio_presentacion", 0.0)) > 0:
                detalle.subtotal = round4(piezas * _num(getattr(detalle, "precio_presentacion", 0.0)))
            else:
                detalle.subtotal = round4(detalle.cantidad_calculada * _num(detalle.precio_unitario))
        else:
            detalle.piezas_compra = 0
            detalle.cantidad_comprada = detalle.cantidad_calculada
            detalle.subtotal = round4(detalle.cantidad_calculada * _num(detalle.precio_unitario))

        if detalle.tipo_insumo == "material":
            costo_materiales += detalle.subtotal
        elif detalle.tipo_insumo == "mano_obra":
            costo_mano_obra += detalle.subtotal
        elif detalle.tipo_insumo == "maquinaria":
            costo_maquinaria += detalle.subtotal
        elif detalle.tipo_insumo == "auxiliar":
            costo_materiales += detalle.subtotal

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


def recalcular_partida_obra(partida):
    apu = getattr(partida, "apu", None)
    if apu:
        recalcular_apu(apu)
        partida.clave = partida.clave or getattr(apu, "clave", None)
        partida.concepto = partida.concepto or getattr(apu, "concepto", None)
        partida.unidad = partida.unidad or getattr(apu, "unidad", "m2")
        partida.capitulo = partida.capitulo or getattr(apu, "capitulo", None) or getattr(apu, "categoria", None)
        partida.subcapitulo = partida.subcapitulo or getattr(apu, "subcapitulo", None)
        partida.rendimiento = _num(partida.rendimiento or getattr(apu, "rendimiento_base", 0.0))
        partida.precio_unitario = round4(getattr(apu, "precio_unitario", 0.0))
        partida.importe_directo = round4(_num(partida.cantidad) * _num(getattr(apu, "costo_directo", 0.0)))
        partida.importe_venta = round4(_num(partida.cantidad) * _num(partida.precio_unitario))
    return partida


def recalcular_obra(obra):
    subtotal_directo = 0.0
    extra_directos = 0.0
    extra_indirectos = 0.0
    extra_cargos = 0.0
    retenciones = 0.0

    for idx, partida in enumerate(getattr(obra, "partidas", []) or [], start=1):
        if not _num(getattr(partida, "orden", 0)):
            partida.orden = idx
        recalcular_partida_obra(partida)
        subtotal_directo += _num(getattr(partida, "importe_directo", 0.0))

    for cargo in getattr(obra, "cargos", []) or []:
        cargo.importe = round4(_num(getattr(cargo, "cantidad", 0.0)) * _num(getattr(cargo, "precio_unitario", 0.0)))
        incidencia = getattr(cargo, "incidencia", "indirecto")
        if incidencia == "directo_global":
            extra_directos += cargo.importe
        elif incidencia == "cargo_adicional":
            extra_cargos += cargo.importe
        elif incidencia == "retencion":
            retenciones += cargo.importe
        else:
            extra_indirectos += cargo.importe

    subtotal_directo += extra_directos
    obra.subtotal_directo = round4(subtotal_directo)
    indirecto_pct = _num(getattr(obra, "indirecto_pct", 0.0))
    indirecto_desglosado = _num(getattr(obra, "indirecto_campo_pct", 0.0)) + _num(getattr(obra, "indirecto_oficina_pct", 0.0))
    if indirecto_desglosado > 0:
        indirecto_pct = indirecto_desglosado
        obra.indirecto_pct = round4(indirecto_desglosado)

    indirecto_monto = (subtotal_directo * (indirecto_pct / 100.0)) + extra_indirectos
    base_fin = subtotal_directo + indirecto_monto

    financiamiento_monto = base_fin * (_num(getattr(obra, "financiamiento_pct", 0.0)) / 100.0)
    base_utilidad = base_fin + financiamiento_monto

    utilidad_monto = base_utilidad * (_num(getattr(obra, "utilidad_pct", 0.0)) / 100.0)
    base_cargos = base_utilidad + utilidad_monto

    cargos_monto = (base_cargos * (_num(getattr(obra, "cargos_adicionales_pct", 0.0)) / 100.0)) + extra_cargos
    obra.indirecto_monto = round4(indirecto_monto)
    obra.financiamiento_monto = round4(financiamiento_monto)
    obra.utilidad_monto = round4(utilidad_monto)
    obra.cargos_adicionales_monto = round4(cargos_monto)
    obra.retenciones_monto = round4(retenciones)
    obra.total_venta = round4(base_cargos + cargos_monto)
    obra.total_neto = round4(obra.total_venta - obra.retenciones_monto)
    return obra


def explosion_insumos_obra(obra):
    acumulado = {}
    for partida in getattr(obra, "partidas", []) or []:
        apu = getattr(partida, "apu", None)
        if not apu:
            continue
        recalcular_apu(apu)
        cantidad_partida = _num(getattr(partida, "cantidad", 0.0))
        for detalle in getattr(apu, "detalles", []) or []:
            cantidad_base = _num(getattr(detalle, "cantidad_calculada", 0.0))
            cantidad_total = round4(cantidad_base * cantidad_partida)
            key = (
                getattr(detalle, "tipo_insumo", ""),
                getattr(detalle, "codigo", "") or getattr(detalle, "descripcion", ""),
                getattr(detalle, "unidad", ""),
            )
            bucket = acumulado.setdefault(
                key,
                {
                    "tipo": getattr(detalle, "tipo_insumo", ""),
                    "codigo": getattr(detalle, "codigo", None),
                    "descripcion": getattr(detalle, "descripcion", ""),
                    "unidad": getattr(detalle, "unidad", ""),
                    "cantidad": 0.0,
                    "precio_unitario": _num(getattr(detalle, "precio_unitario", 0.0)),
                    "importe": 0.0,
                    "partidas": set(),
                },
            )
            bucket["cantidad"] = round4(bucket["cantidad"] + cantidad_total)
            bucket["importe"] = round4(bucket["importe"] + (cantidad_total * _num(getattr(detalle, "precio_unitario", 0.0))))
            bucket["partidas"].add(getattr(partida, "concepto", None) or getattr(apu, "concepto", ""))

    rows = list(acumulado.values())
    rows.sort(key=lambda item: (item["tipo"], item["descripcion"]))
    for row in rows:
        row["partidas"] = sorted(p for p in row["partidas"] if p)
    return rows


def programa_obra(obra):
    partidas = []
    total_jornadas = 0.0
    fecha_cursor = getattr(obra, "fecha_inicio", None)
    intervalo = max(1, int(_num(getattr(obra, "programa_intervalo_dias", 7), 7)))
    frentes = _num(getattr(obra, "frentes", 1.0), 1.0) or 1.0
    calendario = []
    for partida in getattr(obra, "partidas", []) or []:
        apu = getattr(partida, "apu", None)
        if not apu:
            continue
        recalcular_apu(apu)
        jornadas_unitarias = _num(getattr(apu, "jornadas_estimadas", 0.0))
        jornadas = round4((jornadas_unitarias * _num(getattr(partida, "cantidad", 0.0))) / frentes)
        total_jornadas += jornadas
        duracion_dias = max(1, int(round(jornadas))) if jornadas else 1
        fecha_inicio_partida = fecha_cursor
        fecha_fin_partida = None
        periodo = None
        if fecha_inicio_partida:
            fecha_fin_partida = fecha_inicio_partida + timedelta(days=max(duracion_dias - 1, 0))
            periodo_idx = int(((fecha_inicio_partida - getattr(obra, "fecha_inicio", fecha_inicio_partida)).days // intervalo) + 1)
            periodo = f"P{periodo_idx:02d}"
            fecha_cursor = fecha_fin_partida + timedelta(days=1)
        partidas.append(
            {
                "orden": getattr(partida, "orden", 0),
                "capitulo": getattr(partida, "capitulo", None),
                "concepto": getattr(partida, "concepto", None) or getattr(apu, "concepto", ""),
                "cantidad": _num(getattr(partida, "cantidad", 0.0)),
                "unidad": getattr(partida, "unidad", ""),
                "jornadas": jornadas,
                "inicio": fecha_inicio_partida,
                "fin": fecha_fin_partida,
                "periodo": periodo,
            }
        )
        if periodo:
            calendario.append(
                {
                    "periodo": periodo,
                    "orden": getattr(partida, "orden", 0),
                    "capitulo": getattr(partida, "capitulo", None),
                    "concepto": getattr(partida, "concepto", None) or getattr(apu, "concepto", ""),
                    "inicio": fecha_inicio_partida,
                    "fin": fecha_fin_partida,
                    "jornadas": jornadas,
                }
            )

    if not _num(getattr(obra, "plazo_dias", 0)) and total_jornadas:
        obra.plazo_dias = max(1, int(round(total_jornadas)))
    if getattr(obra, "fecha_inicio", None) and getattr(obra, "plazo_dias", 0):
        obra.fecha_fin = obra.fecha_inicio + timedelta(days=max(int(_num(getattr(obra, "plazo_dias", 0))) - 1, 0))

    return {
        "partidas": partidas,
        "calendario": calendario,
        "total_jornadas": round4(total_jornadas),
        "plazo_dias": int(_num(getattr(obra, "plazo_dias", 0))),
    }


def programa_recursos_obra(obra):
    recursos = {}
    fecha_inicio_obra = getattr(obra, "fecha_inicio", None)
    intervalo = max(1, int(_num(getattr(obra, "programa_intervalo_dias", 7), 7)))
    calendario_map = {
        item.get("orden"): item
        for item in programa_obra(obra).get("calendario", [])
    }

    for partida in getattr(obra, "partidas", []) or []:
        apu = getattr(partida, "apu", None)
        if not apu:
            continue
        recalcular_apu(apu)
        cantidad_partida = _num(getattr(partida, "cantidad", 0.0))
        prog = calendario_map.get(getattr(partida, "orden", None), {})
        partida_inicio = prog.get("inicio")
        partida_fin = prog.get("fin")

        for detalle in getattr(apu, "detalles", []) or []:
            if getattr(detalle, "tipo_insumo", "") not in {"mano_obra", "maquinaria", "auxiliar"}:
                continue
            cantidad_recurso = round4(_num(getattr(detalle, "cantidad_calculada", 0.0)) * cantidad_partida)
            descripcion = getattr(detalle, "descripcion", "") or ""
            key = (
                getattr(detalle, "tipo_insumo", ""),
                getattr(detalle, "codigo", "") or descripcion,
                getattr(detalle, "unidad", ""),
            )
            bucket = recursos.setdefault(
                key,
                {
                    "tipo": getattr(detalle, "tipo_insumo", ""),
                    "codigo": getattr(detalle, "codigo", None),
                    "descripcion": descripcion,
                    "unidad": getattr(detalle, "unidad", ""),
                    "cantidad_total": 0.0,
                    "importe_total": 0.0,
                    "frentes": set(),
                    "periodos": {},
                },
            )
            bucket["cantidad_total"] = round4(bucket["cantidad_total"] + cantidad_recurso)
            bucket["importe_total"] = round4(bucket["importe_total"] + (cantidad_recurso * _num(getattr(detalle, "precio_unitario", 0.0))))
            if partida_inicio and fecha_inicio_obra:
                periodo_idx = int(((partida_inicio - fecha_inicio_obra).days // intervalo) + 1)
                periodo = f"P{periodo_idx:02d}"
            else:
                periodo = "P01"
            per = bucket["periodos"].setdefault(
                periodo,
                {
                    "periodo": periodo,
                    "cantidad": 0.0,
                    "inicio": partida_inicio,
                    "fin": partida_fin,
                    "partidas": set(),
                },
            )
            per["cantidad"] = round4(per["cantidad"] + cantidad_recurso)
            if partida_inicio and (per["inicio"] is None or partida_inicio < per["inicio"]):
                per["inicio"] = partida_inicio
            if partida_fin and (per["fin"] is None or partida_fin > per["fin"]):
                per["fin"] = partida_fin
            per["partidas"].add(getattr(partida, "concepto", None) or getattr(apu, "concepto", ""))

    rows = []
    for item in recursos.values():
        periodos = list(item["periodos"].values())
        periodos.sort(key=lambda p: p["periodo"])
        for period in periodos:
            period["partidas"] = sorted(p for p in period["partidas"] if p)
        item["periodos"] = periodos
        rows.append(item)
    rows.sort(key=lambda item: (item["tipo"], item["descripcion"]))
    return rows
