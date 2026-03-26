from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from models import db, PUObra, PUPartida, PUPartidaInsumo, PURecurso, PUSobrecosto


pu_bp = Blueprint("pu", __name__, template_folder="templates")

RESOURCE_TYPES = {
    "material": "Materiales",
    "mano_obra": "Mano de obra",
    "maquinaria": "Maquinaria",
}

ROW_TYPES = {
    "material": "Material",
    "mano_obra": "Mano de obra",
    "maquinaria": "Maquinaria",
    "porcentaje_mo": "% sobre mano de obra",
    "porcentaje_cd": "% sobre costo directo",
    "otro": "Otro",
}


def _to_float(value, default=0.0) -> float:
    try:
        return float(str(value or "").replace(",", "").strip() or default)
    except Exception:
        return float(default)


def _to_int(value, default=0) -> int:
    try:
        return int(str(value or "").strip() or default)
    except Exception:
        return int(default)


def _to_date(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except Exception:
        return None


def _ensure_sobrecosto(obra: PUObra) -> PUSobrecosto:
    if obra.sobrecosto:
        return obra.sobrecosto
    sob = PUSobrecosto(obra=obra)
    db.session.add(sob)
    db.session.flush()
    return sob


def _partida_metrics(partida: PUPartida, sob: PUSobrecosto | None = None) -> dict:
    sob = sob or _ensure_sobrecosto(partida.obra)

    materiales = 0.0
    mano_obra = 0.0
    maquinaria = 0.0
    otros = 0.0

    fixed_rows: list[PUPartidaInsumo] = []
    percent_rows: list[PUPartidaInsumo] = []

    for row in partida.insumos:
        if row.tipo in {"porcentaje_mo", "porcentaje_cd"}:
            percent_rows.append(row)
            continue
        importe = _to_float(row.costo_unitario) * _to_float(row.cantidad)
        row.importe = round(importe, 2)
        fixed_rows.append(row)
        if row.tipo == "material":
            materiales += importe
        elif row.tipo == "mano_obra":
            mano_obra += importe
        elif row.tipo == "maquinaria":
            maquinaria += importe
        else:
            otros += importe

    directo_base = materiales + mano_obra + maquinaria + otros
    porcentaje_total = 0.0

    for row in percent_rows:
        base = mano_obra if row.base_tipo == "mano_obra" else directo_base
        importe = base * (_to_float(row.porcentaje) / 100.0)
        row.importe = round(importe, 2)
        porcentaje_total += importe

    precio_directo = round(directo_base + porcentaje_total, 2)

    indirecto_total_pct = _to_float(sob.indirecto_campo_pct) + _to_float(sob.indirecto_oficina_pct)
    financiamiento_pct = _to_float(sob.financiamiento_pct)
    utilidad_pct = _to_float(sob.utilidad_pct or sob.porcentaje_utilidad_propuesta)
    cargos_pct = _to_float(sob.cargos_adicionales_pct)
    factor_sobrecosto = 1 + ((indirecto_total_pct + financiamiento_pct + utilidad_pct + cargos_pct) / 100.0)
    precio_unitario = round(precio_directo * factor_sobrecosto, 2)
    importe_total = round(precio_unitario * _to_float(partida.cantidad, 1.0), 2)

    partida.precio_directo = precio_directo
    partida.precio_unitario = precio_unitario
    partida.importe_total = importe_total

    return {
        "materiales": round(materiales, 2),
        "mano_obra": round(mano_obra, 2),
        "maquinaria": round(maquinaria, 2),
        "otros": round(otros, 2),
        "porcentajes": round(porcentaje_total, 2),
        "precio_directo": precio_directo,
        "indirecto_total_pct": round(indirecto_total_pct, 4),
        "financiamiento_pct": round(financiamiento_pct, 4),
        "utilidad_pct": round(utilidad_pct, 4),
        "cargos_pct": round(cargos_pct, 4),
        "factor_sobrecosto": round(factor_sobrecosto, 4),
        "precio_unitario": precio_unitario,
        "importe_total": importe_total,
    }


def _obra_summary(obra: PUObra) -> dict:
    sob = _ensure_sobrecosto(obra)
    summary = {
        "partidas": len(obra.partidas),
        "costo_directo": 0.0,
        "precio_total": 0.0,
        "mano_obra_gravable": 0.0,
    }

    for partida in obra.partidas:
        metrics = _partida_metrics(partida, sob)
        summary["costo_directo"] += metrics["precio_directo"]
        summary["precio_total"] += metrics["importe_total"]
        for row in partida.insumos:
            if row.tipo == "mano_obra" and row.gravable:
                summary["mano_obra_gravable"] += row.importe or 0.0

    summary["costo_directo"] = round(summary["costo_directo"], 2)
    summary["precio_total"] = round(summary["precio_total"], 2)
    summary["mano_obra_gravable"] = round(summary["mano_obra_gravable"], 2)
    return summary


@pu_bp.route("/")
@login_required
def obras_index():
    obras = PUObra.query.order_by(PUObra.actualizado_en.desc(), PUObra.id.desc()).all()
    resumenes = {obra.id: _obra_summary(obra) for obra in obras}
    return render_template("pu_obras.html", title="Precios Unitarios", obras=obras, resumenes=resumenes)


@pu_bp.route("/obras/nueva", methods=["POST"])
@login_required
def obra_nueva():
    nombre = (request.form.get("nombre") or "").strip()
    if not nombre:
        flash("El nombre de la obra es obligatorio.", "warning")
        return redirect(url_for("pu.obras_index"))

    obra = PUObra(
        nombre=nombre,
        descripcion=(request.form.get("descripcion") or "").strip() or None,
        empresa=(request.form.get("empresa") or "").strip() or None,
        responsable=(request.form.get("responsable") or "").strip() or None,
        fecha_inicio=_to_date(request.form.get("fecha_inicio")),
        fecha_terminacion=_to_date(request.form.get("fecha_terminacion")),
        plazo_dias=_to_int(request.form.get("plazo_dias"), 0),
    )
    db.session.add(obra)
    db.session.flush()
    _ensure_sobrecosto(obra)
    db.session.commit()
    flash("Obra creada correctamente.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id))


@pu_bp.route("/obras/<int:obra_id>", methods=["GET"])
@login_required
def obra_detalle(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    sob = _ensure_sobrecosto(obra)
    summary = _obra_summary(obra)
    db.session.commit()

    grouped_partidas: dict[str, list[PUPartida]] = defaultdict(list)
    partida_metrics = {}
    for partida in obra.partidas:
        grouped_partidas[partida.capitulo or "General"].append(partida)
        partida_metrics[partida.id] = _partida_metrics(partida, sob)

    recursos = {
        tipo: PURecurso.query.filter_by(tipo=tipo).order_by(PURecurso.descripcion.asc()).all()
        for tipo in RESOURCE_TYPES
    }

    tab = request.args.get("tab", "generales")
    return render_template(
        "pu_obra_detail.html",
        title=f"PU - {obra.nombre}",
        obra=obra,
        sob=sob,
        summary=summary,
        grouped_partidas=grouped_partidas,
        partida_metrics=partida_metrics,
        recursos=recursos,
        resource_types=RESOURCE_TYPES,
        tab=tab,
    )


@pu_bp.route("/obras/<int:obra_id>/generales", methods=["POST"])
@login_required
def obra_actualizar_generales(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    obra.nombre = (request.form.get("nombre") or "").strip()
    obra.descripcion = (request.form.get("descripcion") or "").strip() or None
    obra.direccion = (request.form.get("direccion") or "").strip() or None
    obra.colonia = (request.form.get("colonia") or "").strip() or None
    obra.ciudad = (request.form.get("ciudad") or "").strip() or None
    obra.estado = (request.form.get("estado") or "").strip() or None
    obra.codigo_postal = (request.form.get("codigo_postal") or "").strip() or None
    obra.telefono = (request.form.get("telefono") or "").strip() or None
    obra.correo = (request.form.get("correo") or "").strip() or None
    obra.observaciones = (request.form.get("observaciones") or "").strip() or None
    obra.empresa = (request.form.get("empresa") or "").strip() or None
    obra.encargado = (request.form.get("encargado") or "").strip() or None
    obra.responsable = (request.form.get("responsable") or "").strip() or None
    obra.fecha_inicio = _to_date(request.form.get("fecha_inicio"))
    obra.fecha_terminacion = _to_date(request.form.get("fecha_terminacion"))
    obra.plazo_dias = _to_int(request.form.get("plazo_dias"), 0)
    obra.moneda = (request.form.get("moneda") or "PESOS").strip() or "PESOS"

    if not obra.nombre:
        flash("El nombre de la obra es obligatorio.", "warning")
        return redirect(url_for("pu.obra_detalle", obra_id=obra.id, tab="generales"))

    db.session.commit()
    flash("Datos generales actualizados.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id, tab="generales"))


@pu_bp.route("/obras/<int:obra_id>/sobrecosto", methods=["POST"])
@login_required
def obra_actualizar_sobrecosto(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    sob = _ensure_sobrecosto(obra)

    sob.porcentaje_utilidad_propuesta = _to_float(request.form.get("porcentaje_utilidad_propuesta"), 10.0)
    sob.tasa_interes_usada = _to_float(request.form.get("tasa_interes_usada"))
    sob.porcentaje_puntos_banco = _to_float(request.form.get("porcentaje_puntos_banco"))
    sob.porcentaje_primer_anticipo = _to_float(request.form.get("porcentaje_primer_anticipo"))
    sob.factor_sfp = _to_float(request.form.get("factor_sfp"))
    sob.indicador_economico = (request.form.get("indicador_economico") or "").strip() or None
    sob.tipo_anticipo = (request.form.get("tipo_anticipo") or "").strip() or None
    sob.libro_sobrecosto = (request.form.get("libro_sobrecosto") or "").strip() or None
    sob.programa_obra = (request.form.get("programa_obra") or "").strip() or None
    sob.num_veces = _to_int(request.form.get("num_veces"), 1)
    sob.libro_pie_indirectos = (request.form.get("libro_pie_indirectos") or "").strip() or None
    sob.indirecto_campo_pct = _to_float(request.form.get("indirecto_campo_pct"))
    sob.indirecto_oficina_pct = _to_float(request.form.get("indirecto_oficina_pct"))
    sob.financiamiento_pct = _to_float(request.form.get("financiamiento_pct"))
    sob.utilidad_pct = _to_float(request.form.get("utilidad_pct"), sob.porcentaje_utilidad_propuesta)
    sob.cargos_adicionales_pct = _to_float(request.form.get("cargos_adicionales_pct"))
    sob.factor_pie_indirectos = _to_float(request.form.get("factor_pie_indirectos"), 1.0)

    for partida in obra.partidas:
        _partida_metrics(partida, sob)

    db.session.commit()
    flash("Sobrecosto actualizado. Los indirectos se guardaron manualmente.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id, tab="sobrecosto"))


@pu_bp.route("/obras/<int:obra_id>/partidas/nueva", methods=["POST"])
@login_required
def partida_nueva(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    descripcion = (request.form.get("descripcion") or "").strip()
    if not descripcion:
        flash("La descripción de la partida es obligatoria.", "warning")
        return redirect(url_for("pu.obra_detalle", obra_id=obra.id, tab="presupuesto"))

    partida = PUPartida(
        obra=obra,
        capitulo=(request.form.get("capitulo") or "General").strip() or "General",
        wbs=(request.form.get("wbs") or "").strip() or None,
        codigo=(request.form.get("codigo") or "").strip() or None,
        descripcion=descripcion,
        unidad=(request.form.get("unidad") or "pza").strip() or "pza",
        cantidad=_to_float(request.form.get("cantidad"), 1.0),
    )
    db.session.add(partida)
    db.session.commit()
    flash("Partida creada correctamente.", "success")
    return redirect(url_for("pu.partida_detalle", partida_id=partida.id))


@pu_bp.route("/partidas/<int:partida_id>", methods=["GET"])
@login_required
def partida_detalle(partida_id: int):
    partida = PUPartida.query.get_or_404(partida_id)
    sob = _ensure_sobrecosto(partida.obra)
    metrics = _partida_metrics(partida, sob)
    db.session.commit()
    recursos = {
        tipo: PURecurso.query.filter_by(tipo=tipo).order_by(PURecurso.descripcion.asc()).all()
        for tipo in RESOURCE_TYPES
    }
    return render_template(
        "pu_partida_edit.html",
        title=f"PU - {partida.descripcion}",
        partida=partida,
        obra=partida.obra,
        sob=sob,
        metrics=metrics,
        recursos=recursos,
        resource_types=RESOURCE_TYPES,
        row_types=ROW_TYPES,
    )


@pu_bp.route("/partidas/<int:partida_id>/editar", methods=["POST"])
@login_required
def partida_actualizar(partida_id: int):
    partida = PUPartida.query.get_or_404(partida_id)
    partida.capitulo = (request.form.get("capitulo") or "General").strip() or "General"
    partida.wbs = (request.form.get("wbs") or "").strip() or None
    partida.codigo = (request.form.get("codigo") or "").strip() or None
    partida.descripcion = (request.form.get("descripcion") or "").strip()
    partida.unidad = (request.form.get("unidad") or "pza").strip() or "pza"
    partida.cantidad = _to_float(request.form.get("cantidad"), 1.0)

    if not partida.descripcion:
        flash("La descripción de la partida es obligatoria.", "warning")
        return redirect(url_for("pu.partida_detalle", partida_id=partida.id))

    _partida_metrics(partida, _ensure_sobrecosto(partida.obra))
    db.session.commit()
    flash("Partida actualizada.", "success")
    return redirect(url_for("pu.partida_detalle", partida_id=partida.id))


@pu_bp.route("/partidas/<int:partida_id>/insumos/agregar", methods=["POST"])
@login_required
def partida_insumo_agregar(partida_id: int):
    partida = PUPartida.query.get_or_404(partida_id)
    row_kind = (request.form.get("row_kind") or "recurso").strip()

    if row_kind == "porcentaje":
        descripcion = (request.form.get("descripcion") or "").strip()
        if not descripcion:
            flash("La descripción del cargo porcentual es obligatoria.", "warning")
            return redirect(url_for("pu.partida_detalle", partida_id=partida.id))
        row = PUPartidaInsumo(
            partida=partida,
            tipo=(request.form.get("tipo_porcentaje") or "porcentaje_mo").strip(),
            base_tipo=(request.form.get("base_tipo") or "mano_obra").strip(),
            codigo=(request.form.get("codigo") or "").strip() or None,
            descripcion=descripcion,
            unidad=(request.form.get("unidad") or "%").strip() or "%",
            porcentaje=_to_float(request.form.get("porcentaje")),
            orden=len(partida.insumos) + 1,
            gravable=False,
        )
    else:
        recurso = PURecurso.query.get_or_404(_to_int(request.form.get("recurso_id")))
        row = PUPartidaInsumo(
            partida=partida,
            recurso=recurso,
            tipo=recurso.tipo,
            codigo=recurso.codigo,
            descripcion=recurso.descripcion,
            unidad=recurso.unidad,
            costo_unitario=recurso.costo_base,
            cantidad=_to_float(request.form.get("cantidad"), 1.0),
            orden=len(partida.insumos) + 1,
            gravable=recurso.gravable,
        )

    db.session.add(row)
    _partida_metrics(partida, _ensure_sobrecosto(partida.obra))
    db.session.commit()
    flash("Renglón agregado a la matriz.", "success")
    return redirect(url_for("pu.partida_detalle", partida_id=partida.id))


@pu_bp.route("/insumos/<int:row_id>/actualizar", methods=["POST"])
@login_required
def partida_insumo_actualizar(row_id: int):
    row = PUPartidaInsumo.query.get_or_404(row_id)
    row.codigo = (request.form.get("codigo") or "").strip() or None
    row.descripcion = (request.form.get("descripcion") or "").strip()
    row.unidad = (request.form.get("unidad") or "").strip() or None
    row.costo_unitario = _to_float(request.form.get("costo_unitario"))
    row.cantidad = _to_float(request.form.get("cantidad"))
    row.porcentaje = _to_float(request.form.get("porcentaje"))
    row.base_tipo = (request.form.get("base_tipo") or row.base_tipo or "").strip() or None
    row.gravable = request.form.get("gravable") == "1"

    if not row.descripcion:
        flash("La descripción del renglón es obligatoria.", "warning")
        return redirect(url_for("pu.partida_detalle", partida_id=row.partida_id))

    _partida_metrics(row.partida, _ensure_sobrecosto(row.partida.obra))
    db.session.commit()
    flash("Renglón actualizado.", "success")
    return redirect(url_for("pu.partida_detalle", partida_id=row.partida_id))


@pu_bp.route("/insumos/<int:row_id>/eliminar", methods=["POST"])
@login_required
def partida_insumo_eliminar(row_id: int):
    row = PUPartidaInsumo.query.get_or_404(row_id)
    partida_id = row.partida_id
    partida = row.partida
    db.session.delete(row)
    _partida_metrics(partida, _ensure_sobrecosto(partida.obra))
    db.session.commit()
    flash("Renglón eliminado.", "success")
    return redirect(url_for("pu.partida_detalle", partida_id=partida_id))


@pu_bp.route("/catalogos/<string:tipo>", methods=["GET", "POST"])
@login_required
def catalogo_recurso(tipo: str):
    if tipo not in RESOURCE_TYPES:
        flash("Tipo de catálogo inválido.", "warning")
        return redirect(url_for("pu.obras_index"))

    if request.method == "POST":
        descripcion = (request.form.get("descripcion") or "").strip()
        if not descripcion:
            flash("La descripción del recurso es obligatoria.", "warning")
            return redirect(url_for("pu.catalogo_recurso", tipo=tipo))

        recurso = PURecurso(
            tipo=tipo,
            codigo=(request.form.get("codigo") or "").strip() or None,
            descripcion=descripcion,
            unidad=(request.form.get("unidad") or "").strip() or None,
            costo_base=_to_float(request.form.get("costo_base")),
            familia=(request.form.get("familia") or "").strip() or None,
            gravable=request.form.get("gravable") == "1",
        )
        db.session.add(recurso)
        db.session.commit()
        flash("Recurso agregado al catálogo.", "success")
        return redirect(url_for("pu.catalogo_recurso", tipo=tipo))

    recursos = PURecurso.query.filter_by(tipo=tipo).order_by(PURecurso.descripcion.asc()).all()
    return render_template(
        "pu_catalogo.html",
        title=f"Catálogo {RESOURCE_TYPES[tipo]}",
        tipo=tipo,
        tipos=RESOURCE_TYPES,
        recursos=recursos,
    )


@pu_bp.route("/catalogos/<string:tipo>/<int:recurso_id>/eliminar", methods=["POST"])
@login_required
def catalogo_recurso_eliminar(tipo: str, recurso_id: int):
    recurso = PURecurso.query.get_or_404(recurso_id)
    db.session.delete(recurso)
    db.session.commit()
    flash("Recurso eliminado del catálogo.", "success")
    return redirect(url_for("pu.catalogo_recurso", tipo=tipo))
