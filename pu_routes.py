from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from models import db, PUObra, PUPartida, PUPartidaInsumo, PURecurso, PUSobrecosto


pu_bp = Blueprint("pu", __name__, template_folder="templates")

TIPOS_RECURSO = {
    "material": "Material",
    "mano_obra": "Mano de obra",
    "maquinaria": "Maquinaria",
    "basico": "Básico",
    "extra": "Extra",
}


def _float(value, default: float = 0.0) -> float:
    try:
        return float(str(value or "").replace(",", "").strip() or default)
    except Exception:
        return default


def _ensure_sobrecosto(obra: PUObra) -> PUSobrecosto:
    if obra.sobrecosto:
        return obra.sobrecosto
    sob = PUSobrecosto(obra=obra)
    db.session.add(sob)
    db.session.flush()
    return sob


def _sobrecosto_pct(sob: PUSobrecosto) -> float:
    return (
        _float(sob.indirecto_campo_pct)
        + _float(sob.indirecto_oficina_pct)
        + _float(sob.financiamiento_pct)
        + _float(sob.utilidad_pct)
        + _float(sob.cargos_adicionales_pct)
    )


def _recalcular_partida(partida: PUPartida, sob: PUSobrecosto | None = None) -> dict:
    sob = sob or _ensure_sobrecosto(partida.obra)
    totales = {key: 0.0 for key in TIPOS_RECURSO}

    for insumo in partida.insumos:
        insumo.importe = round(_float(insumo.cantidad) * _float(insumo.costo_unitario), 2)
        if insumo.tipo in totales:
            totales[insumo.tipo] += insumo.importe

    costo_directo = round(sum(totales.values()), 2)
    factor = 1 + (_sobrecosto_pct(sob) / 100.0)
    precio_unitario = round(costo_directo * factor, 2)
    importe = round(precio_unitario * _float(partida.cantidad, 1.0), 2)

    partida.costo_directo = costo_directo
    partida.precio_unitario = precio_unitario
    partida.importe = importe

    return {
        "totales": {k: round(v, 2) for k, v in totales.items()},
        "costo_directo": costo_directo,
        "sobrecosto_pct": round(_sobrecosto_pct(sob), 4),
        "factor": round(factor, 6),
        "precio_unitario": precio_unitario,
        "importe": importe,
    }


def _resumen_obra(obra: PUObra) -> dict:
    sob = _ensure_sobrecosto(obra)
    resumen = {
        "partidas": len(obra.partidas),
        "costo_directo": 0.0,
        "importe": 0.0,
        "material": 0.0,
        "mano_obra": 0.0,
        "maquinaria": 0.0,
        "basico": 0.0,
        "extra": 0.0,
    }
    for partida in obra.partidas:
        metricas = _recalcular_partida(partida, sob)
        resumen["costo_directo"] += metricas["costo_directo"]
        resumen["importe"] += metricas["importe"]
        for tipo, total in metricas["totales"].items():
            resumen[tipo] += total
    return {k: round(v, 2) if isinstance(v, float) else v for k, v in resumen.items()}


@pu_bp.route("/")
@login_required
def obras_index():
    obras = PUObra.query.order_by(PUObra.actualizado_en.desc(), PUObra.id.desc()).all()
    resumenes = {obra.id: _resumen_obra(obra) for obra in obras}
    db.session.commit()
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
        cliente=(request.form.get("cliente") or "").strip() or None,
        ubicacion=(request.form.get("ubicacion") or "").strip() or None,
        descripcion=(request.form.get("descripcion") or "").strip() or None,
        moneda=(request.form.get("moneda") or "MXN").strip() or "MXN",
    )
    db.session.add(obra)
    db.session.flush()
    _ensure_sobrecosto(obra)
    db.session.commit()
    flash("Obra creada.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id))


@pu_bp.route("/obras/<int:obra_id>")
@login_required
def obra_detalle(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    sob = _ensure_sobrecosto(obra)
    metricas = {partida.id: _recalcular_partida(partida, sob) for partida in obra.partidas}
    resumen = _resumen_obra(obra)
    recursos = PURecurso.query.order_by(PURecurso.tipo.asc(), PURecurso.descripcion.asc()).all()
    db.session.commit()
    return render_template(
        "pu_obra_detail.html",
        title=f"Precios Unitarios - {obra.nombre}",
        obra=obra,
        sob=sob,
        resumen=resumen,
        metricas=metricas,
        recursos=recursos,
        tipos_recurso=TIPOS_RECURSO,
    )


@pu_bp.route("/obras/<int:obra_id>/generales", methods=["POST"])
@login_required
def obra_actualizar(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    obra.nombre = (request.form.get("nombre") or "").strip() or obra.nombre
    obra.cliente = (request.form.get("cliente") or "").strip() or None
    obra.ubicacion = (request.form.get("ubicacion") or "").strip() or None
    obra.descripcion = (request.form.get("descripcion") or "").strip() or None
    obra.moneda = (request.form.get("moneda") or "MXN").strip() or "MXN"
    db.session.commit()
    flash("Datos generales actualizados.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id))


@pu_bp.route("/obras/<int:obra_id>/sobrecosto", methods=["POST"])
@login_required
def sobrecosto_actualizar(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    sob = _ensure_sobrecosto(obra)
    sob.indirecto_campo_pct = _float(request.form.get("indirecto_campo_pct"))
    sob.indirecto_oficina_pct = _float(request.form.get("indirecto_oficina_pct"))
    sob.financiamiento_pct = _float(request.form.get("financiamiento_pct"))
    sob.utilidad_pct = _float(request.form.get("utilidad_pct"), 10.0)
    sob.cargos_adicionales_pct = _float(request.form.get("cargos_adicionales_pct"))
    for partida in obra.partidas:
        _recalcular_partida(partida, sob)
    db.session.commit()
    flash("Sobrecosto actualizado.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id))


@pu_bp.route("/obras/<int:obra_id>/partidas/nueva", methods=["POST"])
@login_required
def partida_nueva(obra_id: int):
    obra = PUObra.query.get_or_404(obra_id)
    descripcion = (request.form.get("descripcion") or "").strip()
    if not descripcion:
        flash("La descripción de la partida es obligatoria.", "warning")
        return redirect(url_for("pu.obra_detalle", obra_id=obra.id))
    partida = PUPartida(
        obra=obra,
        capitulo=(request.form.get("capitulo") or "General").strip() or "General",
        clave=(request.form.get("clave") or "").strip() or None,
        descripcion=descripcion,
        unidad=(request.form.get("unidad") or "pza").strip() or "pza",
        cantidad=_float(request.form.get("cantidad"), 1.0),
    )
    db.session.add(partida)
    db.session.commit()
    flash("Partida creada.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra.id))


@pu_bp.route("/partidas/<int:partida_id>/actualizar", methods=["POST"])
@login_required
def partida_actualizar(partida_id: int):
    partida = PUPartida.query.get_or_404(partida_id)
    partida.capitulo = (request.form.get("capitulo") or "General").strip() or "General"
    partida.clave = (request.form.get("clave") or "").strip() or None
    partida.descripcion = (request.form.get("descripcion") or "").strip() or partida.descripcion
    partida.unidad = (request.form.get("unidad") or "pza").strip() or "pza"
    partida.cantidad = _float(request.form.get("cantidad"), 1.0)
    _recalcular_partida(partida)
    db.session.commit()
    flash("Partida actualizada.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=partida.obra_id))


@pu_bp.route("/partidas/<int:partida_id>/eliminar", methods=["POST"])
@login_required
def partida_eliminar(partida_id: int):
    partida = PUPartida.query.get_or_404(partida_id)
    obra_id = partida.obra_id
    db.session.delete(partida)
    db.session.commit()
    flash("Partida eliminada.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra_id))


@pu_bp.route("/partidas/<int:partida_id>/insumos/nuevo", methods=["POST"])
@login_required
def insumo_nuevo(partida_id: int):
    partida = PUPartida.query.get_or_404(partida_id)
    descripcion = (request.form.get("descripcion") or "").strip()
    if not descripcion:
        flash("La descripción del insumo es obligatoria.", "warning")
        return redirect(url_for("pu.obra_detalle", obra_id=partida.obra_id))
    insumo = PUPartidaInsumo(
        partida=partida,
        tipo=(request.form.get("tipo") or "material").strip(),
        codigo=(request.form.get("codigo") or "").strip() or None,
        descripcion=descripcion,
        unidad=(request.form.get("unidad") or "").strip() or None,
        cantidad=_float(request.form.get("cantidad")),
        costo_unitario=_float(request.form.get("costo_unitario")),
    )
    insumo.importe = round(insumo.cantidad * insumo.costo_unitario, 2)
    db.session.add(insumo)
    _recalcular_partida(partida)
    db.session.commit()
    flash("Insumo agregado.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=partida.obra_id))


@pu_bp.route("/insumos/<int:insumo_id>/eliminar", methods=["POST"])
@login_required
def insumo_eliminar(insumo_id: int):
    insumo = PUPartidaInsumo.query.get_or_404(insumo_id)
    partida = insumo.partida
    obra_id = partida.obra_id
    db.session.delete(insumo)
    db.session.flush()
    _recalcular_partida(partida)
    db.session.commit()
    flash("Insumo eliminado.", "success")
    return redirect(url_for("pu.obra_detalle", obra_id=obra_id))


@pu_bp.route("/catalogo", methods=["GET", "POST"])
@login_required
def catalogo():
    if request.method == "POST":
        descripcion = (request.form.get("descripcion") or "").strip()
        if descripcion:
            recurso = PURecurso(
                tipo=(request.form.get("tipo") or "material").strip(),
                codigo=(request.form.get("codigo") or "").strip() or None,
                descripcion=descripcion,
                unidad=(request.form.get("unidad") or "").strip() or None,
                costo_unitario=_float(request.form.get("costo_unitario")),
            )
            db.session.add(recurso)
            db.session.commit()
            flash("Recurso agregado al catálogo.", "success")
        return redirect(url_for("pu.catalogo"))
    recursos = PURecurso.query.order_by(PURecurso.tipo.asc(), PURecurso.descripcion.asc()).all()
    return render_template("pu_catalogo.html", title="Catálogo PU", recursos=recursos, tipos_recurso=TIPOS_RECURSO)
