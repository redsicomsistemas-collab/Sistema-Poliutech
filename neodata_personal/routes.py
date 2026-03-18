import json
import os
import re
from datetime import datetime

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy import or_, text

from models import Cliente, Concepto, Cotizacion, CotizacionDetalle, db
from .calc import recalcular_apu
from .models import APU, APUDetalle, ManoObra, Maquinaria, Material

apu_bp = Blueprint("apu", __name__, url_prefix="/apu", template_folder="templates")

BASE_DIR = os.path.dirname(__file__)
PLANTILLAS_FILE = os.path.join(BASE_DIR, "plantillas.json")
TIPOS_INSUMO = [
    ("material", "Material"),
    ("mano_obra", "Mano de obra"),
    ("maquinaria", "Maquinaria"),
]
TYPE_LABELS = {key: label for key, label in TIPOS_INSUMO}


def _f(value, default=0.0):
    try:
        if value is None or value == "":
            return float(default)
        return float(str(value).replace(",", "").replace("$", "").strip())
    except Exception:
        return float(default)


def _s(value):
    return (value or "").strip() or None


def _load_plantillas():
    try:
        with open(PLANTILLAS_FILE, "r", encoding="utf-8") as file_handle:
            return json.load(file_handle)
    except Exception:
        return []


def _responsable_actual():
    try:
        nombre = (getattr(current_user, "nombre", "") or "").strip()
        if not nombre:
            return None
        first = nombre.split()[0].strip()
        return first[:1].upper() + first[1:].lower() if first else None
    except Exception:
        return None


def _generar_folio():
    prefix = "PTCH-"
    maxn = 0
    rows = db.session.execute(text("SELECT folio FROM cotizacion WHERE folio LIKE 'PTCH-%'")).fetchall()
    for (folio,) in rows:
        match = re.match(r"PTCH-(\d{4})$", str(folio))
        if match:
            maxn = max(maxn, int(match.group(1)))
    return f"{prefix}{maxn + 1:04d}"


def _resource_for_tipo(tipo_insumo):
    return {
        "material": Material,
        "mano_obra": ManoObra,
        "maquinaria": Maquinaria,
    }.get(tipo_insumo)


def _buscar_recurso(tipo_insumo, referencia_id):
    model = _resource_for_tipo(tipo_insumo)
    if not model:
        return None
    return model.query.get(referencia_id)


def _resource_label(item):
    clave = f"{item.clave} · " if getattr(item, "clave", None) else ""
    categoria = f" [{item.categoria}]" if getattr(item, "categoria", None) else ""
    return f"{clave}{item.nombre}{categoria} · {item.unidad} · ${item.precio_unitario or 0:,.2f}"


def _set_apu_defaults(apu):
    if apu.indirecto_pct is None:
        apu.indirecto_pct = 12.0
    if apu.utilidad_pct is None:
        apu.utilidad_pct = 10.0
    if apu.financiamiento_pct is None:
        apu.financiamiento_pct = 2.5
    if apu.cargos_adicionales_pct is None:
        apu.cargos_adicionales_pct = 0.0
    if apu.jornada_horas is None:
        apu.jornada_horas = 8.0
    if apu.cantidad_objetivo is None:
        apu.cantidad_objetivo = 1.0
    if apu.rendimiento_base is None:
        apu.rendimiento_base = 1.0
    if apu.herramienta_menor_pct is None:
        apu.herramienta_menor_pct = 3.0


def _guardar_apu_desde_form(apu):
    apu.clave = _s(request.form.get("clave"))
    apu.concepto = (request.form.get("concepto") or "").strip()
    apu.descripcion = _s(request.form.get("descripcion"))
    apu.categoria = _s(request.form.get("categoria"))
    apu.unidad = (request.form.get("unidad") or "m2").strip() or "m2"
    apu.cantidad_objetivo = _f(request.form.get("cantidad_objetivo"), 1.0)
    apu.rendimiento_base = _f(request.form.get("rendimiento_base"), 1.0)
    apu.jornada_horas = _f(request.form.get("jornada_horas"), 8.0)
    apu.desperdicio_general_pct = _f(request.form.get("desperdicio_general_pct"))
    apu.herramienta_menor_pct = _f(request.form.get("herramienta_menor_pct"))
    apu.indirecto_pct = _f(request.form.get("indirecto_pct"))
    apu.utilidad_pct = _f(request.form.get("utilidad_pct"))
    apu.financiamiento_pct = _f(request.form.get("financiamiento_pct"))
    apu.cargos_adicionales_pct = _f(request.form.get("cargos_adicionales_pct"))
    apu.notas = _s(request.form.get("notas"))
    recalcular_apu(apu)


def _build_catalog_context():
    materiales = Material.query.order_by(Material.nombre.asc()).all()
    mano_obra = ManoObra.query.order_by(ManoObra.nombre.asc()).all()
    maquinarias = Maquinaria.query.order_by(Maquinaria.nombre.asc()).all()
    return {
        "materiales": materiales,
        "mano_obra": mano_obra,
        "maquinarias": maquinarias,
        "catalogo_por_tipo": {
            "material": materiales,
            "mano_obra": mano_obra,
            "maquinaria": maquinarias,
        },
    }


def _decorate_apu(apu):
    recalcular_apu(apu)
    detalles = sorted(apu.detalles, key=lambda item: (item.tipo_insumo or "", item.id or 0))
    tipo_totales = []
    for tipo, label in TIPOS_INSUMO:
        total = sum((d.subtotal or 0.0) for d in detalles if d.tipo_insumo == tipo)
        tipo_totales.append(
            {
                "key": tipo,
                "label": label,
                "total": total,
                "pct_directo": (total / apu.costo_directo * 100.0) if apu.costo_directo else 0.0,
            }
        )

    apu.detalles_ordenados = detalles
    apu.tipo_totales = tipo_totales
    apu.tarjetas_resumen = [
        {"label": "Materiales", "value": apu.costo_materiales, "tone": "sand"},
        {"label": "Mano de obra", "value": apu.costo_mano_obra, "tone": "mint"},
        {"label": "Maquinaria", "value": apu.costo_maquinaria, "tone": "steel"},
        {"label": "Herr. menor", "value": apu.costo_herramienta, "tone": "amber"},
        {"label": "Costo directo", "value": apu.costo_directo, "tone": "navy"},
        {"label": "P.U. final", "value": apu.precio_unitario, "tone": "ink"},
    ]
    apu.sobrecostos = [
        {"label": "Indirectos", "pct": apu.indirecto_pct or 0.0, "amount": apu.indirecto_monto or 0.0},
        {"label": "Financiamiento", "pct": apu.financiamiento_pct or 0.0, "amount": apu.financiamiento_monto or 0.0},
        {"label": "Utilidad", "pct": apu.utilidad_pct or 0.0, "amount": apu.utilidad_monto or 0.0},
        {"label": "Cargos adicionales", "pct": apu.cargos_adicionales_pct or 0.0, "amount": apu.cargos_adicionales_monto or 0.0},
    ]
    apu.resumen_programa = {
        "cantidad_objetivo": apu.cantidad_objetivo or 0.0,
        "rendimiento_base": apu.rendimiento_base or 0.0,
        "jornada_horas": apu.jornada_horas or 0.0,
        "jornadas_estimadas": getattr(apu, "jornadas_estimadas", 0.0),
        "importe_partida": apu.importe_partida or 0.0,
        "factor_sobrecosto": getattr(apu, "factor_sobrecosto", 0.0),
    }
    return apu


def _dashboard_data():
    apus = APU.query.order_by(APU.actualizado_en.desc(), APU.id.desc()).all()
    total_directo = 0.0
    total_venta = 0.0
    total_materiales = Material.query.count()
    total_mano_obra = ManoObra.query.count()
    total_maquinaria = Maquinaria.query.count()

    for apu in apus:
        recalcular_apu(apu)
        total_directo += float(apu.costo_directo or 0)
        total_venta += float(apu.precio_unitario or 0)

    return {
        "totales": [
            {"label": "APU activos", "value": len(apus)},
            {"label": "Materiales", "value": total_materiales},
            {"label": "Mano de obra", "value": total_mano_obra},
            {"label": "Maquinaria", "value": total_maquinaria},
        ],
        "indicadores": {
            "promedio_directo": (total_directo / len(apus)) if apus else 0.0,
            "promedio_venta": (total_venta / len(apus)) if apus else 0.0,
            "relacion_venta_directo": (total_venta / total_directo) if total_directo else 0.0,
        },
        "apus": apus[:20],
    }


def _save_resource(item, include_provider=False):
    item.nombre = (request.form.get("nombre") or "").strip()
    item.clave = _s(request.form.get("clave"))
    item.categoria = _s(request.form.get("categoria"))
    item.unidad = (request.form.get("unidad") or item.unidad or "pza").strip() or "pza"
    item.precio_unitario = _f(request.form.get("precio_unitario"))
    if include_provider:
        item.proveedor = _s(request.form.get("proveedor"))


def _resource_list(model, template_name, title):
    q = (request.args.get("q") or "").strip()
    query = model.query
    if q:
        pattern = f"%{q}%"
        clauses = [model.nombre.ilike(pattern)]
        if hasattr(model, "clave"):
            clauses.append(model.clave.ilike(pattern))
        if hasattr(model, "categoria"):
            clauses.append(model.categoria.ilike(pattern))
        query = query.filter(or_(*clauses))
    items = query.order_by(model.nombre.asc()).all()
    return render_template(template_name, items=items, title=title, q=q)


def _render_apu_form(apu=None, title="APU"):
    if apu:
        _decorate_apu(apu)
        db.session.flush()
    context = _build_catalog_context()
    return render_template(
        "neodata/apu_form.html",
        title=title,
        apu=apu,
        tipos_insumo=TIPOS_INSUMO,
        type_labels=TYPE_LABELS,
        resource_label=_resource_label,
        **context,
    )


@apu_bp.route("/")
@login_required
def index():
    dashboard = _dashboard_data()
    return render_template("neodata/apu_index.html", dashboard=dashboard, title="MAR DATA")


@apu_bp.route("/lista")
@login_required
def apu_list():
    q = (request.args.get("q") or "").strip()
    query = APU.query
    if q:
        pattern = f"%{q}%"
        query = query.filter(or_(APU.concepto.ilike(pattern), APU.clave.ilike(pattern), APU.categoria.ilike(pattern)))
    items = query.order_by(APU.actualizado_en.desc(), APU.id.desc()).all()
    for item in items:
        recalcular_apu(item)
    return render_template("neodata/apu_list.html", items=items, title="Lista de APU", q=q)


@apu_bp.route("/nuevo", methods=["GET", "POST"])
@login_required
def apu_new():
    apu = APU()
    _set_apu_defaults(apu)
    if request.method == "POST":
        _guardar_apu_desde_form(apu)
        db.session.add(apu)
        db.session.commit()
        flash("APU creado correctamente.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))
    return _render_apu_form(apu=apu, title="Nuevo APU")


@apu_bp.route("/<int:apu_id>/editar", methods=["GET", "POST"])
@login_required
def apu_edit(apu_id):
    apu = APU.query.get_or_404(apu_id)
    _set_apu_defaults(apu)
    if request.method == "POST":
        _guardar_apu_desde_form(apu)
        db.session.commit()
        flash("APU actualizado.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))
    return _render_apu_form(apu=apu, title="Editar APU")


@apu_bp.route("/<int:apu_id>/eliminar", methods=["POST"])
@login_required
def apu_delete(apu_id):
    apu = APU.query.get_or_404(apu_id)
    db.session.delete(apu)
    db.session.commit()
    flash("APU eliminado.", "success")
    return redirect(url_for("apu.apu_list"))


@apu_bp.route("/<int:apu_id>/duplicar", methods=["POST"])
@login_required
def apu_duplica(apu_id):
    apu = APU.query.get_or_404(apu_id)
    nuevo = APU(
        clave=f"{apu.clave}-COPIA" if apu.clave else None,
        concepto=f"{apu.concepto} (Copia)",
        descripcion=apu.descripcion,
        categoria=apu.categoria,
        unidad=apu.unidad,
        cantidad_objetivo=apu.cantidad_objetivo,
        rendimiento_base=apu.rendimiento_base,
        jornada_horas=apu.jornada_horas,
        desperdicio_general_pct=apu.desperdicio_general_pct,
        herramienta_menor_pct=apu.herramienta_menor_pct,
        indirecto_pct=apu.indirecto_pct,
        utilidad_pct=apu.utilidad_pct,
        financiamiento_pct=apu.financiamiento_pct,
        cargos_adicionales_pct=apu.cargos_adicionales_pct,
        notas=apu.notas,
    )
    db.session.add(nuevo)
    db.session.flush()

    for detalle in apu.detalles:
        db.session.add(
            APUDetalle(
                apu_id=nuevo.id,
                tipo_insumo=detalle.tipo_insumo,
                referencia_id=detalle.referencia_id,
                descripcion=detalle.descripcion,
                codigo=detalle.codigo,
                categoria=detalle.categoria,
                unidad=detalle.unidad,
                cantidad=detalle.cantidad,
                factor=detalle.factor,
                cuadrilla=detalle.cuadrilla,
                rendimiento=detalle.rendimiento,
                desperdicio_pct=detalle.desperdicio_pct,
                comentario=detalle.comentario,
                precio_unitario=detalle.precio_unitario,
                subtotal=detalle.subtotal,
            )
        )

    recalcular_apu(nuevo)
    db.session.commit()
    flash("APU duplicado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=nuevo.id))


@apu_bp.route("/<int:apu_id>/a-catalogo", methods=["POST"])
@login_required
def apu_to_catalogo(apu_id):
    apu = APU.query.get_or_404(apu_id)
    recalcular_apu(apu)

    filtro = db.func.lower(Concepto.nombre_concepto) == apu.concepto.lower()
    if apu.clave and hasattr(Concepto, "clave"):
        filtro = or_(filtro, db.func.lower(Concepto.clave) == apu.clave.lower())

    concepto = Concepto.query.filter(filtro).first()
    descripcion = apu.descripcion or f"Generado desde MAR DATA {apu.clave or apu.id}"

    if concepto:
        concepto.nombre_concepto = apu.concepto
        concepto.unidad = apu.unidad
        concepto.precio_unitario = apu.precio_unitario
        concepto.sistema = "MAR DATA"
        concepto.descripcion = descripcion
        if apu.clave and hasattr(concepto, "clave"):
            concepto.clave = apu.clave
    else:
        kwargs = dict(
            nombre_concepto=apu.concepto,
            unidad=apu.unidad,
            precio_unitario=apu.precio_unitario,
            sistema="MAR DATA",
            descripcion=descripcion,
        )
        if apu.clave and hasattr(Concepto, "clave"):
            kwargs["clave"] = apu.clave
        concepto = Concepto(**kwargs)
        db.session.add(concepto)

    db.session.commit()
    flash("APU enviado al catalogo de conceptos.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))


@apu_bp.route("/<int:apu_id>/detalle/agregar", methods=["POST"])
@login_required
def apu_detalle_add(apu_id):
    apu = APU.query.get_or_404(apu_id)
    tipo_insumo = (request.form.get("tipo_insumo") or "").strip()
    referencia_id = request.form.get(f"referencia_id_{tipo_insumo}", type=int)
    recurso = _buscar_recurso(tipo_insumo, referencia_id)

    if not recurso:
        flash("No se encontro el insumo seleccionado.", "danger")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    detalle = APUDetalle(
        apu_id=apu.id,
        tipo_insumo=tipo_insumo,
        referencia_id=recurso.id,
        descripcion=recurso.nombre,
        codigo=getattr(recurso, "clave", None),
        categoria=getattr(recurso, "categoria", None),
        unidad=recurso.unidad,
        cantidad=_f(request.form.get("cantidad"), 1.0),
        factor=_f(request.form.get("factor"), 1.0),
        cuadrilla=_f(request.form.get("cuadrilla"), 1.0),
        rendimiento=_f(request.form.get("rendimiento"), 0.0),
        desperdicio_pct=_f(request.form.get("desperdicio_pct"), apu.desperdicio_general_pct or 0.0),
        comentario=_s(request.form.get("comentario")),
        precio_unitario=recurso.precio_unitario,
    )
    db.session.add(detalle)
    recalcular_apu(apu)
    db.session.commit()
    flash("Insumo agregado al APU.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))


@apu_bp.route("/<int:apu_id>/detalle/manual", methods=["POST"])
@login_required
def apu_detalle_manual(apu_id):
    apu = APU.query.get_or_404(apu_id)
    tipo_insumo = (request.form.get("tipo_insumo_manual") or "material").strip()
    descripcion = (request.form.get("descripcion_manual") or "").strip()
    if not descripcion:
        flash("La descripcion del renglon manual es obligatoria.", "danger")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    detalle = APUDetalle(
        apu_id=apu.id,
        tipo_insumo=tipo_insumo,
        descripcion=descripcion,
        codigo=_s(request.form.get("codigo_manual")),
        categoria=_s(request.form.get("categoria_manual")),
        unidad=(request.form.get("unidad_manual") or "pza").strip() or "pza",
        cantidad=_f(request.form.get("cantidad_manual"), 1.0),
        factor=_f(request.form.get("factor_manual"), 1.0),
        cuadrilla=_f(request.form.get("cuadrilla_manual"), 1.0),
        rendimiento=_f(request.form.get("rendimiento_manual"), 0.0),
        desperdicio_pct=_f(request.form.get("desperdicio_manual"), apu.desperdicio_general_pct or 0.0),
        comentario=_s(request.form.get("comentario_manual")),
        precio_unitario=_f(request.form.get("precio_unitario_manual")),
    )
    db.session.add(detalle)
    recalcular_apu(apu)
    db.session.commit()
    flash("Renglon manual agregado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))


@apu_bp.route("/detalle/<int:detalle_id>/actualizar", methods=["POST"])
@login_required
def apu_detalle_update(detalle_id):
    detalle = APUDetalle.query.get_or_404(detalle_id)
    detalle.descripcion = (request.form.get("descripcion") or detalle.descripcion).strip()
    detalle.codigo = _s(request.form.get("codigo"))
    detalle.categoria = _s(request.form.get("categoria"))
    detalle.unidad = (request.form.get("unidad") or detalle.unidad or "pza").strip() or "pza"
    detalle.cantidad = _f(request.form.get("cantidad"), detalle.cantidad)
    detalle.factor = _f(request.form.get("factor"), detalle.factor or 1.0)
    detalle.cuadrilla = _f(request.form.get("cuadrilla"), detalle.cuadrilla or 1.0)
    detalle.rendimiento = _f(request.form.get("rendimiento"), detalle.rendimiento or 0.0)
    detalle.desperdicio_pct = _f(request.form.get("desperdicio_pct"), detalle.desperdicio_pct or 0.0)
    detalle.precio_unitario = _f(request.form.get("precio_unitario"), detalle.precio_unitario)
    detalle.comentario = _s(request.form.get("comentario"))
    recalcular_apu(detalle.apu)
    db.session.commit()
    flash("Renglon actualizado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=detalle.apu_id))


@apu_bp.route("/detalle/<int:detalle_id>/eliminar", methods=["POST"])
@login_required
def apu_detalle_delete(detalle_id):
    detalle = APUDetalle.query.get_or_404(detalle_id)
    apu_id = detalle.apu_id
    apu = detalle.apu
    db.session.delete(detalle)
    recalcular_apu(apu)
    db.session.commit()
    flash("Renglon eliminado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu_id))


@apu_bp.route("/materiales")
@login_required
def materiales_list():
    return _resource_list(Material, "neodata/materiales_list.html", "Materiales")


@apu_bp.route("/materiales/nuevo", methods=["GET", "POST"])
@login_required
def materiales_new():
    item = Material(unidad="kg")
    if request.method == "POST":
        _save_resource(item, include_provider=True)
        db.session.add(item)
        db.session.commit()
        flash("Material creado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template("neodata/recurso_form.html", title="Nuevo material", item=item, back=url_for("apu.materiales_list"), include_provider=True)


@apu_bp.route("/materiales/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def materiales_edit(item_id):
    item = Material.query.get_or_404(item_id)
    if request.method == "POST":
        _save_resource(item, include_provider=True)
        db.session.commit()
        flash("Material actualizado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template("neodata/recurso_form.html", title="Editar material", item=item, back=url_for("apu.materiales_list"), include_provider=True)


@apu_bp.route("/materiales/<int:item_id>/eliminar", methods=["POST"])
@login_required
def materiales_delete(item_id):
    item = Material.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Material eliminado.", "success")
    return redirect(url_for("apu.materiales_list"))


@apu_bp.route("/mano-obra")
@login_required
def mano_obra_list():
    return _resource_list(ManoObra, "neodata/mano_obra_list.html", "Mano de obra")


@apu_bp.route("/mano-obra/nuevo", methods=["GET", "POST"])
@login_required
def mano_obra_new():
    item = ManoObra(unidad="jor")
    if request.method == "POST":
        _save_resource(item)
        db.session.add(item)
        db.session.commit()
        flash("Recurso de mano de obra creado.", "success")
        return redirect(url_for("apu.mano_obra_list"))
    return render_template("neodata/recurso_form.html", title="Nueva mano de obra", item=item, back=url_for("apu.mano_obra_list"), include_provider=False)


@apu_bp.route("/mano-obra/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def mano_obra_edit(item_id):
    item = ManoObra.query.get_or_404(item_id)
    if request.method == "POST":
        _save_resource(item)
        db.session.commit()
        flash("Recurso actualizado.", "success")
        return redirect(url_for("apu.mano_obra_list"))
    return render_template("neodata/recurso_form.html", title="Editar mano de obra", item=item, back=url_for("apu.mano_obra_list"), include_provider=False)


@apu_bp.route("/mano-obra/<int:item_id>/eliminar", methods=["POST"])
@login_required
def mano_obra_delete(item_id):
    item = ManoObra.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Recurso eliminado.", "success")
    return redirect(url_for("apu.mano_obra_list"))


@apu_bp.route("/maquinaria")
@login_required
def maquinaria_list():
    return _resource_list(Maquinaria, "neodata/maquinaria_list.html", "Maquinaria")


@apu_bp.route("/maquinaria/nuevo", methods=["GET", "POST"])
@login_required
def maquinaria_new():
    item = Maquinaria(unidad="hr")
    if request.method == "POST":
        _save_resource(item)
        db.session.add(item)
        db.session.commit()
        flash("Maquinaria creada.", "success")
        return redirect(url_for("apu.maquinaria_list"))
    return render_template("neodata/recurso_form.html", title="Nueva maquinaria", item=item, back=url_for("apu.maquinaria_list"), include_provider=False)


@apu_bp.route("/maquinaria/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def maquinaria_edit(item_id):
    item = Maquinaria.query.get_or_404(item_id)
    if request.method == "POST":
        _save_resource(item)
        db.session.commit()
        flash("Maquinaria actualizada.", "success")
        return redirect(url_for("apu.maquinaria_list"))
    return render_template("neodata/recurso_form.html", title="Editar maquinaria", item=item, back=url_for("apu.maquinaria_list"), include_provider=False)


@apu_bp.route("/maquinaria/<int:item_id>/eliminar", methods=["POST"])
@login_required
def maquinaria_delete(item_id):
    item = Maquinaria.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Maquinaria eliminada.", "success")
    return redirect(url_for("apu.maquinaria_list"))


@apu_bp.route("/plantillas")
@login_required
def plantillas():
    return render_template("neodata/plantillas.html", items=_load_plantillas(), title="Plantillas")


@apu_bp.route("/generador", methods=["GET", "POST"])
@login_required
def generador():
    plantillas_data = _load_plantillas()
    if request.method == "POST":
        plantilla_nombre = (request.form.get("plantilla") or "").strip()
        espesor_mm = _f(request.form.get("espesor_mm"), 1.0)
        rendimiento = _f(request.form.get("rendimiento"), 1.0) or 1.0
        plantilla = next((p for p in plantillas_data if p.get("nombre") == plantilla_nombre), None)

        if not plantilla:
            flash("No se encontro la plantilla seleccionada.", "danger")
            return redirect(url_for("apu.generador"))

        apu = APU(
            clave=plantilla.get("clave"),
            concepto=f"{plantilla.get('nombre')} {espesor_mm:g} mm",
            descripcion=plantilla.get("descripcion"),
            categoria=plantilla.get("categoria"),
            unidad=plantilla.get("unidad") or "m2",
            cantidad_objetivo=1.0,
            rendimiento_base=rendimiento,
            jornada_horas=8.0,
            herramienta_menor_pct=_f(plantilla.get("herramienta_menor"), 3.0),
            indirecto_pct=_f(plantilla.get("indirecto"), 12.0),
            utilidad_pct=_f(plantilla.get("utilidad"), 10.0),
            financiamiento_pct=_f(plantilla.get("financiamiento"), 2.5),
            cargos_adicionales_pct=_f(plantilla.get("cargos"), 0.0),
        )
        db.session.add(apu)
        db.session.flush()

        for componente in plantilla.get("componentes", []):
            tipo = componente.get("tipo")
            model = _resource_for_tipo(tipo)
            if not model:
                continue

            buscar = (componente.get("buscar") or "").strip()
            recurso = model.query.filter(model.nombre.ilike(f"%{buscar}%")).order_by(model.nombre.asc()).first()
            if not recurso:
                continue

            cantidad = _f(componente.get("consumo_fijo"))
            if not cantidad:
                cantidad = _f(componente.get("factor_por_mm")) * espesor_mm

            db.session.add(
                APUDetalle(
                    apu_id=apu.id,
                    tipo_insumo=tipo,
                    referencia_id=recurso.id,
                    descripcion=recurso.nombre,
                    codigo=getattr(recurso, "clave", None),
                    categoria=getattr(recurso, "categoria", None),
                    unidad=recurso.unidad,
                    cantidad=cantidad,
                    factor=1.0,
                    cuadrilla=_f(componente.get("cuadrilla"), 1.0),
                    rendimiento=rendimiento if tipo in {"mano_obra", "maquinaria"} else 0.0,
                    desperdicio_pct=_f(componente.get("desperdicio_pct"), 0.0),
                    comentario=_s(componente.get("comentario")),
                    precio_unitario=recurso.precio_unitario,
                )
            )

        recalcular_apu(apu)
        db.session.commit()
        flash("APU generado desde plantilla.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    return render_template("neodata/generador.html", plantillas=plantillas_data, title="Generador automatico")


@apu_bp.route("/api/suggest")
@login_required
def api_apu_suggest():
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify([])
    filtros = [APU.concepto.ilike(f"%{q}%"), APU.clave.ilike(f"%{q}%")]
    if q.isdigit():
        filtros.append(APU.id == int(q))

    rows = APU.query.filter(or_(*filtros)).order_by(APU.concepto.asc()).limit(15).all()
    return jsonify(
        [
            {
                "id": item.id,
                "concepto": item.concepto,
                "unidad": item.unidad,
                "precio_unitario": item.precio_unitario,
                "clave": item.clave or "",
                "categoria": item.categoria or "",
            }
            for item in rows
        ]
    )


@apu_bp.route("/api/<int:apu_id>/resumen")
@login_required
def api_apu_resumen(apu_id):
    apu = APU.query.get_or_404(apu_id)
    recalcular_apu(apu)
    db.session.commit()
    return jsonify(
        {
            "id": apu.id,
            "clave": apu.clave,
            "concepto": apu.concepto,
            "categoria": apu.categoria,
            "unidad": apu.unidad,
            "precio_unitario": apu.precio_unitario,
            "costo_directo": apu.costo_directo,
            "indirecto_monto": apu.indirecto_monto,
            "financiamiento_monto": apu.financiamiento_monto,
            "utilidad_monto": apu.utilidad_monto,
            "cargos_adicionales_monto": apu.cargos_adicionales_monto,
            "importe_partida": apu.importe_partida,
            "descripcion": apu.descripcion,
        }
    )


@apu_bp.route("/cotizador-rapido", methods=["GET", "POST"])
@login_required
def apu_cotizador_rapido():
    if request.method == "POST":
        apu_id = int(request.form.get("apu_id"))
        cantidad = _f(request.form.get("cantidad"), 1.0)
        nombre_cliente = (request.form.get("cliente") or "").strip()
        empresa = (request.form.get("empresa") or "").strip()
        correo = (request.form.get("correo") or "").strip() or None
        telefono = (request.form.get("telefono") or "").strip() or None
        direccion = (request.form.get("direccion") or "").strip() or None
        notas = (request.form.get("notas") or "").strip() or None

        apu = APU.query.get_or_404(apu_id)
        recalcular_apu(apu)
        db.session.commit()

        concepto = Concepto.query.filter_by(nombre_concepto=apu.concepto).first()
        if not concepto:
            concepto = Concepto(
                nombre_concepto=apu.concepto,
                unidad=apu.unidad,
                precio_unitario=apu.precio_unitario,
                sistema="MAR DATA",
                descripcion=apu.descripcion or f"Generado desde MAR DATA {apu.clave or apu.id}",
            )
            db.session.add(concepto)
            db.session.flush()

        cliente = None
        if nombre_cliente:
            cliente = Cliente.query.filter(db.func.lower(Cliente.nombre_cliente) == nombre_cliente.lower()).first()
            if not cliente:
                cliente = Cliente(
                    nombre_cliente=nombre_cliente,
                    empresa=empresa or None,
                    responsable=_responsable_actual(),
                    correo=correo,
                    telefono=telefono,
                    direccion=direccion,
                )
                db.session.add(cliente)
                db.session.flush()

        subtotal = float(cantidad) * float(apu.precio_unitario or 0)
        iva_porc = 16.0
        iva_monto = subtotal * 0.16
        total = subtotal + iva_monto

        cot = Cotizacion(
            folio=_generar_folio(),
            fecha=datetime.utcnow(),
            cliente_id=cliente.id if cliente else None,
            estatus="PENDIENTE",
            subtotal=round(subtotal, 2),
            descuento_total=0.0,
            iva_porc=iva_porc,
            iva_monto=round(iva_monto, 2),
            total=round(total, 2),
            notas=notas,
            responsable=_responsable_actual(),
        )
        db.session.add(cot)
        db.session.flush()

        det = CotizacionDetalle(
            cotizacion_id=cot.id,
            concepto_id=concepto.id,
            nombre_concepto=apu.concepto,
            unidad=apu.unidad,
            cantidad=float(cantidad),
            precio_unitario=float(apu.precio_unitario or 0),
            sistema="MAR DATA",
            descripcion=f"Creado desde APU {apu.clave or apu.id}",
            subtotal=round(subtotal, 2),
        )
        db.session.add(det)
        db.session.commit()

        flash("Cotizacion rapida generada desde MAR DATA.", "success")
        return redirect(url_for("view_cotizacion", cot_id=cot.id))

    return render_template("apu_cotizador_rapido.html", title="Cotizador rapido MAR DATA")
