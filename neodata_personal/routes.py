
import os
import json
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from models import db, Concepto
from .models import Material, ManoObra, Maquinaria, APU, APUDetalle
from .calc import recalcular_apu

apu_bp = Blueprint("apu", __name__, url_prefix="/apu", template_folder="templates")

BASE_DIR = os.path.dirname(__file__)
PLANTILLAS_FILE = os.path.join(BASE_DIR, "plantillas.json")

def _f(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return default

def _load_plantillas():
    try:
        with open(PLANTILLAS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _search_recurso(tipo, buscar):
    term = (buscar or "").strip().lower()
    if not term:
        return None
    if tipo == "material":
        return Material.query.filter(Material.nombre.ilike(f"%{term}%")).first()
    if tipo == "mano_obra":
        return ManoObra.query.filter(ManoObra.nombre.ilike(f"%{term}%")).first()
    if tipo == "maquinaria":
        return Maquinaria.query.filter(Maquinaria.nombre.ilike(f"%{term}%")).first()
    return None

def _crear_detalle_desde_recurso(apu, tipo, recurso, cantidad):
    if not recurso:
        return None
    d = APUDetalle(
        apu_id=apu.id,
        tipo_insumo=tipo,
        referencia_id=recurso.id,
        descripcion=recurso.nombre,
        unidad=recurso.unidad or "",
        cantidad=float(cantidad or 0),
        precio_unitario=float(recurso.precio_unitario or 0),
    )
    db.session.add(d)
    return d

@apu_bp.route("/")
@login_required
def index():
    return render_template(
        "neodata/apu_index.html",
        total_materiales=Material.query.count(),
        total_mano_obra=ManoObra.query.count(),
        total_maquinaria=Maquinaria.query.count(),
        total_apu=APU.query.count(),
        apus=APU.query.order_by(APU.actualizado_en.desc()).limit(20).all(),
        title="MAR DATA"
    )

@apu_bp.route("/materiales")
@login_required
def materiales_list():
    items = Material.query.order_by(Material.nombre.asc()).all()
    return render_template("neodata/materiales_list.html", items=items, title="Materiales")

@apu_bp.route("/materiales/nuevo", methods=["GET", "POST"])
@login_required
def materiales_new():
    if request.method == "POST":
        item = Material(
            nombre=(request.form.get("nombre") or "").strip(),
            unidad=(request.form.get("unidad") or "kg").strip(),
            precio_unitario=_f(request.form.get("precio_unitario")),
            proveedor=(request.form.get("proveedor") or "").strip() or None,
        )
        if not item.nombre:
            flash("El nombre es obligatorio.", "danger")
            return redirect(url_for("apu.materiales_new"))
        db.session.add(item)
        db.session.commit()
        flash("Material creado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template("neodata/material_form.html", item=None, title="Nuevo material")

@apu_bp.route("/materiales/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def materiales_edit(item_id):
    item = Material.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or "kg").strip()
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        item.proveedor = (request.form.get("proveedor") or "").strip() or None
        db.session.commit()
        flash("Material actualizado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template("neodata/material_form.html", item=item, title="Editar material")

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
    items = ManoObra.query.order_by(ManoObra.nombre.asc()).all()
    return render_template("neodata/mano_obra_list.html", items=items, title="Mano de obra")

@apu_bp.route("/mano-obra/nuevo", methods=["GET", "POST"])
@login_required
def mano_obra_new():
    if request.method == "POST":
        item = ManoObra(
            nombre=(request.form.get("nombre") or "").strip(),
            unidad=(request.form.get("unidad") or "jor").strip(),
            precio_unitario=_f(request.form.get("precio_unitario")),
        )
        if not item.nombre:
            flash("El nombre es obligatorio.", "danger")
            return redirect(url_for("apu.mano_obra_new"))
        db.session.add(item)
        db.session.commit()
        flash("Mano de obra creada.", "success")
        return redirect(url_for("apu.mano_obra_list"))
    return render_template("neodata/recurso_form.html", item=None, title="Nueva mano de obra", back=url_for("apu.mano_obra_list"))

@apu_bp.route("/mano-obra/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def mano_obra_edit(item_id):
    item = ManoObra.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or "jor").strip()
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        db.session.commit()
        flash("Mano de obra actualizada.", "success")
        return redirect(url_for("apu.mano_obra_list"))
    return render_template("neodata/recurso_form.html", item=item, title="Editar mano de obra", back=url_for("apu.mano_obra_list"))

@apu_bp.route("/mano-obra/<int:item_id>/eliminar", methods=["POST"])
@login_required
def mano_obra_delete(item_id):
    item = ManoObra.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Mano de obra eliminada.", "success")
    return redirect(url_for("apu.mano_obra_list"))

@apu_bp.route("/maquinaria")
@login_required
def maquinaria_list():
    items = Maquinaria.query.order_by(Maquinaria.nombre.asc()).all()
    return render_template("neodata/maquinaria_list.html", items=items, title="Maquinaria")

@apu_bp.route("/maquinaria/nuevo", methods=["GET", "POST"])
@login_required
def maquinaria_new():
    if request.method == "POST":
        item = Maquinaria(
            nombre=(request.form.get("nombre") or "").strip(),
            unidad=(request.form.get("unidad") or "hr").strip(),
            precio_unitario=_f(request.form.get("precio_unitario")),
        )
        if not item.nombre:
            flash("El nombre es obligatorio.", "danger")
            return redirect(url_for("apu.maquinaria_new"))
        db.session.add(item)
        db.session.commit()
        flash("Maquinaria creada.", "success")
        return redirect(url_for("apu.maquinaria_list"))
    return render_template("neodata/recurso_form.html", item=None, title="Nueva maquinaria", back=url_for("apu.maquinaria_list"))

@apu_bp.route("/maquinaria/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def maquinaria_edit(item_id):
    item = Maquinaria.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or "hr").strip()
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        db.session.commit()
        flash("Maquinaria actualizada.", "success")
        return redirect(url_for("apu.maquinaria_list"))
    return render_template("neodata/recurso_form.html", item=item, title="Editar maquinaria", back=url_for("apu.maquinaria_list"))

@apu_bp.route("/maquinaria/<int:item_id>/eliminar", methods=["POST"])
@login_required
def maquinaria_delete(item_id):
    item = Maquinaria.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Maquinaria eliminada.", "success")
    return redirect(url_for("apu.maquinaria_list"))

@apu_bp.route("/lista")
@login_required
def apu_list():
    items = APU.query.order_by(APU.actualizado_en.desc()).all()
    return render_template("neodata/apu_list.html", items=items, title="APU")

@apu_bp.route("/nuevo", methods=["GET", "POST"])
@login_required
def apu_new():
    if request.method == "POST":
        apu = APU(
            clave=(request.form.get("clave") or "").strip() or None,
            concepto=(request.form.get("concepto") or "").strip(),
            unidad=(request.form.get("unidad") or "m2").strip(),
            indirecto_pct=_f(request.form.get("indirecto_pct")),
            utilidad_pct=_f(request.form.get("utilidad_pct")),
            financiamiento_pct=_f(request.form.get("financiamiento_pct")),
            cargos_adicionales_pct=_f(request.form.get("cargos_adicionales_pct")),
        )
        if not apu.concepto:
            flash("El concepto es obligatorio.", "danger")
            return redirect(url_for("apu.apu_new"))
        db.session.add(apu)
        db.session.commit()
        flash("APU creado. Ahora agrega los insumos.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))
    return render_template(
        "neodata/apu_form.html",
        apu=None,
        materiales=Material.query.order_by(Material.nombre.asc()).all(),
        mano_obra=ManoObra.query.order_by(ManoObra.nombre.asc()).all(),
        maquinarias=Maquinaria.query.order_by(Maquinaria.nombre.asc()).all(),
        title="Nuevo APU"
    )

@apu_bp.route("/<int:apu_id>/editar", methods=["GET", "POST"])
@login_required
def apu_edit(apu_id):
    apu = APU.query.get_or_404(apu_id)
    if request.method == "POST":
        apu.clave = (request.form.get("clave") or "").strip() or None
        apu.concepto = (request.form.get("concepto") or "").strip()
        apu.unidad = (request.form.get("unidad") or "m2").strip()
        apu.indirecto_pct = _f(request.form.get("indirecto_pct"))
        apu.utilidad_pct = _f(request.form.get("utilidad_pct"))
        apu.financiamiento_pct = _f(request.form.get("financiamiento_pct"))
        apu.cargos_adicionales_pct = _f(request.form.get("cargos_adicionales_pct"))
        recalcular_apu(apu)
        db.session.commit()
        flash("Encabezado APU actualizado.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    recalcular_apu(apu)
    db.session.commit()
    return render_template(
        "neodata/apu_form.html",
        apu=apu,
        materiales=Material.query.order_by(Material.nombre.asc()).all(),
        mano_obra=ManoObra.query.order_by(ManoObra.nombre.asc()).all(),
        maquinarias=Maquinaria.query.order_by(Maquinaria.nombre.asc()).all(),
        title=f"Editar APU {apu.concepto}"
    )

@apu_bp.route("/<int:apu_id>/eliminar", methods=["POST"])
@login_required
def apu_delete(apu_id):
    apu = APU.query.get_or_404(apu_id)
    db.session.delete(apu)
    db.session.commit()
    flash("APU eliminado.", "success")
    return redirect(url_for("apu.apu_list"))

@apu_bp.route("/detalle/<int:detalle_id>/actualizar", methods=["POST"])
@login_required
def apu_detalle_update(detalle_id):
    d = APUDetalle.query.get_or_404(detalle_id)
    apu = d.apu
    d.cantidad = _f(request.form.get("cantidad"), d.cantidad)
    d.precio_unitario = _f(request.form.get("precio_unitario"), d.precio_unitario)
    recalcular_apu(apu)
    db.session.commit()
    flash("Renglón actualizado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))

@apu_bp.route("/detalle/<int:detalle_id>/eliminar", methods=["POST"])
@login_required
def apu_detalle_delete(detalle_id):
    d = APUDetalle.query.get_or_404(detalle_id)
    apu = d.apu
    apu_id = d.apu_id
    db.session.delete(d)
    db.session.flush()
    recalcular_apu(apu)
    db.session.commit()
    flash("Renglón eliminado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu_id))

@apu_bp.route("/<int:apu_id>/detalle/agregar", methods=["POST"])
@login_required
def apu_detalle_add(apu_id):
    apu = APU.query.get_or_404(apu_id)
    tipo = (request.form.get("tipo_insumo") or "").strip()
    referencia_id = request.form.get("referencia_id")
    cantidad = _f(request.form.get("cantidad"), 1.0)

    recurso = None
    if tipo == "material":
        recurso = Material.query.get_or_404(int(referencia_id))
    elif tipo == "mano_obra":
        recurso = ManoObra.query.get_or_404(int(referencia_id))
    elif tipo == "maquinaria":
        recurso = Maquinaria.query.get_or_404(int(referencia_id))
    else:
        flash("Tipo de insumo inválido.", "danger")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    _crear_detalle_desde_recurso(apu, tipo, recurso, cantidad)
    db.session.flush()
    recalcular_apu(apu)
    db.session.commit()
    flash("Insumo agregado al APU.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))

@apu_bp.route("/<int:apu_id>/duplicar", methods=["POST"])
@login_required
def apu_duplica(apu_id):
    apu = APU.query.get_or_404(apu_id)
    nuevo = APU(
        clave=None,
        concepto=f"{apu.concepto} (copia)",
        unidad=apu.unidad,
        indirecto_pct=apu.indirecto_pct,
        utilidad_pct=apu.utilidad_pct,
        financiamiento_pct=apu.financiamiento_pct,
        cargos_adicionales_pct=apu.cargos_adicionales_pct,
    )
    db.session.add(nuevo)
    db.session.flush()

    for d in apu.detalles:
        copia = APUDetalle(
            apu_id=nuevo.id,
            tipo_insumo=d.tipo_insumo,
            referencia_id=d.referencia_id,
            descripcion=d.descripcion,
            unidad=d.unidad,
            cantidad=d.cantidad,
            precio_unitario=d.precio_unitario,
            subtotal=d.subtotal,
        )
        db.session.add(copia)

    db.session.flush()
    recalcular_apu(nuevo)
    db.session.commit()
    flash("APU duplicado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=nuevo.id))

@apu_bp.route("/<int:apu_id>/mandar-a-catalogo", methods=["POST"])
@login_required
def apu_to_catalogo(apu_id):
    apu = APU.query.get_or_404(apu_id)
    recalcular_apu(apu)

    concepto = Concepto.query.filter_by(nombre_concepto=apu.concepto).first()
    if not concepto:
        concepto = Concepto(
            nombre_concepto=apu.concepto,
            unidad=apu.unidad,
            precio_unitario=apu.precio_unitario,
            sistema="MAR DATA",
            descripcion=f"Generado desde MAR DATA {apu.clave or apu.id}",
        )
        db.session.add(concepto)
    else:
        concepto.unidad = apu.unidad
        concepto.precio_unitario = apu.precio_unitario
        concepto.sistema = "MAR DATA"
        if not concepto.descripcion:
            concepto.descripcion = f"Actualizado desde MAR DATA {apu.clave or apu.id}"

    db.session.commit()
    flash("APU enviado al catálogo de conceptos.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))

@apu_bp.route("/plantillas")
@login_required
def plantillas():
    items = _load_plantillas()
    return render_template("neodata/plantillas.html", items=items, title="Plantillas MAR DATA")

@apu_bp.route("/generador", methods=["GET", "POST"])
@login_required
def generador():
    plantillas = _load_plantillas()

    if request.method == "POST":
        plantilla_nombre = (request.form.get("plantilla") or "").strip()
        espesor_mm = _f(request.form.get("espesor_mm"), 1.0)
        rendimiento = _f(request.form.get("rendimiento"), 1.0)

        plantilla = next((p for p in plantillas if p.get("nombre") == plantilla_nombre), None)
        if not plantilla:
            flash("Plantilla no encontrada.", "danger")
            return redirect(url_for("apu.generador"))

        if rendimiento <= 0:
            rendimiento = 1.0

        apu = APU(
            clave=plantilla.get("clave"),
            concepto=f"{plantilla.get('nombre')} {espesor_mm:g} mm",
            unidad=plantilla.get("unidad", "m2"),
            indirecto_pct=float(plantilla.get("indirecto", 0)),
            utilidad_pct=float(plantilla.get("utilidad", 0)),
            financiamiento_pct=float(plantilla.get("financiamiento", 0)),
            cargos_adicionales_pct=float(plantilla.get("cargos", 0)),
        )
        db.session.add(apu)
        db.session.flush()

        for comp in plantilla.get("componentes", []):
            tipo = comp.get("tipo")
            recurso = _search_recurso(tipo, comp.get("buscar"))
            if not recurso:
                continue

            consumo_fijo = comp.get("consumo_fijo")
            factor_por_mm = comp.get("factor_por_mm")

            if consumo_fijo is not None:
                cantidad = float(consumo_fijo) / rendimiento
            elif factor_por_mm is not None:
                cantidad = (float(factor_por_mm) * float(espesor_mm)) / rendimiento
            else:
                cantidad = 0.0

            _crear_detalle_desde_recurso(apu, tipo, recurso, cantidad)

        db.session.flush()
        recalcular_apu(apu)
        db.session.commit()
        flash("APU generado automáticamente desde plantilla.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    return render_template("neodata/generador.html", plantillas=plantillas, title="Generador automático MAR DATA")
