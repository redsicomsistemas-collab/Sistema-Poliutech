
from flask import Blueprint, render_template, request, redirect, url_for, flash, abort
from flask_login import login_required
from models import db, Concepto
from .models import Material, ManoObra, Maquinaria, APU, APUDetalle
from .calc import recalcular_apu

apu_bp = Blueprint("apu", __name__, url_prefix="/apu", template_folder="templates")

def _f(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return default

# -------------------------------------------------
# DASHBOARD
# -------------------------------------------------
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
        title="APU / NEODATA PERSONAL"
    )

# -------------------------------------------------
# MATERIALES
# -------------------------------------------------
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
            unidad=(request.form.get("unidad") or "pza").strip(),
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
    return render_template("neodata/material_form.html", item=None, title="Nuevo material", tipo="material")

@apu_bp.route("/materiales/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def materiales_edit(item_id):
    item = Material.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or "pza").strip()
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        item.proveedor = (request.form.get("proveedor") or "").strip() or None
        db.session.commit()
        flash("Material actualizado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template("neodata/material_form.html", item=item, title="Editar material", tipo="material")

@apu_bp.route("/materiales/<int:item_id>/eliminar", methods=["POST"])
@login_required
def materiales_delete(item_id):
    item = Material.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Material eliminado.", "success")
    return redirect(url_for("apu.materiales_list"))

# -------------------------------------------------
# MANO DE OBRA
# -------------------------------------------------
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
    return render_template("neodata/recurso_form.html", item=None, title="Nueva mano de obra", tipo="mano_obra")

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
    return render_template("neodata/recurso_form.html", item=item, title="Editar mano de obra", tipo="mano_obra")

@apu_bp.route("/mano-obra/<int:item_id>/eliminar", methods=["POST"])
@login_required
def mano_obra_delete(item_id):
    item = ManoObra.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Mano de obra eliminada.", "success")
    return redirect(url_for("apu.mano_obra_list"))

# -------------------------------------------------
# MAQUINARIA
# -------------------------------------------------
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
    return render_template("neodata/recurso_form.html", item=None, title="Nueva maquinaria", tipo="maquinaria")

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
    return render_template("neodata/recurso_form.html", item=item, title="Editar maquinaria", tipo="maquinaria")

@apu_bp.route("/maquinaria/<int:item_id>/eliminar", methods=["POST"])
@login_required
def maquinaria_delete(item_id):
    item = Maquinaria.query.get_or_404(item_id)
    db.session.delete(item)
    db.session.commit()
    flash("Maquinaria eliminada.", "success")
    return redirect(url_for("apu.maquinaria_list"))

# -------------------------------------------------
# APU
# -------------------------------------------------
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
    return render_template("neodata/apu_form.html", apu=None, materiales=Material.query.all(), mano_obra=ManoObra.query.all(), maquinarias=Maquinaria.query.all(), title="Nuevo APU")

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

@apu_bp.route("/<int:apu_id>/detalle/agregar", methods=["POST"])
@login_required
def apu_detalle_add(apu_id):
    apu = APU.query.get_or_404(apu_id)
    tipo = (request.form.get("tipo_insumo") or "").strip()
    referencia_id = request.form.get("referencia_id")
    cantidad = _f(request.form.get("cantidad"), 1.0)

    if tipo not in ("material", "mano_obra", "maquinaria"):
        flash("Tipo de insumo inválido.", "danger")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    recurso = None
    if tipo == "material":
        recurso = Material.query.get_or_404(int(referencia_id))
    elif tipo == "mano_obra":
        recurso = ManoObra.query.get_or_404(int(referencia_id))
    elif tipo == "maquinaria":
        recurso = Maquinaria.query.get_or_404(int(referencia_id))

    d = APUDetalle(
        apu_id=apu.id,
        tipo_insumo=tipo,
        referencia_id=recurso.id,
        descripcion=recurso.nombre,
        unidad=recurso.unidad or "",
        cantidad=cantidad,
        precio_unitario=float(recurso.precio_unitario or 0),
    )
    db.session.add(d)
    db.session.flush()
    recalcular_apu(apu)
    db.session.commit()
    flash("Insumo agregado al APU.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))

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
    apu_id = d.apu_id
    apu = d.apu
    db.session.delete(d)
    db.session.flush()
    recalcular_apu(apu)
    db.session.commit()
    flash("Renglón eliminado.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu_id))

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
            descripcion=f"Generado desde APU {apu.clave or apu.id}",
        )
        db.session.add(concepto)
    else:
        concepto.unidad = apu.unidad
        concepto.precio_unitario = apu.precio_unitario
        if not concepto.descripcion:
            concepto.descripcion = f"Actualizado desde APU {apu.clave or apu.id}"

    db.session.commit()
    flash("APU enviado al catálogo de conceptos.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))
