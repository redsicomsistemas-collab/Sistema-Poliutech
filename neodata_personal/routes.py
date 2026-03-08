import os
import json
import re
from datetime import datetime

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from sqlalchemy import or_

from models import db, Concepto, Cliente, Cotizacion, CotizacionDetalle
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
    rows = db.session.execute(db.text("SELECT folio FROM cotizacion WHERE folio LIKE 'PTCH-%'")).fetchall()
    for (folio,) in rows:
        m = re.match(r"PTCH-(\d{4})$", str(folio))
        if m:
            maxn = max(maxn, int(m.group(1)))
    return f"{prefix}{maxn+1:04d}"


def _resource_for_tipo(tipo_insumo):
    mapping = {
        "material": Material,
        "mano_obra": ManoObra,
        "maquinaria": Maquinaria,
    }
    return mapping.get(tipo_insumo)


def _buscar_recurso(tipo_insumo, referencia_id):
    model = _resource_for_tipo(tipo_insumo)
    if not model:
        return None
    return model.query.get(referencia_id)


def _guardar_apu_desde_form(apu):
    apu.clave = (request.form.get("clave") or "").strip() or None
    apu.concepto = (request.form.get("concepto") or "").strip()
    apu.unidad = (request.form.get("unidad") or "m2").strip() or "m2"
    apu.indirecto_pct = _f(request.form.get("indirecto_pct"))
    apu.utilidad_pct = _f(request.form.get("utilidad_pct"))
    apu.financiamiento_pct = _f(request.form.get("financiamiento_pct"))
    apu.cargos_adicionales_pct = _f(request.form.get("cargos_adicionales_pct"))
    recalcular_apu(apu)


def _render_apu_form(apu=None, title="APU"):
    if apu:
        recalcular_apu(apu)
        db.session.flush()
    return render_template(
        "neodata/apu_form.html",
        title=title,
        apu=apu,
        materiales=Material.query.order_by(Material.nombre.asc()).all(),
        mano_obra=ManoObra.query.order_by(ManoObra.nombre.asc()).all(),
        maquinarias=Maquinaria.query.order_by(Maquinaria.nombre.asc()).all(),
    )


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
        title="MAR DATA",
    )


@apu_bp.route("/lista")
@login_required
def apu_list():
    items = APU.query.order_by(APU.actualizado_en.desc(), APU.id.desc()).all()
    return render_template("neodata/apu_list.html", items=items, title="Lista de APU")


@apu_bp.route("/nuevo", methods=["GET", "POST"])
@login_required
def apu_new():
    if request.method == "POST":
        apu = APU()
        _guardar_apu_desde_form(apu)
        db.session.add(apu)
        db.session.commit()
        flash("APU creado correctamente.", "success")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))
    return _render_apu_form(title="Nuevo APU")


@apu_bp.route("/<int:apu_id>/editar", methods=["GET", "POST"])
@login_required
def apu_edit(apu_id):
    apu = APU.query.get_or_404(apu_id)
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
        unidad=apu.unidad,
        indirecto_pct=apu.indirecto_pct,
        utilidad_pct=apu.utilidad_pct,
        financiamiento_pct=apu.financiamiento_pct,
        cargos_adicionales_pct=apu.cargos_adicionales_pct,
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
                unidad=detalle.unidad,
                cantidad=detalle.cantidad,
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
        filtro = db.or_(filtro, db.func.lower(Concepto.clave) == apu.clave.lower())

    concepto = Concepto.query.filter(filtro).first()

    if concepto:
        concepto.nombre_concepto = apu.concepto
        concepto.unidad = apu.unidad
        concepto.precio_unitario = apu.precio_unitario
        concepto.sistema = "MAR DATA"
        concepto.descripcion = f"Generado desde MAR DATA {apu.clave or apu.id}"
        if apu.clave and hasattr(concepto, "clave"):
            concepto.clave = apu.clave
    else:
        kwargs = dict(
            nombre_concepto=apu.concepto,
            unidad=apu.unidad,
            precio_unitario=apu.precio_unitario,
            sistema="MAR DATA",
            descripcion=f"Generado desde MAR DATA {apu.clave or apu.id}",
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
    referencia_id = request.form.get("referencia_id", type=int)
    cantidad = _f(request.form.get("cantidad"), 1.0)
    recurso = _buscar_recurso(tipo_insumo, referencia_id)

    if not recurso:
        flash("No se encontro el insumo seleccionado.", "danger")
        return redirect(url_for("apu.apu_edit", apu_id=apu.id))

    detalle = APUDetalle(
        apu_id=apu.id,
        tipo_insumo=tipo_insumo,
        referencia_id=recurso.id,
        descripcion=recurso.nombre,
        unidad=recurso.unidad,
        cantidad=cantidad,
        precio_unitario=recurso.precio_unitario,
    )
    db.session.add(detalle)
    recalcular_apu(apu)
    db.session.commit()
    flash("Insumo agregado al APU.", "success")
    return redirect(url_for("apu.apu_edit", apu_id=apu.id))


@apu_bp.route("/detalle/<int:detalle_id>/actualizar", methods=["POST"])
@login_required
def apu_detalle_update(detalle_id):
    detalle = APUDetalle.query.get_or_404(detalle_id)
    detalle.cantidad = _f(request.form.get("cantidad"), detalle.cantidad)
    detalle.precio_unitario = _f(request.form.get("precio_unitario"), detalle.precio_unitario)
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
    items = Material.query.order_by(Material.nombre.asc()).all()
    return render_template("neodata/materiales_list.html", items=items, title="Materiales")


@apu_bp.route("/materiales/nuevo", methods=["GET", "POST"])
@login_required
def materiales_new():
    if request.method == "POST":
        item = Material(
            nombre=(request.form.get("nombre") or "").strip(),
            unidad=(request.form.get("unidad") or "kg").strip() or "kg",
            precio_unitario=_f(request.form.get("precio_unitario")),
            proveedor=(request.form.get("proveedor") or "").strip() or None,
        )
        db.session.add(item)
        db.session.commit()
        flash("Material creado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template(
        "neodata/recurso_form.html",
        title="Nuevo material",
        item=None,
        back=url_for("apu.materiales_list"),
    )


@apu_bp.route("/materiales/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def materiales_edit(item_id):
    item = Material.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or item.unidad or "kg").strip() or "kg"
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        item.proveedor = (request.form.get("proveedor") or "").strip() or None
        db.session.commit()
        flash("Material actualizado.", "success")
        return redirect(url_for("apu.materiales_list"))
    return render_template(
        "neodata/recurso_form.html",
        title="Editar material",
        item=item,
        back=url_for("apu.materiales_list"),
    )


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
            unidad=(request.form.get("unidad") or "jor").strip() or "jor",
            precio_unitario=_f(request.form.get("precio_unitario")),
        )
        db.session.add(item)
        db.session.commit()
        flash("Recurso de mano de obra creado.", "success")
        return redirect(url_for("apu.mano_obra_list"))
    return render_template(
        "neodata/recurso_form.html",
        title="Nueva mano de obra",
        item=None,
        back=url_for("apu.mano_obra_list"),
    )


@apu_bp.route("/mano-obra/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def mano_obra_edit(item_id):
    item = ManoObra.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or item.unidad or "jor").strip() or "jor"
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        db.session.commit()
        flash("Recurso actualizado.", "success")
        return redirect(url_for("apu.mano_obra_list"))
    return render_template(
        "neodata/recurso_form.html",
        title="Editar mano de obra",
        item=item,
        back=url_for("apu.mano_obra_list"),
    )


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
    items = Maquinaria.query.order_by(Maquinaria.nombre.asc()).all()
    return render_template("neodata/maquinaria_list.html", items=items, title="Maquinaria")


@apu_bp.route("/maquinaria/nuevo", methods=["GET", "POST"])
@login_required
def maquinaria_new():
    if request.method == "POST":
        item = Maquinaria(
            nombre=(request.form.get("nombre") or "").strip(),
            unidad=(request.form.get("unidad") or "hr").strip() or "hr",
            precio_unitario=_f(request.form.get("precio_unitario")),
        )
        db.session.add(item)
        db.session.commit()
        flash("Maquinaria creada.", "success")
        return redirect(url_for("apu.maquinaria_list"))
    return render_template(
        "neodata/recurso_form.html",
        title="Nueva maquinaria",
        item=None,
        back=url_for("apu.maquinaria_list"),
    )


@apu_bp.route("/maquinaria/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def maquinaria_edit(item_id):
    item = Maquinaria.query.get_or_404(item_id)
    if request.method == "POST":
        item.nombre = (request.form.get("nombre") or "").strip()
        item.unidad = (request.form.get("unidad") or item.unidad or "hr").strip() or "hr"
        item.precio_unitario = _f(request.form.get("precio_unitario"))
        db.session.commit()
        flash("Maquinaria actualizada.", "success")
        return redirect(url_for("apu.maquinaria_list"))
    return render_template(
        "neodata/recurso_form.html",
        title="Editar maquinaria",
        item=item,
        back=url_for("apu.maquinaria_list"),
    )


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
            unidad=plantilla.get("unidad") or "m2",
            indirecto_pct=_f(plantilla.get("indirecto")),
            utilidad_pct=_f(plantilla.get("utilidad")),
            financiamiento_pct=_f(plantilla.get("financiamiento")),
            cargos_adicionales_pct=_f(plantilla.get("cargos")),
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
            if rendimiento:
                cantidad = cantidad / rendimiento

            db.session.add(
                APUDetalle(
                    apu_id=apu.id,
                    tipo_insumo=tipo,
                    referencia_id=recurso.id,
                    descripcion=recurso.nombre,
                    unidad=recurso.unidad,
                    cantidad=cantidad,
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
    rows = (
        APU.query.filter(or_(APU.concepto.ilike(f"%{q}%"), APU.clave.ilike(f"%{q}%")))
        .order_by(APU.concepto.asc())
        .limit(15)
        .all()
    )
    return jsonify(
        [
            {
                "id": a.id,
                "concepto": a.concepto,
                "unidad": a.unidad,
                "precio_unitario": a.precio_unitario,
                "clave": a.clave or "",
            }
            for a in rows
        ]
    )


@apu_bp.route("/api/<int:apu_id>/resumen")
@login_required
def api_apu_resumen(apu_id):
    a = APU.query.get_or_404(apu_id)
    recalcular_apu(a)
    db.session.commit()
    return jsonify(
        {
            "id": a.id,
            "clave": a.clave,
            "concepto": a.concepto,
            "unidad": a.unidad,
            "precio_unitario": a.precio_unitario,
            "costo_directo": a.costo_directo,
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
                descripcion=f"Generado desde MAR DATA {apu.clave or apu.id}",
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
