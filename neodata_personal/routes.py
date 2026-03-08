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

@apu_bp.route("/")
@login_required
def index():
    return render_template("neodata/apu_index.html",
        total_materiales=Material.query.count(),
        total_mano_obra=ManoObra.query.count(),
        total_maquinaria=Maquinaria.query.count(),
        total_apu=APU.query.count(),
        apus=APU.query.order_by(APU.actualizado_en.desc()).limit(20).all(),
        title="MAR DATA")

@apu_bp.route("/api/suggest")
@login_required
def api_apu_suggest():
    q = (request.args.get("q") or "").strip()
    if len(q) < 1:
        return jsonify([])
    rows = (APU.query.filter(or_(APU.concepto.ilike(f"%{q}%"), APU.clave.ilike(f"%{q}%")))
            .order_by(APU.concepto.asc()).limit(15).all())
    return jsonify([{"id":a.id,"concepto":a.concepto,"unidad":a.unidad,"precio_unitario":a.precio_unitario,"clave":a.clave or ""} for a in rows])

@apu_bp.route("/api/<int:apu_id>/resumen")
@login_required
def api_apu_resumen(apu_id):
    a = APU.query.get_or_404(apu_id)
    recalcular_apu(a)
    db.session.commit()
    return jsonify({"id":a.id,"clave":a.clave,"concepto":a.concepto,"unidad":a.unidad,"precio_unitario":a.precio_unitario,"costo_directo":a.costo_directo})

@apu_bp.route("/cotizador-rapido", methods=["GET","POST"])
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
            concepto = Concepto(nombre_concepto=apu.concepto, unidad=apu.unidad, precio_unitario=apu.precio_unitario, sistema="MAR DATA", descripcion=f"Generado desde MAR DATA {apu.clave or apu.id}")
            db.session.add(concepto)
            db.session.flush()

        cliente = None
        if nombre_cliente:
            cliente = Cliente.query.filter(db.func.lower(Cliente.nombre_cliente) == nombre_cliente.lower()).first()
            if not cliente:
                cliente = Cliente(nombre_cliente=nombre_cliente, empresa=empresa or None, responsable=_responsable_actual(), correo=correo, telefono=telefono, direccion=direccion)
                db.session.add(cliente)
                db.session.flush()

        subtotal = float(cantidad) * float(apu.precio_unitario or 0)
        iva_porc = 16.0
        iva_monto = subtotal * 0.16
        total = subtotal + iva_monto

        cot = Cotizacion(folio=_generar_folio(), fecha=datetime.utcnow(), cliente_id=cliente.id if cliente else None, estatus="PENDIENTE", subtotal=round(subtotal,2), descuento_total=0.0, iva_porc=iva_porc, iva_monto=round(iva_monto,2), total=round(total,2), notas=notas, responsable=_responsable_actual())
        db.session.add(cot)
        db.session.flush()

        det = CotizacionDetalle(cotizacion_id=cot.id, concepto_id=concepto.id, nombre_concepto=apu.concepto, unidad=apu.unidad, cantidad=float(cantidad), precio_unitario=float(apu.precio_unitario or 0), sistema="MAR DATA", descripcion=f"Creado desde APU {apu.clave or apu.id}", subtotal=round(subtotal,2))
        db.session.add(det)
        db.session.commit()

        flash("Cotización rápida generada desde MAR DATA.", "success")
        return redirect(url_for("view_cotizacion", cot_id=cot.id))

    return render_template("apu_cotizador_rapido.html", title="Cotizador rápido MAR DATA")
