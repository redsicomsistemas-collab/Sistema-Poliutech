from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
from models import db, Cotizacion, CotizacionItem
from utils.export_utils import exportar_cotizacion

cotizador_bp = Blueprint("cotizador", __name__, template_folder="templates")

def _to_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace("$", "").replace(",", "").strip())
    except Exception:
        return default

@cotizador_bp.route("/cotizador/")
def cotizador_home():
    return render_template("cotizador.html")

@cotizador_bp.post("/guardar")
def guardar():
    data = request.get_json(silent=True) or {}
    header = data.get("header", {})
    items = data.get("items", [])

    folio = header.get("folio") or f"MAR-{int(datetime.utcnow().timestamp())}"
    subtotal = 0.0
    for it in items:
        cantidad = _to_float(it.get("cantidad"), 0.0)
        precio = _to_float(it.get("precio_unitario"), 0.0)
        subtotal += _to_float(it.get("importe"), cantidad * precio)
    descuento_total = min(max(_to_float(header.get("descuento_total"), 0.0), 0.0), subtotal)
    iva_porc = _to_float(header.get("iva_porc"), 16.0)
    subtotal_desc = subtotal - descuento_total
    iva_monto = subtotal_desc * (iva_porc / 100.0)
    total = subtotal_desc + iva_monto

    cot = Cotizacion(
        folio=folio,
        cliente=header.get("cliente", "").strip() or "SIN CLIENTE",
        telefono=header.get("telefono", ""),
        correo=header.get("correo", ""),
        empresa=header.get("empresa", ""),
        proyecto=header.get("proyecto", ""),
        subtotal=subtotal,
        descuento_total=descuento_total,
        iva_porc=iva_porc,
        iva_monto=iva_monto,
        total=total,
        estatus=header.get("estatus", "PENDIENTE"),
        fecha_creacion=datetime.utcnow()
    )
    db.session.add(cot)
    db.session.flush()

    for it in items:
        cantidad = _to_float(it.get("cantidad"), 0.0)
        precio = _to_float(it.get("precio_unitario"), 0.0)
        importe = _to_float(it.get("importe"), cantidad * precio)
        db.session.add(CotizacionItem(
            cantidad=cantidad,
            unidad=it.get("unidad", ""),
            concepto=it.get("concepto", ""),
            precio_unitario=precio,
            importe=importe,
            cotizacion_id=cot.id
        ))

    db.session.commit()

    files = exportar_cotizacion(cot.id)
    return jsonify({"ok": True, "id": cot.id, "folio": cot.folio, "exports": files})
