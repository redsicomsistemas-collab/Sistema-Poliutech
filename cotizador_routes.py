from flask import Blueprint, render_template, request, jsonify
from datetime import datetime
from models import db, Cotizacion, CotizacionItem
from utils.export_utils import exportar_cotizacion

cotizador_bp = Blueprint("cotizador", __name__, template_folder="templates")

@cotizador_bp.route("/cotizador/")
def cotizador_home():
    return render_template("cotizador.html")

@cotizador_bp.post("/guardar")
def guardar():
    data = request.get_json(silent=True) or {}
    header = data.get("header", {})
    items = data.get("items", [])

    folio = header.get("folio") or f"MAR-{int(datetime.utcnow().timestamp())}"
    cot = Cotizacion(
        folio=folio,
        cliente=header.get("cliente", "").strip() or "SIN CLIENTE",
        telefono=header.get("telefono", ""),
        correo=header.get("correo", ""),
        empresa=header.get("empresa", ""),
        proyecto=header.get("proyecto", ""),
        total=float(header.get("total", 0) or 0),
        estatus=header.get("estatus", "PENDIENTE"),
        fecha_creacion=datetime.utcnow()
    )
    db.session.add(cot)
    db.session.flush()

    for it in items:
        cantidad = float(it.get("cantidad", 0) or 0)
        precio = float(it.get("precio_unitario", 0) or 0)
        importe = float(it.get("importe", cantidad * precio))
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
