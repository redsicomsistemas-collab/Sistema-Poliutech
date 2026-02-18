from flask import Blueprint, render_template, jsonify, request
from sqlalchemy import func
from datetime import datetime
from models import db, Cotizacion, Catalogo

dashboard_bp = Blueprint("dashboard", __name__, template_folder="templates")

@dashboard_bp.route("/dashboard/")
def dashboard_home():
    return render_template("dashboard.html")

@dashboard_bp.get("/api/cotizaciones")
def api_cotizaciones():
    estatus = request.args.get("estatus") or None
    fecha_ini = request.args.get("fecha_ini") or None
    fecha_fin = request.args.get("fecha_fin") or None
    monto_min = request.args.get("monto_min") or None
    monto_max = request.args.get("monto_max") or None

    q = Cotizacion.query
    if estatus:
        q = q.filter(Cotizacion.estatus == estatus)
    if fecha_ini:
        dt = datetime.fromisoformat(fecha_ini)
        q = q.filter(Cotizacion.fecha_creacion >= dt)
    if fecha_fin:
        dt = datetime.fromisoformat(fecha_fin)
        q = q.filter(Cotizacion.fecha_creacion <= dt)
    if monto_min:
        try:
            q = q.filter(Cotizacion.total >= float(monto_min))
        except:
            pass
    if monto_max:
        try:
            q = q.filter(Cotizacion.total <= float(monto_max))
        except:
            pass

    data = [
        {
            "id": c.id,
            "folio": c.folio,
            "cliente": c.cliente,
            "empresa": c.empresa,
            "total": c.total,
            "estatus": c.estatus,
            "fecha": c.fecha_creacion.isoformat()
        }
        for c in q.order_by(Cotizacion.fecha_creacion.desc()).all()
    ]
    return jsonify({"items": data})

@dashboard_bp.get("/api/catalogo_totales")
def api_catalogo_totales():
    total_items = db.session.query(func.count(Catalogo.id)).scalar() or 0
    promedio_precio = db.session.query(func.avg(Catalogo.precio)).scalar() or 0.0
    total_valor = db.session.query(func.sum(Catalogo.precio)).scalar() or 0.0
    return jsonify({
        "total_items": int(total_items),
        "promedio_precio": float(round(promedio_precio, 2)),
        "total_valor": float(round(total_valor, 2))
    })
