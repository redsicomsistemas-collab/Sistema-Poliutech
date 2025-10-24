from __future__ import annotations

# =========================================================
# MARWHATS (checkpoint) - Sistema Poliutech
# Integración solicitada:
# - Logo en PDF (static/logo.jpg) y pie de página corporativo.
# - Abrir PDF en nueva pestaña al GUARDAR/ACTUALIZAR cotización.
# - Edición de cotizaciones en /cotizaciones/<int:cot_id>/editar.
# - Título del PDF = folio (evitar "anonymous").
# - Mantiene dashboard, métricas, filtros, importación, Twilio y scheduler.
# - PDF: "Representante" en vez de "Estatus", logo a la izquierda sin deformar,
#   footer de 2 líneas con división (static/division.png).
# =========================================================

import os, io, csv, sys, math, traceback
from datetime import datetime, timedelta
from typing import Iterable, Optional, List

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

# ReportLab (PDF)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    Table, TableStyle, Paragraph, SimpleDocTemplate,
    Spacer
)
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.utils import ImageReader

# Twilio + Scheduler
from twilio.rest import Client as TwilioClient
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
DEFAULT_SECRET_KEY = "poliutech_mar_checkpoint_superseguro"
DEFAULT_DATABASE_URL = "sqlite:///mar3.db"

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP    = os.getenv("TWILIO_WHATSAPP", "whatsapp:+14155238886").strip()

DEFAULT_ADMIN_WHATSAPP_RECIPIENTS = (
    "whatsapp:+5215521323076,whatsapp:+5215610035643,whatsapp:+14055619808"
)
ADMIN_WHATSAPP_RECIPIENTS = os.getenv(
    "ADMIN_WHATSAPP_RECIPIENTS",
    DEFAULT_ADMIN_WHATSAPP_RECIPIENTS
).strip()

ADMIN_LIST: List[str] = [
    x.strip() for x in ADMIN_WHATSAPP_RECIPIENTS.split(",") if x.strip()
]

# Flask + DB
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", DEFAULT_SECRET_KEY)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# Twilio (opcional)
twilio_client: Optional[TwilioClient] = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    try:
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        print("[Twilio] Cliente inicializado.")
    except Exception as e:
        print(f"[Twilio] No se pudo inicializar: {e}", file=sys.stderr)
else:
    print("[Twilio] SIN credenciales. Envío WhatsApp deshabilitado.", file=sys.stderr)

# ---------------------------------------------------------
# Modelos
# ---------------------------------------------------------
class Cliente(db.Model):
    __tablename__ = "cliente"
    id = db.Column(db.Integer, primary_key=True)
    nombre_cliente = db.Column(db.String(120), nullable=False)
    empresa = db.Column(db.String(120))
    responsable = db.Column(db.String(120))
    correo = db.Column(db.String(120))
    telefono = db.Column(db.String(50))
    direccion = db.Column(db.String(200))
    rfc = db.Column(db.String(50))

class Concepto(db.Model):
    __tablename__ = "concepto"
    id = db.Column(db.Integer, primary_key=True)
    nombre_concepto = db.Column(db.String(200), nullable=False)
    unidad = db.Column(db.String(50))
    precio_unitario = db.Column(db.Float, default=0)
    descripcion = db.Column(db.String(500))

class Cotizacion(db.Model):
    __tablename__ = "cotizacion"
    id = db.Column(db.Integer, primary_key=True)
    folio = db.Column(db.String(40), unique=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey("cliente.id"))
    fecha = db.Column(db.DateTime, default=datetime.utcnow)
    estatus = db.Column(db.String(20), default="PENDIENTE")
    subtotal = db.Column(db.Float, default=0.0)
    descuento_total = db.Column(db.Float, default=0.0)
    iva_porc = db.Column(db.Float, default=16.0)
    iva_monto = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)
    notas = db.Column(db.String(500))
    last_whatsapp_at = db.Column(db.DateTime, nullable=True)
    # NUEVO: representante para el PDF
    representante = db.Column(db.String(120))

    cliente = db.relationship("Cliente", backref="cotizaciones")
    detalles = db.relationship("CotizacionDetalle", backref="cotizacion",
                               cascade="all, delete-orphan")

class CotizacionDetalle(db.Model):
    __tablename__ = "cotizacion_detalle"
    id = db.Column(db.Integer, primary_key=True)
    cotizacion_id = db.Column(db.Integer, db.ForeignKey("cotizacion.id"))
    concepto_id = db.Column(db.Integer, db.ForeignKey("concepto.id"), nullable=True)
    nombre_concepto = db.Column(db.String(200), nullable=False)
    unidad = db.Column(db.String(50))
    cantidad = db.Column(db.Float, default=1)
    precio_unitario = db.Column(db.Float, default=0)
    descuento = db.Column(db.Float, default=0)
    descripcion = db.Column(db.String(500))
    subtotal = db.Column(db.Float, default=0)

    concepto = db.relationship("Concepto")

# ---------------------------------------------------------
# Migración simple
# ---------------------------------------------------------
def _table_columns(table_name: str) -> set[str]:
    rows = db.session.execute(text(f"PRAGMA table_info('{table_name}')")).mappings().all()
    return {r["name"] for r in rows}

def ensure_schema():
    db.create_all()
    cols = _table_columns("cotizacion")
    adds = []
    if "subtotal" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN subtotal FLOAT DEFAULT 0.0")
    if "descuento_total" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN descuento_total FLOAT DEFAULT 0.0")
    if "iva_porc" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN iva_porc FLOAT DEFAULT 16.0")
    if "iva_monto" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN iva_monto FLOAT DEFAULT 0.0")
    if "total" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN total FLOAT DEFAULT 0.0")
    if "notas" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN notas VARCHAR(500)")
    if "last_whatsapp_at" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN last_whatsapp_at TIMESTAMP NULL")
    if "representante" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN representante VARCHAR(120)")
    for sql in adds:
        db.session.execute(text(sql))
    if adds:
        db.session.commit()

with app.app_context():
    ensure_schema()

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def generar_folio() -> str:
    n = db.session.query(db.func.count(Cotizacion.id)).scalar() or 0
    return f"PTCH-{n+1:04d}"

def fmt(n: float) -> float:
    try:
        return round(float(n or 0), 2)
    except Exception:
        return 0.0

def parse_float(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace("$", "").replace(",", "").strip()
        return float(s) if s else default
    except Exception:
        return default

def normalize_whatsapp(number: str) -> str:
    if not number:
        return ""
    n = number.strip()
    if n.startswith("whatsapp:"):
        return n
    if n.startswith("+"):
        return f"whatsapp:{n}"
    digits = "".join(ch for ch in n if ch.isdigit())
    if not digits:
        return ""
    return f"whatsapp:+52{digits}"

def can_send_whatsapp() -> bool:
    return bool(twilio_client and TWILIO_WHATSAPP and ADMIN_LIST)

def send_whatsapp_multi(to_list: Iterable[str], body: str) -> None:
    if not to_list:
        return
    if not can_send_whatsapp():
        print("[Twilio] Configuración incompleta; omito envío.")
        return
    for to in to_list:
        to_norm = normalize_whatsapp(to)
        if not to_norm:
            continue
        try:
            twilio_client.messages.create(from_=TWILIO_WHATSAPP, to=to_norm, body=body)
        except Exception as e:
            print(f"[Twilio] ERROR enviando a {to_norm}: {e}", file=sys.stderr)
            traceback.print_exc()

# ---------------------------------------------------------
# Rutas: Dashboard / Catálogos / Cotizador
# ---------------------------------------------------------
@app.route("/")
def index():
    total_cotizaciones = Cotizacion.query.count()
    total_importe = db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0
    total_catalogo = Concepto.query.count()
    cotizaciones = Cotizacion.query.order_by(Cotizacion.fecha.desc()).limit(100).all()
    return render_template("dashboard.html",
                           title="Sistema Poliutech",
                           total_cotizaciones=total_cotizaciones,
                           total_importe=float(total_importe),
                           total_catalogo=total_catalogo,
                           cotizaciones=cotizaciones)

@app.route("/cotizador")
def cotizador():
    return render_template("cotizador.html", title="Nuevo - Sistema Poliutech")

@app.route("/admin/catalogos")
def admin_catalogos():
    clientes = Cliente.query.order_by(Cliente.id.desc()).limit(10).all()
    conceptos = Concepto.query.order_by(Concepto.id.desc()).limit(10).all()
    return render_template("admin_catalogos.html", title="Admin Catálogos",
                           clientes=clientes, conceptos=conceptos)

# ---------------------------------------------------------
# Autocompletar
# ---------------------------------------------------------
@app.route("/api/clientes/suggest")
def api_clientes_suggest():
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])
    res = (Cliente.query
           .filter(Cliente.nombre_cliente.ilike(f"%{q}%"))
           .order_by(Cliente.nombre_cliente).limit(10).all())
    return jsonify([{
        "label": f"{c.nombre_cliente} · {c.empresa}" if c.empresa else c.nombre_cliente,
        "nombre_cliente": c.nombre_cliente,
        "empresa": c.empresa,
        "responsable": c.responsable,
        "correo": c.correo,
        "telefono": c.telefono,
        "direccion": c.direccion,
        "rfc": c.rfc,
    } for c in res])

@app.route("/api/conceptos/suggest")
def api_conceptos_suggest():
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])
    res = (Concepto.query
           .filter(Concepto.nombre_concepto.ilike(f"%{q}%"))
           .order_by(Concepto.nombre_concepto).limit(10).all())
    return jsonify([{
        "label": c.nombre_concepto,
        "nombre_concepto": c.nombre_concepto,
        "unidad": c.unidad,
        "precio_unitario": c.precio_unitario,
        "descripcion": c.descripcion
    } for c in res])

# ---------------------------------------------------------
# Crear cotización (abre PDF en nueva pestaña al terminar)
# ---------------------------------------------------------
@app.route("/cotizaciones/crear", methods=["POST"])
def crear_cotizacion():
    f = request.form

    # Cliente
    nombre_cliente = (f.get("cliente_nombre") or "").strip()
    empresa = (f.get("empresa") or "").strip()
    cliente = None
    if nombre_cliente:
        cliente = Cliente.query.filter_by(nombre_cliente=nombre_cliente, empresa=empresa).first()
        if not cliente:
            cliente = Cliente(
                nombre_cliente=nombre_cliente,
                empresa=empresa or None,
                responsable=(f.get("responsable") or "").strip() or None,
                correo=(f.get("correo") or "").strip() or None,
                telefono=(f.get("telefono") or "").strip() or None,
                direccion=(f.get("direccion") or "").strip() or None,
                rfc=(f.get("rfc") or "").strip() or None,
            )
            db.session.add(cliente)
            db.session.flush()

    iva_porc = parse_float(f.get("iva_porc"), 16.0)

    cot = Cotizacion(
        folio=generar_folio(),
        cliente_id=cliente.id if cliente else None,
        estatus=(f.get("estatus") or "PENDIENTE").upper(),
        notas=f.get("notas"),
        last_whatsapp_at=None,
        representante=(f.get("representante") or "").strip() or None
    )
    db.session.add(cot)
    db.session.flush()

    # Detalles
    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    descuentos = f.getlist("item_descuento[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    descuento_total = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios), len(descuentos))
    for i in range(n):
        nom = (nombres[i] if i < len(nombres) else "").strip()
        if not nom:
            continue
        uni = (unidades[i] if i < len(unidades) else "").strip()
        cant = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        pu   = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        dsc  = parse_float(descuentos[i] if i < len(descuentos) else 0, 0.0)
        dsc  = max(0.0, min(dsc, 100.0))

        line_subtotal = cant * pu * (1 - dsc/100)
        subtotal += line_subtotal
        descuento_total += cant * pu * (dsc/100)

        concepto = Concepto.query.filter_by(nombre_concepto=nom).first()
        det = CotizacionDetalle(
            cotizacion_id=cot.id,
            concepto_id=concepto.id if concepto else None,
            nombre_concepto=nom,
            unidad=uni,
            cantidad=cant,
            precio_unitario=pu,
            descuento=dsc,
            descripcion=(descripciones[i] if i < len(descripciones) else "") or "",
            subtotal=line_subtotal
        )
        db.session.add(det)

    iva_monto = subtotal * (iva_porc/100.0)
    total = subtotal + iva_monto
    cot.subtotal = fmt(subtotal)
    cot.descuento_total = fmt(descuento_total)
    cot.iva_porc = fmt(iva_porc)
    cot.iva_monto = fmt(iva_monto)
    cot.total = fmt(total)
    db.session.commit()

    # WhatsApp admins
    try:
        msg = (
            "🧾 *Nueva Cotización Creada*\\n"
            f"Folio: *{cot.folio}*\\n"
            f"Estatus: *{cot.estatus}*\\n"
            f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\\n"
            f"Total: ${cot.total:.2f}"
        )
        send_whatsapp_multi(ADMIN_LIST, msg)
    except Exception as e:
        print(f"[WARN] WhatsApp creación ({cot.folio}): {e}", file=sys.stderr)

    # Abrir PDF en nueva pestaña y regresar al cotizador
    pdf_url = url_for("export_cotizacion_pdf", cot_id=cot.id)
    volver = url_for("cotizador")
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Creada {cot.folio}</title></head>
<body>
<script>
window.open("{pdf_url}", "_blank");
window.location.href = "{volver}";
</script>
<p>Abrir PDF: <a href="{pdf_url}" target="_blank">aquí</a>. Volver: <a href="{volver}">cotizador</a>.</p>
</body></html>"""

# ---------------------------------------------------------
# Edición de cotización
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/editar")
def editar_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_edit.html", c=c, title=f"Editar {c.folio}")

@app.route("/cotizaciones/<int:cot_id>/actualizar", methods=["POST"])
def actualizar_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    f = request.form

    # Actualizar/crear cliente
    nombre_cliente = (f.get("cliente_nombre") or "").strip()
    empresa = (f.get("empresa") or "").strip()
    if nombre_cliente:
        cliente = Cliente.query.filter_by(nombre_cliente=nombre_cliente, empresa=empresa).first()
        if not cliente:
            cliente = Cliente(
                nombre_cliente=nombre_cliente,
                empresa=empresa or None,
                responsable=(f.get("responsable") or "").strip() or None,
                correo=(f.get("correo") or "").strip() or None,
                telefono=(f.get("telefono") or "").strip() or None,
                direccion=(f.get("direccion") or "").strip() or None,
                rfc=(f.get("rfc") or "").strip() or None,
            )
            db.session.add(cliente)
            db.session.flush()
        c.cliente_id = cliente.id

    c.estatus = (f.get("estatus") or c.estatus).upper()
    c.notas = f.get("notas")
    c.representante = (f.get("representante") or "").strip() or c.representante
    iva_porc = parse_float(f.get("iva_porc"), c.iva_porc or 16.0)

    # Borrar detalles y re-crear para simplificar
    for d in list(c.detalles):
        db.session.delete(d)

    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    descuentos = f.getlist("item_descuento[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    descuento_total = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios), len(descuentos))
    for i in range(n):
        nom = (nombres[i] if i < len(nombres) else "").strip()
        if not nom:
            continue
        uni = (unidades[i] if i < len(unidades) else "").strip()
        cant = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        pu   = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        dsc  = parse_float(descuentos[i] if i < len(descuentos) else 0, 0.0)
        dsc  = max(0.0, min(dsc, 100.0))

        line_subtotal = cant * pu * (1 - dsc/100)
        subtotal += line_subtotal
        descuento_total += cant * pu * (dsc/100)

        concepto = Concepto.query.filter_by(nombre_concepto=nom).first()
        det = CotizacionDetalle(
            cotizacion_id=c.id,
            concepto_id=concepto.id if concepto else None,
            nombre_concepto=nom,
            unidad=uni,
            cantidad=cant,
            precio_unitario=pu,
            descuento=dsc,
            descripcion=(descripciones[i] if i < len(descripciones) else "") or "",
            subtotal=line_subtotal
        )
        db.session.add(det)

    iva_monto = subtotal * (iva_porc/100.0)
    total = subtotal + iva_monto
    c.subtotal = fmt(subtotal)
    c.descuento_total = fmt(descuento_total)
    c.iva_porc = fmt(iva_porc)
    c.iva_monto = fmt(iva_monto)
    c.total = fmt(total)

    db.session.commit()

    # Abrir PDF en nueva pestaña y mostrar vista
    pdf_url = url_for("export_cotizacion_pdf", cot_id=c.id)
    detalle = url_for("view_cotizacion", cot_id=c.id)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Actualizada {c.folio}</title></head>
<body>
<script>
window.open("{pdf_url}", "_blank");
window.location.href = "{detalle}";
</script>
<p>Abrir PDF: <a href="{pdf_url}" target="_blank">aquí</a>. Ver detalle: <a href="{detalle}">cotización</a>.</p>
</body></html>"""

# =========================================================
#  VER COTIZACIÓN (Vista rápida)
# =========================================================
@app.route("/cotizaciones/<int:cot_id>/ver")
def ver_cotizacion(cot_id):
    cot = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_view.html", c=cot, title=f"Vista de {cot.folio}")

# =========================================================
#  ELIMINAR COTIZACIÓN
# =========================================================
@app.route("/cotizaciones/<int:cot_id>/eliminar")
def eliminar_cotizacion(cot_id):
    cot = Cotizacion.query.get_or_404(cot_id)
    try:
        for d in cot.detalles:
            db.session.delete(d)
        db.session.delete(cot)
        db.session.commit()
        flash(f"Cotización {cot.folio} eliminada correctamente.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error al eliminar la cotización: {str(e)}", "danger")
    return redirect(url_for("index"))

# ---------------------------------------------------------
# Listas / Detalle
# ---------------------------------------------------------
@app.route("/cotizaciones")
def list_cotizaciones():
    page = int(request.args.get("p", 1) or 1)
    per_page = 25
    q = Cotizacion.query.order_by(Cotizacion.fecha.desc())
    total = q.count()
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    items = q.offset((page-1)*per_page).limit(per_page).all()
    return render_template("cotizaciones_list.html", items=items, page=page, pages=pages,
                           total=total, title="Cotizaciones · Sistema Poliutech")

@app.route("/cotizaciones/<int:cot_id>")
def view_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_view.html", c=c, title=f"Ver {c.folio}")

# ---------------------------------------------------------
# Exportaciones (CSV / PDF con logo, footer y título folio)
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/export.csv")
def export_cotizacion_csv(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["Folio","Fecha","Estatus","Representante","Cliente","Empresa","Subtotal","Desc Total","IVA %","IVA $","Total","Notas"])
    w.writerow([
        c.folio, c.fecha.strftime("%Y-%m-%d %H:%M"), c.estatus, (c.representante or ""),
        c.cliente.nombre_cliente if c.cliente else "",
        c.cliente.empresa if c.cliente else "",
        f"{c.subtotal:.2f}", f"{c.descuento_total:.2f}",
        f"{c.iva_porc:.2f}", f"{c.iva_monto:.2f}",
        f"{c.total:.2f}", (c.notas or "")
    ])
    w.writerow([])
    w.writerow(["Cant","Unidad","Concepto","PU","Desc %","Subtotal","Descripción"])
    for d in c.detalles:
        w.writerow([
            d.cantidad, d.unidad or "", d.nombre_concepto,
            f"{d.precio_unitario:.2f}", f"{d.descuento:.2f}",
            f"{d.subtotal:.2f}", (d.descripcion or "")
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={'Content-Disposition': f'attachment; filename="{c.folio or "cotizacion"}.csv"'}
    )

# ======= NUEVA VERSIÓN PDF EMPRESARIAL =======
@app.route("/cotizaciones/<int:cot_id>/export.pdf")
def export_cotizacion_pdf(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm, topMargin=58*mm, bottomMargin=28*mm
    )
    styles = getSampleStyleSheet()
    elems = []

    from reportlab.lib.styles import ParagraphStyle

    # estilos extra
    styles.add(ParagraphStyle(name="Titulo", fontSize=14, leading=18, textColor=colors.HexColor("#0d47a1"), alignment=1))
    styles.add(ParagraphStyle(name="Encabezado", fontSize=9, leading=12, spaceAfter=4))
    styles.add(ParagraphStyle(name="NormalRight", fontSize=9, alignment=2))

    # Encabezado corporativo (logo grande a la izquierda SIN deformarse)
    def encabezado(canv, doc_):
        canv.saveState()
        # franja azul superior
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1]-22, A4[0], 22, stroke=0, fill=1)

        # Logo
        logo_path = os.path.join(app.static_folder or "static", "logo.jpg")
        x_logo = 20  # margen izquierdo
        y_logo = A4[1] - 22 - 5  # debajo de la franja
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 60*mm
                # mantener relación de aspecto
                scale = max_w / float(iw)
                w = max_w
                h = ih * scale
                # ubicar el logo
                canv.drawImage(img, x_logo, y_logo - h, width=w, height=h, mask="auto")
            except Exception:
                pass

        # Títulos a la derecha
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.setFont("Helvetica-Bold", 14)
        canv.drawRightString(A4[0]-28, A4[1]-40, "COTIZACIÓN POLIUTECH")
        canv.setFont("Helvetica", 10)
        canv.setFillColor(colors.black)
        canv.drawRightString(A4[0]-28, A4[1]-56, "Recubrimientos Especializados")
        canv.restoreState()

    # Footer corporativo con división.png y 2 líneas
    def footer(canv, doc_):
        canv.saveState()
        # división gráfica
        divider_path = os.path.join(app.static_folder or "static", "division.png")
        if os.path.exists(divider_path):
            try:
                div_img = ImageReader(divider_path)
                iw, ih = div_img.getSize()
                # ancho interior
                left = 20*mm
                right = A4[0] - 20*mm
                target_w = right - left
                scale = target_w / float(iw)
                w = target_w
                h = ih * scale
                canv.drawImage(div_img, left, 40, width=w, height=h, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#555555"))
        texto = (
            "POLIUTECH – Recubrimientos Especializados\n"
            "Campos Elíseos 223 Oficina 602 Col. Polanco V Sección, Miguel Hidalgo, CDMX 11560 · "
            "Tel: 55 5938 6530 / 55 5938 0536 · info@poliutech.com · www.poliutech.com"
        )
        # 2 líneas
        tobj = canv.beginText(20*mm, 28)
        for line in texto.split("\n"):
            tobj.textLine(line)
        canv.drawText(tobj)

        # título del documento (folio)
        try:
            canv.setTitle(c.folio or "Cotizacion")
        except Exception:
            pass
        canv.restoreState()

    # Datos generales (Representante en vez de Estatus)
    elems.append(Paragraph(f"<b>Folio:</b> {c.folio}", styles["Encabezado"]))
    elems.append(Paragraph(f"<b>Fecha:</b> {c.fecha.strftime('%d/%m/%Y %H:%M')} | "
                           f"<b>Representante:</b> {c.representante or ''}", styles["Encabezado"]))
    elems.append(Spacer(1, 6))

    # Cliente
    if c.cliente:
        cli = c.cliente
        for txt in [
            f"<b>Cliente:</b> {cli.nombre_cliente or ''}",
            f"<b>Empresa:</b> {cli.empresa or ''}",
            f"<b>Correo:</b> {cli.correo or ''}",
            f"<b>Teléfono:</b> {cli.telefono or ''}",
            f"<b>RFC:</b> {cli.rfc or ''}",
        ]:
            elems.append(Paragraph(txt, styles["Encabezado"]))
        elems.append(Spacer(1, 10))

    # Tabla de renglones
    data = [["Cant", "Unidad", "Concepto", "Precio Unit.", "Desc %", "Subtotal"]]
    for d in c.detalles:
        data.append([
            f"{d.cantidad:.2f}",
            d.unidad or "",
            Paragraph(d.nombre_concepto, styles["Normal"]),
            f"${d.precio_unitario:.2f}",
            f"{d.descuento:.2f}",
            f"${d.subtotal:.2f}",
        ])
    tbl = Table(data, colWidths=[18*mm, 20*mm, 70*mm, 25*mm, 20*mm, 25*mm], repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#0d47a1")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("ALIGN", (3,1), (-1,-1), "RIGHT"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    elems.append(tbl)
    elems.append(Spacer(1, 12))

    # Totales
    tot_data = [
        ["Subtotal:", f"${c.subtotal:.2f}"],
        [f"IVA ({c.iva_porc:.2f}%):", f"${c.iva_monto:.2f}"],
        ["Total:", f"${c.total:.2f}"],
    ]
    t2 = Table(tot_data, colWidths=[40*mm, 35*mm], hAlign="RIGHT")
    t2.setStyle(TableStyle([
        ("FONTNAME", (0,0), (-1,-1), "Helvetica-Bold"),
        ("ALIGN", (1,0), (1,-1), "RIGHT"),
        ("LINEBELOW", (0,-1), (-1,-1), 0.5, colors.black),
        ("BACKGROUND", (0,0), (-1,-1), colors.whitesmoke),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
    ]))
    elems.append(t2)
    elems.append(Spacer(1, 10))

    # Notas
    if c.notas:
        elems.append(Paragraph("<b>Notas:</b>", styles["Encabezado"]))
        elems.append(Paragraph(c.notas, styles["Normal"]))
        elems.append(Spacer(1, 10))

    # Build
    doc.build(
        elems,
        onFirstPage=lambda canv, d: (encabezado(canv, d), footer(canv, d)),
        onLaterPages=lambda canv, d: (encabezado(canv, d), footer(canv, d))
    )

    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="application/pdf",
        headers={'Content-Disposition': f'inline; filename="{c.folio}.pdf"'}
    )

# ---------------------------------------------------------
# Admin: importación catálogos
# ---------------------------------------------------------
@app.route("/admin/catalogos/upload", methods=["POST"])
def upload_catalogo():
    tipo = (request.form.get("tipo") or "").strip()
    file = request.files.get("archivo")
    if not tipo or not file or not getattr(file, "filename", ""):
        flash("Debe seleccionar un tipo y un archivo válido.", "danger")
        return redirect(url_for("admin_catalogos"))

    import pandas as pd
    ext = os.path.splitext(file.filename)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(file)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(file)
        else:
            flash("Formato no compatible. Usa CSV o XLSX.", "danger")
            return redirect(url_for("admin_catalogos"))
    except Exception as e:
        flash(f"Error leyendo archivo: {e}", "danger")
        return redirect(url_for("admin_catalogos"))

    try:
        registros = 0
        if tipo == "Clientes":
            for _, r in df.iterrows():
                nombre_cliente = str(r.get("nombre_cliente","")).strip()
                if not nombre_cliente:
                    continue
                c = Cliente(
                    nombre_cliente=nombre_cliente,
                    empresa=str(r.get("empresa","")).strip() or None,
                    responsable=str(r.get("responsable","")).strip() or None,
                    correo=str(r.get("correo","")).strip() or None,
                    telefono=str(r.get("telefono","")).strip() or None,
                    direccion=str(r.get("direccion","")).strip() or None,
                    rfc=str(r.get("rfc","")).strip() or None,
                )
                db.session.add(c); registros += 1
        elif tipo == "Conceptos":
            for _, r in df.iterrows():
                nombre_concepto = str(r.get("nombre_concepto","")).strip()
                if not nombre_concepto:
                    continue
                pu = r.get("precio_unitario", 0)
                try:
                    pu = float(str(pu).replace("$","").replace(",","")) if pu not in [None,""] else 0
                except Exception:
                    pu = 0
                c = Concepto(
                    nombre_concepto=nombre_concepto,
                    unidad=str(r.get("unidad","")).strip() or None,
                    precio_unitario=pu,
                    descripcion=str(r.get("descripcion","")).strip() or None,
                )
                db.session.add(c); registros += 1

        db.session.commit()
        flash(f"Catálogo de {tipo.lower()} cargado correctamente ({registros} registros).", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error al importar: {e}", "danger")

    return redirect(url_for("admin_catalogos"))

# ---------------------------------------------------------
# API Datos Dashboard
# ---------------------------------------------------------
@app.route("/api/cotizaciones/search")
def api_cotizaciones_search():
    q = Cotizacion.query.join(Cliente, isouter=True)
    estatus = (request.args.get("estatus") or "").strip()
    fi = (request.args.get("fi") or "").strip()
    ff = (request.args.get("ff") or "").strip()
    mmin = (request.args.get("mmin") or "").strip()
    mmax = (request.args.get("mmax") or "").strip()

    if estatus:
        q = q.filter(Cotizacion.estatus == estatus)
    if fi:
        try: q = q.filter(Cotizacion.fecha >= datetime.fromisoformat(fi))
        except Exception: pass
    if ff:
        try: q = q.filter(Cotizacion.fecha <= datetime.fromisoformat(ff))
        except Exception: pass
    if mmin:
        try: q = q.filter(Cotizacion.total >= float(mmin))
        except Exception: pass
    if mmax:
        try: q = q.filter(Cotizacion.total <= float(mmax))
        except Exception: pass

    q = q.order_by(Cotizacion.fecha.desc()).limit(500)
    data = []
    for c in q.all():
        data.append({
            "id": c.id,
            "folio": c.folio,
            "cliente": c.cliente.nombre_cliente if c.cliente else "",
            "empresa": c.cliente.empresa if c.cliente else "",
            "fecha": c.fecha.strftime("%Y-%m-%d %H:%M"),
            "estatus": c.estatus,
            "total": round(c.total or 0, 2),
            "export_csv": url_for("export_cotizacion_csv", cot_id=c.id),
            "export_pdf": url_for("export_cotizacion_pdf", cot_id=c.id),
        })
    return jsonify(data)

@app.route("/api/dashboard/metrics")
def api_dashboard_metrics():
    rows = db.session.query(
        db.func.strftime("%Y-%m", Cotizacion.fecha).label("ym"),
        db.func.count(Cotizacion.id),
        db.func.coalesce(db.func.sum(Cotizacion.total), 0)
    ).group_by("ym").order_by("ym").all()
    series = [{"mes": ym, "cotizaciones": int(c), "total": float(t)} for ym, c, t in rows]
    kpis = {
        "total_cotizaciones": Cotizacion.query.count(),
        "total_importe": float(db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0),
        "total_catalogo": Concepto.query.count(),
    }
    return jsonify({"series": series, "kpis": kpis})

@app.route("/api/dashboard/status_breakdown")
def api_dashboard_status_breakdown():
    rows = db.session.query(Cotizacion.estatus, db.func.count(Cotizacion.id)) \
                     .group_by(Cotizacion.estatus).all()
    categorias = ["ENVIADA", "PENDIENTE", "GANADA", "PERDIDA"]
    conteos_map = {estatus: cnt for estatus, cnt in rows}
    conteos = [int(conteos_map.get(cat, 0)) for cat in categorias]
    total = sum(conteos)
    porcentajes = [round((c * 100.0 / total), 2) if total > 0 else 0 for c in conteos]
    return jsonify({"labels": categorias, "counts": conteos, "percentages": porcentajes, "total": total})

# ---------------------------------------------------------
# Salud / Debug / Recordatorios
# ---------------------------------------------------------
@app.route("/health")
def health():
    return jsonify({"status": "ok", "now_utc": datetime.utcnow().isoformat()}), 200

@app.route("/debug/send_test")
def debug_send_test():
    msg = "✅ Mensaje de prueba - Sistema Poliutech (debug_send_test)."
    send_whatsapp_multi(ADMIN_LIST, msg)
    return jsonify({"sent": True, "to": ADMIN_LIST})

@app.route("/debug/force_reminders")
def debug_force_reminders():
    try:
        enviar_notificaciones_pendientes()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

def enviar_notificaciones_pendientes():
    with app.app_context():
        ahora = datetime.utcnow()
        hace_24h = ahora - timedelta(hours=24)
        pendientes = Cotizacion.query.filter_by(estatus="PENDIENTE").all()
        for cot in pendientes:
            if cot.last_whatsapp_at is None or cot.last_whatsapp_at <= hace_24h:
                try:
                    body = (
                        "🔔 *Recordatorio: Cotización PENDIENTE*\\n"
                        f"Folio: *{cot.folio}*\\n"
                        f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\\n"
                        f"Total: ${cot.total:.2f}"
                    )
                    send_whatsapp_multi(ADMIN_LIST, body)
                    cot.last_whatsapp_at = ahora
                    db.session.commit()
                except Exception as e:
                    print(f"[Scheduler] ERROR recordatorio ({cot.folio}): {e}", file=sys.stderr)

# Evitar doble scheduler en debug
scheduler: Optional[BackgroundScheduler] = None
try:
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(enviar_notificaciones_pendientes, "interval", minutes=60,
                          id="pending_quotes_reminder", replace_existing=True)
        scheduler.start()
        print("[Scheduler] Iniciado (interval=60m).")
except Exception as e:
    print(f"[Scheduler] No pudo iniciar: {e}", file=sys.stderr)

# ---------------------------------------------------------
# Fallbacks de templates mínimos (si no existen)
# ---------------------------------------------------------
from jinja2 import TemplateNotFound
from markupsafe import escape, Markup

_real_render_template = render_template
def render_template(name, **ctx):
    try:
        return _real_render_template(name, **ctx)
    except TemplateNotFound:
        # dashboard fallback
        if name == "dashboard.html":
            total_cotizaciones = ctx.get("total_cotizaciones", 0)
            total_importe = ctx.get("total_importe", 0.0)
            total_catalogo = ctx.get("total_catalogo", 0)
            cotizaciones = ctx.get("cotizaciones", [])
            rows = ""
            for c in cotizaciones:
                rows += (
                    "<tr>"
                    f"<td>{escape(c.folio)}</td>"
                    f"<td>{c.fecha.strftime('%Y-%m-%d %H:%M')}</td>"
                    f"<td><span>{escape(c.estatus)}</span></td>"
                    f"<td>${c.total:.2f}</td>"
                    f"<td>"
                    f"<a href='{url_for('view_cotizacion', cot_id=c.id)}'>Ver</a> · "
                    f"<a target='_blank' href='{url_for('export_cotizacion_pdf', cot_id=c.id)}'>PDF</a> · "
                    f"<a href='{url_for('export_cotizacion_csv', cot_id=c.id)}'>CSV</a> · "
                    f"<a href='{url_for('editar_cotizacion', cot_id=c.id)}'>Editar</a>"
                    f"</td>"
                    "</tr>"
                )
            html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Dashboard'))}</title>
<style>body{{font-family:system-ui; margin:24px}} table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #ddd;padding:8px}}</style>
</head><body>
<h1>MARWHATS · Dashboard</h1>
<p> Cotizaciones: <b>{total_cotizaciones}</b> · Importe total: <b>${total_importe:.2f}</b> · Conceptos: <b>{total_catalogo}</b></p>
<p><a href="{url_for('cotizador')}">Crear nueva</a> · <a href="{url_for('admin_catalogos')}">Admin catálogos</a></p>
<table><thead><tr><th>Folio</th><th>Fecha</th><th>Estatus</th><th>Total</th><th>Acciones</th></tr></thead>
<tbody>{rows}</tbody></table>
</body></html>"""
            return html

        # cotizador fallback (incluye REPRESENTANTE)
        if name == "cotizador.html":
            html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Cotizador'))}</title>
<style>body{{font-family:system-ui; margin:24px}} .item{{border:1px dashed #ccc;padding:8px;margin:8px 0}}</style>
</head><body>
<h1>Nueva cotización</h1>
<form method="post" action="{url_for('crear_cotizacion')}">
  <h3>Cliente</h3>
  <p><label>Nombre: <input name="cliente_nombre" required></label></p>
  <p><label>Empresa: <input name="empresa"></label></p>
  <p><label>Responsable: <input name="responsable"></label></p>
  <p><label>Correo: <input name="correo" type="email"></label></p>
  <p><label>Teléfono: <input name="telefono"></label></p>
  <p><label>Dirección: <input name="direccion"></label></p>
  <p><label>RFC: <input name="rfc"></label></p>
  <p><label>Representante: <input name="representante" placeholder="Nombre del representante"></label></p>

  <h3>Items</h3>
  <div id="items">
    <div class="item">
      <p><label>Concepto: <input name="item_nombre_concepto[]"></label></p>
      <p><label>Unidad: <input name="item_unidad[]"></label></p>
      <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
      <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
      <p><label>Desc %: <input name="item_descuento[]" value="0"></label></p>
      <p><label>Descripción:<br><textarea name="item_descripcion[]"></textarea></label></p>
    </div>
  </div>
  <p><button type="button" onclick="addItem()">Agregar renglón</button></p>

  <h3>Totales</h3>
  <p><label>IVA %: <input name="iva_porc" value="16"></label></p>
  <p><label>Estatus:
    <select name="estatus">
      <option value="PENDIENTE">PENDIENTE</option>
      <option value="ENVIADA">ENVIADA</option>
      <option value="GANADA">GANADA</option>
      <option value="PERDIDA">PERDIDA</option>
    </select>
  </label></p>
  <p><label>Notas:<br><textarea name="notas"></textarea></label></p>

  <p><button>Guardar cotización</button> <a href="{url_for('index')}">Volver</a></p>
</form>
<script>
function addItem(){{
  const d=document.createElement('div'); d.className='item';
  d.innerHTML=`<p><label>Concepto: <input name="item_nombre_concepto[]"></label></p>
  <p><label>Unidad: <input name="item_unidad[]"></label></p>
  <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
  <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
  <p><label>Desc %: <input name="item_descuento[]" value="0"></label></p>
  <p><label>Descripción:<br><textarea name="item_descripcion[]"></textarea></label></p>`;
  document.getElementById('items').appendChild(d);
}}
</script>
</body></html>"""
            return html

        # editor fallback (incluye REPRESENTANTE)
        if name == "cotizacion_edit.html":
            c = ctx["c"]
            def row(d):
                return f"""<div class="item">
<p><label>Concepto: <input name="item_nombre_concepto[]" value="{escape(d.nombre_concepto)}"></label></p>
<p><label>Unidad: <input name="item_unidad[]" value="{escape(d.unidad or '')}"></label></p>
<p><label>Cantidad: <input name="item_cantidad[]" value="{d.cantidad}"></label></p>
<p><label>Precio: <input name="item_precio[]" value="{d.precio_unitario}"></label></p>
<p><label>Desc %: <input name="item_descuento[]" value="{d.descuento}"></label></p>
<p><label>Descripción:<br><textarea name="item_descripcion[]">{escape(d.descripcion or '')}</textarea></label></p>
</div>"""
            items_html = "".join(row(d) for d in c.detalles)
            html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Editar'))}</title>
<style>body{{font-family:system-ui;margin:24px}} .item{{border:1px dashed #ccc;padding:8px;margin:8px 0}}</style>
</head><body>
<h1>Editar {escape(c.folio)}</h1>
<form method="post" action="{url_for('actualizar_cotizacion', cot_id=c.id)}">
  <h3>Cliente</h3>
  <p><label>Nombre: <input name="cliente_nombre" value="{escape(c.cliente.nombre_cliente if c.cliente else '')}" required></label></p>
  <p><label>Empresa: <input name="empresa" value="{escape(c.cliente.empresa if c.cliente else '')}"></label></p>
  <p><label>Responsable: <input name="responsable" value="{escape(c.cliente.responsable if c.cliente else '')}"></label></p>
  <p><label>Correo: <input name="correo" type="email" value="{escape(c.cliente.correo if c.cliente else '')}"></label></p>
  <p><label>Teléfono: <input name="telefono" value="{escape(c.cliente.telefono if c.cliente else '')}"></label></p>
  <p><label>Dirección: <input name="direccion" value="{escape(c.cliente.direccion if c.cliente else '')}"></label></p>
  <p><label>RFC: <input name="rfc" value="{escape(c.cliente.rfc if c.cliente else '')}"></label></p>
  <p><label>Representante: <input name="representante" value="{escape(c.representante or '')}" placeholder="Nombre del representante"></label></p>

  <h3>Items</h3>
  <div id="items">{items_html}</div>
  <p><button type="button" onclick="addItem()">Agregar renglón</button></p>

  <h3>Totales</h3>
  <p><label>IVA %: <input name="iva_porc" value="{c.iva_porc}"></label></p>
  <p><label>Estatus:
    <select name="estatus">
      <option {'selected' if c.estatus=='PENDIENTE' else ''}>PENDIENTE</option>
      <option {'selected' if c.estatus=='ENVIADA' else ''}>ENVIADA</option>
      <option {'selected' if c.estatus=='GANADA' else ''}>GANADA</option>
      <option {'selected' if c.estatus=='PERDIDA' else ''}>PERDIDA</option>
    </select>
  </label></p>
  <p><label>Notas:<br><textarea name="notas">{escape(c.notas or '')}</textarea></label></p>

  <p><button>Guardar cambios</button> <a href="{url_for('view_cotizacion', cot_id=c.id)}">Cancelar</a></p>
</form>
<script>
function addItem(){{
  const d=document.createElement('div'); d.className='item';
  d.innerHTML=`<p><label>Concepto: <input name="item_nombre_concepto[]"></label></p>
  <p><label>Unidad: <input name="item_unidad[]"></label></p>
  <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
  <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
  <p><label>Desc %: <input name="item_descuento[]" value="0"></label></p>
  <p><label>Descripción:<br><textarea name="item_descripcion[]"></textarea></label></p>`;
  document.getElementById('items').appendChild(d);
}}
</script>
</body></html>"""
            return html

        if name == "cotizaciones_list.html":
            items = ctx.get("items", [])
            page = ctx.get("page", 1); pages = ctx.get("pages", 1); total = ctx.get("total", 0)
            trs = "".join(
                f"<tr><td>{escape(c.folio)}</td><td>{c.fecha.strftime('%Y-%m-%d %H:%M')}</td>"
                f"<td>{escape(c.estatus)}</td><td>${c.total:.2f}</td>"
                f"<td><a href='{url_for('view_cotizacion', cot_id=c.id)}'>Ver</a> · "
                f"<a href='{url_for('editar_cotizacion', cot_id=c.id)}'>Editar</a></td></tr>"
                for c in items
            )
            nav = " ".join(
                f"<a href='?p={i}'>{i}</a>" if i!=page else f"<b>[{i}]</b>" for i in range(1, pages+1)
            )
            return f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Listado'))}</title></head>
<body>
<h1>Cotizaciones</h1>
<p>Página {page}/{pages} · Total {total}</p>
<p>{nav}</p>
<table border="1" cellspacing="0" cellpadding="6">
<thead><tr><th>Folio</th><th>Fecha</th><th>Estatus</th><th>Total</th><th>Acciones</th></tr></thead>
<tbody>{trs}</tbody></table>
<p>{nav}</p>
<p><a href="{url_for('index')}">Volver</a></p>
</body></html>"""

        if name == "cotizacion_view.html":
            c = ctx.get("c")
            det_rows = "".join(
                f"<tr><td>{d.cantidad:.2f}</td><td>{escape(d.unidad or '')}</td>"
                f"<td>{escape(d.nombre_concepto)}</td><td>${d.precio_unitario:.2f}</td>"
                f"<td>{d.descuento:.2f}</td><td>${d.subtotal:.2f}</td></tr>"
                for d in c.detalles
            )
            return f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Cotización'))}</title></head>
<body>
<h1>{escape(c.folio)}</h1>
<p>Fecha: {c.fecha.strftime('%Y-%m-%d %H:%M')} · Estatus: {escape(c.estatus)} · Representante: {escape(c.representante or '')}</p>
<p><a target="_blank" href="{url_for('export_cotizacion_pdf', cot_id=c.id)}">Ver PDF</a> ·
<a href="{url_for('export_cotizacion_csv', cot_id=c.id)}">Descargar CSV</a> ·
<a href="{url_for('editar_cotizacion', cot_id=c.id)}">Editar</a></p>
<h3>Renglones</h3>
<table border="1" cellspacing="0" cellpadding="6">
<thead><tr><th>Cant</th><th>Unidad</th><th>Concepto</th><th>P.U.</th><th>Desc %</th><th>Subtotal</th></tr></thead>
<tbody>{det_rows}</tbody></table>
<h3>Totales</h3>
<p>Subtotal: ${c.subtotal:.2f} · IVA ({c.iva_porc:.2f}%): ${c.iva_monto:.2f} · <b>Total: ${c.total:.2f}</b></p>
<p><a href="{url_for('index')}">Volver</a></p>
</body></html>"""

        # fallback genérico
        return f"Vista {escape(name)} no disponible", 200

# Reemplazar render_template con el shim
import types as _types
render_template = _types.FunctionType(render_template.__code__, globals(), "render_template")

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        os.makedirs(app.static_folder or "static", exist_ok=True)
    except Exception:
        pass
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=True)
