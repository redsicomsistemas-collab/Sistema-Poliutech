from __future__ import annotations

# =========================================================
# MARWHATS - Sistema Poliutech
# Cambios:
# - "Sistema" en renglones (se elimina "Descuento")
# - Autocreación de clientes y conceptos si no existen
# - PDF: logo izq sin deformar, footer centrado con división, firma abajo,
#   cantidad en letra con “XX/100 M.N.”, separadores de miles
# - Exportación a Excel (.xlsx) con hoja "Cotización" y nombre = folio
# - Estatus editable vía API sin cambiar vistas
# - Generador de folio robusto (evita UNIQUE)
# =========================================================

import os, io, csv, sys, math, re, traceback
from datetime import datetime, timedelta
from typing import Iterable, Optional, List

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, abort
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text, func

# ReportLab (PDF)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    Table, TableStyle, Paragraph, SimpleDocTemplate,
    Spacer, Image
)
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.utils import ImageReader

# Excel
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side, NamedStyle
except Exception:
    Workbook = None  # para que la app arranque aunque no esté openpyxl

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

ADMIN_LIST: List[str] = [x.strip() for x in ADMIN_WHATSAPP_RECIPIENTS.split(",") if x.strip()]

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
    descuento_total = db.Column(db.Float, default=0.0)  # legado (no se usa)
    iva_porc = db.Column(db.Float, default=16.0)
    iva_monto = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)
    notas = db.Column(db.String(3000))
    last_whatsapp_at = db.Column(db.DateTime, nullable=True)
    representante = db.Column(db.String(120))

    cliente = db.relationship("Cliente", backref="cotizaciones")
    detalles = db.relationship("CotizacionDetalle", backref="cotizacion",
                               cascade="all, delete-orphan")

class CotizacionDetalle(db.Model):
    __tablename__ = "cotizacion_detalle"
    id = db.Column(db.Integer, primary_key=True)
    cotizacion_id = db.Column(db.Integer, db.ForeignKey("cotizacion.id"))
    concepto_id = db.Column(db.Integer, db.ForeignKey("concepto.id"), nullable=True)
    nombre_concepto = db.Column(db.String(500), nullable=False)
    unidad = db.Column(db.String(50))
    cantidad = db.Column(db.Float, default=1)
    precio_unitario = db.Column(db.Float, default=0)
    # descuento = db.Column(db.Float, default=0)  # legado
    sistema = db.Column(db.String(200))  # NUEVO
    descripcion = db.Column(db.String(1000))
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

    # columnas en cotizacion
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
        adds.append("ALTER TABLE cotizacion ADD COLUMN notas VARCHAR(3000)")
    if "last_whatsapp_at" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN last_whatsapp_at TIMESTAMP NULL")
    if "representante" not in cols:
        adds.append("ALTER TABLE cotizacion ADD COLUMN representante VARCHAR(120)")
    for sql in adds:
        db.session.execute(text(sql))

    # columnas en detalle
    dcols = _table_columns("cotizacion_detalle")
    dadds = []
    if "sistema" not in dcols:
        dadds.append("ALTER TABLE cotizacion_detalle ADD COLUMN sistema VARCHAR(200)")
    if "descripcion" not in dcols:
        dadds.append("ALTER TABLE cotizacion_detalle ADD COLUMN descripcion VARCHAR(1000)")
    for sql in dadds:
        db.session.execute(text(sql))

    if adds or dadds:
        db.session.commit()

with app.app_context():
    ensure_schema()

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def generar_folio() -> str:
    """
    Robustez: obtiene el número máximo de PTCH-#### y suma 1.
    Si aún existe por carrera, intenta siguientes 10 valores.
    """
    prefix = "PTCH-"
    # Buscar el mayor consecutivo actual
    maxn = 0
    rows = db.session.execute(text(
        "SELECT folio FROM cotizacion WHERE folio LIKE 'PTCH-%'"
    )).fetchall()
    for (folio,) in rows:
        m = re.match(r"PTCH-(\d{4})$", str(folio))
        if m:
            n = int(m.group(1))
            if n > maxn:
                maxn = n
    for i in range(1, 11):
        cand = f"{prefix}{maxn+i:04d}"
        exists = db.session.execute(text(
            "SELECT 1 FROM cotizacion WHERE folio=:f LIMIT 1"
        ), {"f": cand}).fetchone()
        if not exists:
            return cand
    # fallback timestamp
    return f"{prefix}{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

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

def money(n: float) -> str:
    # separador de miles con coma, 2 decimales
    try:
        return "${:,.2f}".format(float(n or 0))
    except Exception:
        return "${:,.2f}".format(0)

def cantidad_en_letra_mn(total: float) -> str:
    """
    En palabras + 'XX/100 M.N.' sin mostrar el número como dígito (salvo fracción XX/100).
    """
    try:
        from num2words import num2words
    except Exception:
        # Fallback muy básico
        entero = int(total)
        cents = int(round((total - entero) * 100)) % 100
        return f"Cantidad en letra: {entero} pesos {cents:02d}/100 M.N."
    entero = int(total)
    cents = int(round((total - entero) * 100)) % 100
    palabras = num2words(entero, lang="es").replace(" y cero", "").strip()
    # Capitalizar primera letra
    if palabras:
        palabras = palabras[0].upper() + palabras[1:]
    return f"Cantidad en letra: {palabras} pesos {cents:02d}/100 M.N."

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

    # Cliente (crear si no existe)
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

    # Crear cotización
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
    sistemas = f.getlist("item_sistema[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios))
    for i in range(n):
        nom = (nombres[i] if i < len(nombres) else "").strip()
        if not nom:
            continue
        uni = (unidades[i] if i < len(unidades) else "").strip()
        cant = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        pu   = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        sis  = (sistemas[i] if i < len(sistemas) else "").strip()
        desc = (descripciones[i] if i < len(descripciones) else "") or ""

        line_subtotal = cant * pu
        subtotal += line_subtotal

        # Autocrear concepto si no existe
        concepto = Concepto.query.filter_by(nombre_concepto=nom).first()
        if not concepto:
            concepto = Concepto(
                nombre_concepto=nom,
                unidad=uni or None,
                precio_unitario=pu,
                descripcion=desc or None
            )
            db.session.add(concepto)
            db.session.flush()

        det = CotizacionDetalle(
            cotizacion_id=cot.id,
            concepto_id=concepto.id if concepto else None,
            nombre_concepto=nom,
            unidad=uni,
            cantidad=cant,
            precio_unitario=pu,
            sistema=sis or None,
            descripcion=desc,
            subtotal=line_subtotal
        )
        db.session.add(det)

    iva_monto = subtotal * (iva_porc/100.0)
    total = subtotal + iva_monto
    cot.subtotal = fmt(subtotal)
    cot.iva_porc = fmt(iva_porc)
    cot.iva_monto = fmt(iva_monto)
    cot.total = fmt(total)
    db.session.commit()

    # WhatsApp admins
    try:
        msg = (
            "🧾 *Nueva Cotización Creada*\n"
            f"Folio: *{cot.folio}*\n"
            f"Estatus: *{cot.estatus}*\n"
            f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\n"
            f"Total: {money(cot.total)}"
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

    # Cliente (crear si no existe)
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

    # Borrar detalles y re-crear
    for d in list(c.detalles):
        db.session.delete(d)

    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    sistemas = f.getlist("item_sistema[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios))
    for i in range(n):
        nom = (nombres[i] if i < len(nombres) else "").strip()
        if not nom:
            continue
        uni = (unidades[i] if i < len(unidades) else "").strip()
        cant = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        pu   = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        sis  = (sistemas[i] if i < len(sistemas) else "").strip()
        desc = (descripciones[i] if i < len(descripciones) else "") or ""

        line_subtotal = cant * pu
        subtotal += line_subtotal

        concepto = Concepto.query.filter_by(nombre_concepto=nom).first()
        if not concepto:
            concepto = Concepto(
                nombre_concepto=nom,
                unidad=uni or None,
                precio_unitario=pu,
                descripcion=desc or None
            )
            db.session.add(concepto)
            db.session.flush()

        det = CotizacionDetalle(
            cotizacion_id=c.id,
            concepto_id=concepto.id if concepto else None,
            nombre_concepto=nom,
            unidad=uni,
            cantidad=cant,
            precio_unitario=pu,
            sistema=sis or None,
            descripcion=desc,
            subtotal=line_subtotal
        )
        db.session.add(det)

    iva_monto = subtotal * (iva_porc/100.0)
    total = subtotal + iva_monto
    c.subtotal = fmt(subtotal)
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
# API: actualizar estatus (Dashboard inline)
# ---------------------------------------------------------
# =========================================================
# ACTUALIZACIÓN DE ESTATUS DESDE DASHBOARD (inline)
# =========================================================
@app.route("/api/cotizaciones/<int:cot_id>/estatus", methods=["POST"])
def api_update_estatus(cot_id):
    c = Cotizacion.query.get_or_404(cot_id)
    data = request.get_json(force=True)
    nuevo = (data.get("estatus") or "").upper().strip()

    if nuevo not in ["PENDIENTE", "ENVIADA", "GANADA", "PERDIDA"]:
        return jsonify({"ok": False, "error": "Estatus inválido"}), 400

    anterior = c.estatus
    c.estatus = nuevo
    db.session.commit()

    # 🔔 Enviar WhatsApp de notificación si cambió
    try:
        if twilio_client and nuevo != anterior:
            body = (
                f"🔄 *Actualización de estatus*\n"
                f"Folio: *{c.folio}*\n"
                f"Anterior: {anterior}\n"
                f"Nuevo: *{nuevo}*\n"
                f"Total: ${c.total:,.2f}"
            )
            send_whatsapp_multi(ADMIN_LIST, body)
    except Exception as e:
        print(f"[Twilio] Error al enviar notificación de estatus: {e}")

    return jsonify({"ok": True, "estatus": nuevo})



# ---------------------------------------------------------
# Exportaciones (CSV / Excel / PDF)
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/export.csv")
def export_cotizacion_csv(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["Folio","Fecha","Estatus","Representante","Cliente","Empresa","Subtotal","IVA %","IVA $","Total","Notas"])
    w.writerow([
        c.folio, c.fecha.strftime("%Y-%m-%d %H:%M"), c.estatus, (c.representante or ""),
        c.cliente.nombre_cliente if c.cliente else "",
        c.cliente.empresa if c.cliente else "",
        f"{c.subtotal:.2f}",
        f"{c.iva_porc:.2f}", f"{c.iva_monto:.2f}",
        f"{c.total:.2f}", (c.notas or "")
    ])
    w.writerow([])
    w.writerow(["Cant","Unidad","Concepto","Sistema","PU","Subtotal","Descripción"])
    for d in c.detalles:
        w.writerow([
            d.cantidad, d.unidad or "", d.nombre_concepto, d.sistema or "",
            f"{d.precio_unitario:.2f}", f"{d.subtotal:.2f}", (d.descripcion or "")
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={'Content-Disposition': f'attachment; filename="{c.folio or "cotizacion"}.csv"'}
    )
# =========================================================
# EXPORTAR COTIZACIÓN A EXCEL (formato similar al PDF)
# =========================================================
@app.route("/cotizaciones/<int:cot_id>/export.xlsx")
def export_cotizacion_excel(cot_id: int):
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from io import BytesIO

    c = Cotizacion.query.get_or_404(cot_id)

    wb = Workbook()
    ws = wb.active
    ws.title = "Cotización"

    # Encabezado principal
    ws.merge_cells("A1:F1")
    ws["A1"] = f"COTIZACIÓN POLIUTECH - {c.folio}"
    ws["A1"].font = Font(bold=True, size=14, color="0D47A1")
    ws["A1"].alignment = Alignment(horizontal="center")

    # Datos generales
    ws["A3"], ws["B3"] = "Cliente:", c.cliente.nombre_cliente if c.cliente else ""
    ws["A4"], ws["B4"] = "Empresa:", c.cliente.empresa if c.cliente else ""
    ws["A5"], ws["B5"] = "Fecha:", c.fecha.strftime("%d/%m/%Y %H:%M")
    ws["A6"], ws["B6"] = "Representante:", c.representante or ""
    ws["A7"], ws["B7"] = "Estatus:", c.estatus

       # Tabla de detalles
    headers = ["Cant", "Unidad", "Concepto", "Sistema", "Precio Unitario", "Subtotal"]
    ws.append([])
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="0D47A1")
    header_font = Font(bold=True, color="FFFFFF")
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=9, column=col)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    row = 10
    for d in c.detalles:
        ws.append([
            d.cantidad,
            d.unidad or "",
            d.nombre_concepto,
            getattr(d, "sistema", ""),  # <- en caso de que exista el campo nuevo
            d.precio_unitario,
            d.subtotal
        ])
        row += 1


    # Totales
    ws.append([])
    ws.append(["", "", "", "Subtotal", "", c.subtotal])
    ws.append(["", "", "", f"IVA {c.iva_porc}%", "", c.iva_monto])
    ws.append(["", "", "", "Total", "", c.total])

    # Notas
    ws.append([])
    if c.notas:
        ws.append(["Notas:", c.notas])

    # Formato general
    thin = Side(border_style="thin", color="CCCCCC")
    for r in ws.iter_rows(min_row=9, max_col=6):
        for cell in r:
            cell.border = Border(top=thin, left=thin, right=thin, bottom=thin)
            cell.alignment = Alignment(vertical="center")

    # Ajuste de anchos
    widths = [8, 12, 50, 15, 10, 15]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[chr(64+i)].width = w

    # Guardar a memoria
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{c.folio}.xlsx"'}
    )

@app.route("/cotizaciones/<int:cot_id>/export.xlsx")
def export_cotizacion_xlsx(cot_id: int):
    if Workbook is None:
        abort(501, description="openpyxl no instalado en el servidor.")
    c = Cotizacion.query.get_or_404(cot_id)
    wb = Workbook()
    ws = wb.active
    ws.title = "Cotización"

    # Estilos
    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    right = Alignment(horizontal="right", vertical="center")
    left = Alignment(horizontal="left", vertical="top", wrap_text=True)
    header_fill = PatternFill("solid", fgColor="0D47A1")  # azul
    white = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="DDDDDD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    ws.merge_cells("A1:F1"); ws["A1"] = f"COTIZACIÓN {c.folio}"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A1"].alignment = center

    ws.append(["Folio", c.folio, "", "Fecha", c.fecha.strftime("%d/%m/%Y %H:%M"), ""])
    ws.append(["Cliente", (c.cliente.nombre_cliente if c.cliente else ""), "", "Empresa", (c.cliente.empresa if c.cliente else ""), ""])
    ws.append(["Representante", c.representante or "", "", "Estatus", c.estatus, ""])
    ws.append([])
    # Encabezados tabla
    headers = ["Cant", "Unidad", "Concepto", "Sistema", "Precio Unit.", "Subtotal"]
    ws.append(headers)
    for col, name in enumerate(headers, 1):
        cell = ws.cell(row=ws.max_row, column=col)
        cell.fill = header_fill; cell.font = white; cell.alignment = center; cell.border = border

    for d in c.detalles:
        ws.append([d.cantidad, d.unidad or "", d.nombre_concepto, d.sistema or "", float(d.precio_unitario or 0), float(d.subtotal or 0)])
        for col in range(1, 7):
            ws.cell(row=ws.max_row, column=col).border = border
            if col in (5, 6):
                ws.cell(row=ws.max_row, column=col).number_format = '"$"#,##0.00'
            if col in (1,):
                ws.cell(row=ws.max_row, column=col).number_format = '0.00'
            if col in (3,):
                ws.cell(row=ws.max_row, column=col).alignment = left

    ws.append([])
    start = ws.max_row + 1
    ws.append(["", "", cantidad_en_letra_mn(c.total)])
    ws.append(["", "Subtotal:", float(c.subtotal or 0)])
    ws.append(["", f"IVA ({c.iva_porc:.2f}%):", float(c.iva_monto or 0)])
    ws.append(["", "Total:", float(c.total or 0)])
    # Formatos totales
    for r in range(start+1, start+4):
        ws.cell(row=r, column=2).font = bold
        ws.cell(row=r, column=3).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=3).alignment = right

    # Anchos
    ws.column_dimensions["A"].width = 10
    ws.column_dimensions["B"].width = 10
    ws.column_dimensions["C"].width = 70
    ws.column_dimensions["D"].width = 25
    ws.column_dimensions["E"].width = 15
    ws.column_dimensions["F"].width = 15

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    return Response(
        bio.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{c.folio}.xlsx"'}
    )

# ======= PDF =======
@app.route("/cotizaciones/<int:cot_id>/export.pdf")
def export_cotizacion_pdf(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm, topMargin=58*mm, bottomMargin=36*mm
    )
    styles = getSampleStyleSheet()
    elems = []

    from reportlab.lib.styles import ParagraphStyle
    styles.add(ParagraphStyle(name="Encabezado", fontSize=9, leading=12, spaceAfter=4))
    styles.add(ParagraphStyle(name="NormalRight", fontSize=9, alignment=2))

    # Encabezado corporativo
    def encabezado(canv, doc_):
        canv.saveState()
        # franja azul superior
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1]-22, A4[0], 22, stroke=0, fill=1)

        # Logo
        logo_path = os.path.join(app.static_folder or "static", "logo.jpg")
        x_logo = 20
        y_logo = A4[1] - 22 - 5
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 60*mm
                scale = max_w / float(iw)
                w = max_w
                h = ih * scale
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

    # Footer corporativo + firma (firma justo arriba del pie)
    def footer(canv, doc_):
        canv.saveState()

        # Firma centrada justo arriba del footer
        canv.setFont("Helvetica", 9)
        canv.setFillColor(colors.black)
        y_firma = 80  # un poco encima del pie
        canv.drawCentredString(A4[0]/2, y_firma + 20, "Atte.")
        canv.setFont("Helvetica-Bold", 9)
        canv.drawCentredString(A4[0]/2, y_firma + 8, "Ing. César Antonio Garza Guerrero")
        canv.setFont("Helvetica", 9)
        canv.drawCentredString(A4[0]/2, y_firma - 4, "DIRECTOR GENERAL")

        # División
        division_path = os.path.join(app.static_folder or "static", "division.png")
        if os.path.exists(division_path):
            try:
                canv.drawImage(division_path, (A4[0]-155*mm)/2, 45, width=155*mm, height=3*mm, mask="auto")
            except Exception:
                pass

        # Texto footer
        canv.setFont("Helvetica-Bold", 9)
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.drawCentredString(A4[0]/2, 35, "POLIUTECH – Recubrimientos Especializados")

        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#333333"))
        line1 = "Campos Elíseos 223 Oficina 602 · Col. Polanco V Sección · Miguel Hidalgo, CDMX 11560"
        line2 = "Tel: 55 5938 6530 / 55 5938 0536 · info@poliutech.com · www.poliutech.com"
        canv.drawCentredString(A4[0]/2, 25, line1)
        canv.drawCentredString(A4[0]/2, 15, line2)

        # título del documento
        try:
            canv.setTitle(c.folio or "Cotizacion")
        except Exception:
            pass
        canv.restoreState()

    # Datos generales
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

    # Tabla
    data = [["Cant", "Unidad", "Concepto", "Sistema", "Precio Unit.", "Subtotal"]]
    for d in c.detalles:
        data.append([
            f"{d.cantidad:.2f}",
            d.unidad or "",
            Paragraph(d.nombre_concepto, styles["Normal"]),
            Paragraph(d.sistema or "", styles["Normal"]),
            money(d.precio_unitario),
            money(d.subtotal),
        ])
    tbl = Table(data, colWidths=[18*mm, 20*mm, 70*mm, 30*mm, 25*mm, 25*mm], repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#0d47a1")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("ALIGN", (4,1), (-1,-1), "RIGHT"),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("FONTSIZE", (0,0), (-1,-1), 9),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    elems.append(tbl)
    elems.append(Spacer(1, 12))

    # -----------------------------------------------------
# 🔠 CANTIDAD EN LETRA (correctamente escrita en español)
# -----------------------------------------------------
try:
    from num2words import num2words

    total = float(c.total or 0)
    enteros = int(total)
    centavos = int(round((total - enteros) * 100))

    # Convertir parte entera a texto (num2words en español)
    palabras = num2words(enteros, lang='es')

    # Ajuste de estilo: "uno peso" -> "un peso"
    if palabras.endswith(" uno"):
        palabras = palabras[:-4] + " un"

    # Determinar singular/plural
    palabra_peso = "peso" if enteros == 1 else "pesos"

    # Crear el texto final
    cantidad_letra = f"{palabras.capitalize()} {palabra_peso} {centavos:02d}/100 M.N."

    elems.append(Spacer(1, 8))
    elems.append(Paragraph(f"<b>Cantidad en letra:</b> {cantidad_letra}", styles["Normal"]))
    elems.append(Spacer(1, 6))

except Exception as e:
    print(f"[PDF] Error generando cantidad en letra: {e}")


    # Totales
    tot_data = [
        ["Subtotal:", money(c.subtotal)],
        [f"IVA ({c.iva_porc:.2f}%):", money(c.iva_monto)],
        ["Total:", money(c.total)],
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
    elems.append(Spacer(1, 6))

    # Notas (respetar saltos de línea)
    if c.notas:
        elems.append(Paragraph("<b>Notas:</b>", styles["Encabezado"]))
        for line in str(c.notas).replace("\r\n", "\n").split("\n"):
            if line.strip():
                elems.append(Paragraph(line.strip(), styles["Normal"]))
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
            "export_xlsx": url_for("export_cotizacion_xlsx", cot_id=c.id),
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
                        "🔔 *Recordatorio: Cotización PENDIENTE*\n"
                        f"Folio: *{cot.folio}*\n"
                        f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\n"
                        f"Total: {money(cot.total)}"
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
                    f"<td>{escape('${:,.2f}'.format(c.total))}</td>"
                    f"<td>"
                    f"<a href='{url_for('view_cotizacion', cot_id=c.id)}'>Ver</a> · "
                    f"<a target='_blank' href='{url_for('export_cotizacion_pdf', cot_id=c.id)}'>PDF</a> · "
                    f"<a href='{url_for('export_cotizacion_csv', cot_id=c.id)}'>CSV</a> · "
                    f"<a href='{url_for('export_cotizacion_xlsx', cot_id=c.id)}'>Excel</a> · "
                    f"<a href='{url_for('editar_cotizacion', cot_id=c.id)}'>Editar</a>"
                    f"</td>"
                    "</tr>"
                )
            html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Dashboard'))}</title>
<style>
body{{font-family:system-ui; margin:24px}}
table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #ddd;padding:8px}}
</style>
<script>
async function setEstatus(id, est){{
  const fd = new FormData(); fd.append('estatus', est);
  const r = await fetch('/api/cotizaciones/'+id+'/estatus', {{method:'POST', body:fd}});
  if(r.ok) location.reload(); else alert('Error al cambiar estatus');
}}
</script>
</head><body>
<h1>MARWHATS · Dashboard</h1>
<p> Cotizaciones: <b>{total_cotizaciones}</b> · Importe total: <b>${total_importe:.2f}</b> · Conceptos: <b>{total_catalogo}</b></p>
<p><a href="{url_for('cotizador')}">Crear nueva</a> · <a href="{url_for('admin_catalogos')}">Admin catálogos</a></p>
<table>
<thead><tr><th>Folio</th><th>Fecha</th><th>Estatus</th><th>Total</th><th>Acciones</th></tr></thead>
<tbody>{rows}</tbody></table>
</body></html>"""
            return html

        # cotizador fallback (incluye REPRESENTANTE y SISTEMA)
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
      <p><label>Sistema: <input name="item_sistema[]"></label></p>
      <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
      <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
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
  <p><label>Notas:<br><textarea name="notas" rows="6" placeholder="Puedes pegar aquí las CONDICIONES COMERCIALES y se respetarán los saltos de línea."></textarea></label></p>

  <p><button>Guardar cotización</button> <a href="{url_for('index')}">Volver</a></p>
</form>
<script>
function addItem(){{
  const d=document.createElement('div'); d.className='item';
  d.innerHTML=`<p><label>Concepto: <input name="item_nombre_concepto[]"></label></p>
  <p><label>Unidad: <input name="item_unidad[]"></label></p>
  <p><label>Sistema: <input name="item_sistema[]"></label></p>
  <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
  <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
  <p><label>Descripción:<br><textarea name="item_descripcion[]"></textarea></label></p>`;
  document.getElementById('items').appendChild(d);
}}
</script>
</body></html>"""
            return html

        # editor fallback (incluye SISTEMA)
        if name == "cotizacion_edit.html":
            c = ctx["c"]
            def row(d):
                return f"""<div class="item">
<p><label>Concepto: <input name="item_nombre_concepto[]" value="{escape(d.nombre_concepto)}"></label></p>
<p><label>Unidad: <input name="item_unidad[]" value="{escape(d.unidad or '')}"></label></p>
<p><label>Sistema: <input name="item_sistema[]" value="{escape(d.sistema or '')}"></label></p>
<p><label>Cantidad: <input name="item_cantidad[]" value="{d.cantidad}"></label></p>
<p><label>Precio: <input name="item_precio[]" value="{d.precio_unitario}"></label></p>
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
  <p><label>Notas:<br><textarea name="notas" rows="6">{escape(c.notas or '')}</textarea></label></p>

  <p><button>Guardar cambios</button> <a href="{url_for('view_cotizacion', cot_id=c.id)}">Cancelar</a></p>
</form>
<script>
function addItem(){{
  const d=document.createElement('div'); d.className='item';
  d.innerHTML=`<p><label>Concepto: <input name="item_nombre_concepto[]"></label></p>
  <p><label>Unidad: <input name="item_unidad[]"></label></p>
  <p><label>Sistema: <input name="item_sistema[]"></label></p>
  <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
  <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
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
                f"<td>{escape(c.estatus)}</td><td>{escape('${:,.2f}'.format(c.total))}</td>"
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
                f"<td>{escape(d.nombre_concepto)}</td><td>{escape(d.sistema or '')}</td>"
                f"<td>{'${:,.2f}'.format(d.precio_unitario)}</td>"
                f"<td>{'${:,.2f}'.format(d.subtotal)}</td></tr>"
                for d in c.detalles
            )
            return f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>{escape(ctx.get('title','Cotización'))}</title></head>
<body>
<h1>{escape(c.folio)}</h1>
<p>Fecha: {c.fecha.strftime('%Y-%m-%d %H:%M')} · Estatus: {escape(c.estatus)} · Representante: {escape(c.representante or '')}</p>
<p><a target="_blank" href="{url_for('export_cotizacion_pdf', cot_id=c.id)}">Ver PDF</a> ·
<a href="{url_for('export_cotizacion_csv', cot_id=c.id)}">Descargar CSV</a> ·
<a href="{url_for('export_cotizacion_xlsx', cot_id=c.id)}">Descargar Excel</a> ·
<a href="{url_for('editar_cotizacion', cot_id=c.id)}">Editar</a></p>
<h3>Renglones</h3>
<table border="1" cellspacing="0" cellpadding="6">
<thead><tr><th>Cant</th><th>Unidad</th><th>Concepto</th><th>Sistema</th><th>P.U.</th><th>Subtotal</th></tr></thead>
<tbody>{det_rows}</tbody></table>
<h3>Totales</h3>
<p>Subtotal: {'${:,.2f}'.format(c.subtotal)} · IVA ({c.iva_porc:.2f}%): {'${:,.2f}'.format(c.iva_monto)} · <b>Total: {'${:,.2f}'.format(c.total)}</b></p>
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
