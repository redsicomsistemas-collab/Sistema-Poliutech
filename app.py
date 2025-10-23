from __future__ import annotations

# =========================================================
# MARWHATS (checkpoint) - app.py  [ADMIN ONLY con config local embebida]
# Basado en MAR9_ADMIN_ONLY estable + mejoras de robustez.
#
# Mantiene:
#  - Dashboard, métricas, filtros y gráficas (APIs /api/dashboard/*).
#  - Cotizador con autocompletar (clientes y conceptos).
#  - Importación de catálogos CSV/XLSX (Clientes / Conceptos).
#  - Exportaciones CSV y PDF (ReportLab).
#  - API de búsqueda para el dashboard (fechas, montos, estatus).
#  - WhatsApp (Twilio) al crear cotización.
#  - WhatsApp al cambiar estatus (ENVIADA, GANADA, PERDIDA, PENDIENTE).
#  - Recordatorios cada 24h si sigue PENDIENTE (control last_whatsapp_at).
#
# Cambios clave en esta versión:
#  - SIN .env: configuración embebida al inicio (se puede editar aquí mismo).
#  - ADMIN ONLY con múltiples destinos: ADMIN_WHATSAPP_RECIPIENTS (coma-sep).
#  - Manejo robusto de Twilio (omite envío si falta config; no rompe la app).
#  - Scheduler no se duplica con el reloader en modo debug.
#  - Rutas utilitarias de depuración y administración opcionales.
# =========================================================

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, Response, abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from datetime import datetime, timedelta
import os, io, csv, sys, math
import pandas as pd

# PDF / Reportes
from reportlab.lib.pagesizes import A4, letter
from reportlab.platypus import Table, TableStyle, Paragraph, SimpleDocTemplate, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet

# WhatsApp (Twilio) + Scheduler
from twilio.rest import Client as TwilioClient
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------
# 0) CONFIGURACIÓN LOCAL EMBEBIDA (SIN .env)
#    - Puedes editar estos valores directamente.
#    - Si Twilio no es válido, la app sigue corriendo; solo se omiten envíos.
# ---------------------------------------------------------

# Clave Flask y base de datos (SQLite local por defecto)
LOCAL_FLASK_SECRET_KEY = "poliutech_mar_checkpoint_superseguro"
LOCAL_DATABASE_URL     = "sqlite:///mar3.db"

# Twilio: usa tus valores reales para enviar WhatsApp
import os

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN")
ADMIN_WHATSAPP     = os.getenv("ADMIN_WHATSAPP", "whatsapp:+5215521323076")


# Admin(es) destino (ADMIN ONLY). Uno o varios, separados por coma:
# Formato aceptado: "whatsapp:+52155...." o solo "+52155..." o "5512345678" (se normaliza a +52 si falta)
LOCAL_ADMIN_WHATSAPP_RECIPIENTS = "whatsapp:+5215521323076,whatsapp:+5215610035643,whatsapp:+14055619808"

# ---------------------------------------------------------
# INYECCIÓN EN os.environ (para no tocar el resto del código)
# ---------------------------------------------------------

# ---------------------------------------------------------
# 1) FLASK + SQLALCHEMY (instancias globales)
# ---------------------------------------------------------
app = Flask(__name__)
db = SQLAlchemy()

# ---------------------------------------------------------
# 2) CREDENCIALES / CONFIG (leídas ya desde os.environ)
# ---------------------------------------------------------
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN",  "").strip()
TWILIO_WHATSAPP    = os.getenv("TWILIO_WHATSAPP",    "").strip()  # ej. whatsapp:+14155238886

_admin_list_env = os.getenv("ADMIN_WHATSAPP_RECIPIENTS", "").strip()
if _admin_list_env:
    ADMIN_WHATSAPP_LIST = [x.strip() for x in _admin_list_env.split(",") if x.strip()]
else:
    _single = os.getenv("ADMIN_WHATSAPP", "").strip()
    ADMIN_WHATSAPP_LIST = [_single] if _single else []

# Cliente Twilio (opcional; la app funciona aunque no exista)
twilio_client = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    try:
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        print("[Twilio] Cliente inicializado.")
    except Exception as e:
        print(f"[Twilio] No se pudo inicializar el cliente: {e}", file=sys.stderr)
else:
    print("[Twilio] Credenciales no configuradas; se omiten envíos.", file=sys.stderr)

def _can_send_whatsapp() -> bool:
    """True si hay cliente Twilio, remitente y al menos un admin destino."""
    return bool(twilio_client and TWILIO_WHATSAPP and ADMIN_WHATSAPP_LIST)

def normalize_whatsapp(number: str) -> str:
    """
    Acepta formatos:
      - "whatsapp:+52155..."  -> OK
      - "+52155..."           -> agrega prefijo whatsapp:
      - "5512345678"          -> agrega +52 y prefijo whatsapp:
    """
    if not number:
        return ""
    n = number.strip()
    if n.startswith("whatsapp:"):
        return n
    if n.startswith("+"):
        return f"whatsapp:{n}"
    # MX por defecto:
    digits = "".join(ch for ch in n if ch.isdigit())
    if not digits:
        return ""
    return f"whatsapp:+52{digits}"

def send_whatsapp_multi(to_list, body: str):
    """
    Envía WhatsApp a múltiples destinatarios (ADMIN ONLY).
    Si configuración no está completa, lo omite sin romper.
    """
    if not to_list:
        print("[Twilio] Sin destinatarios; omito envío.")
        return
    if not _can_send_whatsapp():
        print("[Twilio] Configuración incompleta; omito envío.")
        return

    for to in to_list:
        to_norm = normalize_whatsapp(to)
        if not to_norm:
            print(f"[Twilio] Destinatario inválido: {to}")
            continue
        try:
            print(f"[Twilio] Enviando a {to_norm} :: {body}")
            msg = twilio_client.messages.create(from_=TWILIO_WHATSAPP, to=to_norm, body=body)
            print(f"[Twilio] OK SID={msg.sid}")
        except Exception as e:
            print(f"[Twilio] ERROR enviando a {to_norm}: {e}", file=sys.stderr)

# ---------------------------------------------------------
# 3) MODELOS
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

    def __repr__(self):
        return f"<Cliente {self.id} {self.nombre_cliente}>"

class Concepto(db.Model):
    __tablename__ = "concepto"
    id = db.Column(db.Integer, primary_key=True)
    nombre_concepto = db.Column(db.String(200), nullable=False)
    unidad = db.Column(db.String(50))
    precio_unitario = db.Column(db.Float, default=0)
    descripcion = db.Column(db.String(500))

    def __repr__(self):
        return f"<Concepto {self.id} {self.nombre_concepto}>"

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
    # Control de recordatorios (último WhatsApp enviado por PENDIENTE)
    last_whatsapp_at = db.Column(db.DateTime, nullable=True)

    cliente = db.relationship("Cliente", backref="cotizaciones")
    detalles = db.relationship("CotizacionDetalle", backref="cotizacion", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Cotizacion {self.folio or self.id} {self.estatus} ${self.total:.2f}>"

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
# 4) CONFIGURAR APP Y DB
# ---------------------------------------------------------
def configure_app(app_instance: Flask):
    app_instance.secret_key = os.getenv("FLASK_SECRET_KEY", LOCAL_FLASK_SECRET_KEY)
    app_instance.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", LOCAL_DATABASE_URL)
    app_instance.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app_instance)
    return app_instance

app = configure_app(app)

def _table_columns(table_name: str):
    """Retorna el set de nombres de columnas de una tabla SQLite."""
    res = db.session.execute(text(f"PRAGMA table_info('{table_name}')")).mappings().all()
    return {row["name"] for row in res}

def ensure_schema():
    """Crea tablas y asegura columnas nuevas (migración simple)."""
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
    for sql in adds:
        db.session.execute(text(sql))
    if adds:
        db.session.commit()

with app.app_context():
    ensure_schema()

# ---------------------------------------------------------
# 5) HELPERS
# ---------------------------------------------------------
def generar_folio():
    """Genera folio incremental: PTCH-0001, PTCH-0002, ..."""
    n = db.session.query(db.func.count(Cotizacion.id)).scalar() or 0
    return f"PTCH-{n+1:04d}"


def fmt(n: float) -> float:
    """Convierte a float con dos decimales."""
    try:
        return round(float(n or 0), 2)
    except:
        return 0.0

def _parse_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace("$", "").replace(",", "").strip()
        return float(s) if s else default
    except:
        return default

# ---------------------------------------------------------
# 6) RUTAS PRINCIPALES (VISTAS HTML)
# ---------------------------------------------------------
@app.route("/")
def index():
    """
    Dashboard:
      - KPIs: total cotizaciones, importe total, total catálogo.
      - Lista (últimas 100) con acciones (export CSV/PDF, cambio estatus).
    """
    total_cotizaciones = Cotizacion.query.count()
    total_importe = db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0
    total_catalogo = Concepto.query.count()
    cotizaciones = Cotizacion.query.order_by(Cotizacion.fecha.desc()).limit(100).all()
    return render_template("dashboard.html",
                           total_cotizaciones=total_cotizaciones,
                           total_importe=float(total_importe),
                           total_catalogo=total_catalogo,
                           cotizaciones=cotizaciones)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "now_utc": datetime.utcnow().isoformat()}), 200

@app.route("/cotizador")
def cotizador():
    """Pantalla del cotizador (form apunta a url_for('crear_cotizacion'))."""
    return render_template("cotizador.html")

@app.route("/admin/catalogos")
def admin_catalogos():
    """
    Subida de catálogos. Tabla muestra últimos registros cargados.
    Form apunta a url_for('upload_catalogo').
    """
    clientes = Cliente.query.order_by(Cliente.id.desc()).limit(10).all()
    conceptos = Concepto.query.order_by(Concepto.id.desc()).limit(10).all()
    return render_template("admin_catalogos.html", clientes=clientes, conceptos=conceptos)

# ---------------------------------------------------------
# 7) AUTOCOMPLETAR (AJAX)
# ---------------------------------------------------------
@app.route("/api/clientes/suggest")
def api_clientes_suggest():
    """Sugerencias de clientes por nombre_cliente ilike %q%."""
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])
    res = Cliente.query.filter(Cliente.nombre_cliente.ilike(f"%{q}%")) \
                       .order_by(Cliente.nombre_cliente).limit(10).all()
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
    """Sugerencias de conceptos por nombre_concepto ilike %q%."""
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])
    res = Concepto.query.filter(Concepto.nombre_concepto.ilike(f"%{q}%")) \
                         .order_by(Concepto.nombre_concepto).limit(10).all()
    return jsonify([{
        "label": c.nombre_concepto,
        "nombre_concepto": c.nombre_concepto,
        "unidad": c.unidad,
        "precio_unitario": c.precio_unitario,
        "descripcion": c.descripcion
    } for c in res])

# ---------------------------------------------------------
# 8) CREAR COTIZACIÓN (POST) + WhatsApp inmediato al ADMIN
# ---------------------------------------------------------
@app.route("/cotizaciones/crear", methods=["POST"])
def crear_cotizacion():
    """
    Crea cotización a partir del formulario del cotizador.
    Crea también el cliente si no existe (por nombre + empresa).
    Genera renglones, calcula totales.
    Al final: envía WhatsApp inmediato a los ADMIN(s).
    """
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

    # IVA
    iva_porc = _parse_float(f.get("iva_porc"), 16.0)

    # Cabecera cotización
    cot = Cotizacion(
        folio=generar_folio(),
        cliente_id=cliente.id if cliente else None,
        estatus=(f.get("estatus") or "PENDIENTE").upper(),
        notas=f.get("notas"),
        last_whatsapp_at=None
    )
    db.session.add(cot)
    db.session.flush()

    # Renglones
    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    descuentos = f.getlist("item_descuento[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    descuento_total = 0.0

    num_items = max(len(nombres), len(unidades), len(cantidades), len(precios), len(descuentos))
    for i in range(num_items):
        nom = (nombres[i] if i < len(nombres) else "").strip()
        if not nom:
            continue
        uni = (unidades[i] if i < len(unidades) else "").strip()
        cant = _parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        pu   = _parse_float(precios[i] if i < len(precios) else 0, 0.0)
        dsc  = _parse_float(descuentos[i] if i < len(descuentos) else 0, 0.0)
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

    # WhatsApp inmediato a ADMIN(s) (no al cliente)
    try:
        msg = (
            "🧾 *Nueva Cotización Creada*\n"
            f"Folio: *{cot.folio}*\n"
            f"Estatus: *{cot.estatus}*\n"
            f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\n"
            f"Total: ${cot.total:.2f}"
        )
        send_whatsapp_multi(ADMIN_WHATSAPP_LIST, msg)
    except Exception as e:
        print(f"[WARN] No se pudo enviar WhatsApp de creación ({cot.folio}): {e}", file=sys.stderr)

    flash(f"Cotización {cot.folio} creada correctamente.", "success")
    return redirect(url_for("cotizador"))

# ---------------------------------------------------------
# 9) ADMIN: Importación de catálogos (CSV/XLSX)
# ---------------------------------------------------------
@app.route("/admin/catalogos/upload", methods=["POST"])
def upload_catalogo():
    """Sube catálogos de Clientes o Conceptos desde CSV/XLSX."""
    tipo = (request.form.get("tipo") or "").strip()
    file = request.files.get("archivo")
    if not tipo or not file or not getattr(file, "filename", ""):
        flash("Debe seleccionar un tipo y un archivo válido.", "danger")
        return redirect(url_for("admin_catalogos"))

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
                pu = _parse_float(r.get("precio_unitario", 0), 0.0)
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
# 10) API: Búsqueda para dashboard (filtros)
# ---------------------------------------------------------
@app.route("/api/cotizaciones/search")
def api_cotizaciones_search():
    """
    Filtros:
      - estatus
      - fecha inicial (fi) / final (ff) ISO-8601
      - monto mínimo (mmin) / máximo (mmax)
    Retorna hasta 500 registros con URLs de export.
    """
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
        except: pass
    if ff:
        try: q = q.filter(Cotizacion.fecha <= datetime.fromisoformat(ff))
        except: pass
    if mmin:
        try: q = q.filter(Cotizacion.total >= float(mmin))
        except: pass
    if mmax:
        try: q = q.filter(Cotizacion.total <= float(mmax))
        except: pass

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

# ---------------------------------------------------------
# 11) API: Métricas para gráficas de dashboard
# ---------------------------------------------------------
@app.route("/api/dashboard/metrics")
def api_dashboard_metrics():
    """Serie por mes (YYYY-MM), conteo y total + KPIs globales."""
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
    """Conteo por estatus para gráfica de barras."""
    rows = db.session.query(Cotizacion.estatus, db.func.count(Cotizacion.id)).group_by(Cotizacion.estatus).all()
    categorias = ["ENVIADA", "PENDIENTE", "GANADA", "PERDIDA"]
    conteos_map = {estatus: cnt for estatus, cnt in rows}
    conteos = [int(conteos_map.get(cat, 0)) for cat in categorias]
    total = sum(conteos)
    porcentajes = [round((c * 100.0 / total), 2) if total > 0 else 0 for c in conteos]
    return jsonify({"labels": categorias, "counts": conteos, "percentages": porcentajes, "total": total})

# ---------------------------------------------------------
# 12) CAMBIO DE ESTATUS + WhatsApp al/los ADMIN(s)
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/update_status", methods=["POST"])
def update_cotizacion_status(cot_id):
    """Actualiza estatus y envía WhatsApp a ADMIN(s) con mensaje contextual."""
    nuevo_estatus = (request.form.get("estatus") or "").upper()
    if nuevo_estatus not in ["PENDIENTE", "ENVIADA", "GANADA", "PERDIDA"]:
        flash("Estatus no válido.", "danger")
        return redirect(url_for("index"))
        @app.route("/cotizaciones/<int:cot_id>/edit")
def edit_cotizacion(cot_id):
    cot = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_edit.html", c=cot)


    cot = Cotizacion.query.get_or_404(cot_id)
    anterior = cot.estatus
    cot.estatus = nuevo_estatus
    db.session.commit()

    try:
        msg = None
        if nuevo_estatus == "ENVIADA":
            msg = (
                "📤 *Cotización ENVIADA*\n"
                f"Folio: *{cot.folio}*\n"
                f"Total: ${cot.total:.2f}"
            )
        elif nuevo_estatus == "GANADA":
            msg = (
                "🏆 *Cotización GANADA*\n"
                f"Folio: *{cot.folio}*\n"
                f"Total cerrado: ${cot.total:.2f}"
            )
        elif nuevo_estatus == "PERDIDA":
            msg = (
                "💸 *Cotización PERDIDA*\n"
                f"Folio: *{cot.folio}*\n"
                f"Cliente: {cot.cliente.nombre_cliente if cot.cliente else 'N/A'}"
            )
        elif nuevo_estatus == "PENDIENTE" and anterior != "PENDIENTE":
            msg = (
                "⏳ *Cotización en PENDIENTE*\n"
                f"Folio: *{cot.folio}*\n"
                "Se enviarán recordatorios cada 24h."
            )
        if msg:
            send_whatsapp_multi(ADMIN_WHATSAPP_LIST, msg)
    except Exception as e:
        print(f"[WARN] No se pudo enviar WhatsApp de estatus ({cot.folio}): {e}", file=sys.stderr)

    flash(f"Estatus de {cot.folio} actualizado a {nuevo_estatus}.", "success")
    return redirect(url_for("index"))

# ---------------------------------------------------------
# 13) EXPORTACIONES CSV / PDF
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/export.csv")
def export_cotizacion_csv(cot_id):
    """Exporta una cotización a CSV (cabecera + renglones)."""
    c = Cotizacion.query.get_or_404(cot_id)
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(["Folio","Fecha","Estatus","Cliente","Empresa","Subtotal","Desc Total","IVA %","IVA $","Total","Notas"])
    w.writerow([
        c.folio, c.fecha.strftime("%Y-%m-%d %H:%M"),
        c.estatus,
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
        headers={"Content-Disposition": f"attachment; filename={c.folio or 'cotizacion'}.csv"}
    )

@app.route("/cotizaciones/<int:cot_id>/export.pdf")
def export_cotizacion_pdf(cot_id):
    """Exporta una cotización a PDF con logo y pie corporativo Poliutech."""
    c = Cotizacion.query.get_or_404(cot_id)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm, topMargin=16*mm, bottomMargin=16*mm
    )
    styles = getSampleStyleSheet()
    elems = []

    # Logo
# --- Logo corporativo ---
logo_path = os.path.join(app.static_folder or "static", "logo.jpg")
if os.path.exists(logo_path):
    try:
        from reportlab.platypus import Image as RLImage, Table, TableStyle
        logo = RLImage(logo_path, width=45*mm, height=25*mm)
        header_table = Table(
            [[logo, Paragraph("<b>Cotización Poliutech</b><br/>Recubrimientos Especializados", styles["Title"])]],
            colWidths=[50*mm, 120*mm]
        )
        header_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (0, 0), "LEFT"),
            ("ALIGN", (1, 0), (1, 0), "RIGHT")
        ]))
        elems.append(header_table)
        elems.append(Spacer(1, 12))
    except Exception as e:
        print(f"[PDF] Error cargando logo: {e}")


    elems.append(Paragraph("<b>Cotización Poliutech</b>", styles["Title"]))
    elems.append(Paragraph("Recubrimientos Especializados", styles["Normal"]))
    elems.append(Spacer(1, 12))
    logo_path = os.path.join(app.static_folder or "static", "logo.jpg")

    elems.append(Paragraph(f"<b>Folio:</b> {c.folio}", styles["Heading3"]))
    elems.append(Paragraph(
        f"<b>Fecha:</b> {c.fecha.strftime('%Y-%m-%d %H:%M')} &nbsp;&nbsp; "
        f"<b>Estatus:</b> {c.estatus}", styles["Normal"]
    ))
    elems.append(Spacer(1, 6))

    # Cliente
    if c.cliente:
        for ln in [
            f"<b>Cliente:</b> {c.cliente.nombre_cliente or ''}",
            f"<b>Empresa:</b> {c.cliente.empresa or ''}",
            f"<b>Responsable:</b> {c.cliente.responsable or ''}",
            f"<b>Correo:</b> {c.cliente.correo or ''}",
            f"<b>Teléfono:</b> {c.cliente.telefono or ''}",
            f"<b>Dirección:</b> {c.cliente.direccion or ''}",
            f"<b>RFC:</b> {c.cliente.rfc or ''}",
        ]:
            elems.append(Paragraph(ln, styles["Normal"]))
        elems.append(Spacer(1, 12))

    # Tabla de renglones
    data = [["Cant","Unidad","Concepto","P. Unit","Desc %","Subtotal"]]
    for d in c.detalles:
        data.append([
            f"{d.cantidad:.2f}", d.unidad or "", d.nombre_concepto,
            f"${d.precio_unitario:.2f}", f"{d.descuento:.2f}", f"${d.subtotal:.2f}"
        ])
    tbl = Table(data, colWidths=[20*mm,20*mm,70*mm,25*mm,20*mm,25*mm])
    tbl.setStyle(TableStyle([
        ("GRID",(0,0),(-1,-1),0.25,colors.grey),
        ("BACKGROUND",(0,0),(-1,0),colors.lightgrey),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("ALIGN",(0,0),(0,-1),"RIGHT"),
        ("ALIGN",(3,1),(-1,-1),"RIGHT"),
    ]))
    elems.append(tbl)
    elems.append(Spacer(1,10))

    # Totales
    tot_data = [
        ["Subtotal:", f"${c.subtotal:.2f}"],
        [f"IVA ({c.iva_porc:.2f}%):", f"${c.iva_monto:.2f}"],
        ["Total:", f"${c.total:.2f}"],
    ]
    t2 = Table(tot_data, colWidths=[40*mm,35*mm], hAlign="RIGHT")
    t2.setStyle(TableStyle([
        ("GRID",(0,0),(-1,-1),0.25,colors.grey),
        ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),
        ("BACKGROUND",(0,-1),(-1,-1),colors.whitesmoke),
        ("ALIGN",(1,0),(1,-1),"RIGHT"),
    ]))
    elems.append(t2)

    # Pie de página corporativo
elems.append(Spacer(1, 15))
elems.append(Paragraph(
    "<para align='center'>Campos Elíseos 223 Oficina 602 · Col. Polanco V Sección · C.P. 11560, CDMX<br/>"
    "Tel. 55 5938 6530 – 55 5938 0536 · info@poliutech.com · www.poliutech.com</para>",
    styles["Normal"]
))


    def set_title(canvas, doc_obj):
        try: canvas.setTitle(c.folio or "Cotizacion")
        except: pass

    doc.build(elems, onFirstPage=set_title, onLaterPages=set_title)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="application/pdf",
        headers={"Content-Disposition": f"inline; filename={c.folio}.pdf"})

      
    
# ---------------------------------------------------------
# 14) LISTADOS / UTILIDADES ADMIN (opcionales, ayudan a operar)
# ---------------------------------------------------------
@app.route("/cotizaciones")
def list_cotizaciones():
    """Lista simple de cotizaciones con paginación mínima por query param ?p=1."""
    page = int(request.args.get("p", 1) or 1)
    per_page = 25
    q = Cotizacion.query.order_by(Cotizacion.fecha.desc())
    total = q.count()
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    items = q.offset((page-1)*per_page).limit(per_page).all()
    return render_template("cotizaciones_list.html", items=items, page=page, pages=pages, total=total)

@app.route("/cotizaciones/<int:cot_id>")
def view_cotizacion(cot_id):
    """Vista simple de una cotización (detalle)."""
    c = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_view.html", c=c)

@app.route("/clientes")
def list_clientes():
    """Listado simple de clientes (últimos 200)."""
    clientes = Cliente.query.order_by(Cliente.id.desc()).limit(200).all()
    return render_template("clientes_list.html", clientes=clientes)

@app.route("/conceptos")
def list_conceptos():
    """Listado simple de conceptos (últimos 200)."""
    conceptos = Concepto.query.order_by(Concepto.id.desc()).limit(200).all()
    return render_template("conceptos_list.html", conceptos=conceptos)

# ---------------------------------------------------------
# 15) DEBUG / TEST ROUTES (puedes borrar si no las quieres)
# ---------------------------------------------------------
@app.route("/debug/send_test")
def debug_send_test():
    """
    Envía un WhatsApp de prueba a los ADMIN(s).
    No falla la app si Twilio no está listo; solo loguea.
    """
    msg = "✅ Mensaje de prueba MARWHATS (debug_send_test)."
    send_whatsapp_multi(ADMIN_WHATSAPP_LIST, msg)
    return jsonify({"sent": True, "to": ADMIN_WHATSAPP_LIST, "note": "Si Twilio no está configurado, solo se registró el intento."})

@app.route("/debug/force_reminders")
def debug_force_reminders():
    """
    Fuerza el job de recordatorios a ejecutarse una vez.
    Útil para pruebas manuales.
    """
    try:
        enviar_notificaciones_pendientes()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------------------------------------------------------
# 16) SCHEDULER: WhatsApp cada 24h si PENDIENTE (ADMIN ONLY)
# ---------------------------------------------------------
def enviar_notificaciones_pendientes():
    """
    Envia WhatsApp a ADMIN(s) por cada cotización en PENDIENTE
    si han pasado >= 24h desde el último envío (last_whatsapp_at).
    """
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
                        f"Total: ${cot.total:.2f}"
                    )
                    send_whatsapp_multi(ADMIN_WHATSAPP_LIST, body)
                    cot.last_whatsapp_at = ahora
                    db.session.commit()
                    print(f"[Scheduler] Recordatorio enviado: {cot.folio}")
                except Exception as e:
                    print(f"[Scheduler] ERROR enviando recordatorio ({cot.folio}): {e}", file=sys.stderr)

# Inicia scheduler (daemon). Evita doble arranque con el reloader.
scheduler = None
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
    scheduler = BackgroundScheduler(daemon=True)
    # Checa cada 60 min. Solo envía si pasaron >= 24h reales (control por last_whatsapp_at).
    scheduler.add_job(enviar_notificaciones_pendientes, "interval", minutes=60, id="pending_quotes_reminder", replace_existing=True)
    try:
        scheduler.start()
        print("[Scheduler] Iniciado (interval=60m).")
    except Exception as e:
        print(f"[Scheduler] No pudo iniciar: {e}", file=sys.stderr)

# ---------------------------------------------------------
# 17) TEMPLATES DE FALLBACK MINIMALISTAS (por si no existen)
#     - Estos HTML inline evitan 500 si aún no tienes templates.
#     - Si ya tienes tus templates en /templates, puedes borrar esta sección.
# ---------------------------------------------------------
from markupsafe import escape


def _html_layout(title, body):
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>{escape(title)}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu; margin:24px;}}
table{{border-collapse:collapse; width:100%;}}
th,td{{border:1px solid #ddd; padding:8px; font-size:14px;}}
th{{background:#f2f5ff; text-align:left;}}
.kpi{{display:inline-block; margin-right:16px; padding:8px 12px; background:#f6f9ff; border:1px solid #dfe7ff; border-radius:8px;}}
.btn{{display:inline-block; padding:6px 10px; border:1px solid #ccc; border-radius:6px; text-decoration:none; color:#111; background:#fff; margin-right:6px}}
.btn:hover{{background:#f6f6f6;}}
.badge{{display:inline-block; padding:2px 8px; border-radius:999px; background:#eef; border:1px solid #ccd; font-size:12px}}
</style>
</head>
<body>
{body}
</body>
</html>"""

@app.errorhandler(404)
def _404(e):
    return _html_layout("404", "<h2>404</h2><p>Recurso no encontrado.</p>"), 404

@app.errorhandler(500)
def _500(e):
    return _html_layout("500", "<h2>500</h2><p>Error interno del servidor.</p>"), 500

# Si falta dashboard.html
@app.template_global()
def _render_dashboard_fallback(total_cotizaciones, total_importe, total_catalogo, cotizaciones):
    rows = ""
    for c in cotizaciones:
        rows += f"<tr><td>{escape(c.folio)}</td><td>{c.fecha.strftime('%Y-%m-%d %H:%M')}</td><td><span class='badge'>{escape(c.estatus)}</span></td><td>${c.total:.2f}</td><td><a class='btn' href='{url_for('view_cotizacion', cot_id=c.id)}'>Ver</a><a class='btn' href='{url_for('export_cotizacion_pdf', cot_id=c.id)}'>PDF</a><a class='btn' href='{url_for('export_cotizacion_csv', cot_id=c.id)}'>CSV</a></td></tr>"
    body = f"""
<h1>MARWHATS · Dashboard</h1>
<div class="kpi">Cotizaciones: <b>{total_cotizaciones}</b></div>
<div class="kpi">Importe Total: <b>${total_importe:.2f}</b></div>
<div class="kpi">Conceptos en Catálogo: <b>{total_catalogo}</b></div>
<p style="margin-top:12px"><a class="btn" href="{url_for('cotizador')}">Ir a Cotizador</a> <a class="btn" href="{url_for('admin_catalogos')}">Admin Catálogos</a> <a class="btn" href="{url_for('list_cotizaciones')}">Ver cotizaciones</a></p>
<table>
<thead><tr><th>Folio</th><th>Fecha</th><th>Estatus</th><th>Total</th><th>Acciones</th></tr></thead>
<tbody>{rows}</tbody>
</table>
"""
    return Markup(_html_layout("Dashboard", body))

# shim de render_template para fallback si no hay archivos
from jinja2 import TemplateNotFound

_real_render_template = render_template
def render_template(name, **ctx):
    try:
        return _real_render_template(name, **ctx)
    except TemplateNotFound:
        if name == "dashboard.html":
            return _render_dashboard_fallback(**ctx)
        if name == "cotizador.html":
            # Form mini para crear cotizaciones
            body = f"""
<h1>MARWHATS · Cotizador</h1>
<form method="post" action="{url_for('crear_cotizacion')}">
  <h3>Cliente</h3>
  <p><label>Nombre: <input name="cliente_nombre" required></label></p>
  <p><label>Empresa: <input name="empresa"></label></p>
  <p><label>Responsable: <input name="responsable"></label></p>
  <p><label>Correo: <input name="correo" type="email"></label></p>
  <p><label>Teléfono: <input name="telefono"></label></p>
  <p><label>Dirección: <input name="direccion"></label></p>
  <p><label>RFC: <input name="rfc"></label></p>

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

  <p><button class="btn">Crear cotización</button> <a class="btn" href="{url_for('index')}">Volver</a></p>
</form>
<script>
function addItem(){{
  const d=document.createElement('div');
  d.className='item';
  d.innerHTML = `
    <hr>
    <p><label>Concepto: <input name="item_nombre_concepto[]"></label></p>
    <p><label>Unidad: <input name="item_unidad[]"></label></p>
    <p><label>Cantidad: <input name="item_cantidad[]" value="1"></label></p>
    <p><label>Precio: <input name="item_precio[]" value="0"></label></p>
    <p><label>Desc %: <input name="item_descuento[]" value="0"></label></p>
    <p><label>Descripción:<br><textarea name="item_descripcion[]"></textarea></label></p>`;
  document.getElementById('items').appendChild(d);
}}
</script>
"""
            return _html_layout("Cotizador", body)
        if name == "admin_catalogos.html":
            body = f"""
<h1>MARWHATS · Admin Catálogos</h1>
<form method="post" enctype="multipart/form-data" action="{url_for('upload_catalogo')}">
  <p><label>Tipo:
    <select name="tipo" required>
      <option value="Clientes">Clientes</option>
      <option value="Conceptos">Conceptos</option>
    </select>
  </label></p>
  <p><input type="file" name="archivo" required></p>
  <p><button class="btn">Subir</button> <a class="btn" href="{url_for('index')}">Volver</a></p>
</form>
<h3>Últimos clientes</h3>
<ul>{"".join(f"<li>{escape(c.nombre_cliente)} ({escape(c.empresa or '')})</li>" for c in ctx.get('clientes', []))}</ul>
<h3>Últimos conceptos</h3>
<ul>{"".join(f"<li>{escape(c.nombre_concepto)} - ${c.precio_unitario:.2f}</li>" for c in ctx.get('conceptos', []))}</ul>
"""
            return _html_layout("Admin Catálogos", body)
        if name == "cotizaciones_list.html":
            items = ctx.get("items", [])
            page = ctx.get("page", 1); pages = ctx.get("pages", 1)
            total = ctx.get("total", 0)
            trs = ""
            for c in items:
                trs += f"<tr><td>{escape(c.folio)}</td><td>{c.fecha.strftime('%Y-%m-%d %H:%M')}</td><td>{escape(c.estatus)}</td><td>${c.total:.2f}</td><td><a class='btn' href='{url_for('view_cotizacion', cot_id=c.id)}'>Ver</a></td></tr>"
            pag = f"<p>Página {page} de {pages} · Total {total} registros</p>"
            nav = "<p>" + " ".join(
                f"<a class='btn' href='?p={i}'>{i}</a>" if i != page else f"<span class='btn' style='background:#eef'>[{i}]</span>"
                for i in range(1, pages+1)
            ) + "</p>"
            body = f"<h1>Listado de Cotizaciones</h1>{pag}{nav}<table><thead><tr><th>Folio</th><th>Fecha</th><th>Estatus</th><th>Total</th><th>Acciones</th></tr></thead><tbody>{trs}</tbody></table>{nav}<p><a class='btn' href='{url_for('index')}'>Volver</a></p>"
            return _html_layout("Cotizaciones", body)
        if name == "cotizacion_view.html":
            c = ctx.get("c")
            if not c:
                return _html_layout("No encontrado", "<p>Cotización no encontrada</p>")
            det_rows = "".join(
                f"<tr><td>{d.cantidad:.2f}</td><td>{escape(d.unidad or '')}</td><td>{escape(d.nombre_concepto)}</td><td>${d.precio_unitario:.2f}</td><td>{d.descuento:.2f}</td><td>${d.subtotal:.2f}</td></tr>"
                for d in c.detalles
            )
            body = f"""
<h1>Cotización {escape(c.folio or str(c.id))}</h1>
<p><b>Fecha:</b> {c.fecha.strftime('%Y-%m-%d %H:%M')} · <b>Estatus:</b> {escape(c.estatus)}</p>
<h3>Cliente</h3>
<p>{escape(c.cliente.nombre_cliente) if c.cliente else '—'} {('· ' + escape(c.cliente.empresa)) if c.cliente and c.cliente.empresa else ''}</p>
<h3>Renglones</h3>
<table><thead><tr><th>Cant</th><th>Unidad</th><th>Concepto</th><th>P.U.</th><th>Desc %</th><th>Subtotal</th></tr></thead><tbody>{det_rows}</tbody></table>
<h3>Totales</h3>
<p>Subtotal: ${c.subtotal:.2f} · IVA ({c.iva_porc:.2f}%): ${c.iva_monto:.2f} · <b>Total: ${c.total:.2f}</b></p>
<p><a class="btn" href="{url_for('export_cotizacion_pdf', cot_id=c.id)}">Ver PDF</a>
<a class="btn" href="{url_for('export_cotizacion_csv', cot_id=c.id)}">Descargar CSV</a>
<a class="btn" href="{url_for('index')}">Volver</a></p>
"""
            return _html_layout("Ver Cotización", body)
        # Fallback genérico si piden un template inexistente
        return _html_layout("Vista no disponible", f"<p>La vista <code>{escape(name)}</code> no existe aún.</p>")

# Monkey patch
import types as _types
_original_render_template = render_template
render_template = _types.FunctionType(render_template.__code__, globals(), "render_template")

# ---------------------------------------------------------
# 18) MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    # Ejecuta Flask en modo debug (puedes cambiar a False en producción)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
# =========================================================
#  Sistema Poliutech - app.py  (Checkpoint: MARWHATS / MAR_BIEN)
# ---------------------------------------------------------
#  Características:
#   - Dashboard con métricas, filtros y exportaciones.
#   - Cotizador con autocompletado (clientes y conceptos).
#   - Exportaciones CSV y PDF (ReportLab) con branding Poliutech.
#   - Importación de catálogos (CSV/XLSX) con pandas.
#   - WhatsApp (Twilio) al crear cotización y cambiar estatus.
#   - Recordatorios cada 24h si estatus PENDIENTE (solo admin).
#   - Envíos de WhatsApp SOLO a administradores (multi-destinatario).
#   - Sin .env obligatorio (env opcional); seguro para GitHub (sin secretos).
#
#  Compatibilidad:
#   - Flask 3.x, SQLAlchemy 2.x, APScheduler 3.10+, Twilio 9.x, pandas 2.2.x,
#     ReportLab 4.x. Para Render, fija Python 3.11.9 vía runtime.txt.
#
#  Branding:
#   - Encabezados web y PDFs: “Sistema Poliutech” / “Cotización Poliutech”.
#   - Logo en PDF (static/logo.jpg) y divisor (static/division.png).
#   - Folios: PTCH-0001, PTCH-0002, ...
#
#  Seguridad:
#   - NUNCA embebemos SID/TOKEN de Twilio en el código. Se toman de ENV o se omiten.
#   - Si no hay credenciales, los envíos WhatsApp se desactivan silenciosamente.
#
#  Endpoints utilitarios:
#   - /health                         : ping de salud.
#   - /debug/send_test                : envía WhatsApp de prueba a admins.
#   - /debug/force_reminders          : ejecuta un ciclo de recordatorios.
#
#  Autoría:
#   - Proyecto adaptado y consolidado para despliegue en Render / hosting.
# =========================================================



# -------------------------------
#  Imports
# -------------------------------
import os
import io
import csv
import sys
import math
import traceback
from datetime import datetime, timedelta
from typing import List, Optional, Iterable

from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, jsonify, Response
)

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

# PDF / Reportes
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    Table, TableStyle, Paragraph, SimpleDocTemplate,
    Spacer, Image
)
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet

# WhatsApp (Twilio) + Scheduler
from twilio.rest import Client as TwilioClient
from apscheduler.schedulers.background import BackgroundScheduler

# =========================================================
#  Configuración base (segura para GitHub/Render)
# =========================================================
#
# - No dependemos de .env forzosamente.
# - Si TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN faltan -> WhatsApp OFF (seguro).
# - Puedes editar los destinatarios admin directamente o por ENV.
# - Base local: SQLite (mar3.db). Cambia DATABASE_URL a Postgres si gustas.

# Flask / DB
DEFAULT_SECRET_KEY = "poliutech_mar_checkpoint_superseguro"   # Puede cambiarse
DEFAULT_DATABASE_URL = "sqlite:///mar3.db"

# Twilio (no secretos embebidos)
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP    = os.getenv("TWILIO_WHATSAPP", "whatsapp:+14155238886").strip()

# Admin(es) destino (multi): CSV “whatsapp:+52..., whatsapp:+52...”
DEFAULT_ADMIN_WHATSAPP_RECIPIENTS = (
    "whatsapp:+5215521323076,whatsapp:+5215610035643,whatsapp:+14055619808"
)
ADMIN_WHATSAPP_RECIPIENTS = os.getenv(
    "ADMIN_WHATSAPP_RECIPIENTS",
DEFAULT_ADMIN_WHATSAPP_RECIPIENTS = (
    "whatsapp:+5215521323076,whatsapp:+5215610035643,whatsapp:+14055619808"
).strip()

# Construcción de lista de admins
ADMIN_LIST: List[str] = [
    x.strip() for x in ADMIN_WHATSAPP_RECIPIENTS.split(",") if x.strip()
]

# Inicializa Twilio si hay credenciales
twilio_client: Optional[TwilioClient] = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    try:
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        print("[Twilio] Cliente inicializado.")
    except Exception as e:
        print(f"[Twilio] No se pudo inicializar: {e}", file=sys.stderr)
else:
    print("[Twilio] SIN credenciales. Envío WhatsApp deshabilitado.", file=sys.stderr)

# Flask/SQLAlchemy
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", DEFAULT_SECRET_KEY)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# =========================================================
#  Modelos
# =========================================================

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

    cliente = db.relationship("Cliente", backref="cotizaciones")
    detalles = db.relationship(
        "CotizacionDetalle",
        backref="cotizacion",
        cascade="all, delete-orphan"
    )


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

# =========================================================
#  Migración simple / ensure_schema
# =========================================================

def _table_columns(table_name: str) -> set[str]:
    """
    Retorna el set de nombres de columnas desde PRAGMA de SQLite.
    (Funciona también con otros backends que soporten PRAGMA).
    """
    res = db.session.execute(text(f"PRAGMA table_info('{table_name}')")) \
                    .mappings().all()
    return {row["name"] for row in res}

def ensure_schema() -> None:
    """
    Crea tablas y asegura columnas nuevas (migración simple incremental).
    """
    db.create_all()
    cols = _table_columns("cotizacion")
    adds: List[str] = []
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

    for sql in adds:
        db.session.execute(text(sql))
    if adds:
        db.session.commit()

with app.app_context():
    ensure_schema()

# =========================================================
#  Helpers
# =========================================================

def generar_folio() -> str:
    """
    Genera folio incremental tipo PTCH-0001, PTCH-0002, ...
    Basado en el conteo actual de Cotizacion.id.
    """
    n = db.session.query(db.func.count(Cotizacion.id)).scalar() or 0
    return f"PTCH-{n + 1:04d}"


def fmt(n: float) -> float:
    """Convierte a float con dos decimales, tolerante a nulos/strings."""
    try:
        return round(float(n or 0), 2)
    except Exception:
        return 0.0


def parse_float(v, default: float = 0.0) -> float:
    """Parsea textos tipo '$1,234.50' a float seguro."""
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
    """
    Normaliza números a formato de Twilio “whatsapp:+52...”
    """
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
    # Por defecto, anteponemos +52 si no hay prefijo.
    return f"whatsapp:+52{digits}"


def can_send_whatsapp() -> bool:
    """True si hay twilio_client y un from_ configurado y al menos un admin."""
    return bool(twilio_client and TWILIO_WHATSAPP and ADMIN_LIST)


def send_whatsapp_multi(to_list: Iterable[str], body: str) -> None:
    """
    Envía un WhatsApp a cada destinatario admin.
    Si no hay credenciales, omite envío (no rompe).
    """
    if not to_list:
        print("[Twilio] Sin destinatarios; omito envío.")
        return
    if not can_send_whatsapp():
        print("[Twilio] Configuración incompleta; omito envío.")
        return

    for to in to_list:
        to_norm = normalize_whatsapp(to)
        if not to_norm:
            print(f"[Twilio] Destinatario inválido: {to}")
            continue
        try:
            print(f"[Twilio] Enviando a {to_norm} :: {body}")
            msg = twilio_client.messages.create(
                from_=TWILIO_WHATSAPP,
                to=to_norm,
                body=body
            )
            print(f"[Twilio] OK SID={msg.sid}")
        except Exception as e:
            print(f"[Twilio] ERROR enviando a {to_norm}: {e}", file=sys.stderr)
            traceback.print_exc()


# =========================================================
#  Rutas principales y vistas
# =========================================================

@app.route("/")
def index():
    """
    Dashboard:
      - KPIs: total cotizaciones, importe total, total catálogo.
      - Lista (últimas 100) con acciones (ver, export PDF/CSV).
    """
    total_cotizaciones = Cotizacion.query.count()
    total_importe = db.session.query(
        db.func.coalesce(db.func.sum(Cotizacion.total), 0)
    ).scalar() or 0
    total_catalogo = Concepto.query.count()
    cotizaciones = Cotizacion.query.order_by(Cotizacion.fecha.desc()).limit(100).all()
    return render_template(
        "dashboard.html",
        title="Sistema Poliutech",
        total_cotizaciones=total_cotizaciones,
        total_importe=float(total_importe),
        total_catalogo=total_catalogo,
        cotizaciones=cotizaciones
    )


@app.route("/cotizador")
def cotizador():
    """
    Pantalla del cotizador. El formulario debe apuntar a url_for('crear_cotizacion').
    Autocompletado en front usando /api/clientes/suggest y /api/conceptos/suggest.
    """
    return render_template("cotizador.html", title="Nuevo - Sistema Poliutech")


@app.route("/admin/catalogos")
def admin_catalogos():
    """
    Subida de catálogos para Clientes y Conceptos.
    """
    clientes = Cliente.query.order_by(Cliente.id.desc()).limit(10).all()
    conceptos = Concepto.query.order_by(Concepto.id.desc()).limit(10).all()
    return render_template(
        "admin_catalogos.html",
        title="Admin Catálogos",
        clientes=clientes,
        conceptos=conceptos
    )

# -------------------------------
#  Autocompletar (AJAX)
# -------------------------------

@app.route("/api/clientes/suggest")
def api_clientes_suggest():
    """
    Sugerencias de clientes: filtra por nombre_cliente ilike %q%.
    """
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])
    res = (Cliente.query
           .filter(Cliente.nombre_cliente.ilike(f"%{q}%"))
           .order_by(Cliente.nombre_cliente)
           .limit(10).all())
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
    """
    Sugerencias de conceptos: filtra por nombre_concepto ilike %q%.
    """
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])
    res = (Concepto.query
           .filter(Concepto.nombre_concepto.ilike(f"%{q}%"))
           .order_by(Concepto.nombre_concepto)
           .limit(10).all())
    return jsonify([{
        "label": c.nombre_concepto,
        "nombre_concepto": c.nombre_concepto,
        "unidad": c.unidad,
        "precio_unitario": c.precio_unitario,
        "descripcion": c.descripcion
    } for c in res])

# -------------------------------
#  Crear cotización + WhatsApp
# -------------------------------

@app.route("/cotizaciones/crear", methods=["POST"])
def crear_cotizacion():
    """
    Crea cotización a partir del formulario del cotizador.
    - Crea también el cliente si no existe (por nombre + empresa).
    - Genera renglones (CotizacionDetalle), calcula totales.
    - Envía WhatsApp inmediato al ADMIN (si hay credenciales).
    """
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

    # IVA
    iva_porc = parse_float(f.get("iva_porc"), 16.0)

    # Cabecera cotización
    cot = Cotizacion(
        folio=generar_folio(),
        cliente_id=cliente.id if cliente else None,
        estatus=(f.get("estatus") or "PENDIENTE").upper(),
        notas=f.get("notas"),
        last_whatsapp_at=None
    )
    db.session.add(cot)
    db.session.flush()

    # Renglones
    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    descuentos = f.getlist("item_descuento[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    descuento_total = 0.0

    num_items = max(len(nombres), len(unidades), len(cantidades), len(precios), len(descuentos))
    for i in range(num_items):
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

    # WhatsApp inmediato al ADMIN (multi)
    try:
        msg = (
            "🧾 *Nueva Cotización Creada*\n"
            f"Folio: *{cot.folio}*\n"
            f"Estatus: *{cot.estatus}*\n"
            f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\n"
            f"Total: ${cot.total:.2f}"
        )
        send_whatsapp_multi(ADMIN_LIST, msg)
    except Exception as e:
        print(f"[WARN] No se pudo enviar WhatsApp de creación ({cot.folio}): {e}", file=sys.stderr)

    flash(f"Cotización {cot.folio} creada correctamente.", "success")
    return redirect(url_for("cotizador"))

# -------------------------------
#  Importación de catálogos
# -------------------------------

@app.route("/admin/catalogos/upload", methods=["POST"])
def upload_catalogo():
    """
    Sube catálogos de Clientes o Conceptos desde CSV/XLSX.
    """
    tipo = (request.form.get("tipo") or "").strip()
    file = request.files.get("archivo")
    if not tipo or not file or not getattr(file, "filename", ""):
        flash("Debe seleccionar un tipo y un archivo válido.", "danger")
        return redirect(url_for("admin_catalogos"))

    ext = os.path.splitext(file.filename)[1].lower()
    import pandas as pd
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

# -------------------------------
#  API: búsqueda para dashboard
# -------------------------------

@app.route("/api/cotizaciones/search")
def api_cotizaciones_search():
    """
    Endpoint para que el dashboard filtre cotizaciones por:
      - estatus
      - fecha inicial (fi) / final (ff) ISO-8601
      - monto mínimo (mmin) / máximo (mmax)
    Retorna hasta 500 registros con URLs de export.
    """
    q = Cotizacion.query.join(Cliente, isouter=True)
    estatus = (request.args.get("estatus") or "").strip()
    fi = (request.args.get("fi") or "").strip()
    ff = (request.args.get("ff") or "").strip()
    mmin = (request.args.get("mmin") or "").strip()
    mmax = (request.args.get("mmax") or "").strip()

    if estatus:
        q = q.filter(Cotizacion.estatus == estatus)
    if fi:
        try:
            q = q.filter(Cotizacion.fecha >= datetime.fromisoformat(fi))
        except Exception:
            pass
    if ff:
        try:
            q = q.filter(Cotizacion.fecha <= datetime.fromisoformat(ff))
        except Exception:
            pass
    if mmin:
        try:
            q = q.filter(Cotizacion.total >= float(mmin))
        except Exception:
            pass
    if mmax:
        try:
            q = q.filter(Cotizacion.total <= float(mmax))
        except Exception:
            pass

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

# -------------------------------
#  API: métricas para gráficas
# -------------------------------

@app.route("/api/dashboard/metrics")
def api_dashboard_metrics():
    """
    Serie por mes (YYYY-MM), conteo y total.
    KPIs globales (conteos, sumas).
    """
    rows = db.session.query(
        db.func.strftime("%Y-%m", Cotizacion.fecha).label("ym"),
        db.func.count(Cotizacion.id),
        db.func.coalesce(db.func.sum(Cotizacion.total), 0)
    ).group_by("ym").order_by("ym").all()
    series = [{"mes": ym, "cotizaciones": int(c), "total": float(t)} for ym, c, t in rows]
    kpis = {
        "total_cotizaciones": Cotizacion.query.count(),
        "total_importe": float(db.session.query(
            db.func.coalesce(db.func.sum(Cotizacion.total), 0)
        ).scalar() or 0),
        "total_catalogo": Concepto.query.count(),
    }
    return jsonify({"series": series, "kpis": kpis})


@app.route("/api/dashboard/status_breakdown")
def api_dashboard_status_breakdown():
    """
    Conteo por estatus: ENVIADA, PENDIENTE, GANADA, PERDIDA
    """
    rows = db.session.query(Cotizacion.estatus, db.func.count(Cotizacion.id)) \
                     .group_by(Cotizacion.estatus).all()
    categorias = ["ENVIADA", "PENDIENTE", "GANADA", "PERDIDA"]
    conteos_map = {estatus: cnt for estatus, cnt in rows}
    conteos = [int(conteos_map.get(cat, 0)) for cat in categorias]
    total = sum(conteos)
    porcentajes = [round((c * 100.0 / total), 2) if total > 0 else 0 for c in conteos]
    return jsonify({
        "labels": categorias,
        "counts": conteos,
        "percentages": porcentajes,
        "total": total
    })

# -------------------------------
#  Cambio de estatus + WhatsApp
# -------------------------------

@app.route("/cotizaciones/<int:cot_id>/update_status", methods=["POST"])
def update_cotizacion_status(cot_id: int):
    """
    Actualiza estatus y envía WhatsApp al ADMIN con mensaje contextual.
    """
    nuevo_estatus = (request.form.get("estatus") or "").upper()
    if nuevo_estatus not in ["PENDIENTE", "ENVIADA", "GANADA", "PERDIDA"]:
        flash("Estatus no válido.", "danger")
        return redirect(url_for("index"))

    cot = Cotizacion.query.get_or_404(cot_id)
    anterior = cot.estatus
    cot.estatus = nuevo_estatus
    db.session.commit()

    try:
        msg = None
        if nuevo_estatus == "ENVIADA":
            msg = (
                "📤 *Cotización ENVIADA*\n"
                f"Folio: *{cot.folio}*\n"
                f"Total: ${cot.total:.2f}"
            )
        elif nuevo_estatus == "GANADA":
            msg = (
                "🏆 *Cotización GANADA*\n"
                f"Folio: *{cot.folio}*\n"
                f"Total cerrado: ${cot.total:.2f}"
            )
        elif nuevo_estatus == "PERDIDA":
            msg = (
                "💸 *Cotización PERDIDA*\n"
                f"Folio: *{cot.folio}*\n"
                f"Cliente: {cot.cliente.nombre_cliente if cot.cliente else 'N/A'}"
            )
        elif nuevo_estatus == "PENDIENTE" and anterior != "PENDIENTE":
            msg = (
                "⏳ *Cotización en PENDIENTE*\n"
                f"Folio: *{cot.folio}*\n"
                "Se enviarán recordatorios cada 24h."
            )
        if msg:
            send_whatsapp_multi(ADMIN_LIST, msg)
    except Exception as e:
        print(f"[WARN] No se pudo enviar WhatsApp de estatus ({cot.folio}): {e}", file=sys.stderr)

    flash(f"Estatus de {cot.folio} actualizado a {nuevo_estatus}.", "success")
    return redirect(url_for("index"))

# -------------------------------
#  Exportaciones CSV / PDF
# -------------------------------

@app.route("/cotizaciones/<int:cot_id>/export.csv")
def export_cotizacion_csv(cot_id: int):
    """
    Exporta una cotización a CSV:
      - Encabezado de cabecera (folio, fecha, cliente, totales).
      - Tabla de renglones (cantidad, unidad, concepto, precios).
    """
    c = Cotizacion.query.get_or_404(cot_id)
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow([
        "Folio", "Fecha", "Estatus", "Cliente", "Empresa",
        "Subtotal", "Desc Total", "IVA %", "IVA $", "Total", "Notas"
    ])
    w.writerow([
        c.folio, c.fecha.strftime("%Y-%m-%d %H:%M"),
        c.estatus,
        c.cliente.nombre_cliente if c.cliente else "",
        c.cliente.empresa if c.cliente else "",
        f"{c.subtotal:.2f}", f"{c.descuento_total:.2f}",
        f"{c.iva_porc:.2f}", f"{c.iva_monto:.2f}",
        f"{c.total:.2f}", (c.notas or "")
    ])
    w.writerow([])
    w.writerow(["Cant", "Unidad", "Concepto", "PU", "Desc %", "Subtotal", "Descripción"])
    for d in c.detalles:
        w.writerow([
            d.cantidad, d.unidad or "", d.nombre_concepto,
            f"{d.precio_unitario:.2f}", f"{d.descuento:.2f}",
            f"{d.subtotal:.2f}", (d.descripcion or "")
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={c.folio or 'cotizacion'}.csv"}
    )


@app.route("/cotizaciones/<int:cot_id>/export.pdf")
def export_cotizacion_pdf(cot_id):
    """Exporta una cotización a PDF con logo y pie corporativo Poliutech."""
    c = Cotizacion.query.get_or_404(cot_id)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm, topMargin=16*mm, bottomMargin=16*mm
    )
    styles = getSampleStyleSheet()
    elems = []

    # Logo
    # --- Logo corporativo ---
logo_path = os.path.join(app.static_folder or "static", "logo.jpg")
if os.path.exists(logo_path):
    try:
        from reportlab.platypus import Image as RLImage, Table, TableStyle
        logo = RLImage(logo_path, width=45*mm, height=25*mm)
        header_table = Table(
            [[logo, Paragraph("<b>Cotización Poliutech</b><br/>Recubrimientos Especializados", styles["Title"])]],
            colWidths=[50*mm, 120*mm]
        )
        header_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (0, 0), "LEFT"),
            ("ALIGN", (1, 0), (1, 0), "RIGHT")
        ]))
        elems.append(header_table)
        elems.append(Spacer(1, 12))
    except Exception as e:
        print(f"[PDF] Error cargando logo: {e}")


    elems.append(Paragraph("<b>Cotización Poliutech</b>", styles["Title"]))
    elems.append(Paragraph("Recubrimientos Especializados", styles["Normal"]))
    elems.append(Spacer(1, 12))

    elems.append(Paragraph(f"<b>Folio:</b> {c.folio}", styles["Heading3"]))
    elems.append(Paragraph(
        f"<b>Fecha:</b> {c.fecha.strftime('%Y-%m-%d %H:%M')} &nbsp;&nbsp; "
        f"<b>Estatus:</b> {c.estatus}", styles["Normal"]
    ))
    elems.append(Spacer(1, 6))

    # Cliente
    if c.cliente:
        for ln in [
            f"<b>Cliente:</b> {c.cliente.nombre_cliente or ''}",
            f"<b>Empresa:</b> {c.cliente.empresa or ''}",
            f"<b>Responsable:</b> {c.cliente.responsable or ''}",
            f"<b>Correo:</b> {c.cliente.correo or ''}",
            f"<b>Teléfono:</b> {c.cliente.telefono or ''}",
            f"<b>Dirección:</b> {c.cliente.direccion or ''}",
            f"<b>RFC:</b> {c.cliente.rfc or ''}",
        ]:
            elems.append(Paragraph(ln, styles["Normal"]))
        elems.append(Spacer(1, 12))

    # Tabla de renglones
    data = [["Cant","Unidad","Concepto","P. Unit","Desc %","Subtotal"]]
    for d in c.detalles:
        data.append([
            f"{d.cantidad:.2f}", d.unidad or "", d.nombre_concepto,
            f"${d.precio_unitario:.2f}", f"{d.descuento:.2f}", f"${d.subtotal:.2f}"
        ])
    tbl = Table(data, colWidths=[20*mm,20*mm,70*mm,25*mm,20*mm,25*mm])
    tbl.setStyle(TableStyle([
        ("GRID",(0,0),(-1,-1),0.25,colors.grey),
        ("BACKGROUND",(0,0),(-1,0),colors.lightgrey),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("ALIGN",(0,0),(0,-1),"RIGHT"),
        ("ALIGN",(3,1),(-1,-1),"RIGHT"),
    ]))
    elems.append(tbl)
    elems.append(Spacer(1,10))

    # Totales
    tot_data = [
        ["Subtotal:", f"${c.subtotal:.2f}"],
        [f"IVA ({c.iva_porc:.2f}%):", f"${c.iva_monto:.2f}"],
        ["Total:", f"${c.total:.2f}"],
    ]
    t2 = Table(tot_data, colWidths=[40*mm,35*mm], hAlign="RIGHT")
    t2.setStyle(TableStyle([
        ("GRID",(0,0),(-1,-1),0.25,colors.grey),
        ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),
        ("BACKGROUND",(0,-1),(-1,-1),colors.whitesmoke),
        ("ALIGN",(1,0),(1,-1),"RIGHT"),
    ]))
    elems.append(t2)

# Pie de página corporativo
elems.append(Spacer(1, 15))
elems.append(Paragraph(
    "<para align='center'>Campos Elíseos 223 Oficina 602 · Col. Polanco V Sección · C.P. 11560, CDMX<br/>"
    "Tel. 55 5938 6530 – 55 5938 0536 · info@poliutech.com · www.poliutech.com</para>",
    styles["Normal"]
))


    def set_title(canvas, doc_obj):
        try: canvas.setTitle(c.folio or "Cotizacion")
        except: pass

    doc.build(elems, onFirstPage=set_title, onLaterPages=set_title)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="application/pdf",
        headers={"Content-Disposition": f"inline; filename={c.folio}.pdf"})
# -------------------------------
#  Vistas de listas / detalle
# -------------------------------

@app.route("/cotizaciones")
def list_cotizaciones():
    """
    Lista paginada simple (reutiliza dashboard.html para mostrar).
    """
    page = int(request.args.get("p", 1) or 1)
    per_page = 25
    q = Cotizacion.query.order_by(Cotizacion.fecha.desc())
    total = q.count()
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    items = q.offset((page-1)*per_page).limit(per_page).all()

    # Reuso del template de dashboard para simplicidad
    return render_template(
        "dashboard.html",
        title="Cotizaciones · Sistema Poliutech",
        total_cotizaciones=total,
        total_importe=0,
        total_catalogo=0,
        cotizaciones=items
    )


@app.route("/cotizaciones/<int:cot_id>")
def view_cotizacion(cot_id: int):
    """
    Vista de una sola cotización (reutiliza dashboard para mostrar una fila).
    """
    c = Cotizacion.query.get_or_404(cot_id)
    return render_template(
        "dashboard.html",
        title=f"Ver {c.folio}",
        total_cotizaciones=1,
        total_importe=c.total,
        total_catalogo=0,
        cotizaciones=[c]
    )

# -------------------------------
#  Endpoints utilitarios
# -------------------------------

@app.route("/health")
def health():
    return jsonify({"status": "ok", "now_utc": datetime.utcnow().isoformat()}), 200


@app.route("/debug/send_test")
def debug_send_test():
    """
    Envío de WhatsApp de prueba a los admins (si hay credenciales).
    """
    msg = "✅ Mensaje de prueba - Sistema Poliutech (debug_send_test)."
    send_whatsapp_multi(ADMIN_LIST, msg)
    return jsonify({"sent": True, "to": ADMIN_LIST})


@app.route("/debug/force_reminders")
def debug_force_reminders():
    """
    Ejecuta manualmente el trabajo de recordatorios (24h) una vez.
    """
    try:
        enviar_notificaciones_pendientes()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# =========================================================
#  Scheduler: recordatorios cada 24h (PENDIENTE)
# =========================================================

def enviar_notificaciones_pendientes():
    """
    Envia WhatsApp al ADMIN por cada cotización en PENDIENTE
    si han pasado >= 24h desde el último envío (last_whatsapp_at).
    """
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
                        f"Total: ${cot.total:.2f}"
                    )
                    send_whatsapp_multi(ADMIN_LIST, body)
                    cot.last_whatsapp_at = ahora
                    db.session.commit()
                    print(f"[Scheduler] Recordatorio enviado: {cot.folio}")
                except Exception as e:
                    print(f"[Scheduler] ERROR enviando recordatorio ({cot.folio}): {e}", file=sys.stderr)

# Inicia scheduler solo en proceso principal (evitar doble en debug reloader)
scheduler: Optional[BackgroundScheduler] = None
try:
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(
            enviar_notificaciones_pendientes,
            "interval",
            minutes=60,
            id="pending_quotes_reminder",
            replace_existing=True
        )
        scheduler.start()
        print("[Scheduler] Iniciado (interval=60m).")
except Exception as e:
    print(f"[Scheduler] No pudo iniciar: {e}", file=sys.stderr)

# =========================================================
#  Main
# =========================================================

if __name__ == "__main__":
    # Asegura carpeta static para PDF logos en entornos simples
    try:
        os.makedirs(app.static_folder or "static", exist_ok=True)
    except Exception:
        pass

    # Host 0.0.0.0 y PORT para Render / hosting
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=True
    )

@app.route("/cotizaciones/<int:cot_id>/editar")
def editar_cotizacion(cot_id):
    c = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_edit.html", c=c, title=f"Editar {c.folio}")

@app.route("/cotizaciones/<int:cot_id>/actualizar", methods=["POST"])
def actualizar_cotizacion(cot_id):
    c = Cotizacion.query.get_or_404(cot_id)
    f = request.form
    c.estatus = (f.get("estatus") or c.estatus).upper()
    c.notas = f.get("notas", c.notas)
    db.session.commit()
    pdf_url = url_for("export_cotizacion_pdf", cot_id=c.id)
    detalle = url_for("view_cotizacion", cot_id=c.id)
    return f'''<!DOCTYPE html><html><head><meta charset="utf-8"><title>Actualizada {c.folio}</title></head><body><script>window.open("{pdf_url}", "_blank");window.location.href="{detalle}";</script></body></html>'''
