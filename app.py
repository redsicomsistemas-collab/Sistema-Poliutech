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
ADEFAULT_ADMIN_WHATSAPP_RECIPIENTS = (
    "whatsapp:+5215521323076,whatsapp:+5215610035643,whatsapp:+14055619808"
)

ADMIN_WHATSAPP_RECIPIENTS = os.getenv(
    "ADMIN_WHATSAPP_RECIPIENTS",
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
    try:
        canvas.setTitle(c.folio or "Cotizacion")
    except:
        pass

    doc.build(elems, onFirstPage=set_title, onLaterPages=set_title)
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{c.folio}.pdf"'}
    )

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
