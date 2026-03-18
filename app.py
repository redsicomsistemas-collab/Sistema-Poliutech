# =========================================================
# app.py — MARWHATS (checkpoint) / Poliutech
# Limpio + Roles (ADMIN / USER) + Filtro por Responsable
# =========================================================
from __future__ import annotations

import os, io, csv, sys, math, re, json, traceback, unicodedata, smtplib
import mimetypes
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, List
from urllib.parse import urlparse
from pathlib import Path
from email.message import EmailMessage
from email.utils import getaddresses
from html import escape


# -------------------------------
# Condiciones comerciales
# -------------------------------
# Ya no se agregan condiciones por defecto. Solo se exporta lo capturado
# por el usuario y, cuando aplique, la trazabilidad de la zona.
DEFAULT_CONDICIONES: list[str] = []



def _split_notas_y_zona(notas_raw: str) -> tuple[str, str]:
    notas_raw = (notas_raw or "").strip()
    extras = []
    zona_line = ""
    for ln in notas_raw.splitlines():
        s = ln.strip()
        if not s:
            continue
        if s.lower().startswith("zona:"):
            zona_line = s
        else:
            extras.append(s)
    return "\n".join(extras).strip(), zona_line

def _condiciones_comerciales_finales(notas_raw: str) -> list[str]:
    extras_txt, zona_line = _split_notas_y_zona(notas_raw)
    items = list(DEFAULT_CONDICIONES)
    if zona_line:
        items.append(zona_line)
    if extras_txt:
        for ln in extras_txt.splitlines():
            s = ln.strip()
            if s:
                items.append(s)
    return items

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, abort, g
)

from sqlalchemy import text, or_, case

# ReportLab (PDF)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Table, TableStyle, Paragraph, SimpleDocTemplate, Spacer, KeepTogether
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.lib.enums import TA_JUSTIFY

# Excel
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
except Exception:
    Workbook = None  # la app sigue arrancando aunque falte openpyxl

# Twilio + Scheduler
from twilio.rest import Client as TwilioClient
from apscheduler.schedulers.background import BackgroundScheduler

# Auth (Flask-Login)
from flask_login import LoginManager, login_user, login_required, logout_user, current_user

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
TZ_CDMX = ZoneInfo("America/Mexico_City")

def now_cdmx_naive() -> datetime:
    """Hora CDMX (naive). Úsala para timestamps en DB/UX sin desfases."""
    return datetime.now(TZ_CDMX).replace(tzinfo=None)

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

SMTP_HOST = os.getenv("SMTP_HOST", "servidor15.escala.net.mx").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "26"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "cotizaciones@poliutech.com").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "Cotizaciones2025@").strip()
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USERNAME).strip()

# Usa SIEMPRE los modelos desde models.py para evitar duplicados
from models import db, Cliente, Concepto, Cotizacion, CotizacionDetalle, Usuario, ActivityLog
from neodata_personal.routes import apu_bp  # módulo MAR DATA
from neodata_personal.models import APU

# ---------------------------------------------------------
# Flask + DB + Login
# ---------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", DEFAULT_SECRET_KEY)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)
app.register_blueprint(apu_bp)  # registrar módulo MAR DATA


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

@login_manager.user_loader
def load_user(user_id):
    try:
        return Usuario.query.get(int(user_id))
    except Exception:
        return None

# ---------------------------------------------------------
# 🔒 Enforce login for ALL pages (except /login + static)
# ---------------------------------------------------------
@app.before_request
def _require_login_everywhere():
    """Protege TODAS las páginas del sistema.

    Si el usuario NO está autenticado, lo mandamos a /login.
    Esto cubre cualquier ruta/HTML aunque olvides poner @login_required.
    """
    # Permitir estáticos
    if request.path.startswith("/static/") or request.endpoint == "static":
        return

    # Permitir login y endpoints "públicos" mínimos
    if request.path == "/login" or request.endpoint == "login":
        return
    if request.path in ("/health", "/ping"):
        return

    # Si ya está logueado, ok
    if current_user.is_authenticated:
        return

    # Redirigir a login, preservando a dónde quería ir
    nxt = request.full_path
    if nxt.endswith("?"):
        nxt = nxt[:-1]
    return redirect(url_for("login", next=nxt))

# ---------------------------------------------------------
# Bitácora de actividad (Audit Log)
# ---------------------------------------------------------
def _safe_join_keys(keys, limit=60):
    try:
        if not keys:
            return None
        out = []
        for k in list(keys)[:limit]:
            # evitamos guardar cosas sensibles por nombre
            lk = str(k).lower()
            if any(x in lk for x in ["pass", "password", "clave", "token", "secret"]):
                out.append(f"{k}=<hidden>")
            else:
                out.append(str(k))
        s = ", ".join(out)
        return s[:780]
    except Exception:
        return None

def _get_client_ip():
    # Render / proxies: X-Forwarded-For suele venir
    xf = request.headers.get("X-Forwarded-For", "")
    if xf:
        return xf.split(",")[0].strip()[:60]
    return (request.remote_addr or "")[:60]



# ---------------------------------------------------------
# Audit retention / cleanup (keep DB from growing forever)
# ---------------------------------------------------------
AUDIT_LOG_RETENTION_DAYS = int(os.getenv("AUDIT_LOG_RETENTION_DAYS", "90"))
AUDIT_CLEANUP_EVERY_HOURS = int(os.getenv("AUDIT_CLEANUP_EVERY_HOURS", "24"))

def _audit_cleanup_stamp_path() -> str:
    try:
        os.makedirs(app.instance_path, exist_ok=True)
    except Exception:
        pass
    return os.path.join(app.instance_path, "audit_cleanup_stamp.txt")

def _should_run_audit_cleanup(now: datetime) -> bool:
    # Run cleanup at most once per AUDIT_CLEANUP_EVERY_HOURS (best-effort).
    try:
        stamp_path = _audit_cleanup_stamp_path()
        if not os.path.exists(stamp_path):
            return True
        raw = Path(stamp_path).read_text(encoding="utf-8", errors="ignore").strip()
        if not raw:
            return True
        try:
            last = datetime.fromisoformat(raw)
        except Exception:
            return True
        delta = now - last
        return delta.total_seconds() >= AUDIT_CLEANUP_EVERY_HOURS * 3600
    except Exception:
        # If anything weird, skip (never break requests)
        return False

def _mark_audit_cleanup(now: datetime) -> None:
    try:
        Path(_audit_cleanup_stamp_path()).write_text(now.isoformat(), encoding="utf-8")
    except Exception:
        pass

def cleanup_audit_logs(retention_days: int | None = None) -> int:
    # Delete ActivityLog rows older than retention_days. Returns deleted count (best-effort).
    try:
        days = int(retention_days if retention_days is not None else AUDIT_LOG_RETENTION_DAYS)
        cutoff = now_cdmx_naive() - timedelta(days=days)
        deleted = ActivityLog.query.filter(ActivityLog.fecha < cutoff).delete(synchronize_session=False)
        db.session.commit()
        return int(deleted or 0)
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return 0

def maybe_cleanup_audit_logs() -> None:
    # Best-effort periodic cleanup.
    try:
        now = now_cdmx_naive()
        if _should_run_audit_cleanup(now):
            cleanup_audit_logs()
            _mark_audit_cleanup(now)
    except Exception:
        pass
def _describe_action():
    # Acción legible sin datos sensibles
    try:
        ep = (request.endpoint or "").strip()
        m = request.method
        p = request.path

        # Login explícito
        if ep == "login" and m == "POST":
            nombre = (
                request.form.get("nombre")
                or request.form.get("username")
                or request.form.get("usuario")
                or request.form.get("user")
                or ""
            ).strip()[:60]
            return f"LOGIN intento usuario={nombre}"

        if ep == "logout":
            return "LOGOUT"

        # Cotizaciones (patrones comunes)
        if "cotizacion" in p.lower():
            return f"{m} {p}"

        if "cliente" in p.lower():
            return f"{m} {p}"

        if "catalog" in p.lower() or "catalogo" in p.lower():
            return f"{m} {p}"

        # Default
        return f"{m} {p}"
    except Exception:
        return f"{request.method} {request.path}"

@app.before_request
def _audit_before_request():
    try:
        # Ignorar estáticos y healthchecks
        if request.path.startswith("/static/") or request.path == "/favicon.ico":
            g._skip_audit = True
            return
        g._skip_audit = False

        g._audit_started_at = now_cdmx_naive()

        # Captura keys sin valores
        form_keys = None
        json_keys = None
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            if request.form:
                form_keys = _safe_join_keys(request.form.keys())
            j = request.get_json(silent=True)
            if isinstance(j, dict):
                json_keys = _safe_join_keys(j.keys())

        g._audit_payload = {
            "form_keys": form_keys,
            "json_keys": json_keys,
            "query_string": (request.query_string.decode("utf-8", "ignore")[:780] if request.query_string else None),
        }
    except Exception:
        # no rompemos request por falla de bitácora
        g._skip_audit = True

@app.after_request
def _audit_after_request(response):
    try:
        if getattr(g, "_skip_audit", False):
            return response

        # Usuario
        usuario = "ANON"
        usuario_id = None
        rol = None
        try:
            if current_user and getattr(current_user, "is_authenticated", False):
                usuario = (getattr(current_user, "nombre", None) or "ANON")[:60]
                usuario_id = getattr(current_user, "id", None)
                rol = getattr(current_user, "rol", None)
        except Exception:
            pass

        # acción
        accion = _describe_action()

        log = ActivityLog(
            fecha=now_cdmx_naive(),
            usuario_id=usuario_id,
            usuario=usuario,
            rol=rol,
            metodo=request.method,
            ruta=(request.path or "")[:300],
            endpoint=(request.endpoint or "")[:120] if request.endpoint else None,
            status_code=int(getattr(response, "status_code", 0) or 0),
            ip=_get_client_ip(),
            user_agent=(request.headers.get("User-Agent", "")[:300] if request.headers else None),
            query_string=(g._audit_payload.get("query_string") if hasattr(g, "_audit_payload") else None),
            form_keys=(g._audit_payload.get("form_keys") if hasattr(g, "_audit_payload") else None),
            json_keys=(g._audit_payload.get("json_keys") if hasattr(g, "_audit_payload") else None),
            accion=accion[:500],
        )
        db.session.add(log)
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
    return response

# ---------------------------------------------------------
# Twilio init (Render)
# ---------------------------------------------------------
twilio_client: Optional[TwilioClient] = None

def init_twilio_client():
    global twilio_client, TWILIO_WHATSAPP
    try:
        sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
        token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
        wsp_from = os.getenv("TWILIO_WHATSAPP", "").strip()

        if sid and token and wsp_from:
            twilio_client = TwilioClient(sid, token)
            TWILIO_WHATSAPP = wsp_from
            print("[Twilio] Cliente inicializado correctamente.")
            print(f"[Twilio] Remitente WhatsApp: {TWILIO_WHATSAPP}")
        else:
            twilio_client = None
            print("[Twilio] Configuración incompleta. WhatsApp deshabilitado.")
    except Exception as e:
        twilio_client = None
        print(f"[Twilio] Error al inicializar cliente: {e}", file=sys.stderr)

with app.app_context():
    init_twilio_client()

# ---------------------------------------------------------
# Migraciones mínimas (SQLite)
# ---------------------------------------------------------
def _table_columns(table_name: str) -> set[str]:
    rows = db.session.execute(text(f"PRAGMA table_info('{table_name}')")).mappings().all()
    return {r["name"] for r in rows}

def ensure_schema():
    """Crea tablas si no existen y agrega/normaliza columnas clave."""
    print("🔍 Verificando estructura de la base de datos...")
    db.create_all()

    # --- CLIENTE.responsable ---
    try:
        cols_cli = _table_columns("cliente")
        if "responsable" not in cols_cli:
            db.session.execute(text("ALTER TABLE cliente ADD COLUMN responsable VARCHAR(120)"))
            db.session.commit()
            print("✅ Campo 'responsable' agregado en 'cliente'.")
    except Exception as e:
        print("⚠️ ensure_schema(cliente.responsable):", e)

    # --- CLIENTE.sistema (si existía en tu proyecto) ---
    try:
        cols_cli = _table_columns("cliente")
        if "sistema" not in cols_cli:
            db.session.execute(text("ALTER TABLE cliente ADD COLUMN sistema VARCHAR(120)"))
            db.session.commit()
            print("✅ Campo 'sistema' agregado en 'cliente'.")
    except Exception as e:
        print("⚠️ ensure_schema(cliente.sistema):", e)

    # --- COTIZACION.responsable ---
    try:
        cols_cot = _table_columns("cotizacion")
        if "responsable" not in cols_cot:
            if "representante" in cols_cot:
                db.session.execute(text("ALTER TABLE cotizacion ADD COLUMN responsable VARCHAR(120)"))
                try:
                    db.session.execute(text("UPDATE cotizacion SET responsable = representante WHERE responsable IS NULL"))
                except Exception:
                    pass
                db.session.commit()
                print("✅ Campo 'responsable' creado y poblado desde 'representante'.")
            else:
                db.session.execute(text("ALTER TABLE cotizacion ADD COLUMN responsable VARCHAR(120)"))
                db.session.commit()
                print("✅ Campo 'responsable' agregado en 'cotizacion'.")
    except Exception as e:
        print("⚠️ ensure_schema(cotizacion.responsable):", e)

    # --- Otros mínimos para estabilidad ---
    try:
        cols = _table_columns("cotizacion")
        for col, stmt in [
            ("subtotal", "ALTER TABLE cotizacion ADD COLUMN subtotal FLOAT DEFAULT 0.0"),
            ("descuento_total", "ALTER TABLE cotizacion ADD COLUMN descuento_total FLOAT DEFAULT 0.0"),
            ("iva_porc", "ALTER TABLE cotizacion ADD COLUMN iva_porc FLOAT DEFAULT 16.0"),
            ("iva_monto", "ALTER TABLE cotizacion ADD COLUMN iva_monto FLOAT DEFAULT 0.0"),
            ("total", "ALTER TABLE cotizacion ADD COLUMN total FLOAT DEFAULT 0.0"),
            ("notas", "ALTER TABLE cotizacion ADD COLUMN notas VARCHAR(3000)"),
            ("last_whatsapp_at", "ALTER TABLE cotizacion ADD COLUMN last_whatsapp_at TIMESTAMP NULL"),
        ]:
            if col not in cols:
                try:
                    db.session.execute(text(stmt))
                except Exception:
                    pass
        db.session.commit()
    except Exception as e:
        print("⚠️ ensure_schema(cotizacion extras):", e)

    try:
        dcols = _table_columns("cotizacion_detalle")
        if "sistema" not in dcols:
            db.session.execute(text("ALTER TABLE cotizacion_detalle ADD COLUMN sistema VARCHAR(200)"))
        if "descripcion" not in dcols:
            db.session.execute(text("ALTER TABLE cotizacion_detalle ADD COLUMN descripcion VARCHAR(1000)"))
        for col, stmt in [
            ("capitulo", "ALTER TABLE cotizacion_detalle ADD COLUMN capitulo VARCHAR(120)"),
            ("origen", "ALTER TABLE cotizacion_detalle ADD COLUMN origen VARCHAR(50)"),
            ("apu_id", "ALTER TABLE cotizacion_detalle ADD COLUMN apu_id INTEGER"),
            ("apu_clave", "ALTER TABLE cotizacion_detalle ADD COLUMN apu_clave VARCHAR(80)"),
            ("apu_directo", "ALTER TABLE cotizacion_detalle ADD COLUMN apu_directo FLOAT DEFAULT 0.0"),
            ("apu_resumen_json", "ALTER TABLE cotizacion_detalle ADD COLUMN apu_resumen_json TEXT"),
        ]:
            if col not in dcols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(detalle extras):", e)

    try:
        apu_cols = _table_columns("apu")
        for col, stmt in [
            ("clave", "ALTER TABLE apu ADD COLUMN clave VARCHAR(50)"),
            ("concepto", "ALTER TABLE apu ADD COLUMN concepto VARCHAR(300)"),
            ("descripcion", "ALTER TABLE apu ADD COLUMN descripcion TEXT"),
            ("categoria", "ALTER TABLE apu ADD COLUMN categoria VARCHAR(120)"),
            ("unidad", "ALTER TABLE apu ADD COLUMN unidad VARCHAR(50) DEFAULT 'm2'"),
            ("cantidad_objetivo", "ALTER TABLE apu ADD COLUMN cantidad_objetivo FLOAT DEFAULT 1.0"),
            ("rendimiento_base", "ALTER TABLE apu ADD COLUMN rendimiento_base FLOAT DEFAULT 1.0"),
            ("jornada_horas", "ALTER TABLE apu ADD COLUMN jornada_horas FLOAT DEFAULT 8.0"),
            ("desperdicio_general_pct", "ALTER TABLE apu ADD COLUMN desperdicio_general_pct FLOAT DEFAULT 0.0"),
            ("herramienta_menor_pct", "ALTER TABLE apu ADD COLUMN herramienta_menor_pct FLOAT DEFAULT 0.0"),
            ("notas", "ALTER TABLE apu ADD COLUMN notas TEXT"),
            ("indirecto_pct", "ALTER TABLE apu ADD COLUMN indirecto_pct FLOAT DEFAULT 0.0"),
            ("utilidad_pct", "ALTER TABLE apu ADD COLUMN utilidad_pct FLOAT DEFAULT 0.0"),
            ("financiamiento_pct", "ALTER TABLE apu ADD COLUMN financiamiento_pct FLOAT DEFAULT 0.0"),
            ("cargos_adicionales_pct", "ALTER TABLE apu ADD COLUMN cargos_adicionales_pct FLOAT DEFAULT 0.0"),
            ("costo_materiales", "ALTER TABLE apu ADD COLUMN costo_materiales FLOAT DEFAULT 0.0"),
            ("costo_mano_obra", "ALTER TABLE apu ADD COLUMN costo_mano_obra FLOAT DEFAULT 0.0"),
            ("costo_maquinaria", "ALTER TABLE apu ADD COLUMN costo_maquinaria FLOAT DEFAULT 0.0"),
            ("costo_herramienta", "ALTER TABLE apu ADD COLUMN costo_herramienta FLOAT DEFAULT 0.0"),
            ("costo_directo", "ALTER TABLE apu ADD COLUMN costo_directo FLOAT DEFAULT 0.0"),
            ("indirecto_monto", "ALTER TABLE apu ADD COLUMN indirecto_monto FLOAT DEFAULT 0.0"),
            ("financiamiento_monto", "ALTER TABLE apu ADD COLUMN financiamiento_monto FLOAT DEFAULT 0.0"),
            ("utilidad_monto", "ALTER TABLE apu ADD COLUMN utilidad_monto FLOAT DEFAULT 0.0"),
            ("cargos_adicionales_monto", "ALTER TABLE apu ADD COLUMN cargos_adicionales_monto FLOAT DEFAULT 0.0"),
            ("precio_unitario", "ALTER TABLE apu ADD COLUMN precio_unitario FLOAT DEFAULT 0.0"),
            ("importe_partida", "ALTER TABLE apu ADD COLUMN importe_partida FLOAT DEFAULT 0.0"),
            ("creado_en", "ALTER TABLE apu ADD COLUMN creado_en DATETIME"),
            ("actualizado_en", "ALTER TABLE apu ADD COLUMN actualizado_en DATETIME"),
        ]:
            if col not in apu_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(apu):", e)

    try:
        apu_det_cols = _table_columns("apu_detalle")
        for col, stmt in [
            ("tipo_insumo", "ALTER TABLE apu_detalle ADD COLUMN tipo_insumo VARCHAR(20) DEFAULT 'material'"),
            ("referencia_id", "ALTER TABLE apu_detalle ADD COLUMN referencia_id INTEGER"),
            ("descripcion", "ALTER TABLE apu_detalle ADD COLUMN descripcion VARCHAR(300) DEFAULT ''"),
            ("codigo", "ALTER TABLE apu_detalle ADD COLUMN codigo VARCHAR(60)"),
            ("categoria", "ALTER TABLE apu_detalle ADD COLUMN categoria VARCHAR(120)"),
            ("unidad", "ALTER TABLE apu_detalle ADD COLUMN unidad VARCHAR(50) DEFAULT 'kg'"),
            ("cantidad", "ALTER TABLE apu_detalle ADD COLUMN cantidad FLOAT DEFAULT 0.0"),
            ("factor", "ALTER TABLE apu_detalle ADD COLUMN factor FLOAT DEFAULT 1.0"),
            ("cuadrilla", "ALTER TABLE apu_detalle ADD COLUMN cuadrilla FLOAT DEFAULT 1.0"),
            ("rendimiento", "ALTER TABLE apu_detalle ADD COLUMN rendimiento FLOAT DEFAULT 0.0"),
            ("desperdicio_pct", "ALTER TABLE apu_detalle ADD COLUMN desperdicio_pct FLOAT DEFAULT 0.0"),
            ("comentario", "ALTER TABLE apu_detalle ADD COLUMN comentario VARCHAR(500)"),
            ("precio_unitario", "ALTER TABLE apu_detalle ADD COLUMN precio_unitario FLOAT DEFAULT 0.0"),
            ("subtotal", "ALTER TABLE apu_detalle ADD COLUMN subtotal FLOAT DEFAULT 0.0"),
        ]:
            if col not in apu_det_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(apu_detalle):", e)

    for table_name in ("apu_material", "apu_mano_obra", "apu_maquinaria"):
        try:
            cols = _table_columns(table_name)
            for col, stmt in [
                ("clave", f"ALTER TABLE {table_name} ADD COLUMN clave VARCHAR(60)"),
                ("categoria", f"ALTER TABLE {table_name} ADD COLUMN categoria VARCHAR(120)"),
            ]:
                if col not in cols:
                    db.session.execute(text(stmt))
            db.session.commit()
        except Exception as e:
            print(f"[WARN] ensure_schema({table_name}):", e)

# ---------------------------------------------------------
# Seed: usuarios base (idempotente)
# ---------------------------------------------------------
def seed_default_users():
    """Crea usuarios base si no existen (no duplica)."""
    defaults = [
        ("Ing. Antonio Azcona", "Azcona123!", "USER"),
        ("Joandlc", "Joan123!", "USER"),
        ("JSolis", "Solis123!", "ADMIN"),
    ]
    created = 0
    for nombre, password, rol in defaults:
        try:
            exists = Usuario.query.filter(db.func.lower(Usuario.nombre) == nombre.lower()).first()
            if exists:
                continue
            u = Usuario(nombre=nombre, rol=rol)
            # Usa el helper del modelo para hashear
            try:
                u.set_password(password)
            except Exception:
                from werkzeug.security import generate_password_hash
                u.password = generate_password_hash(password)
            db.session.add(u)
            created += 1
        except Exception:
            continue
    try:
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
    if created:
        print(f"✅ Seed users: creados {created} usuario(s).")

with app.app_context():
    ensure_schema()

with app.app_context():
    seed_default_users()


# ==============================
# SETUP TEMPORAL ADMIN
# ==============================
@app.route("/setup_admin")
def setup_admin():
    nombre = "Rafa"       # ← cámbialo si quieres
    password = "1234"     # ← cámbialo si quieres
    rol = "ADMIN"         # ADMIN o USER

    u = Usuario.query.filter_by(nombre=nombre).first()
    if u:
        return f"Ya existe el usuario {nombre}"

    u = Usuario(nombre=nombre, rol=rol)
    u.set_password(password)
    db.session.add(u)
    db.session.commit()

    return f"✅ Usuario creado: {nombre} / {password} ({rol})"

# ---------------------------------------------------------
# Helpers (roles + formatting)
# ---------------------------------------------------------
def is_admin() -> bool:
    return bool(getattr(current_user, "is_authenticated", False) and (getattr(current_user, "rol", "") or "").upper() == "ADMIN")

def normalize_user_role(value: str) -> str:
    rol = (value or "").strip().upper()
    return "ADMIN" if rol == "ADMIN" else "USER"

def admin_users_base_query():
    admin_first = case((db.func.upper(Usuario.rol) == "ADMIN", 0), else_=1)
    return Usuario.query.order_by(admin_first, Usuario.nombre.asc())

def responsable_actual() -> str:
    """
    Regla: "solo el primer nombre" (ej. 'Rafa').
    Si el usuario no tiene nombre, regresa vacío.
    """
    nombre = (getattr(current_user, "nombre", "") or "").strip()
    if not nombre:
        return ""
    first = nombre.split()[0].strip()
    # Title-case para igualar tu formato en BD (Rafa, Cesar, etc.)
    return first[:1].upper() + first[1:].lower() if first else ""

def require_owner_or_admin(cot: Cotizacion) -> None:
    if is_admin():
        return
    ra = responsable_actual()
    if not ra or (cot.responsable or "") != ra:
        abort(403)

def require_cliente_owner_or_admin(cli: Cliente) -> None:
    if is_admin():
        return
    ra = responsable_actual()
    if not ra or (cli.responsable or "") != ra:
        abort(403)



def _build_dashboard_cotizaciones_query(
    *,
    desde: str = "",
    hasta: str = "",
    estatus: str = "",
    cliente: str = "",
):
    q = Cotizacion.query.outerjoin(Cliente, Cotizacion.cliente_id == Cliente.id)

    if not is_admin():
        q = q.filter(Cotizacion.responsable == responsable_actual())

    if desde:
        try:
            d = datetime.strptime(desde, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Filtro 'Desde' invalido") from exc
        q = q.filter(Cotizacion.fecha >= d)

    if hasta:
        try:
            h = datetime.strptime(hasta, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
        except ValueError as exc:
            raise ValueError("Filtro 'Hasta' invalido") from exc
        q = q.filter(Cotizacion.fecha <= h)

    if estatus:
        q = q.filter(Cotizacion.estatus == estatus)

    cliente = (cliente or "").strip().lower()
    if cliente:
        pattern = f"%{cliente}%"
        q = q.filter(or_(
            db.func.lower(db.func.coalesce(Cliente.nombre_cliente, "")).like(pattern),
            db.func.lower(db.func.coalesce(Cliente.empresa, "")).like(pattern),
        ))

    return q
def generar_folio() -> str:
    prefix = "PTCH-"
    maxn = 0
    rows = db.session.execute(text("SELECT folio FROM cotizacion WHERE folio LIKE 'PTCH-%'")).fetchall()
    for (folio,) in rows:
        m = re.match(r"PTCH-(\d{4})$", str(folio))
        if m:
            n = int(m.group(1))
            maxn = max(maxn, n)
    for i in range(1, 11):
        cand = f"{prefix}{maxn+i:04d}"
        exists = db.session.execute(text("SELECT 1 FROM cotizacion WHERE folio=:f LIMIT 1"), {"f": cand}).fetchone()
        if not exists:
            return cand
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


def parse_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        s = str(v).strip()
        return int(float(s)) if s else default
    except Exception:
        return default


def _safe_detalle_kwargs(**kwargs):
    valid = set(getattr(CotizacionDetalle, "__table__").columns.keys())
    return {k: v for k, v in kwargs.items() if k in valid}


def _truncate_pdf_text(value, limit=90):
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def parse_datetime_flexible(v) -> Optional[datetime]:
    if v in (None, ""):
        return None
    if isinstance(v, datetime):
        return v
    raw = str(v).strip()
    if not raw:
        return None
    candidates = [raw, raw.replace("Z", "+00:00"), raw + " 00:00:00"]
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
    ]
    for cand in candidates:
        try:
            return datetime.fromisoformat(cand)
        except Exception:
            pass
        for fmt_s in formats:
            try:
                return datetime.strptime(cand, fmt_s)
            except Exception:
                continue
    return None


def _append_note(base: Optional[str], extra: Optional[str]) -> Optional[str]:
    b = (base or "").strip()
    e = (extra or "").strip()
    if not e:
        return b or None
    return f"{b}\n{e}".strip() if b else e


def sample_import_payload() -> dict:
    return {
        "folio": "COT-2026-02-026-2",
        "fecha": "2026-02-26",
        "estatus": "PENDIENTE",
        "responsable": responsable_actual() or "",
        "cliente": {
            "nombre_cliente": "Ing. Adriana Vazquez / Ing. Karla Reyes",
            "empresa": "GIA",
            "correo": "",
            "telefono": "",
            "direccion": "Oracle, Guadalajara",
            "rfc": ""
        },
        "zona": "",
        "iva_porc": 16,
        "notas": "Importada desde cotizacion externa.\nVigencia de la cotizacion: 30 dias.\nAnticipo: 50%.\nEl precio se respeta siempre que se haga el trabajo total en aplicacion continua.\nEl precio no respeta siempre que las areas no sean continuas.\nSe requiere muestreo de tablero de 150 cm a 150 cm por ejecucion que impide la instalacion del sistema.\nEsperando contar con su preferencia me despido y quedo a sus apreciables ordenes.",
        "items": [
            {
                "nombre_concepto": "Suministro y aplicacion de sistema impermeable de curado rapido sobre superficie de concreto",
                "unidad": "m2",
                "cantidad": 880,
                "precio_unitario": 1907.69,
                "sistema": "TREMPROOF JARDIN",
                "descripcion": "Incluye: preparacion de superficie por medios manual mecanicos hasta alcanzar perfil de anclaje; limpieza y sello de juntas con sellador de poliuretano flexible; aplicacion de Tremproof 250 GC; aplicacion de Vapor Barrier; trazo, corte y colocacion de Eucodrain H15P Geotextil; incluye material, equipos, herramienta y personal altamente especializado."
            }
        ]
    }


def _normalize_text_for_match(value: str) -> str:
    raw = str(value or "")
    normalized = unicodedata.normalize("NFKD", raw)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower().strip()


def _clean_pdf_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _extract_pdf_text_and_tables(pdf_bytes: bytes) -> tuple[str, list[list[list[str]]]]:
    try:
        import pdfplumber
    except Exception as e:
        raise ValueError("El servidor no tiene habilitada la lectura de PDFs. Instala las dependencias del proyecto.") from e

    text_parts: list[str] = []
    tables: list[list[list[str]]] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text.strip():
                text_parts.append(page_text)
            for table in page.extract_tables() or []:
                normalized_rows = []
                for row in table or []:
                    cells = [_clean_pdf_text(cell) for cell in (row or [])]
                    if any(cells):
                        normalized_rows.append(cells)
                if normalized_rows:
                    tables.append(normalized_rows)

    full_text = "\n".join(text_parts).strip()
    if not full_text:
        raise ValueError("No se pudo extraer texto legible del PDF.")
    return full_text, tables


def _extract_prefixed_line(text: str, prefix: str) -> str:
    prefix_norm = _normalize_text_for_match(prefix)
    for line in text.splitlines():
        clean = line.strip()
        if not clean:
            continue
        norm = _normalize_text_for_match(clean)
        if norm.startswith(prefix_norm):
            parts = clean.split(":", 1)
            return parts[1].strip() if len(parts) > 1 else clean
    return ""


def _parse_spanish_date_from_pdf(text: str) -> Optional[datetime]:
    match = re.search(r"Ciudad de Mexico a\s+(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})", _normalize_text_for_match(text), re.IGNORECASE)
    if not match:
        return None

    months = {
        "enero": 1,
        "febrero": 2,
        "marzo": 3,
        "abril": 4,
        "mayo": 5,
        "junio": 6,
        "julio": 7,
        "agosto": 8,
        "septiembre": 9,
        "setiembre": 9,
        "octubre": 10,
        "noviembre": 11,
        "diciembre": 12,
    }
    day = int(match.group(1))
    month_name = _normalize_text_for_match(match.group(2))
    month = months.get(month_name)
    year = int(match.group(3))
    if not month:
        return None
    return datetime(year, month, day)


def _parse_pdf_currency(value: str) -> float:
    match = re.search(r"([\d,]+\.\d{1,2})", str(value or ""))
    return parse_float(match.group(1), 0.0) if match else 0.0


def _parse_pdf_quantity_and_unit(value: str) -> tuple[float, str]:
    raw = str(value or "").replace(",", "")
    match = re.search(r"([\d.]+)\s*([A-Za-z0-9/]+)?", raw)
    if not match:
        return 0.0, ""
    quantity = parse_float(match.group(1), 0.0)
    unit = (match.group(2) or "").strip()
    unit = unit.replace("?", "2")
    return quantity, unit.lower()


def _build_concept_name(system: str, description: str) -> str:
    base = re.split(r"incluye\s*:", description, maxsplit=1, flags=re.IGNORECASE)[0].strip(" .;:-")
    if len(base) >= 12:
        return base[:220]
    return (system or description or "Concepto importado")[:220]


def _extract_items_from_sistema_descripcion_pdf_text(text: str) -> list[dict]:
    lines = [_clean_pdf_text(line) for line in (text or "").splitlines() if _clean_pdf_text(line)]
    if not lines:
        return []

    def is_header_or_footer(value: str) -> bool:
        norm = _normalize_text_for_match(value)
        return any(
            norm.startswith(prefix)
            for prefix in (
                "folio:",
                "campos eliseos",
                "telefonos",
                "www.poliutech.com",
                "empresa 100% mexicana",
                "ciudad de mexico a",
                "atte.",
                "ing.",
                "director general",
                "sistema descripcion unidad cantidad p. unitario importe",
                "condiciones comerciales",
            )
        )

    def is_unit_line(value: str) -> bool:
        return _normalize_text_for_match(value) in {"m2", "m 2", "m?"}

    def is_numeric_line(value: str) -> bool:
        return bool(re.fullmatch(r"[\d,.]+", value.strip()))

    def is_money_line(value: str) -> bool:
        return bool(re.fullmatch(r"\$\s*[\d,]+\.\d{1,2}", value.strip()))

    def parse_inline_values(value: str):
        match = re.search(r"(?i)(m2|m?|m\s*2)\s+([\d,.]+)\s+\$\s*([\d,]+\.\d{1,2})\s+\$\s*([\d,]+\.\d{2})", value)
        if not match:
            return None
        return {
            "unidad": "m2",
            "cantidad": parse_float(match.group(2), 0.0),
            "precio_unitario": parse_float(match.group(3), 0.0),
            "subtotal_pdf": parse_float(match.group(4), 0.0),
        }

    def is_system_like(value: str) -> bool:
        s = value.strip()
        if len(s) > 40:
            return False
        letters = [ch for ch in s if ch.isalpha()]
        if not letters:
            return False
        upper_ratio = sum(1 for ch in letters if ch.isupper()) / max(len(letters), 1)
        return upper_ratio >= 0.75

    start_idx = 0
    for idx, line in enumerate(lines):
        norm = _normalize_text_for_match(line)
        if norm.startswith("sistema descripcion unidad cantidad"):
            start_idx = idx + 1
            break

    items = []
    chunk = []
    i = start_idx
    while i < len(lines):
        line = lines[i]
        norm = _normalize_text_for_match(line)
        if norm.startswith(("subtotal", "iva", "total", "condiciones comerciales")):
            break
        if is_header_or_footer(line):
            i += 1
            continue

        parsed = parse_inline_values(line)
        if parsed is None and is_unit_line(line):
            unidad = "m2"
            j = i + 1
            while j < len(lines) and is_header_or_footer(lines[j]):
                j += 1
            if j < len(lines) and is_numeric_line(lines[j]):
                cantidad = parse_float(lines[j], 0.0)
                j += 1
                while j < len(lines) and is_header_or_footer(lines[j]):
                    j += 1
                if j < len(lines) and is_money_line(lines[j]):
                    precio = parse_float(lines[j], 0.0)
                    j += 1
                    while j < len(lines) and is_header_or_footer(lines[j]):
                        j += 1
                    if j < len(lines) and is_money_line(lines[j]):
                        subtotal = parse_float(lines[j], 0.0)
                        parsed = {
                            "unidad": unidad,
                            "cantidad": cantidad,
                            "precio_unitario": precio,
                            "subtotal_pdf": subtotal,
                        }
                        i = j
        if parsed is not None:
            parts = [part for part in chunk if not is_header_or_footer(part)]
            system_lines = []
            description_lines = []
            seen_description = False
            for part in parts:
                if not seen_description and is_system_like(part):
                    system_lines.append(part)
                else:
                    seen_description = True
                    description_lines.append(part)
            if not description_lines and system_lines:
                description_lines = system_lines[:]
                system_lines = []
            descripcion = " ".join(description_lines).strip()
            sistema = " ".join(system_lines).strip() or None
            if descripcion and parsed["cantidad"] > 0 and parsed["precio_unitario"] > 0:
                items.append({
                    "nombre_concepto": descripcion,
                    "unidad": parsed["unidad"],
                    "cantidad": parsed["cantidad"],
                    "precio_unitario": parsed["precio_unitario"],
                    "sistema": sistema,
                    "descripcion": descripcion,
                    "subtotal_pdf": parsed["subtotal_pdf"],
                })
            chunk = []
            i += 1
            continue

        chunk.append(line)
        i += 1

    return items


def _extract_items_from_pdf_tables(tables: list[list[list[str]]]) -> list[dict]:
    def find_column_indexes(header_cells: list[str], aliases: dict[str, tuple[str, ...]]) -> dict[str, int]:
        normalized = [_normalize_text_for_match(cell) for cell in header_cells]
        indexes: dict[str, int] = {}
        for field, options in aliases.items():
            for idx, cell in enumerate(normalized):
                if any(option in cell for option in options):
                    indexes[field] = idx
                    break
        return indexes

    def get_cell(cells: list[str], index_map: dict[str, int], field: str) -> str:
        idx = index_map.get(field)
        if idx is None or idx >= len(cells):
            return ""
        return cells[idx]

    def extract_money_values(cells: list[str]) -> list[float]:
        values = []
        for cell in cells:
            cell_text = str(cell or "")
            if "$" not in cell_text:
                continue
            amount = _parse_pdf_currency(cell_text)
            if amount > 0:
                values.append(amount)
        return values

    def append_continuation(target: dict, extra_text: str) -> None:
        extra = _clean_pdf_text(extra_text)
        if not extra:
            return
        current_name = _clean_pdf_text(target.get("nombre_concepto") or "")
        current_desc = _clean_pdf_text(target.get("descripcion") or current_name)
        target["nombre_concepto"] = _clean_pdf_text(f"{current_name} {extra}")
        target["descripcion"] = _clean_pdf_text(f"{current_desc} {extra}")

    aliases_variants = [
        {
            "concepto": ("concepto", "descripcion del trabajo", "descripcion"),
            "unidad": ("unidad", "uni.", "area / unidad", "area", "?rea / unidad"),
            "cantidad": ("cantidad",),
            "precio_unitario": ("p.u.", "p. unitario", "precio unitario", "p unitario"),
            "importe": ("importe", "subtotal"),
            "sistema": ("sistema",),
            "codigo": ("codigo", "c?digo"),
        },
    ]

    items: list[dict] = []
    active_header_map: dict[str, int] | None = None

    for table in tables:
        if not table:
            continue

        header_row = None
        header_map = None
        for row in table[:4]:
            cells = [_clean_pdf_text(cell) for cell in row]
            if len(cells) < 4:
                continue
            for aliases in aliases_variants:
                indexes = find_column_indexes(cells, aliases)
                has_amounts = "precio_unitario" in indexes and "importe" in indexes
                has_shape = (
                    ("concepto" in indexes or "sistema" in indexes)
                    and has_amounts
                    and ("cantidad" in indexes or "unidad" in indexes)
                )
                if has_shape:
                    header_row = row
                    header_map = indexes
                    break
            if header_map:
                break

        if header_map:
            active_header_map = header_map
            start_index = table.index(header_row) + 1
        elif active_header_map:
            header_map = active_header_map
            start_index = 0
        else:
            continue

        for row in table[start_index:]:
            cells = [_clean_pdf_text(cell) for cell in row]
            if not any(cells):
                continue

            row_norm = _normalize_text_for_match(" ".join(cells))
            if row_norm.startswith(("subtotal", "iva", "total", "condiciones comerciales")):
                break

            concepto = get_cell(cells, header_map, "concepto")
            sistema = get_cell(cells, header_map, "sistema")
            unidad_cell = get_cell(cells, header_map, "unidad")
            cantidad_cell = get_cell(cells, header_map, "cantidad")
            precio_cell = get_cell(cells, header_map, "precio_unitario")
            importe_cell = get_cell(cells, header_map, "importe")

            cantidad = parse_float(cantidad_cell, 0.0) if cantidad_cell else 0.0
            unidad = ""
            if unidad_cell:
                parsed_qty, parsed_unit = _parse_pdf_quantity_and_unit(unidad_cell)
                if cantidad <= 0 and parsed_qty > 0:
                    cantidad = parsed_qty
                unidad = parsed_unit or unidad_cell.strip()

            money_values = extract_money_values(cells)
            precio_unitario = _parse_pdf_currency(precio_cell)
            subtotal_pdf = _parse_pdf_currency(importe_cell)
            if precio_unitario <= 0 and money_values:
                precio_unitario = money_values[0]
            if subtotal_pdf <= 0 and len(money_values) >= 2:
                subtotal_pdf = money_values[-1]

            if cantidad <= 0 or precio_unitario <= 0:
                continuation_bits = []
                for idx, cell in enumerate(cells):
                    if not cell:
                        continue
                    if idx == header_map.get("codigo"):
                        continue
                    if idx == header_map.get("unidad"):
                        continue
                    if idx == header_map.get("cantidad"):
                        continue
                    if idx == header_map.get("precio_unitario"):
                        continue
                    if idx == header_map.get("importe"):
                        continue
                    continuation_bits.append(cell)
                continuation_text = " ".join(bit for bit in continuation_bits if bit)
                if items and continuation_text and not row_norm.startswith(("subtotal", "iva", "total")):
                    append_continuation(items[-1], continuation_text)
                continue

            if not concepto and not sistema:
                continue

            descripcion = concepto or sistema
            items.append({
                "nombre_concepto": concepto or _build_concept_name(sistema, descripcion),
                "unidad": unidad or "m2",
                "cantidad": cantidad,
                "precio_unitario": precio_unitario,
                "sistema": sistema or None,
                "descripcion": descripcion,
                "subtotal_pdf": subtotal_pdf if subtotal_pdf > 0 else None,
            })

    return items


def _looks_like_partida_numbers_as_quantity(items: list[dict]) -> bool:
    if not items or len(items) < 2:
        return False
    quantities = []
    for item in items:
        try:
            quantities.append(float(item.get("cantidad") or 0))
        except Exception:
            return False
    expected = [float(i) for i in range(1, len(quantities) + 1)]
    return quantities == expected


def _extract_items_from_pdf_text(text: str) -> list[dict]:
    lines = [_clean_pdf_text(line) for line in (text or "").splitlines() if _clean_pdf_text(line)]
    if not lines:
        return []

    def is_code_line(value: str) -> bool:
        return bool(re.fullmatch(r"\d{2}", value.strip()))

    def is_unit_line(value: str) -> bool:
        return _normalize_text_for_match(value) in {"m2", "m 2", "m?"}

    def is_money_line(value: str) -> bool:
        return bool(re.fullmatch(r"\$\s*[\d,]+\.\d{2}", value.strip()))

    def is_numeric_line(value: str) -> bool:
        return bool(re.fullmatch(r"[\d,.]+", value.strip()))

    def is_header_or_footer(value: str) -> bool:
        norm = _normalize_text_for_match(value)
        return any(
            norm.startswith(prefix)
            for prefix in (
                "campos eliseos",
                "telefonos",
                "www.poliutech.com",
                "empresa 100% mexicana",
                "ciudad de mexico a",
                "atte.",
                "ing.",
                "director general",
                "codigo concepto unidad cantidad p.u. importe",
            )
        )

    start_idx = 0
    for idx, line in enumerate(lines):
        if "codigo" in _normalize_text_for_match(line) and "importe" in _normalize_text_for_match(line):
            start_idx = idx + 1
            break

    items: list[dict] = []
    i = start_idx
    while i < len(lines):
        line = lines[i]
        norm = _normalize_text_for_match(line)
        if norm.startswith("subtotal") or norm.startswith("iva") or norm.startswith("total"):
            break
        if is_header_or_footer(line):
            i += 1
            continue
        if not is_code_line(line):
            i += 1
            continue

        i += 1
        desc_lines: list[str] = []
        while i < len(lines):
            current = lines[i]
            if is_header_or_footer(current):
                i += 1
                continue
            if is_unit_line(current):
                i += 1
                break
            desc_lines.append(current)
            i += 1

        while i < len(lines) and (is_header_or_footer(lines[i]) or not is_numeric_line(lines[i])):
            if is_code_line(lines[i]) or _normalize_text_for_match(lines[i]).startswith(("subtotal", "iva", "total")):
                break
            i += 1
        if i >= len(lines) or not is_numeric_line(lines[i]):
            break
        quantity = parse_float(lines[i], 0.0)
        i += 1

        while i < len(lines) and not is_money_line(lines[i]):
            if is_header_or_footer(lines[i]):
                i += 1
                continue
            break
        if i >= len(lines) or not is_money_line(lines[i]):
            break
        unit_price = _parse_pdf_currency(lines[i])
        i += 1

        while i < len(lines) and not is_money_line(lines[i]):
            if is_header_or_footer(lines[i]):
                i += 1
                continue
            break
        if i >= len(lines) or not is_money_line(lines[i]):
            break
        line_subtotal = _parse_pdf_currency(lines[i])
        i += 1

        continuation: list[str] = []
        while i < len(lines):
            current = lines[i]
            current_norm = _normalize_text_for_match(current)
            if current_norm.startswith(("subtotal", "iva", "total")) or is_code_line(current):
                break
            if is_header_or_footer(current) or is_money_line(current) or is_numeric_line(current) or is_unit_line(current):
                i += 1
                continue
            continuation.append(current)
            i += 1

        description = " ".join(desc_lines + continuation).strip()
        items.append({
            "nombre_concepto": _build_concept_name("", description),
            "unidad": "m2",
            "cantidad": quantity,
            "precio_unitario": unit_price,
            "sistema": None,
            "descripcion": description,
            "subtotal_pdf": line_subtotal,
        })

    return items


def _extract_items_from_pdf_block_regex(text: str) -> list[dict]:
    compact = re.sub(r"\n+", "\n", text or "")
    pattern = re.compile(
        r"(?ms)^\s*(?P<codigo>\d{2})\s+"
        r"(?P<descripcion>.*?)\s+"
        r"(?P<unidad>M2|M\s*2|M?)\s+"
        r"(?P<cantidad>[\d,.]+)\s+"
        r"\$(?P<precio>[\d,]+\.\d{2})\s+"
        r"\$(?P<subtotal>[\d,]+\.\d{2})\s*"
        r"(?=\d{2}\s+|Subtotal\s+\$|IVA\s+\d|Total\s+\$|$)",
    )
    items: list[dict] = []
    for match in pattern.finditer(compact):
        description = _clean_pdf_text(match.group("descripcion"))
        quantity = parse_float(match.group("cantidad"), 0.0)
        unit_price = parse_float(match.group("precio"), 0.0)
        line_subtotal = parse_float(match.group("subtotal"), 0.0)
        if quantity <= 0 or unit_price <= 0 or line_subtotal <= 0:
            continue
        items.append({
            "nombre_concepto": _build_concept_name("", description),
            "unidad": "m2",
            "cantidad": quantity,
            "precio_unitario": unit_price,
            "sistema": None,
            "descripcion": description,
            "subtotal_pdf": line_subtotal,
        })
    return items


def _extract_conditions_from_pdf(text: str) -> str:
    match = re.search(r"CONDICIONES COMERCIALES\s*:(.*?)(?:Esperando contar con su preferencia|Atte\.|Ing\.)", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    lines = []
    for raw in match.group(1).splitlines():
        clean = raw.strip().lstrip("-?* ").strip()
        if clean:
            lines.append(clean)
    return "\n".join(lines)


def build_import_payload_from_pdf(pdf_bytes: bytes, filename: str, responsable_hint: Optional[str] = None) -> dict:
    text, tables = _extract_pdf_text_and_tables(pdf_bytes)
    normalized_text = _normalize_text_for_match(text)

    # Regla principal: si pdfplumber detecta una tabla con encabezados reconocibles,
    # se respeta el mapeo directo de columnas y no se intenta adivinar.
    items = _extract_items_from_pdf_tables(tables)

    # Fallbacks solo cuando no hubo tabla reconocible.
    if not items:
        if "sistema descripcion unidad cantidad" in normalized_text and "p. unitario" in normalized_text:
            items = _extract_items_from_sistema_descripcion_pdf_text(text)
        elif "codigo" in normalized_text and "cantidad" in normalized_text and "importe" in normalized_text:
            items = _extract_items_from_pdf_text(text)
            if not items:
                items = _extract_items_from_pdf_block_regex(text)
        else:
            items = _extract_items_from_pdf_text(text)
            if not items:
                items = _extract_items_from_pdf_block_regex(text)

    if _looks_like_partida_numbers_as_quantity(items):
        text_items = []
        if "sistema descripcion unidad cantidad" in normalized_text and "p. unitario" in normalized_text:
            text_items = _extract_items_from_sistema_descripcion_pdf_text(text)
        elif "codigo" in normalized_text and "cantidad" in normalized_text and "importe" in normalized_text:
            text_items = _extract_items_from_pdf_text(text)
            if not text_items:
                text_items = _extract_items_from_pdf_block_regex(text)
        if text_items and not _looks_like_partida_numbers_as_quantity(text_items):
            items = text_items

    if not items:
        raise ValueError("No pude identificar conceptos importables dentro del PDF.")

    folio_match = re.search(r"Folio\s*:\s*([A-Z0-9\-]+)", text, re.IGNORECASE)
    folio = folio_match.group(1).strip() if folio_match else None

    fecha = _parse_spanish_date_from_pdf(text) or now_cdmx_naive()
    contacto = _extract_prefixed_line(text, "Con atencion a")
    empresa = _extract_prefixed_line(text, "Empresa")

    ubicacion = ""
    location_match = re.search(r"se realizaran\s+en\s+(.+?)(?:\.|\n)", _normalize_text_for_match(text), re.IGNORECASE)
    if location_match:
        ubicacion = _clean_pdf_text(location_match.group(1))

    iva_porc = 16.0
    iva_pct_match = re.search(r"IVA\s*(\d+(?:\.\d+)?)\s*%", text, re.IGNORECASE)
    if iva_pct_match:
        iva_porc = parse_float(iva_pct_match.group(1), 16.0)

    notas = "Importada desde PDF externo."
    conditions = _extract_conditions_from_pdf(text)
    if conditions:
        notas = _append_note(notas, conditions)

    total_match = re.search(r"Total\s*\$?\s*([\d,]+\.\d{2})", text, re.IGNORECASE)
    if total_match:
        notas = _append_note(notas, f"Total detectado en PDF: ${parse_float(total_match.group(1), 0.0):,.2f}")

    cliente_nombre = contacto or empresa or Path(filename).stem[:120]
    return {
        "folio": folio,
        "fecha": fecha.isoformat(sep=" "),
        "estatus": "PENDIENTE",
        "responsable": responsable_hint or "",
        "cliente": {
            "nombre_cliente": cliente_nombre,
            "empresa": empresa or None,
            "correo": None,
            "telefono": None,
            "direccion": ubicacion or None,
            "rfc": None,
        },
        "zona": "",
        "iva_porc": iva_porc,
        "notas": notas,
        "items": items,
    }


def _normalize_import_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("El JSON debe ser un objeto.")

    cliente_in = payload.get("cliente") or {}
    if not isinstance(cliente_in, dict):
        raise ValueError("'cliente' debe ser un objeto.")

    items_in = payload.get("items") or payload.get("conceptos") or payload.get("detalles") or []
    if not isinstance(items_in, list) or not items_in:
        raise ValueError("Debes enviar al menos un concepto en 'items'.")

    cliente = {
        "nombre_cliente": (cliente_in.get("nombre_cliente") or cliente_in.get("cliente") or payload.get("cliente_nombre") or payload.get("cliente") or "").strip(),
        "empresa": (cliente_in.get("empresa") or payload.get("empresa") or "").strip() or None,
        "correo": (cliente_in.get("correo") or payload.get("correo") or "").strip() or None,
        "telefono": (cliente_in.get("telefono") or payload.get("telefono") or "").strip() or None,
        "direccion": (cliente_in.get("direccion") or payload.get("direccion") or "").strip() or None,
        "rfc": (cliente_in.get("rfc") or payload.get("rfc") or "").strip() or None,
    }
    if not cliente["nombre_cliente"]:
        raise ValueError("Falta 'cliente.nombre_cliente'.")

    normalized_items = []
    for idx, item in enumerate(items_in, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"El concepto #{idx} debe ser un objeto.")
        nombre = (item.get("nombre_concepto") or item.get("concepto") or item.get("nombre") or "").strip()
        if not nombre:
            raise ValueError(f"El concepto #{idx} no tiene nombre.")
        normalized_items.append({
            "nombre_concepto": nombre,
            "unidad": (item.get("unidad") or "").strip(),
            "cantidad": parse_float(item.get("cantidad"), 1.0),
            "precio_unitario": parse_float(item.get("precio_unitario", item.get("precio")), 0.0),
            "sistema": (item.get("sistema") or "").strip() or None,
            "descripcion": (item.get("descripcion") or "").strip(),
            "subtotal_pdf": parse_float(item.get("subtotal_pdf", item.get("importe")), 0.0),
        })

    return {
        "folio": (payload.get("folio") or payload.get("folio_externo") or "").strip() or None,
        "fecha": parse_datetime_flexible(payload.get("fecha")) or now_cdmx_naive(),
        "estatus": (payload.get("estatus") or "PENDIENTE").strip().upper(),
        "responsable": (payload.get("responsable") or "").strip() or None,
        "cliente": cliente,
        "zona": (payload.get("zona") or "").strip(),
        "iva_porc": parse_float(payload.get("iva_porc"), 16.0),
        "notas": (payload.get("notas") or "").strip() or None,
        "items": normalized_items,
    }


def _find_or_create_cliente_import(cliente_data: dict, responsable_final: Optional[str]) -> Cliente:
    nombre_cliente = (cliente_data.get("nombre_cliente") or "").strip()
    empresa = (cliente_data.get("empresa") or "").strip()

    q = Cliente.query.filter(db.func.lower(Cliente.nombre_cliente) == nombre_cliente.lower())
    if empresa:
        q = q.filter(db.func.lower(Cliente.empresa) == empresa.lower())
    cliente = q.first()
    if cliente:
        return cliente

    cliente = Cliente(
        nombre_cliente=nombre_cliente,
        empresa=empresa or None,
        responsable=responsable_final,
        correo=cliente_data.get("correo"),
        telefono=cliente_data.get("telefono"),
        direccion=cliente_data.get("direccion"),
        rfc=cliente_data.get("rfc"),
    )
    db.session.add(cliente)
    db.session.flush()
    return cliente


def _pick_import_folio(preferred_folio: Optional[str]) -> str:
    preferred = (preferred_folio or "").strip()
    if preferred:
        exists = db.session.execute(text("SELECT 1 FROM cotizacion WHERE folio=:f LIMIT 1"), {"f": preferred}).fetchone()
        if not exists:
            return preferred
    return generar_folio()


def import_external_quote_payload(payload: dict, source_label: Optional[str] = None) -> Cotizacion:
    normalized = _normalize_import_payload(payload)
    responsable_final = normalized["responsable"] or None
    cliente = _find_or_create_cliente_import(normalized["cliente"], responsable_final)

    subtotal = 0.0
    detail_rows = []
    for item in normalized["items"]:
        line_subtotal = fmt(item.get("subtotal_pdf") or (item["cantidad"] * item["precio_unitario"]))
        subtotal += line_subtotal
        detail_rows.append((item, line_subtotal))

    zona = normalized["zona"]
    desc_porc = float({
        "Zona Norte": 10.0,
        "Zona Centro": 5.0,
        "Bajio": 10.0,
        "Zona Sur": 15.0,
        "Frontera": 8.0,
    }.get(zona, 0.0))
    descuento_total = subtotal * (desc_porc / 100.0)
    subtotal_desc = subtotal - descuento_total
    iva_monto = subtotal_desc * (normalized["iva_porc"] / 100.0)
    total = subtotal_desc + iva_monto

    notas = normalized["notas"]
    if source_label:
        notas = _append_note(notas, f"Importada desde: {source_label}")
    if normalized["folio"]:
        notas = _append_note(notas, f"Folio externo original: {normalized['folio']}")
    if zona and desc_porc > 0:
        notas = _append_note(notas, f"Zona: {zona} ({int(desc_porc)}% descuento)")

    cot = Cotizacion(
        folio=_pick_import_folio(normalized["folio"]),
        fecha=normalized["fecha"],
        cliente_id=cliente.id,
        estatus=normalized["estatus"],
        subtotal=fmt(subtotal),
        descuento_total=fmt(descuento_total),
        iva_porc=fmt(normalized["iva_porc"]),
        iva_monto=fmt(iva_monto),
        total=fmt(total),
        notas=notas,
        last_whatsapp_at=None,
        responsable=responsable_final,
    )
    db.session.add(cot)
    db.session.flush()

    for item, line_subtotal in detail_rows:
        concepto = Concepto.query.filter_by(nombre_concepto=item["nombre_concepto"]).first()
        if not concepto:
            concepto = Concepto(
                nombre_concepto=item["nombre_concepto"],
                unidad=item["unidad"] or None,
                precio_unitario=item["precio_unitario"],
                descripcion=item["descripcion"] or None,
            )
            db.session.add(concepto)
            db.session.flush()

        det = CotizacionDetalle(
            cotizacion_id=cot.id,
            concepto_id=concepto.id if concepto else None,
            nombre_concepto=item["nombre_concepto"],
            unidad=item["unidad"],
            cantidad=item["cantidad"],
            precio_unitario=item["precio_unitario"],
            sistema=item["sistema"],
            descripcion=item["descripcion"],
            subtotal=line_subtotal,
        )
        db.session.add(det)

    db.session.commit()
    return cot

def money(n: float) -> str:
    try:
        return "${:,.2f}".format(float(n or 0))
    except Exception:
        return "${:,.2f}".format(0)

def cantidad_en_letra_mn(total: float) -> str:
    try:
        from num2words import num2words
    except Exception:
        entero = int(total)
        cents = int(round((total - entero) * 100)) % 100
        return f"Cantidad en letra: {entero} pesos {cents:02d}/100 M.N."
    entero = int(total)
    cents = int(round((total - entero) * 100)) % 100
    palabras = num2words(entero, lang="es").strip()
    if palabras.endswith(" uno"):
        palabras = palabras[:-4] + " un"
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
    # Si ya viene con 52, lo dejamos; si no, lo anteponemos
    if digits.startswith("52"):
        return f"whatsapp:+{digits}"
    return f"whatsapp:+52{digits}"

def can_send_whatsapp() -> bool:
    return bool(twilio_client and TWILIO_WHATSAPP and ADMIN_LIST)

def send_whatsapp_multi(to_list: Iterable[str], body: str) -> None:
    if not to_list:
        return
    if not can_send_whatsapp():
        print("[Twilio] Config incompleta; omito envío.")
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
# 🔐 Login / Logout
# ---------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        # Acepta varios names posibles del form
        nombre = (
            request.form.get("nombre")
            or request.form.get("username")
            or request.form.get("usuario")
            or request.form.get("user")
            or ""
        ).strip()

        password = (
            request.form.get("password")
            or request.form.get("clave")
            or request.form.get("pass")
            or ""
        ).strip()

        # DEBUG mínimo (se ve en logs de Render)
        print("[LOGIN] form keys:", list(request.form.keys()))
        print("[LOGIN] nombre recibido:", repr(nombre))

        # Match case-insensitive (Admin vs admin, Rafa vs rafa)
        u = Usuario.query.filter(db.func.lower(Usuario.nombre) == nombre.lower()).first()
        print("[LOGIN] usuario encontrado:", u)

        if not u:
            flash("Credenciales inválidas.", "danger")
            return redirect(url_for("login"))

        ok = u.check_password(password)
        print("[LOGIN] password ok:", ok)

        if not ok:
            flash("Credenciales inválidas.", "danger")
            return redirect(url_for("login"))

        login_user(u)
        # Redirige a la página solicitada originalmente (si viene)
        nxt = request.args.get("next")
        if nxt:
            try:
                # Evita open-redirect (solo paths internos)
                p = urlparse(nxt)
                if p.netloc == "" and (nxt.startswith("/") or nxt.startswith("?")):
                    return redirect(nxt)
            except Exception:
                pass
        return redirect(url_for("index"))

    return render_template("login.html", title="Login")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

# ---------------------------------------------------------
# Dashboard / Catálogos / Cotizador
# ---------------------------------------------------------
@app.route("/")
@login_required
def index():
    # ADMIN: ve todo
    # USER: ve SOLO lo suyo por responsable
    if is_admin():
        total_cotizaciones = Cotizacion.query.count()
        total_importe = db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0
        cotizaciones = Cotizacion.query.order_by(Cotizacion.fecha.desc()).limit(100).all()
    else:
        ra = responsable_actual()
        total_cotizaciones = Cotizacion.query.filter_by(responsable=ra).count()
        total_importe = (db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0))
                         .filter(Cotizacion.responsable == ra).scalar() or 0)
        cotizaciones = (Cotizacion.query.filter_by(responsable=ra)
                        .order_by(Cotizacion.fecha.desc()).limit(100).all())

    total_catalogo = Concepto.query.count()

    return render_template(
        "dashboard.html",
        title="Sistema MAR",
        total_cotizaciones=total_cotizaciones,
        total_importe=float(total_importe),
        total_catalogo=total_catalogo,
        cotizaciones=cotizaciones,
        show_splash=True
    )

@app.route("/cotizador")
@login_required
def cotizador():
    apu_catalog = [
        {
            "id": a.id,
            "clave": a.clave or "",
            "concepto": a.concepto or "",
            "categoria": getattr(a, "categoria", "") or "",
            "unidad": a.unidad or "",
            "precio_unitario": float(a.precio_unitario or 0),
            "costo_directo": float(a.costo_directo or 0),
            "descripcion": getattr(a, "descripcion", "") or "",
            "indirecto_monto": float(getattr(a, "indirecto_monto", 0) or 0),
            "financiamiento_monto": float(getattr(a, "financiamiento_monto", 0) or 0),
            "utilidad_monto": float(getattr(a, "utilidad_monto", 0) or 0),
            "cargos_adicionales_monto": float(getattr(a, "cargos_adicionales_monto", 0) or 0),
        }
        for a in APU.query.order_by(APU.concepto.asc()).all()
    ]
    return render_template("cotizador.html", title="Nuevo - Sistema MAR", apu_catalog=apu_catalog)


@app.route("/admin/cotizaciones/importar", methods=["GET", "POST"])
@login_required
def importar_cotizacion_externa():
    if not is_admin():
        abort(403)

    detected = None

    if request.method == "POST":
        uploaded = request.files.get("cotizacion_pdf")
        responsable_destino = (request.form.get("responsable_destino") or "").strip() or responsable_actual()

        if not uploaded or not (uploaded.filename or "").strip():
            flash("Selecciona un PDF antes de importar.", "danger")
        else:
            try:
                pdf_bytes = uploaded.read()
                if not pdf_bytes:
                    raise ValueError("El archivo PDF llego vacio.")

                payload = build_import_payload_from_pdf(
                    pdf_bytes,
                    uploaded.filename or "cotizacion.pdf",
                    responsable_hint=responsable_destino,
                )
                detected = _normalize_import_payload(payload)
                subtotal_detectado = sum((it.get("cantidad") or 0) * (it.get("precio_unitario") or 0) for it in detected["items"])
                total_detectado = subtotal_detectado * (1 + ((detected.get("iva_porc") or 0) / 100.0))
                detected["total_calculado"] = fmt(total_detectado)
                cot = import_external_quote_payload(payload, source_label=uploaded.filename or "cotizacion.pdf")
                flash(f"Cotizacion importada correctamente: {cot.folio}", "success")
                return redirect(url_for("view_cotizacion", cot_id=cot.id))
            except Exception as e:
                try:
                    print(f"[IMPORTADOR PDF] ERROR: {e}", file=sys.stderr)
                    traceback.print_exc()
                except Exception:
                    pass
                flash(f"No se pudo importar la cotizacion: {e}", "danger")

    return render_template(
        "cotizacion_import.html",
        title="Importar cotizacion - Sistema MAR",
        detected=detected,
    )
@app.route("/admin/catalogos")
@login_required
def admin_catalogos():
    page_clientes = request.args.get("page_clientes", 1, type=int)
    page_conceptos = request.args.get("page_conceptos", 1, type=int)

    qc = Cliente.query
    if not is_admin():
        qc = qc.filter(Cliente.responsable == responsable_actual())

    clientes_pag = qc.order_by(Cliente.id.desc()).paginate(page=page_clientes, per_page=10, error_out=False)
    conceptos_pag = Concepto.query.order_by(Concepto.id.desc()).paginate(page=page_conceptos, per_page=10, error_out=False)

    return render_template(
        "admin_catalogos.html",
        title="Admin Catálogos",
        clientes=clientes_pag.items,
        clientes_pag=clientes_pag,
        conceptos=conceptos_pag.items,
        conceptos_pag=conceptos_pag
    )

# ---------------------------------------------------------
# Autocompletar (con filtro por responsable en clientes)
# ---------------------------------------------------------
@app.route("/api/clientes/suggest")
@login_required
def api_clientes_suggest():
    q = (request.args.get("q", "")).strip()
    if len(q) < 1:
        return jsonify([])

    resq = (Cliente.query
            .filter(
                (Cliente.nombre_cliente.ilike(f"%{q}%")) |
                (Cliente.empresa.ilike(f"%{q}%"))
            ))

    if not is_admin():
        resq = resq.filter(Cliente.responsable == responsable_actual())

    res = (resq.order_by(Cliente.nombre_cliente).limit(10).all())

    return jsonify([{
        "label": f"{c.nombre_cliente} · {c.empresa}" if c.empresa else c.nombre_cliente,
        "nombre_cliente": c.nombre_cliente,
        "empresa": c.empresa,
        "responsable": c.responsable,
        "correo": c.correo,
        "telefono": c.telefono,
        "direccion": c.direccion,
        "rfc": c.rfc,
        "sistema": getattr(c, "sistema", "") or ""
    } for c in res])

@app.route("/api/conceptos/suggest")
@login_required
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
# Crear/Editar/Ver/Exportar Cotizaciones
# ---------------------------------------------------------
@app.route("/cotizaciones/crear", methods=["POST"])
@login_required
def crear_cotizacion():
    f = request.form

    nombre_cliente = (f.get("cliente") or f.get("cliente_nombre") or "").strip()
    empresa = (f.get("empresa") or "").strip()

    # === responsable_final ===
    # USER: siempre su nombre (primer nombre)
    # ADMIN: puede mandar responsable desde form; si no manda, queda vacío
    if is_admin():
        responsable_final = (f.get("responsable") or "").strip()
        # si admin dejó vacío, NO inventamos; queda None
    else:
        responsable_final = responsable_actual()

    responsable_final = responsable_final or None

    # --- CREAR O BUSCAR CLIENTE ---
    cliente = None
    if nombre_cliente:
        q = Cliente.query.filter(db.func.lower(Cliente.nombre_cliente) == nombre_cliente.lower())
        if empresa:
            q = q.filter(db.func.lower(Cliente.empresa) == empresa.lower())

        if not is_admin():
            q = q.filter(Cliente.responsable == (responsable_final or ""))

        cliente = q.first()

        if not cliente:
            cliente = Cliente(
                nombre_cliente=nombre_cliente.strip(),
                empresa=empresa.strip() or None,
                responsable=responsable_final,
                correo=(f.get("correo") or "").strip() or None,
                telefono=(f.get("telefono") or "").strip() or None,
                direccion=(f.get("direccion") or "").strip() or None,
                rfc=(f.get("rfc") or "").strip() or None,  # en BD, aunque en PDF ya no lo mostramos
            )
            db.session.add(cliente)
            db.session.flush()

    iva_porc = parse_float(f.get("iva_porc"), 16.0)

    # --- Zona (descuento) ---
    zona = (f.get("zona") or "").strip()
    ZONA_PORC = {
        "Zona Norte": 10.0,
        "Zona Centro": 5.0,
        "Bajío": 10.0,
        "Zona Sur": 15.0,
        "Frontera": 8.0,
    }
    desc_porc = float(ZONA_PORC.get(zona, 0.0))

    cot = Cotizacion(
        folio=generar_folio(),
        fecha=now_cdmx_naive(),
        cliente_id=cliente.id if cliente else None,
        estatus=(f.get("estatus") or "PENDIENTE").upper(),
        notas=(f.get("notas") or "").strip() or None,
        last_whatsapp_at=None,
        responsable=responsable_final
    )
    db.session.add(cot)
    db.session.flush()

    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    capitulos = f.getlist("item_capitulo[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    sistemas = f.getlist("item_sistema[]")
    descripciones = f.getlist("item_descripcion[]")
    origenes = f.getlist("item_origen[]")
    apu_ids = f.getlist("item_apu_id[]")
    apu_claves = f.getlist("item_apu_clave[]")
    apu_directos = f.getlist("item_apu_directo[]")
    apu_resumenes = f.getlist("item_apu_resumen[]")

    subtotal = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios))
    for i in range(n):
        nom = (nombres[i] if i < len(nombres) else "").strip()
        if not nom:
            continue
        uni = (unidades[i] if i < len(unidades) else "").strip()
        cap = (capitulos[i] if i < len(capitulos) else "").strip() or None
        cant = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        pu   = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        sis  = (sistemas[i] if i < len(sistemas) else "").strip()
        desc = (descripciones[i] if i < len(descripciones) else "") or ""
        origen = (origenes[i] if i < len(origenes) else "").strip() or None
        apu_id_val = parse_int(apu_ids[i] if i < len(apu_ids) else None, None)
        apu_clave_val = (apu_claves[i] if i < len(apu_claves) else "").strip() or None
        apu_directo_val = parse_float(apu_directos[i] if i < len(apu_directos) else 0, 0.0)
        apu_resumen_val = (apu_resumenes[i] if i < len(apu_resumenes) else "").strip() or None

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

        det = CotizacionDetalle(**_safe_detalle_kwargs(
            cotizacion_id=cot.id,
            concepto_id=concepto.id if concepto else None,
            nombre_concepto=nom,
            unidad=uni,
            capitulo=cap,
            cantidad=cant,
            precio_unitario=pu,
            sistema=sis or None,
            descripcion=desc,
            subtotal=line_subtotal,
            origen=origen,
            apu_id=apu_id_val,
            apu_clave=apu_clave_val,
            apu_directo=apu_directo_val,
            apu_resumen_json=apu_resumen_val,
        ))
        db.session.add(det)

    # --- aplicar descuento por zona sobre subtotal ---
    descuento_total = subtotal * (desc_porc / 100.0)
    subtotal_desc = subtotal - descuento_total
    iva_monto = subtotal_desc * (iva_porc / 100.0)
    total = subtotal_desc + iva_monto

    # --- trazabilidad de zona en Condiciones Comerciales (notas) ---
    if zona and desc_porc > 0:
        zona_line = f"Zona: {zona} ({int(desc_porc)}% descuento)"
        notas = (cot.notas or "").strip()
        # elimina cualquier línea previa de Zona:
        notas_lines = [ln for ln in notas.splitlines() if ln.strip() and not ln.strip().lower().startswith("zona:")]
        notas_lines.append(zona_line)
        cot.notas = "\n".join(notas_lines).strip()

    cot.subtotal = fmt(subtotal)
    cot.descuento_total = fmt(descuento_total)
    cot.iva_porc = fmt(iva_porc)
    cot.iva_monto = fmt(iva_monto)
    cot.total = fmt(total)
    db.session.commit()

    # --- Notificación WhatsApp ---
    try:
        msg = (
            "🧾 *Nueva Cotización Creada*\\n"
            f"Folio: *{cot.folio}*\\n"
            f"Estatus: *{cot.estatus}*\\n"
            f"Fecha (CDMX): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\\n"
            f"Total: {money(cot.total)}"
        )
        send_whatsapp_multi(ADMIN_LIST, msg)
    except Exception as e:
        print(f"[WARN] WhatsApp creación ({cot.folio}): {e}", file=sys.stderr)

    # --- Apertura automática del PDF ---
    pdf_url = url_for("export_cotizacion_pdf", cot_id=cot.id)
    volver = url_for("cotizador")

    return f"""<!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>{cot.folio}</title>
    <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>
    </head>
    <body>
    <script>
      Swal.fire({{
        icon: 'success',
        title: 'Cotización creada con éxito',
        html: 'Folio: <b>{cot.folio}</b><br>Se abrirá el PDF automáticamente.',
        timer: 2500,
        timerProgressBar: true,
        showConfirmButton: false,
        didOpen: () => {{
          window.open("{pdf_url}", "_blank");
          setTimeout(() => {{
            window.location.href = "{volver}";
          }}, 2500);
        }}
      }});
    </script>
    </body>
    </html>"""

@app.route("/cotizaciones/<int:cot_id>/editar")
@login_required
def editar_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)
    # zona actual (si existe) viene persistida en notas como: "Zona: ... (X% descuento)"
    zona_actual = ""
    try:
        if c.notas:
            for ln in str(c.notas).splitlines():
                if ln.strip().lower().startswith("zona:"):
                    # Zona: <NOMBRE> (..)
                    tmp = ln.split(":", 1)[1].strip()
                    zona_actual = tmp.split("(", 1)[0].strip()
                    break
    except Exception:
        zona_actual = ""
    notas_adicionales, _ = _split_notas_y_zona(c.notas or "")
    return render_template("cotizacion_edit.html", c=c, zona_actual=zona_actual, notas_adicionales=notas_adicionales, title=f"Editar {c.folio}")

@app.route("/cotizaciones/<int:cot_id>/actualizar", methods=["POST"])
@login_required
def actualizar_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    f = request.form

    # === CLIENTE ===
    cliente_nombre = (f.get("cliente") or f.get("cliente_nombre") or "").strip()
    empresa = (f.get("empresa") or "").strip()

    # solo admin puede reasignar responsable
    if is_admin():
        responsable_form = (f.get("responsable") or "").strip()
        responsable_final = responsable_form or c.responsable
    else:
        responsable_final = responsable_actual() or c.responsable

    correo = (f.get("correo") or "").strip()
    telefono = (f.get("telefono") or "").strip()
    direccion = (f.get("direccion") or "").strip()
    rfc = (f.get("rfc") or "").strip()

    cliente = None
    if cliente_nombre:
        cliente = Cliente.query.filter_by(nombre_cliente=cliente_nombre).first()
        if cliente and not is_admin():
            require_cliente_owner_or_admin(cliente)

        if not cliente:
            cliente = Cliente(
                nombre_cliente=cliente_nombre,
                empresa=empresa or None,
                responsable=responsable_final or None,
                correo=correo or None,
                telefono=telefono or None,
                direccion=direccion or None,
                rfc=rfc or None,
            )
            db.session.add(cliente)
            db.session.flush()
            print(f"[INFO] Nuevo cliente agregado (en actualización): {cliente_nombre}")
        c.cliente_id = cliente.id

    # === ENCABEZADO ===
    c.estatus = (f.get("estatus") or c.estatus).upper()
    c.notas = (f.get("notas") or "").strip()
    c.responsable = (responsable_final or c.responsable)
    iva_porc = parse_float(f.get("iva_porc"), c.iva_porc or 16.0)

    # --- Zona (descuento) ---
    zona = (f.get("zona") or "").strip()
    ZONA_PORC = {
        "Zona Norte": 10.0,
        "Zona Centro": 5.0,
        "Bajío": 10.0,
        "Zona Sur": 15.0,
        "Frontera": 8.0,
    }
    desc_porc = float(ZONA_PORC.get(zona, 0.0))

    # === LIMPIAR DETALLES EXISTENTES ===
    for d in list(c.detalles):
        db.session.delete(d)

    # === DETALLES NUEVOS ===
    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    capitulos = f.getlist("item_capitulo[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    sistemas = f.getlist("item_sistema[]")
    descripciones = f.getlist("item_descripcion[]")
    origenes = f.getlist("item_origen[]")
    apu_ids = f.getlist("item_apu_id[]")
    apu_claves = f.getlist("item_apu_clave[]")
    apu_directos = f.getlist("item_apu_directo[]")
    apu_resumenes = f.getlist("item_apu_resumen[]")

    subtotal = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios))
    for i in range(n):
        nombre = (nombres[i] if i < len(nombres) else "").strip()
        if not nombre:
            continue
        unidad = (unidades[i] if i < len(unidades) else "").strip()
        capitulo = (capitulos[i] if i < len(capitulos) else "").strip() or None
        cantidad = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        precio = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        sistema = (sistemas[i] if i < len(sistemas) else "").strip()
        descripcion = (descripciones[i] if i < len(descripciones) else "").strip()
        origen = (origenes[i] if i < len(origenes) else "").strip() or None
        apu_id_val = parse_int(apu_ids[i] if i < len(apu_ids) else None, None)
        apu_clave_val = (apu_claves[i] if i < len(apu_claves) else "").strip() or None
        apu_directo_val = parse_float(apu_directos[i] if i < len(apu_directos) else 0, 0.0)
        apu_resumen_val = (apu_resumenes[i] if i < len(apu_resumenes) else "").strip() or None

        linea_subtotal = cantidad * precio
        subtotal += linea_subtotal

        concepto = Concepto.query.filter_by(nombre_concepto=nombre).first()
        if not concepto:
            concepto = Concepto(
                nombre_concepto=nombre,
                unidad=unidad or None,
                precio_unitario=precio,
                descripcion=descripcion or None,
            )
            db.session.add(concepto)
            db.session.flush()
            print(f"[INFO] Nuevo concepto agregado (en actualización): {nombre}")

        det = CotizacionDetalle(**_safe_detalle_kwargs(
            cotizacion_id=c.id,
            concepto_id=concepto.id,
            nombre_concepto=nombre,
            unidad=unidad,
            capitulo=capitulo,
            cantidad=cantidad,
            precio_unitario=precio,
            sistema=sistema or None,
            descripcion=descripcion,
            subtotal=linea_subtotal,
            origen=origen,
            apu_id=apu_id_val,
            apu_clave=apu_clave_val,
            apu_directo=apu_directo_val,
            apu_resumen_json=apu_resumen_val,
        ))
        db.session.add(det)

    # === TOTALES ===
    descuento_total = subtotal * (desc_porc / 100.0)
    subtotal_desc = subtotal - descuento_total
    iva_monto = subtotal_desc * (iva_porc / 100.0)
    total = subtotal_desc + iva_monto

    if zona and desc_porc > 0:
        zona_line = f"Zona: {zona} ({int(desc_porc)}% descuento)"
        notas = (c.notas or "").strip()
        notas_lines = [ln for ln in notas.splitlines() if ln.strip() and not ln.strip().lower().startswith("zona:")]
        notas_lines.append(zona_line)
        c.notas = "\n".join(notas_lines).strip()

    c.subtotal = fmt(subtotal)
    c.descuento_total = fmt(descuento_total)
    c.iva_porc = fmt(iva_porc)
    c.iva_monto = fmt(iva_monto)
    c.total = fmt(total)

    db.session.commit()

    # --- WhatsApp en actualización ---
    try:
        body = (
            "🔄 *Actualización de Cotización*\\n"
            f"Folio: *{c.folio}*\\n"
            f"Estatus: *{c.estatus}*\\n"
            f"Total: {money(c.total)}"
        )
        send_whatsapp_multi(ADMIN_LIST, body)
    except Exception as e:
        print(f"[Twilio] Error en actualización: {e}", file=sys.stderr)

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

@app.route("/cotizaciones/<int:cot_id>/eliminar")
@login_required
def eliminar_cotizacion(cot_id):
    cot = Cotizacion.query.get_or_404(cot_id)
    # ✅ Solo ADMIN puede eliminar
    if not is_admin():
        abort(403)

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


@app.route("/cotizaciones/bulk-eliminar", methods=["POST"])
@login_required
def bulk_eliminar_cotizaciones():
    """Elimina múltiples cotizaciones seleccionadas desde el dashboard.

    ✅ Solo ADMIN.
    """
    if not is_admin():
        return jsonify({"error": "Solo el administrador puede eliminar cotizaciones."}), 403
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids")
    if not isinstance(ids, list):
        # también soporta form-data: ids[]=1&ids[]=2
        ids = request.form.getlist("ids")

    # Normalizar
    norm_ids: List[int] = []
    for x in ids or []:
        try:
            norm_ids.append(int(x))
        except Exception:
            continue

    # limitar para evitar borrados accidentales enormes
    norm_ids = list(dict.fromkeys(norm_ids))[:500]
    if not norm_ids:
        return jsonify({"error": "No se recibieron IDs válidos"}), 400

    deleted_ids: List[int] = []
    skipped = 0

    try:
        for cot_id in norm_ids:
            cot = Cotizacion.query.get(cot_id)
            if not cot:
                skipped += 1
                continue

            # (Admin-only) — no validación de ownership

            for d in list(cot.detalles):
                db.session.delete(d)
            db.session.delete(cot)
            deleted_ids.append(cot_id)

        db.session.commit()
        return jsonify({"deleted": len(deleted_ids), "skipped": skipped, "deleted_ids": deleted_ids})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@app.route("/cotizaciones/bulk-eliminar-filtradas", methods=["POST"])
@login_required
def bulk_eliminar_filtradas():
    """Elimina cotizaciones visibles por filtros del dashboard.

    ✅ Solo ADMIN.
    Recibe JSON: { filters: { desde:'YYYY-MM-DD', hasta:'YYYY-MM-DD', estatus:'', cliente:'' } }
    """
    if not is_admin():
        return jsonify({"error": "Solo el administrador puede eliminar cotizaciones."}), 403

    payload = request.get_json(silent=True) or {}
    filters = payload.get("filters") or {}

    desde_s = (filters.get("desde") or "").strip()
    hasta_s = (filters.get("hasta") or "").strip()
    estatus_s = (filters.get("estatus") or "").strip()
    cliente_s = (filters.get("cliente") or "").strip().lower()

    try:
        q = _build_dashboard_cotizaciones_query(
            desde=desde_s,
            hasta=hasta_s,
            estatus=estatus_s,
            cliente=cliente_s,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    q = q.order_by(Cotizacion.fecha.desc())

    MAX_DELETE = 2000
    items = q.limit(MAX_DELETE + 1).all()
    if len(items) > MAX_DELETE:
        return jsonify({
            "error": f"Demasiadas cotizaciones para eliminar ({MAX_DELETE}+). Ajusta filtros y vuelve a intentar."
        }), 400

    if not items:
        return jsonify({"deleted": 0, "deleted_ids": []})

    deleted_ids: List[int] = []
    try:
        for cot in items:
            cot_id = cot.id
            for d in list(cot.detalles):
                db.session.delete(d)
            db.session.delete(cot)
            deleted_ids.append(cot_id)

        db.session.commit()
        return jsonify({"deleted": len(deleted_ids), "deleted_ids": deleted_ids})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@app.route("/cotizaciones")
@login_required
def list_cotizaciones():
    page = int(request.args.get("p", 1) or 1)
    per_page = 25

    q = Cotizacion.query
    if not is_admin():
        q = q.filter(Cotizacion.responsable == responsable_actual())

    q = q.order_by(Cotizacion.fecha.desc())

    total = q.count()
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    items = q.offset((page-1)*per_page).limit(per_page).all()

    return render_template(
        "cotizaciones_list.html",
        items=items, page=page, pages=pages, total=total,
        title="Cotizaciones · Sistema MAR"
    )

@app.route("/cotizaciones/<int:cot_id>")
@login_required
def view_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)
    zona_actual = ""
    try:
        if c.notas:
            for ln in str(c.notas).splitlines():
                if ln.strip().lower().startswith("zona:"):
                    tmp = ln.split(":", 1)[1].strip()
                    zona_actual = tmp.split("(", 1)[0].strip()
                    break
    except Exception:
        zona_actual = ""
    condiciones_finales = _condiciones_comerciales_finales(c.notas or "")
    notas_adicionales, _ = _split_notas_y_zona(c.notas or "")
    return render_template("cotizacion_view.html", c=c, zona_actual=zona_actual, condiciones_finales=condiciones_finales, notas_adicionales=notas_adicionales, title=f"Ver {c.folio}")

@app.route("/cotizaciones/<int:cot_id>/ver")
@login_required
def ver_cotizacion(cot_id: int):
    cot = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(cot)
    condiciones_finales = _condiciones_comerciales_finales(cot.notas or "")
    notas_adicionales, _ = _split_notas_y_zona(cot.notas or "")
    return render_template("cotizacion_view.html", c=cot, condiciones_finales=condiciones_finales, notas_adicionales=notas_adicionales, title=f"Vista de {cot.folio}")

# ---------------------------------------------------------
# API: actualizar estatus (inline) + WhatsApp
# ---------------------------------------------------------
@app.route("/api/cotizaciones/<int:cot_id>/estatus", methods=["POST"])
@login_required
def api_update_estatus(cot_id):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    ct = request.headers.get("Content-Type", "")
    if "application/json" in ct:
        data = request.get_json(silent=True) or {}
        nuevo = (data.get("estatus") or "").upper().strip()
    else:
        nuevo = (request.form.get("estatus") or "").upper().strip()

    if nuevo not in ["PENDIENTE", "ENVIADA", "GANADA", "PERDIDA"]:
        return jsonify({"ok": False, "error": "Estatus inválido"}), 400

    anterior = c.estatus
    if nuevo == anterior:
        return jsonify({"ok": True, "folio": c.folio, "estatus": nuevo, "mensaje": "Sin cambios."})

    c.estatus = nuevo
    db.session.commit()

    try:
        body = (
            f"🔄 *Actualización de estatus*\\n"
            f"Folio: *{c.folio}*\\n"
            f"Anterior: {anterior}\\n"
            f"Nuevo: *{nuevo}*\\n"
            f"Total: {money(c.total)}"
        )
        send_whatsapp_multi(ADMIN_LIST, body)
    except Exception as e:
        print(f"[Twilio] Error al enviar notificación de estatus: {e}", file=sys.stderr)

    return jsonify({
        "ok": True,
        "folio": c.folio,
        "estatus": nuevo,
        "mensaje": f"Estatus de la cotización {c.folio} actualizado a {nuevo}."
    })

# ---------------------------------------------------------
# Exportaciones CSV / Excel
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/export.csv")
@login_required
def export_cotizacion_csv(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    output = io.StringIO()
    w = csv.writer(output)

    w.writerow(["Folio","Fecha","Estatus","Representante","Cliente","Empresa","Subtotal","IVA %","IVA $","Total","Notas"])
    w.writerow([
        c.folio, c.fecha.strftime("%Y-%m-%d %H:%M"), c.estatus, (c.responsable or ""),
        c.cliente.nombre_cliente if c.cliente else "",
        c.cliente.empresa if c.cliente else "",
        f"{c.subtotal:.2f}",
        f"{c.iva_porc:.2f}", f"{c.iva_monto:.2f}",
        f"{c.total:.2f}", (c.notas or "")
    ])
    w.writerow([])
    w.writerow(["Capitulo","Cant","Unidad","Concepto","Sistema","PU","Subtotal","Descripción"])
    for d in c.detalles:
        w.writerow([
            getattr(d, "capitulo", "") or "", d.cantidad, d.unidad or "", d.nombre_concepto, d.sistema or "",
            f"{d.precio_unitario:.2f}", f"{d.subtotal:.2f}", (d.descripcion or "")
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={'Content-Disposition': f'attachment; filename="{c.folio or "cotizacion"}.csv"'}
    )

@app.route("/cotizaciones/<int:cot_id>/export.xlsx")
@login_required
def export_cotizacion_xlsx(cot_id: int):
    if Workbook is None:
        abort(501, description="openpyxl no instalado en el servidor.")
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    wb = Workbook()
    ws = wb.active
    ws.title = "Cotización"

    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    right = Alignment(horizontal="right", vertical="center")
    left = Alignment(horizontal="left", vertical="top", wrap_text=True)
    header_fill = PatternFill("solid", fgColor="0D47A1")
    white = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="DDDDDD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    ws.merge_cells("A1:G1"); ws["A1"] = f"COTIZACIÓN {c.folio}"
    ws["A1"].font = Font(bold=True, size=14); ws["A1"].alignment = center

    ws.append(["Folio", c.folio, "", "Fecha", c.fecha.strftime("%d/%m/%Y %H:%M"), ""])
    ws.append(["Cliente", (c.cliente.nombre_cliente if c.cliente else ""), "", "Empresa", (c.cliente.empresa if c.cliente else ""), ""])
    ws.append(["Representante", c.responsable or "", "", "Estatus", c.estatus, ""])
    ws.append([])

    headers = ["Capitulo", "Cant", "Unidad", "Concepto", "Sistema", "Precio Unit.", "Subtotal"]
    ws.append(headers)
    for col in range(1, 8):
        cell = ws.cell(row=ws.max_row, column=col)
        cell.fill = header_fill; cell.font = white; cell.alignment = center; cell.border = border

    for d in c.detalles:
        ws.append([getattr(d, "capitulo", "") or "", d.cantidad, d.unidad or "", d.nombre_concepto, d.sistema or "",
                   float(d.precio_unitario or 0), float(d.subtotal or 0)])
        r = ws.max_row
        for col in range(1, 8):
            ws.cell(row=r, column=col).border = border
        ws.cell(row=r, column=2).number_format = '0.00'
        ws.cell(row=r, column=6).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=7).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=4).alignment = left

    ws.append([])
    ws.append(["", "", cantidad_en_letra_mn(c.total)])
    ws.append(["", "Subtotal:", float(c.subtotal or 0)])
    ws.append(["", f"IVA ({c.iva_porc:.2f}%):", float(c.iva_monto or 0)])
    ws.append(["", "Total:", float(c.total or 0)])
    for r in range(ws.max_row-2, ws.max_row+1):
        ws.cell(row=r, column=2).font = bold
        ws.cell(row=r, column=3).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=3).alignment = right

    ws.column_dimensions["A"].width = 10
    ws.column_dimensions["B"].width = 10
    ws.column_dimensions["C"].width = 70
    ws.column_dimensions["D"].width = 25
    ws.column_dimensions["E"].width = 15
    ws.column_dimensions["F"].width = 15

    bio = io.BytesIO()
    wb.save(bio); bio.seek(0)
    return Response(
        bio.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{c.folio}.xlsx"'}
    )


def _email_body_cotizacion(c: Cotizacion) -> str:
    cli = c.cliente
    atencion = ""
    if cli:
        atencion = (cli.nombre_cliente or cli.empresa or "").strip()

    return (
        f"Con atención a: {atencion}\n\n"
        "Buenas tardes, por medio de la presente hacemos llegar la cotización requerida.\n\n"
        "Cualquier duda, estamos a sus órdenes.\n"
        "Saludos cordiales.\n"
    )


def _email_signature_text() -> str:
    return (
        "\n"
        "POLIUTECH RECUBRIMIENTOS ESPECIALIZADOS\n"
        "oficinas: 5559380536, 5559386530\n"
        "Número celular: 5534662836\n"
        "Correo electrónico: cotizaciones@poliutech.com\n"
        "www.poliutech.com\n"
    )


def _email_body_cotizacion_html(c: Cotizacion) -> str:
    cli = c.cliente
    atencion = ""
    if cli:
        atencion = escape((cli.nombre_cliente or cli.empresa or "").strip())

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #222; line-height: 1.45;">
        <p style="margin: 0 0 16px 0;">Con atención a: {atencion}</p>
        <p style="margin: 0 0 16px 0;">Buenas tardes, por medio de la presente hacemos llegar la cotización requerida.</p>
        <p style="margin: 0 0 22px 0;">Cualquier duda, estamos a sus órdenes.<br>Saludos cordiales.</p>

        <div style="padding-top: 14px; border-top: 1px solid #cfcfcf; max-width: 620px;">
          <div style="font-size: 14px; margin-bottom: 14px;">
            <div style="font-weight: 700;">POLIUTECH RECUBRIMIENTOS ESPECIALIZADOS</div>
            <div>oficinas:. 5559380536, 5559386530</div>
            <div>Número celular. 5534662836</div>
            <div>Correo electrónico : <a href="mailto:cotizaciones@poliutech.com">cotizaciones@poliutech.com</a></div>
            <div><a href="https://www.poliutech.com" target="_blank">www.poliutech.com</a></div>
          </div>
          <div>
            <img src="cid:poliutech-logo" alt="Poliutech" style="display:block; width:280px; height:auto; border:0;">
          </div>
        </div>
      </body>
    </html>
    """.strip()


def _parse_email_list(raw: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if isinstance(raw, (list, tuple)):
        parts = [str(item or "").strip() for item in raw]
        candidate = ",".join([part for part in parts if part])
    else:
        candidate = str(raw or "").strip()

    if not candidate:
        return []

    emails: list[str] = []
    for _, addr in getaddresses([candidate]):
        addr = (addr or "").strip()
        if not addr:
            continue
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", addr):
            raise ValueError(f"Correo inválido: {addr}")
        emails.append(addr)
    return emails


def _send_cotizacion_email(c: Cotizacion, recipient: str, cc: list[str] | None = None, bcc: list[str] | None = None) -> None:
    cc = cc or []
    bcc = bcc or []
    pdf_response = export_cotizacion_pdf(c.id)
    pdf_response.direct_passthrough = False
    pdf_bytes = pdf_response.get_data()

    msg = EmailMessage()
    msg["Subject"] = f"Cotización {c.folio}"
    msg["From"] = SMTP_FROM
    msg["To"] = recipient
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg.set_content(_email_body_cotizacion(c) + _email_signature_text())
    msg.add_alternative(_email_body_cotizacion_html(c), subtype="html")

    logo_path = Path(app.static_folder or "static") / "logo.png"
    if logo_path.exists():
        logo_bytes = logo_path.read_bytes()
        mime_type, _ = mimetypes.guess_type(str(logo_path))
        maintype, subtype = ("image", "jpeg")
        if mime_type and "/" in mime_type:
            maintype, subtype = mime_type.split("/", 1)
        html_part = msg.get_body(preferencelist=("html",))
        if html_part is not None:
            html_part.add_related(
                logo_bytes,
                maintype=maintype,
                subtype=subtype,
                cid="<poliutech-logo>",
            )

    msg.add_attachment(
        pdf_bytes,
        maintype="application",
        subtype="pdf",
        filename=f"{c.folio}.pdf",
    )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.ehlo()
        smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(msg, to_addrs=[recipient, *cc, *bcc])


@app.route("/api/cotizaciones/<int:cot_id>/send-email", methods=["POST"])
@login_required
def api_send_cotizacion_email(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    data = request.get_json(silent=True) or {}
    recipient = (data.get("to") or "").strip()
    if not recipient and c.cliente:
        recipient = (c.cliente.correo or "").strip()
    cc_raw = data.get("cc")
    bcc_raw = data.get("bcc")

    if not recipient:
        return jsonify({"ok": False, "error": "La cotización no tiene un correo destino."}), 400

    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", recipient):
        return jsonify({"ok": False, "error": "El correo destino no es válido."}), 400

    try:
        cc = _parse_email_list(cc_raw)
        bcc = _parse_email_list(bcc_raw)
        _send_cotizacion_email(c, recipient, cc=cc, bcc=bcc)
        return jsonify({
            "ok": True,
            "folio": c.folio,
            "to": recipient,
            "cc": cc,
            "bcc_count": len(bcc),
            "message": f"Cotización {c.folio} enviada a {recipient}."
        })
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        print(f"[MAIL] Error enviando cotización {c.folio} a {recipient}: {e}", file=sys.stderr)
        return jsonify({"ok": False, "error": f"No se pudo enviar el correo: {e}"}), 500

# ---------------------------------------------------------

@app.route("/cotizaciones/export/dashboard.xlsx")
@login_required
def export_dashboard_cotizaciones_xlsx():
    if Workbook is None:
        abort(501, description="openpyxl no instalado en el servidor.")

    desde = (request.args.get("desde") or "").strip()
    hasta = (request.args.get("hasta") or "").strip()
    estatus = (request.args.get("estatus") or "").strip()
    cliente = (request.args.get("cliente") or "").strip()

    try:
        cotizaciones = (_build_dashboard_cotizaciones_query(
            desde=desde,
            hasta=hasta,
            estatus=estatus,
            cliente=cliente,
        ).order_by(Cotizacion.fecha.desc()).all())
    except ValueError as exc:
        abort(400, description=str(exc))

    wb = Workbook()
    ws = wb.active
    ws.title = "Cotizaciones"

    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left = Alignment(horizontal="left", vertical="top", wrap_text=True)
    header_fill = PatternFill("solid", fgColor="0D47A1")
    white = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="DDDDDD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    ws.merge_cells("A1:J1")
    ws["A1"] = "REPORTE DE COTIZACIONES"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A1"].alignment = center

    filtros_texto = []
    if desde:
        filtros_texto.append(f"Desde: {desde}")
    if hasta:
        filtros_texto.append(f"Hasta: {hasta}")
    if estatus:
        filtros_texto.append(f"Estatus: {estatus}")
    if cliente:
        filtros_texto.append(f"Cliente/Empresa: {cliente}")
    if not filtros_texto:
        filtros_texto.append("Sin filtros")

    ws.merge_cells("A2:J2")
    ws["A2"] = " | ".join(filtros_texto)
    ws["A2"].alignment = left

    headers = ["Folio", "Fecha", "Cliente", "Empresa", "Responsable", "Estatus", "Subtotal", "IVA %", "IVA $", "Total"]
    ws.append([])
    ws.append(headers)

    header_row = ws.max_row
    for col in range(1, len(headers) + 1):
        cell = ws.cell(row=header_row, column=col)
        cell.fill = header_fill
        cell.font = white
        cell.alignment = center
        cell.border = border

    for c in cotizaciones:
        ws.append([
            c.folio or "",
            c.fecha.strftime("%Y-%m-%d %H:%M") if c.fecha else "",
            c.cliente.nombre_cliente if c.cliente else "",
            c.cliente.empresa if c.cliente else "",
            c.responsable or "",
            c.estatus or "",
            float(c.subtotal or 0),
            float(c.iva_porc or 0),
            float(c.iva_monto or 0),
            float(c.total or 0),
        ])
        row = ws.max_row
        for col in range(1, len(headers) + 1):
            ws.cell(row=row, column=col).border = border
        for col in (7, 9, 10):
            ws.cell(row=row, column=col).number_format = '"$"#,##0.00'
        ws.cell(row=row, column=8).number_format = '0.00'
        ws.cell(row=row, column=1).alignment = left
        ws.cell(row=row, column=2).alignment = center
        ws.cell(row=row, column=3).alignment = left
        ws.cell(row=row, column=4).alignment = left

    total_row = ws.max_row + 2
    ws.cell(row=total_row, column=9, value="Total exportado:").font = bold
    ws.cell(row=total_row, column=10, value=f"=SUM(J{header_row + 1}:J{ws.max_row})")
    ws.cell(row=total_row, column=10).font = bold
    ws.cell(row=total_row, column=10).number_format = '"$"#,##0.00'

    ws.auto_filter.ref = f"A{header_row}:J{max(header_row, ws.max_row)}"
    ws.freeze_panes = f"A{header_row + 1}"
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 28
    ws.column_dimensions["D"].width = 28
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 14
    ws.column_dimensions["G"].width = 14
    ws.column_dimensions["H"].width = 10
    ws.column_dimensions["I"].width = 14
    ws.column_dimensions["J"].width = 14

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    stamp = now_cdmx_naive().strftime("%Y%m%d_%H%M%S")
    return Response(
        bio.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="cotizaciones_dashboard_{stamp}.xlsx"'}
    )


@app.route("/api/dashboard/filter-summary")
@login_required
def api_dashboard_filter_summary():
    desde = (request.args.get("desde") or "").strip()
    hasta = (request.args.get("hasta") or "").strip()
    estatus = (request.args.get("estatus") or "").strip()
    cliente = (request.args.get("cliente") or "").strip()

    try:
        q = _build_dashboard_cotizaciones_query(
            desde=desde,
            hasta=hasta,
            estatus=estatus,
            cliente=cliente,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    cot_subq = q.with_entities(Cotizacion.id).subquery()
    cot_ids_select = db.select(cot_subq.c.id)

    total_importe = (
        db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0))
        .filter(Cotizacion.id.in_(cot_ids_select))
        .scalar()
        or 0
    )
    total_cotizaciones = (
        db.session.query(db.func.count())
        .select_from(cot_subq)
        .scalar()
        or 0
    )
    total_conceptos = (
        db.session.query(db.func.count(CotizacionDetalle.id))
        .filter(CotizacionDetalle.cotizacion_id.in_(cot_ids_select))
        .scalar()
        or 0
    )

    return jsonify({
        "total_importe": float(total_importe),
        "total_cotizaciones": int(total_cotizaciones),
        "total_conceptos": int(total_conceptos),
    })

# PDF - Diseño corporativo
# - Quitar RFC
# - "Condiciones comerciales"
# - RESPONSABLE: poner valor debajo del label (rellena el “espacio en blanco”)
# ---------------------------------------------------------
def draw_watermark(canvas, app):
    try:
        import os
        watermark_path = os.path.join(app.static_folder, "watermark.png")
        if os.path.exists(watermark_path):
            canvas.saveState()
            canvas.setFillAlpha(0.08)
            img = ImageReader(watermark_path)
            page_width, page_height = canvas._pagesize
            width = 300
            height = 300
            x = (page_width - width) / 2
            y = (page_height / 2) - 150
            canvas.drawImage(img, x, y, width=width, height=height, mask='auto')
            canvas.restoreState()
    except Exception:
        pass


@app.route("/cotizaciones/<int:cot_id>/export.pdf")
@login_required
def export_cotizacion_pdf(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=10*mm, rightMargin=10*mm,
        topMargin=34*mm, bottomMargin=38*mm
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Encabezado", fontName="Helvetica", fontSize=9, leading=12, spaceAfter=4, splitLongWords=False))
    styles.add(ParagraphStyle(name="NormalCell", fontName="Helvetica", fontSize=8, leading=10, splitLongWords=False))
    styles.add(ParagraphStyle(name="NormalRight", fontName="Helvetica", fontSize=8, leading=10, alignment=2, splitLongWords=False))
    styles.add(ParagraphStyle(name="NormalCenter", fontName="Helvetica", fontSize=8, leading=10, alignment=1, splitLongWords=False))

    elems = []

    def encabezado(canv, doc_):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1]-40, A4[0], 40, stroke=0, fill=1)

        logo_path = os.path.join(app.static_folder or "static", "logo.png")
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 25 * mm
                scale = max_w / iw
                w = max_w
                h = ih * scale
                x_logo = 12
                y_logo = A4[1] - h - 8
                canv.drawImage(img, x_logo, y_logo, width=w, height=h, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 14)
        canv.setFillColor(colors.white)
        canv.drawRightString(A4[0]-12, A4[1]-18, "COTIZACIÓN POLIUTECH")
        canv.setFont("Helvetica", 10)
        canv.drawRightString(A4[0]-12, A4[1]-31, "Recubrimientos Especializados")
        canv.restoreState()

    def footer(canv, doc_):
        canv.saveState()
        y_firma = 80
        canv.setFont("Helvetica", 9)
        canv.setFillColor(colors.black)
        canv.drawCentredString(A4[0]/2, y_firma + 18, "Atte.")
        canv.setFont("Helvetica-Bold", 9)
        canv.drawCentredString(A4[0]/2, y_firma + 6, "Ing. César Antonio Garza Guerrero")
        canv.setFont("Helvetica", 9)
        canv.drawCentredString(A4[0]/2, y_firma - 6, "DIRECTOR GENERAL")

        division_path = os.path.join(app.static_folder or "static", "division.png")
        if os.path.exists(division_path):
            try:
                canv.drawImage(division_path, (A4[0]-155*mm)/2, 45, width=155*mm, height=3*mm, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 9)
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.drawCentredString(A4[0]/2, 35, "POLIUTECH – Recubrimientos Especializados")

        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#333333"))
        line1 = "Campos Elíseos 223 Oficina 602 · Col. Polanco V Sección · Miguel Hidalgo, CDMX 11560"
        line2 = "Tel: 55 5938 6530 / 55 5938 0536 · info@poliutech.com · www.poliutech.com"
        canv.drawCentredString(A4[0]/2, 25, line1)
        canv.drawCentredString(A4[0]/2, 15, line2)

        try:
            canv.setTitle(c.folio or "Cotizacion")
        except Exception:
            pass

        canv.restoreState()

    # === DATOS PRINCIPALES ===
    cli = c.cliente
    cliente_nombre = cli.nombre_cliente if cli else ""
    cliente_empresa = cli.empresa if cli else ""
    cliente_correo = cli.correo if cli else ""
    cliente_telefono = cli.telefono if cli else ""

    meta_data = [
        [
            Paragraph(f"<b>Folio:</b> {c.folio}", styles["Encabezado"]),
            Paragraph(f"<b>Fecha:</b> {c.fecha.strftime('%d/%m/%Y %H:%M')}", styles["Encabezado"]),
        ],
        [
            Paragraph(f"<b>Responsable:</b> {c.responsable or ''}", styles["Encabezado"]),
            Paragraph(f"<b>Cliente:</b> {cliente_nombre}", styles["Encabezado"]),
        ],
        [
            Paragraph(f"<b>Empresa:</b> {cliente_empresa}", styles["Encabezado"]),
            Paragraph(f"<b>Correo:</b> {cliente_correo}", styles["Encabezado"]),
        ],
        [
            Paragraph(f"<b>Teléfono:</b> {cliente_telefono}", styles["Encabezado"]),
            Paragraph("", styles["Encabezado"]),
        ],
    ]
    meta_tbl = Table(meta_data, colWidths=[95*mm, 95*mm], hAlign="LEFT")
    meta_tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    elems.append(meta_tbl)
    elems.append(Spacer(1, 4))

    # === TABLA DE CONCEPTOS ===
    data = [["Capítulo", "Concepto", "Uni.", "Cant.", "Sistema", "Precio Unitario", "Subtotal"]]
    for d in c.detalles:
        data.append([
            Paragraph(_truncate_pdf_text(getattr(d, "capitulo", "") or "-", 28), styles["NormalCenter"]),
            Paragraph(_truncate_pdf_text(d.nombre_concepto or "-", 120), styles["NormalCell"]),
            Paragraph(d.unidad or "-", styles["NormalCenter"]),
            Paragraph(f"{(d.cantidad or 0):.2f}", styles["NormalCenter"]),
            Paragraph(_truncate_pdf_text(d.sistema or "-", 40), styles["NormalCenter"]),
            Paragraph(money(d.precio_unitario), styles["NormalRight"]),
            Paragraph(money(d.subtotal), styles["NormalRight"]),
        ])

    tbl = Table(
        data,
        colWidths=[20*mm, 72*mm, 12*mm, 16*mm, 26*mm, 22*mm, 22*mm],
        repeatRows=1,
        hAlign="CENTER"
    )
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d47a1")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 1), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("WORDWRAP", (0, 0), (-1, -1), True),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))

    elems.append(tbl)
    elems.append(Spacer(1, 6))

    # === CANTIDAD EN LETRA ===
    resumen_elems = []
    try:
        from num2words import num2words
        total = float(c.total or 0)
        enteros = int(total)
        centavos = int(round((total - enteros) * 100)) % 100
        palabras = num2words(enteros, lang='es').strip()
        if palabras.endswith(" uno"):
            palabras = palabras[:-4] + " un"
        palabras = palabras.capitalize()
        cantidad_letra = f"{palabras} pesos {centavos:02d}/100 M.N."
        resumen_elems.append(Paragraph(f"<b>Cantidad en letra:</b> {cantidad_letra}", styles["Encabezado"]))
        resumen_elems.append(Spacer(1, 4))
    except Exception as e:
        print(f"[PDF] num2words error: {e}", file=sys.stderr)

    # === TOTALES ===
    # === TOTALES (con descuento si aplica) ===
    subtotal = float(c.subtotal or 0)
    descuento = float(c.descuento_total or 0)
    subtotal_desc = subtotal - descuento

    tot_data = [["Subtotal:", money(subtotal)]]
    if descuento and descuento > 0.0001:
        tot_data.append(["Descuento:", "-" + money(descuento)])
        tot_data.append(["Subtotal c/ desc.:", money(subtotal_desc)])
    tot_data.extend([
        [f"IVA ({c.iva_porc:.2f}%):", money(c.iva_monto)],
        ["Total:", money(c.total)],
    ])
    t2 = Table(tot_data, colWidths=[45*mm, 35*mm], hAlign="RIGHT")
    t2.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, colors.black),
    ]))
    resumen_elems.append(t2)
    elems.append(KeepTogether(resumen_elems))
    elems.append(Spacer(1, 6))

    # === CONDICIONES COMERCIALES ===
    condiciones = _condiciones_comerciales_finales(c.notas or "")
    if condiciones:
        elems.append(Paragraph("<b>Condiciones Comerciales:</b>", styles["Encabezado"]))
        nota_style = ParagraphStyle(
            "NotasJustify",
            parent=styles["Normal"],
            alignment=TA_JUSTIFY,
            leading=11,
            fontSize=9,
            leftIndent=8,
        )
        bullets = "<br/>".join([f"• {x}" for x in condiciones if str(x).strip()])
        elems.append(Paragraph(bullets, nota_style))
        elems.append(Spacer(1, 8))

    doc.build(
        elems,
        onFirstPage=lambda canv, d: (draw_watermark(canv, app), encabezado(canv, d), footer(canv, d)),
        onLaterPages=lambda canv, d: (draw_watermark(canv, app), encabezado(canv, d), footer(canv, d))
    )

    buf.seek(0)
    response = Response(
        buf.getvalue(),
        mimetype="application/pdf",
        headers={'Content-Disposition': f'inline; filename="{c.folio}.pdf"'}
    )
    response.direct_passthrough = False
    return response

@app.route("/cotizaciones/<int:cot_id>/pdf")
@login_required
def export_cotizacion_pdf_alias(cot_id: int):
    return export_cotizacion_pdf(cot_id)

# ---------------------------------------------------------
# PDF por FOLIO (compatibilidad)
# Soporta URLs tipo: /cotizaciones/PTCH-0002/export.pdf
# ---------------------------------------------------------
@app.route("/cotizaciones/<string:folio>/export.pdf")
@login_required
def export_cotizacion_pdf_by_folio(folio: str):
    folio = (folio or "").strip()
    if not folio:
        abort(404)
    c = Cotizacion.query.filter_by(folio=folio).first_or_404()
    require_owner_or_admin(c)
    return export_cotizacion_pdf(c.id)

# ---------------------------------------------------------
# API Dashboard (series / kpis / breakdown) — FILTRADO por responsable
# ---------------------------------------------------------
@app.route("/api/cotizaciones/search")
@login_required
def api_cotizaciones_search():
    q = Cotizacion.query.join(Cliente, isouter=True)

    if not is_admin():
        q = q.filter(Cotizacion.responsable == responsable_actual())

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
@login_required
def api_dashboard_metrics():
    # Filtrado para USER
    base = db.session.query(
        db.func.strftime("%Y-%m", Cotizacion.fecha).label("ym"),
        db.func.count(Cotizacion.id),
        db.func.coalesce(db.func.sum(Cotizacion.total), 0)
    )
    if not is_admin():
        base = base.filter(Cotizacion.responsable == responsable_actual())

    rows = base.group_by("ym").order_by("ym").all()
    series = [{"mes": ym, "cotizaciones": int(c), "total": float(t)} for ym, c, t in rows]

    if is_admin():
        kpis = {
            "total_cotizaciones": Cotizacion.query.count(),
            "total_importe": float(db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0),
            "total_catalogo": Concepto.query.count(),
        }
    else:
        ra = responsable_actual()
        kpis = {
            "total_cotizaciones": Cotizacion.query.filter(Cotizacion.responsable == ra).count(),
            "total_importe": float((db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0))
                                    .filter(Cotizacion.responsable == ra).scalar() or 0)),
            "total_catalogo": Concepto.query.count(),
        }

    return jsonify({"series": series, "kpis": kpis})

@app.route("/api/dashboard/status_breakdown")
@login_required
def api_dashboard_status_breakdown():
    q = db.session.query(Cotizacion.estatus, db.func.count(Cotizacion.id))
    if not is_admin():
        q = q.filter(Cotizacion.responsable == responsable_actual())

    rows = q.group_by(Cotizacion.estatus).all()
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
    return jsonify({"status": "ok", "now_cdmx": now_cdmx_naive().isoformat()}), 200

@app.route("/debug/send_test")
@login_required
def debug_send_test():
    if not is_admin():
        abort(403)
    msg = "✅ Mensaje de prueba - Sistema Poliutech (debug_send_test)."
    send_whatsapp_multi(ADMIN_LIST, msg)
    return jsonify({"sent": True, "to": ADMIN_LIST})

@app.route("/debug/force_reminders")
@login_required
def debug_force_reminders():
    if not is_admin():
        abort(403)
    try:
        enviar_notificaciones_pendientes()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

def enviar_notificaciones_pendientes():
    with app.app_context():
        ahora = now_cdmx_naive()
        hace_24h = ahora - timedelta(hours=24)

        q = Cotizacion.query.filter_by(estatus="PENDIENTE")
        # En recordatorios, normalmente quieres avisar admins siempre (no depende de rol)
        pendientes = q.all()

        for cot in pendientes:
            if cot.last_whatsapp_at is None or cot.last_whatsapp_at <= hace_24h:
                try:
                    body = (
                        "🔔 *Recordatorio: Cotización PENDIENTE*\\n"
                        f"Folio: *{cot.folio}*\\n"
                        f"Fecha (CDMX): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\\n"
                        f"Total: {money(cot.total)}"
                    )
                    send_whatsapp_multi(ADMIN_LIST, body)
                    cot.last_whatsapp_at = ahora
                    db.session.commit()
                except Exception as e:
                    print(f"[Scheduler] ERROR recordatorio ({cot.folio}): {e}", file=sys.stderr)

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

@app.route("/admin/bitacora")
@login_required
def admin_bitacora():
    if not is_admin():
        abort(403)

    page = int(request.args.get("page", 1) or 1)
    per_page = int(request.args.get("per", 100) or 100)
    per_page = max(20, min(per_page, 300))

    q = (request.args.get("q") or "").strip()
    usuario_f = (request.args.get("usuario") or "").strip()
    metodo_f = (request.args.get("metodo") or "").strip().upper()
    status_f = (request.args.get("status") or "").strip()

    query = ActivityLog.query

    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            ActivityLog.usuario.ilike(like),
            ActivityLog.ruta.ilike(like),
            ActivityLog.accion.ilike(like),
            ActivityLog.endpoint.ilike(like),
        ))
    if usuario_f:
        query = query.filter(ActivityLog.usuario == usuario_f)
    if metodo_f:
        query = query.filter(ActivityLog.metodo == metodo_f)
    if status_f.isdigit():
        query = query.filter(ActivityLog.status_code == int(status_f))

    total = query.count()
    logs = (query.order_by(ActivityLog.fecha.desc())
                .offset((page - 1) * per_page)
                .limit(per_page)
                .all())

    # usuarios distintos para dropdown
    usuarios = [u[0] for u in db.session.query(ActivityLog.usuario).distinct().order_by(ActivityLog.usuario).all()]

    return render_template(
        "admin_bitacora.html",
        logs=logs,
        page=page,
        per_page=per_page,
        total=total,
        q=q,
        usuario_f=usuario_f,
        metodo_f=metodo_f,
        status_f=status_f,
        usuarios=usuarios,
    )

# ---------------------------------------------------------
@app.route("/admin/usuarios", methods=["GET", "POST"])
@login_required
def admin_usuarios():
    if not is_admin():
        abort(403)

    if request.method == "POST":
        nombre = (request.form.get("nombre") or "").strip()
        password = (request.form.get("password") or "").strip()
        rol = normalize_user_role(request.form.get("rol"))

        if not nombre:
            flash("El nombre del usuario es obligatorio.", "danger")
            return redirect(url_for("admin_usuarios"))
        if not password:
            flash("La contrasena es obligatoria para crear un usuario.", "danger")
            return redirect(url_for("admin_usuarios"))

        exists = Usuario.query.filter(db.func.lower(Usuario.nombre) == nombre.lower()).first()
        if exists:
            flash("Ya existe un usuario con ese nombre.", "danger")
            return redirect(url_for("admin_usuarios"))

        nuevo = Usuario(nombre=nombre, rol=rol)
        nuevo.set_password(password)
        db.session.add(nuevo)
        db.session.commit()
        flash(f"Usuario '{nombre}' creado correctamente.", "success")
        return redirect(url_for("admin_usuarios"))

    q = (request.args.get("q") or "").strip()
    usuarios_query = admin_users_base_query()
    if q:
        usuarios_query = usuarios_query.filter(Usuario.nombre.ilike(f"%{q}%"))

    usuarios = usuarios_query.all()
    total_admins = Usuario.query.filter(db.func.upper(Usuario.rol) == "ADMIN").count()
    return render_template(
        "admin_usuarios.html",
        usuarios=usuarios,
        q=q,
        total=len(usuarios),
        total_admins=total_admins,
    )

@app.route("/admin/usuarios/<int:user_id>/editar", methods=["POST"])
@login_required
def admin_usuario_editar(user_id: int):
    if not is_admin():
        abort(403)

    usuario = Usuario.query.get_or_404(user_id)
    nombre = (request.form.get("nombre") or "").strip()
    password = (request.form.get("password") or "").strip()
    rol = normalize_user_role(request.form.get("rol"))

    if not nombre:
        flash("El nombre del usuario es obligatorio.", "danger")
        return redirect(url_for("admin_usuarios"))

    duplicado = Usuario.query.filter(
        db.func.lower(Usuario.nombre) == nombre.lower(),
        Usuario.id != usuario.id,
    ).first()
    if duplicado:
        flash("Ya existe otro usuario con ese nombre.", "danger")
        return redirect(url_for("admin_usuarios"))

    if usuario.id == current_user.id and rol != "ADMIN":
        admins_restantes = Usuario.query.filter(
            db.func.upper(Usuario.rol) == "ADMIN",
            Usuario.id != usuario.id,
        ).count()
        if admins_restantes == 0:
            flash("No puedes quitar el rol ADMIN al unico administrador del sistema.", "danger")
            return redirect(url_for("admin_usuarios"))

    usuario.nombre = nombre
    usuario.rol = rol
    if password:
        usuario.set_password(password)

    db.session.commit()
    flash(f"Usuario '{nombre}' actualizado correctamente.", "success")
    return redirect(url_for("admin_usuarios"))

@app.route("/admin/usuarios/<int:user_id>/eliminar", methods=["POST"])
@login_required
def admin_usuario_eliminar(user_id: int):
    if not is_admin():
        abort(403)

    usuario = Usuario.query.get_or_404(user_id)

    if usuario.id == current_user.id:
        flash("No puedes eliminar tu propio usuario mientras tienes la sesion activa.", "danger")
        return redirect(url_for("admin_usuarios"))

    if (usuario.rol or "").upper() == "ADMIN":
        admins_restantes = Usuario.query.filter(
            db.func.upper(Usuario.rol) == "ADMIN",
            Usuario.id != usuario.id,
        ).count()
        if admins_restantes == 0:
            flash("No puedes eliminar al ultimo administrador del sistema.", "danger")
            return redirect(url_for("admin_usuarios"))

    nombre = usuario.nombre
    db.session.delete(usuario)
    db.session.commit()
    flash(f"Usuario '{nombre}' eliminado correctamente.", "success")
    return redirect(url_for("admin_usuarios"))

# Blueprints (Catálogos) — si existen en tu repo
# ---------------------------------------------------------
try:
    from catalogos_routes import bp as catalogos_bp
    app.register_blueprint(catalogos_bp, url_prefix="/catalogos")
except Exception as e:
    print(f"[WARN] No se pudo cargar blueprint catalogos_routes: {e}", file=sys.stderr)

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        os.makedirs(app.static_folder or "static", exist_ok=True)
    except Exception:
        pass
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
