# =========================================================
# app.py — MARWHATS (checkpoint) / Poliutech
# Limpio + Roles (ADMIN / USER) + Filtro por Responsable
# =========================================================
from __future__ import annotations

import os, io, csv, sys, math, re, traceback
from datetime import datetime, timedelta
from typing import Iterable, Optional, List
from urllib.parse import urlparse
from pathlib import Path

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, abort, g
)

from sqlalchemy import text, or_

# ReportLab (PDF)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Table, TableStyle, Paragraph, SimpleDocTemplate, Spacer
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

# WhatsApp: enviar a un "admin principal" para evitar quemar límites (1 evento = 1 mensaje).
PRIMARY_ADMIN_WHATSAPP = os.getenv("PRIMARY_ADMIN_WHATSAPP", "").strip()
if not PRIMARY_ADMIN_WHATSAPP and ADMIN_LIST:
    PRIMARY_ADMIN_WHATSAPP = ADMIN_LIST[0]

# Recordatorios inteligentes (opción B): solo PENDIENTE con antigüedad mínima y solo al admin principal.
PENDING_REMINDER_MIN_DAYS = int(os.getenv("PENDING_REMINDER_MIN_DAYS", "1"))

# Usa SIEMPRE los modelos desde models.py para evitar duplicados
from models import db, Cliente, Concepto, Cotizacion, CotizacionDetalle, Usuario, ActivityLog
# ---------------------------------------------------------
# Flask + DB + Login
# ---------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", DEFAULT_SECRET_KEY)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

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
        cutoff = datetime.utcnow() - timedelta(days=days)
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
        now = datetime.utcnow()
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

        g._audit_started_at = datetime.utcnow()

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
            fecha=datetime.utcnow(),
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
        db.session.commit()
    except Exception as e:
        print("⚠️ ensure_schema(detalle extras):", e)

# ---------------------------------------------------------
# Seed: usuarios base (idempotente)
# ---------------------------------------------------------
def seed_default_users():
    """Crea usuarios base si no existen (no duplica)."""
    defaults = [
        ("Ing. Antonio Azcona", "Azcona123!", "USER"),
        ("Ing. José Solis", "Solis123!", "USER"),
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

def send_whatsapp_admin(body: str, *, to_all: bool = False) -> None:
    """Envia WhatsApp a admins.
    - to_all=False: solo al admin principal (evita reventar límites)
    - to_all=True: a toda la lista
    """
    if to_all:
        send_whatsapp_admin(body)
        return
    if PRIMARY_ADMIN_WHATSAPP:
        send_whatsapp_multi([PRIMARY_ADMIN_WHATSAPP], body)
    else:
        # fallback: primer admin si existe
        send_whatsapp_multi(ADMIN_LIST[:1], body)

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
        title="Sistema Poliutech",
        total_cotizaciones=total_cotizaciones,
        total_importe=float(total_importe),
        total_catalogo=total_catalogo,
        cotizaciones=cotizaciones,
        show_splash=True
    )

@app.route("/cotizador")
@login_required
def cotizador():
    return render_template("cotizador.html", title="Nuevo - Sistema Poliutech")

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
        cliente_id=cliente.id if cliente else None,
        estatus=(f.get("estatus") or "PENDIENTE").upper(),
        notas=f.get("notas"),
        last_whatsapp_at=None,
        responsable=responsable_final
    )
    db.session.add(cot)
    db.session.flush()

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
            f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\\n"
            f"Total: {money(cot.total)}"
        )
        send_whatsapp_admin(msg)
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
    return render_template("cotizacion_edit.html", c=c, zona_actual=zona_actual, title=f"Editar {c.folio}")

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
    _prev_estatus = (c.estatus or "").upper()
    c.estatus = (f.get("estatus") or c.estatus).upper()
    # Si vuelve a PENDIENTE, reiniciamos recordatorios
    if c.estatus == "PENDIENTE" and _prev_estatus != "PENDIENTE":
        c.last_whatsapp_at = None
    c.notas = f.get("notas") or c.notas
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
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    sistemas = f.getlist("item_sistema[]")
    descripciones = f.getlist("item_descripcion[]")

    subtotal = 0.0
    n = max(len(nombres), len(unidades), len(cantidades), len(precios))
    for i in range(n):
        nombre = (nombres[i] if i < len(nombres) else "").strip()
        if not nombre:
            continue
        unidad = (unidades[i] if i < len(unidades) else "").strip()
        cantidad = parse_float(cantidades[i] if i < len(cantidades) else 0, 0.0)
        precio = parse_float(precios[i] if i < len(precios) else 0, 0.0)
        sistema = (sistemas[i] if i < len(sistemas) else "").strip()
        descripcion = (descripciones[i] if i < len(descripciones) else "").strip()

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

        det = CotizacionDetalle(
            cotizacion_id=c.id,
            concepto_id=concepto.id,
            nombre_concepto=nombre,
            unidad=unidad,
            cantidad=cantidad,
            precio_unitario=precio,
            sistema=sistema or None,
            descripcion=descripcion,
            subtotal=linea_subtotal
        )
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
        send_whatsapp_admin(body)
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

    q = Cotizacion.query

    # fechas (inclusive)
    try:
        if desde_s:
            d = datetime.strptime(desde_s, "%Y-%m-%d")
            q = q.filter(Cotizacion.fecha >= d)
    except Exception:
        return jsonify({"error": "Filtro 'Desde' inválido"}), 400

    try:
        if hasta_s:
            h = datetime.strptime(hasta_s, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
            q = q.filter(Cotizacion.fecha <= h)
    except Exception:
        return jsonify({"error": "Filtro 'Hasta' inválido"}), 400

    if estatus_s:
        q = q.filter(Cotizacion.estatus == estatus_s)

    if cliente_s:
        q = q.join(Cliente, Cotizacion.cliente_id == Cliente.id)
        like = f"%{cliente_s}%"
        q = q.filter(or_(
            db.func.lower(Cliente.nombre_cliente).like(like),
            db.func.lower(Cliente.empresa).like(like)
        ))

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
        title="Cotizaciones · Sistema Poliutech"
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
    return render_template("cotizacion_view.html", c=c, zona_actual=zona_actual, title=f"Ver {c.folio}")

@app.route("/cotizaciones/<int:cot_id>/ver")
@login_required
def ver_cotizacion(cot_id: int):
    cot = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(cot)
    return render_template("cotizacion_view.html", c=cot, title=f"Vista de {cot.folio}")

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
    # Si vuelve a PENDIENTE, reiniciamos recordatorios
    if nuevo == "PENDIENTE":
        c.last_whatsapp_at = None
    db.session.commit()

    try:
        body = (
            f"🔄 *Actualización de estatus*\\n"
            f"Folio: *{c.folio}*\\n"
            f"Anterior: {anterior}\\n"
            f"Nuevo: *{nuevo}*\\n"
            f"Total: {money(c.total)}"
        )
        send_whatsapp_admin(body)
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

    ws.merge_cells("A1:F1"); ws["A1"] = f"COTIZACIÓN {c.folio}"
    ws["A1"].font = Font(bold=True, size=14); ws["A1"].alignment = center

    ws.append(["Folio", c.folio, "", "Fecha", c.fecha.strftime("%d/%m/%Y %H:%M"), ""])
    ws.append(["Cliente", (c.cliente.nombre_cliente if c.cliente else ""), "", "Empresa", (c.cliente.empresa if c.cliente else ""), ""])
    ws.append(["Representante", c.responsable or "", "", "Estatus", c.estatus, ""])
    ws.append([])

    headers = ["Cant", "Unidad", "Concepto", "Sistema", "Precio Unit.", "Subtotal"]
    ws.append(headers)
    for col in range(1, 7):
        cell = ws.cell(row=ws.max_row, column=col)
        cell.fill = header_fill; cell.font = white; cell.alignment = center; cell.border = border

    for d in c.detalles:
        ws.append([d.cantidad, d.unidad or "", d.nombre_concepto, d.sistema or "",
                   float(d.precio_unitario or 0), float(d.subtotal or 0)])
        r = ws.max_row
        for col in range(1, 7):
            ws.cell(row=r, column=col).border = border
        ws.cell(row=r, column=1).number_format = '0.00'
        ws.cell(row=r, column=5).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=6).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=3).alignment = left

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

# ---------------------------------------------------------
# PDF - Diseño corporativo
# - Quitar RFC
# - "Condiciones comerciales"
# - RESPONSABLE: poner valor debajo del label (rellena el “espacio en blanco”)
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/export.pdf")
@login_required
def export_cotizacion_pdf(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=58*mm, bottomMargin=38*mm
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Encabezado", fontSize=9, leading=12, spaceAfter=4))
    styles.add(ParagraphStyle(name="NormalRight", fontSize=9, alignment=2))
    styles.add(ParagraphStyle(name="NormalCenter", fontSize=9, alignment=1))

    elems = []

    def encabezado(canv, doc_):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1]-40, A4[0], 40, stroke=0, fill=1)

        logo_path = os.path.join(app.static_folder or "static", "logo.jpg")
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 50 * mm
                scale = max_w / iw
                w = max_w
                h = ih * scale
                x_logo = 25
                y_logo = A4[1] - h - 15
                canv.drawImage(img, x_logo, y_logo, width=w, height=h, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 14)
        canv.setFillColor(colors.white)
        canv.drawRightString(A4[0]-25, A4[1]-20, "COTIZACIÓN POLIUTECH")
        canv.setFont("Helvetica", 10)
        canv.drawRightString(A4[0]-25, A4[1]-33, "Recubrimientos Especializados")
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
    elems.append(Paragraph(f"<b>Folio:</b> {c.folio}", styles["Encabezado"]))
    elems.append(Paragraph(f"<b>Fecha:</b> {c.fecha.strftime('%d/%m/%Y %H:%M')}", styles["Encabezado"]))

    # 👇 Aquí va el ajuste que pediste:
    # Mantiene el label RESPONSABLE y pone el valor debajo para ocupar ese espacio.
    elems.append(Paragraph(f"<b>RESPONSABLE:</b><br/>{c.responsable or ''}", styles["Encabezado"]))
    elems.append(Spacer(1, 8))

    if c.cliente:
        cli = c.cliente
        for txt in [
            f"<b>Cliente:</b> {cli.nombre_cliente or ''}",
            f"<b>Empresa:</b> {cli.empresa or ''}",
            f"<b>Correo:</b> {cli.correo or ''}",
            f"<b>Teléfono:</b> {cli.telefono or ''}",
        ]:
            elems.append(Paragraph(txt, styles["Encabezado"]))
        elems.append(Spacer(1, 10))

    # === TABLA DE CONCEPTOS ===
    data = [["Concepto", "Uni.", "Cant.", "Sistema", "Precio Unitario", "Subtotal"]]
    for d in c.detalles:
        data.append([
            Paragraph(d.nombre_concepto or "-", styles["Normal"]),
            Paragraph(d.unidad or "-", styles["NormalCenter"]),
            Paragraph(f"{(d.cantidad or 0):.2f}", styles["NormalCenter"]),
            Paragraph(d.sistema or "-", styles["NormalCenter"]),
            Paragraph(money(d.precio_unitario), styles["NormalRight"]),
            Paragraph(money(d.subtotal), styles["NormalRight"]),
        ])

    tbl = Table(
        data,
        colWidths=[70*mm, 18*mm, 20*mm, 30*mm, 30*mm, 30*mm],
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
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("WORDWRAP", (0, 0), (-1, -1), True),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))

    elems.append(tbl)
    elems.append(Spacer(1, 10))

    # === CANTIDAD EN LETRA ===
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
        elems.append(Paragraph(f"<b>Cantidad en letra:</b> {cantidad_letra}", styles["Encabezado"]))
        elems.append(Spacer(1, 6))
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
    elems.append(t2)
    elems.append(Spacer(1, 10))

    # === CONDICIONES COMERCIALES ===
    if c.notas:
        elems.append(Paragraph("<b>Condiciones Comerciales:</b>", styles["Encabezado"]))

        nota_style = ParagraphStyle(
            "NotasJustify",
            parent=styles["Normal"],
            alignment=TA_JUSTIFY,
            leading=11,
            fontSize=9
        )

        notas_text = str(c.notas).replace("\r\n", "<br/>").replace("\n", "<br/>")
        elems.append(Paragraph(notas_text, nota_style))
        elems.append(Spacer(1, 8))

    doc.build(
        elems,
        onFirstPage=lambda canv, d: (encabezado(canv, d), footer(canv, d)),
        onLaterPages=lambda canv, d: (encabezado(canv, d), footer(canv, d))
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
    return jsonify({"status": "ok", "now_utc": datetime.utcnow().isoformat()}), 200

@app.route("/debug/send_test")
@login_required
def debug_send_test():
    if not is_admin():
        abort(403)
    msg = "✅ Mensaje de prueba - Sistema Poliutech (debug_send_test)."
    send_whatsapp_admin(msg)
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
        ahora = datetime.utcnow()
        hace_24h = ahora - timedelta(hours=24)

        q = Cotizacion.query.filter_by(estatus="PENDIENTE")
        # Opción B: solo pendientes con antigüedad mínima
        min_age = ahora - timedelta(days=PENDING_REMINDER_MIN_DAYS)
        q = q.filter(Cotizacion.fecha <= min_age)
        pendientes = q.all()

        for cot in pendientes:
            if cot.last_whatsapp_at is None or cot.last_whatsapp_at <= hace_24h:
                try:
                    body = (
                        "🔔 *Recordatorio: Cotización PENDIENTE*\\n"
                        f"Folio: *{cot.folio}*\\n"
                        f"Fecha (UTC): {cot.fecha.strftime('%d/%m/%Y %H:%M')}\\n"
                        f"Total: {money(cot.total)}"
                    )
                    send_whatsapp_admin(body)
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
            hours=24,
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
