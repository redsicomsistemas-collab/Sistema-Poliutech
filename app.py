from __future__ import annotations

import os, io, csv, sys, math, re, traceback
from datetime import datetime, timedelta
from typing import Iterable, Optional, List

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, abort
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
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.utils import ImageReader

# Excel
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
except Exception:
    Workbook = None  # la app sigue arrancando aunque falte openpyxl

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

# Usa SIEMPRE los modelos desde models.py para evitar duplicados
from models import db, Cliente, Concepto, Cotizacion, CotizacionDetalle

# ---------------------------------------------------------
# Flask + DB
# ---------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", DEFAULT_SECRET_KEY)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Vincula la instancia global de db (creada en models.py) a este app
db.init_app(app)

# ---------------------------------------------------------
# Twilio (opcional)
# ---------------------------------------------------------
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
# Migraciones mínimas
# ---------------------------------------------------------
def _table_columns(table_name: str) -> set[str]:
    rows = db.session.execute(text(f"PRAGMA table_info('{table_name}')")).mappings().all()
    return {r["name"] for r in rows}

def ensure_schema():
    """
    Crea tablas si no existen y agrega columnas mínimas nuevas en cotizacion y cotizacion_detalle.
    """
    db.create_all()

    # Cotizacion
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

    # CotizacionDetalle
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
    prefix = "PTCH-"
    maxn = 0
    rows = db.session.execute(text("SELECT folio FROM cotizacion WHERE folio LIKE 'PTCH-%'")).fetchall()
    for (folio,) in rows:
        m = re.match(r"PTCH-(\d{4})$", str(folio))
        if m:
            n = int(m.group(1))
            if n > maxn:
                maxn = n
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
# Rutas: Dashboard / Catálogos / Cotizador
# ---------------------------------------------------------
@app.route("/")
def index():
    total_cotizaciones = Cotizacion.query.count()
    total_importe = db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0
    total_catalogo = Concepto.query.count()
    cotizaciones = Cotizacion.query.order_by(Cotizacion.fecha.desc()).limit(100).all()

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
def cotizador():
    return render_template("cotizador.html", title="Nuevo - Sistema Poliutech")

@app.route("/admin/catalogos")
def admin_catalogos():
    page_clientes = request.args.get("page_clientes", 1, type=int)
    page_conceptos = request.args.get("page_conceptos", 1, type=int)

    clientes_pag = Cliente.query.order_by(Cliente.id.desc()).paginate(page=page_clientes, per_page=10, error_out=False)
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
# Crear/Editar/Ver/Exportar Cotizaciones
# ---------------------------------------------------------
@app.route("/cotizaciones/crear", methods=["POST"])
def crear_cotizacion():
    f = request.form

    nombre_cliente = (f.get("cliente") or f.get("cliente_nombre") or "").strip()
    empresa = (f.get("empresa") or "").strip()

    # --- CREAR O BUSCAR CLIENTE ---
    cliente = None
    if nombre_cliente:
        q = Cliente.query.filter(db.func.lower(Cliente.nombre_cliente) == nombre_cliente.lower())
        if empresa:
            q = q.filter(db.func.lower(Cliente.empresa) == empresa.lower())
        cliente = q.first()

        if not cliente:
            cliente = Cliente(
                nombre_cliente=nombre_cliente.strip(),
                empresa=empresa.strip() or None,
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

    nombres = f.getlist("item_nombre_concepto[]")
    unidades = f.getlist("item_unidad[]")
    cantidades = f.getlist("item_cantidad[]")
    precios = f.getlist("item_precio[]")
    sistemas = f.getlist("item_sistema[]")  # campo SISTEMA
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

    iva_monto = subtotal * (iva_porc / 100.0)
    total = subtotal + iva_monto
    cot.subtotal = fmt(subtotal)
    cot.iva_porc = fmt(iva_porc)
    cot.iva_monto = fmt(iva_monto)
    cot.total = fmt(total)
    db.session.commit()

    # --- Notificación WhatsApp ---
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
def editar_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_edit.html", c=c, title=f"Editar {c.folio}")

@app.route("/cotizaciones/<int:cot_id>/actualizar", methods=["POST"])
def actualizar_cotizacion(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    f = request.form

    # === CLIENTE ===
    cliente_nombre = (f.get("cliente") or f.get("cliente_nombre") or "").strip()
    empresa = (f.get("empresa") or "").strip()
    responsable = (f.get("responsable") or "").strip()
    correo = (f.get("correo") or "").strip()
    telefono = (f.get("telefono") or "").strip()
    direccion = (f.get("direccion") or "").strip()
    rfc = (f.get("rfc") or "").strip()
    representante = (f.get("representante") or "").strip()

    cliente = None
    if cliente_nombre:
        cliente = Cliente.query.filter_by(nombre_cliente=cliente_nombre).first()
        if not cliente:
            cliente = Cliente(
                nombre_cliente=cliente_nombre,
                empresa=empresa or None,
                responsable=responsable or None,
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
    c.notas = f.get("notas") or c.notas
    c.representante = representante or c.representante
    iva_porc = parse_float(f.get("iva_porc"), c.iva_porc or 16.0)

    # === LIMPIAR DETALLES EXISTENTES ===
    for d in list(c.detalles):
        db.session.delete(d)

    # === DETALLES NUEVOS === (sin descuento; con SISTEMA)
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
    iva_monto = subtotal * (iva_porc / 100.0)
    total = subtotal + iva_monto
    c.subtotal = fmt(subtotal)
    c.iva_porc = fmt(iva_porc)
    c.iva_monto = fmt(iva_monto)
    c.total = fmt(total)

    db.session.commit()

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

@app.route("/cotizaciones/<int:cot_id>/ver")
def ver_cotizacion(cot_id):
    cot = Cotizacion.query.get_or_404(cot_id)
    return render_template("cotizacion_view.html", c=cot, title=f"Vista de {cot.folio}")

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
# API: actualizar estatus (inline)
# ---------------------------------------------------------
@app.route("/api/cotizaciones/<int:cot_id>/estatus", methods=["POST"])
def api_update_estatus(cot_id):
    c = Cotizacion.query.get_or_404(cot_id)

    nuevo = None
    ct = request.headers.get("Content-Type","")
    if "application/json" in ct:
        data = request.get_json(silent=True) or {}
        nuevo = (data.get("estatus") or "").upper().strip()
    else:
        nuevo = (request.form.get("estatus") or "").upper().strip()

    if nuevo not in ["PENDIENTE", "ENVIADA", "GANADA", "PERDIDA"]:
        return jsonify({"ok": False, "error": "Estatus inválido"}), 400

    anterior = c.estatus
    c.estatus = nuevo
    db.session.commit()

    try:
        if twilio_client and nuevo != anterior:
            body = (
                f"🔄 *Actualización de estatus*\n"
                f"Folio: *{c.folio}*\n"
                f"Anterior: {anterior}\n"
                f"Nuevo: *{nuevo}*\n"
                f"Total: {money(c.total)}"
            )
            send_whatsapp_multi(ADMIN_LIST, body)
    except Exception as e:
        print(f"[Twilio] Error al enviar notificación de estatus: {e}", file=sys.stderr)

    return jsonify({"ok": True, "estatus": nuevo})

# ---------------------------------------------------------
# Exportaciones CSV / Excel
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

@app.route("/cotizaciones/<int:cot_id>/export.xlsx")
def export_cotizacion_xlsx(cot_id: int):
    if Workbook is None:
        abort(501, description="openpyxl no instalado en el servidor.")
    c = Cotizacion.query.get_or_404(cot_id)
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
    ws.append(["Representante", c.representante or "", "", "Estatus", c.estatus, ""])
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
# ---------------------------------------------------------
# ---------------------------------------------------------
# ---------------------------------------------------------
# PDF - Diseño corporativo Poliutech (azul, logo izq., A4)
# ---------------------------------------------------------
@app.route("/cotizaciones/<int:cot_id>/export.pdf")
def export_cotizacion_pdf(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)

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

    # === ENCABEZADO ===
    def encabezado(canv, doc_):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1]-40, A4[0], 40, stroke=0, fill=1)

        # Logo
        logo_path = os.path.join(app.static_folder or "static", "logo.jpg")
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 50 * mm  # tamaño más proporcional
                scale = max_w / iw
                w = max_w
                h = ih * scale
                # 🔹 Centrado vertical dentro de la franja azul
                x_logo = 25
                y_logo = A4[1] - h - 15
                canv.drawImage(img, x_logo, y_logo, width=w, height=h, mask="auto")
            except Exception:
                pass


        # Texto superior derecho
        canv.setFont("Helvetica-Bold", 14)
        canv.setFillColor(colors.white)
        canv.drawRightString(A4[0]-25, A4[1]-20, "COTIZACIÓN POLIUTECH")
        canv.setFont("Helvetica", 10)
        canv.drawRightString(A4[0]-25, A4[1]-33, "Recubrimientos Especializados")

        canv.restoreState()

    # === PIE DE PÁGINA CON FIRMA ===
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

        # Línea divisoria
        division_path = os.path.join(app.static_folder or "static", "division.png")
        if os.path.exists(division_path):
            try:
                canv.drawImage(division_path, (A4[0]-155*mm)/2, 45, width=155*mm, height=3*mm, mask="auto")
            except Exception:
                pass

        # Info corporativa
        canv.setFont("Helvetica-Bold", 9)
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.drawCentredString(A4[0]/2, 35, "POLIUTECH – Recubrimientos Especializados")

        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#333333"))
        line1 = "Campos Elíseos 223 Oficina 602 · Col. Polanco V Sección · Miguel Hidalgo, CDMX 11560"
        line2 = "Tel: 55 5938 6530 / 55 5938 0536 · info@poliutech.com · www.poliutech.com"
        canv.drawCentredString(A4[0]/2, 25, line1)
        canv.drawCentredString(A4[0]/2, 15, line2)

        # Título PDF (folio)
        try:
            canv.setTitle(c.folio or "Cotizacion")
        except Exception:
            pass

        canv.restoreState()

    # === DATOS PRINCIPALES ===
    elems.append(Paragraph(f"<b>Folio:</b> {c.folio}", styles["Encabezado"]))
    elems.append(Paragraph(f"<b>Fecha:</b> {c.fecha.strftime('%d/%m/%Y %H:%M')} | "
                           f"<b>Representante:</b> {c.representante or ''}", styles["Encabezado"]))
    elems.append(Spacer(1, 8))

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

    # === TABLA DE CONCEPTOS ===
    data = [["Concepto", "Uni.", "Cant.", "Sistema", "Precio Unitario", "Subtotal"]]
    for d in c.detalles:
        data.append([
            Paragraph(d.nombre_concepto or "-", styles["Normal"]),
            Paragraph(d.unidad or "-", styles["NormalCenter"]),
            Paragraph(f"{d.cantidad:.2f}", styles["NormalCenter"]),
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
    tot_data = [
        ["Subtotal:", money(c.subtotal)],
        [f"IVA ({c.iva_porc:.2f}%):", money(c.iva_monto)],
        ["Total:", money(c.total)],
    ]
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

    # === NOTAS ===
    if c.notas:
        elems.append(Paragraph("<b>Notas:</b>", styles["Encabezado"]))
        for line in str(c.notas).replace("\r\n", "\n").split("\n"):
            if line.strip():
                elems.append(Paragraph(line.strip(), styles["Normal"]))
        elems.append(Spacer(1, 8))

    # === GENERAR PDF ===
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

# ---------------------------------------------------------
# API Dashboard (series / kpis / breakdown)
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
# Blueprints (Catálogos)
# ---------------------------------------------------------
from catalogos_routes import bp as catalogos_bp
app.register_blueprint(catalogos_bp, url_prefix="/catalogos")

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        os.makedirs(app.static_folder or "static", exist_ok=True)
    except Exception:
        pass
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=True)
