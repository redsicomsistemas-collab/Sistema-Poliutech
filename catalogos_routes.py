import csv
import io
import os
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash

# Evitar problemas de import dependiendo de la estructura
try:
    from app import db, Concepto, Cliente
except Exception:
    from models import db, Concepto, Cliente  # fallback si existe models.py

bp = Blueprint("catalogos", __name__, template_folder="templates")

ALLOWED_XLS = {".xlsx", ".xls"}
ALLOWED_TXT = {".csv", ".txt"}

# --- Utilidades de normalización ---
def _normalize_header(h: str) -> str:
    if not h:
        return ""
    s = str(h).strip().lower()
    s = (
        s.replace("á", "a").replace("é", "e").replace("í", "i")
        .replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    )
    s = s.replace("precio unitario", "precio_unitario")
    s = s.replace("concepto/servicio", "concepto")
    s = s.replace("descripcion/observaciones", "descripcion")
    s = s.replace("desc", "descripcion")
    s = s.replace("u.", "unidad")
    s = s.replace("u ", "unidad ")
    s = "_".join(s.split())
    return s

def _to_float(x, default=0.0):
    if x is None:
        return default
    s = str(x).replace(",", ".").replace("$", "").strip()
    try:
        return float(s) if s else default
    except Exception:
        return default

def _read_text_table(text):
    for delim in (",", ";", "|", "\t"):
        sio = io.StringIO(text)
        try:
            reader = csv.DictReader(sio, delimiter=delim)
            rows = []
            if reader.fieldnames:
                for raw in reader:
                    row = {}
                    for k, v in raw.items():
                        row[_normalize_header(k)] = ("" if v is None else str(v).strip())
                    rows.append(row)
            if rows:
                return rows
        except Exception:
            continue
    return []

def _read_xlsx(file_storage):
    try:
        import pandas as pd
        df = pd.read_excel(file_storage, sheet_name=0, dtype=str)
        df.columns = [_normalize_header(c) for c in df.columns]
        records = df.fillna("").to_dict(orient="records")
        return [{k: ("" if v is None else str(v).strip()) for k, v in r.items()} for r in records]
    except Exception:
        return []

# --- Vistas ---
@bp.route("/")
def home():
    # Esta ruta sirve como placeholder; el admin real está en /admin/catalogos
    return redirect(url_for("admin_catalogos"))

@bp.get("/list")
def list_catalogo():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    qtext = (request.args.get("q") or "").strip()

    query = Concepto.query
    if qtext:
        like = f"%{qtext}%"
        query = query.filter((Concepto.nombre_concepto.ilike(like)) | (Concepto.descripcion.ilike(like)))

    total = query.count()
    items = (
        query.order_by(Concepto.id.desc())
             .offset((page - 1) * per_page)
             .limit(per_page)
             .all()
    )

    return jsonify({
        "total": total,
        "page": page,
        "per_page": per_page,
        "items": [
            {
                "id": c.id,
                "nombre_concepto": c.nombre_concepto,
                "descripcion": c.descripcion,
                "unidad": c.unidad,
                "precio_unitario": c.precio_unitario
            }
            for c in items
        ]
    })

@bp.post("/import")
def import_catalogo():
    # Soporta formulario (con flash + redirect) y también retorno JSON (API)
    tipo = (request.form.get("tipo") or request.args.get("tipo") or "").strip()
    file = request.files.get("archivo") or request.files.get("file")

    if not file or file.filename == "":
        if request.accept_mimetypes.accept_json and not request.accept_mimetypes.accept_html:
            return jsonify({"ok": False, "error": "No file"}), 400
        flash("No se adjuntó archivo.", "danger")
        return redirect(url_for("admin_catalogos"))

    filename = file.filename
    ext = os.path.splitext(filename)[1].lower()

    rows = []
    if ext in ALLOWED_XLS:
        rows = _read_xlsx(file)
    else:
        raw = file.read().decode("utf-8", errors="ignore")
        rows = _read_text_table(raw)

    if not rows:
        if request.accept_mimetypes.accept_json and not request.accept_mimetypes.accept_html:
            return jsonify({"ok": False, "error": "No se pudo leer el archivo"}), 400
        flash("No se pudo leer el archivo. Verifique cabeceras y formato.", "danger")
        return redirect(url_for("admin_catalogos"))

    inserted = 0
    if tipo.lower() == "clientes":
        for r in rows:
            nombre = (r.get("nombre_cliente") or r.get("cliente") or r.get("nombre") or "").strip()
            if not nombre:
                continue
            cli = Cliente(
                nombre_cliente=nombre,
                empresa=(r.get("empresa") or "").strip() or None,
                responsable=(r.get("responsable") or "").strip() or None,
                correo=(r.get("correo") or r.get("email") or "").strip() or None,
                telefono=(r.get("telefono") or r.get("tel") or "").strip() or None,
                direccion=(r.get("direccion") or "").strip() or None,
                rfc=(r.get("rfc") or "").strip() or None,
            )
            db.session.add(cli); inserted += 1
    else:
        # Conceptos por default
        for r in rows:
            nombre = (r.get("nombre") or r.get("concepto") or r.get("nombre_concepto") or "").strip()
            if not nombre:
                continue
            desc = (r.get("descripcion") or r.get("detalle") or "").strip()
            unidad = (r.get("unidad") or r.get("u") or "").strip()
            precio_val = _to_float(r.get("precio") or r.get("precio_unitario"))

            c = Concepto(
                nombre_concepto=nombre,
                descripcion=desc or None,
                unidad=unidad or None,
                precio_unitario=precio_val
            )
            db.session.add(c); inserted += 1

    db.session.commit()

    # Formulario (HTML) => flash + redirect
    if request.accept_mimetypes.accept_html:
        flash(f"Importación exitosa: {inserted} registro(s) agregados.", "success")
        return redirect(url_for("admin_catalogos"))

    # API JSON
    return jsonify({"ok": True, "inserted": inserted})
