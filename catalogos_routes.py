import csv
import io
import os
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify
from werkzeug.utils import secure_filename
from models import db, Concepto  # 👈 corregido aquí

catalogos_bp = Blueprint("catalogos", __name__, template_folder="templates")

ALLOWED_XLS = {".xlsx", ".xls"}
ALLOWED_TXT = {".csv", ".txt"}

# --- Funciones de ayuda ---
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
    if not x:
        return default
    s = str(x).replace(",", ".").replace("$", "").strip()
    try:
        return float(s)
    except Exception:
        return default

def _read_text_table(text):
    for delim in (",", ";", "|", "\t"):
        sio = io.StringIO(text)
        try:
            reader = csv.DictReader(sio, delimiter=delim)
            rows = []
            if reader.fieldnames:
                headers = [_normalize_header(h) for h in reader.fieldnames]
                for raw in reader:
                    row = {}
                    for k, v in raw.items():
                        row[_normalize_header(k)] = str(v).strip()
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
        return [{k: str(v).strip() for k, v in r.items()} for r in records]
    except Exception:
        return []

# --- Rutas ---
@catalogos_bp.route("/catalogos/")
def catalogos_home():
    return render_template("catalogos.html")

@catalogos_bp.get("/list")
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

@catalogos_bp.post("/import")
def import_catalogo():
    file = request.files.get("file")
    if not file or file.filename == "":
        return jsonify({"ok": False, "error": "No file"}), 400

    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename)[1].lower()

    rows = []
    if ext in ALLOWED_XLS:
        rows = _read_xlsx(file)
    elif ext in ALLOWED_TXT:
        raw = file.read().decode("utf-8", errors="ignore")
        rows = _read_text_table(raw)
    else:
        raw = file.read().decode("utf-8", errors="ignore")
        rows = _read_text_table(raw)

    if not rows:
        return jsonify({"ok": False, "error": "No se pudo leer el archivo"}), 400

    inserted = 0
    for r in rows:
        nombre = (r.get("nombre") or r.get("concepto") or r.get("nombre_concepto") or "").strip()
        if not nombre:
            continue

        desc = (r.get("descripcion") or r.get("detalle") or "").strip()
        unidad = (r.get("unidad") or r.get("u") or "").strip()
        precio_val = _to_float(r.get("precio") or r.get("precio_unitario"))

        c = Concepto(
            nombre_concepto=nombre,
            descripcion=desc,
            unidad=unidad,
            precio_unitario=precio_val
        )
        db.session.add(c)
        inserted += 1

    db.session.commit()
    return jsonify({"ok": True, "inserted": inserted})
