# =========================================================
# app.py — MARWHATS (checkpoint) / Poliutech
# Limpio + Roles (ADMIN / USER) + Filtro por Responsable
# =========================================================
from __future__ import annotations

import os, io, csv, sys, math, re, json, traceback, unicodedata, smtplib, zipfile, logging
import mimetypes
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, List
from urllib.parse import urlparse
from pathlib import Path
from functools import wraps
from email.message import EmailMessage
from email.utils import getaddresses
from html import escape
import xml.etree.ElementTree as ET
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
try:
    import firebase_admin
    from firebase_admin import credentials as firebase_credentials
    from firebase_admin import messaging as firebase_messaging
except Exception:
    firebase_admin = None
    firebase_credentials = None
    firebase_messaging = None


# -------------------------------
# Condiciones comerciales
# -------------------------------
# Ya no se agregan condiciones por defecto. Solo se exporta lo capturado
# por el usuario y, cuando aplique, la trazabilidad de la zona.
DEFAULT_CONDICIONES: list[str] = []
VALID_ESTATUS = [
    "ENVIADA",
    "PENDIENTE",
    "EN CURSO",
    "O. TERMINADA",
    "FINALIZADA",
    "GANADA",
    "PERDIDA",
]
PROSPECT_STATUS_OPTIONS = [
    "PENDIENTE",
    "CONTACTADO",
    "COTIZADO",
    "FINALIZADO",
    "RECHAZADO",
]
PROVIDER_NUMBERS_JSON = Path(__file__).resolve().parent / "provider_numbers.json"
PROVIDER_NUMBERS_XLSX = Path.home() / "Downloads" / "NUMEROS DE PROVEEDOR POLIUTECH.xlsx"
REGISTRO_OBRAS_JSON = Path(__file__).resolve().parent / "registro_obras.json"
XLSX_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "p": "http://schemas.openxmlformats.org/package/2006/relationships",
}



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


def _excel_col_to_index(ref: str) -> int:
    letters = "".join(ch for ch in (ref or "") if ch.isalpha()).upper()
    idx = 0
    for ch in letters:
        idx = (idx * 26) + (ord(ch) - 64)
    return max(idx - 1, 0)


def _xlsx_cell_text(cell, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join((node.text or "") for node in cell.findall(".//a:t", XLSX_NS)).strip()

    value_node = cell.find("a:v", XLSX_NS)
    raw_value = "" if value_node is None or value_node.text is None else value_node.text
    if cell_type == "s" and raw_value != "":
        try:
            return str(shared_strings[int(raw_value)]).strip()
        except Exception:
            return ""
    return str(raw_value).strip()


def _load_provider_numbers_from_xlsx() -> list[dict]:
    if not PROVIDER_NUMBERS_XLSX.exists():
        return []

    with zipfile.ZipFile(PROVIDER_NUMBERS_XLSX) as workbook_zip:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook_zip.namelist():
            shared_root = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("a:si", XLSX_NS):
                shared_strings.append(
                    "".join((node.text or "") for node in item.findall(".//a:t", XLSX_NS)).strip()
                )

        workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        rels_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib.get("Id"): rel.attrib.get("Target", "")
            for rel in rels_root.findall("p:Relationship", XLSX_NS)
        }

        target_sheet = None
        for sheet in workbook_root.findall("a:sheets/a:sheet", XLSX_NS):
            sheet_name = (sheet.attrib.get("name") or "").strip().lower()
            rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if rel_id and sheet_name == "table 2":
                target = rel_map.get(rel_id, "")
                if target:
                    target_sheet = f"xl/{target.lstrip('/')}"
                    break

        if not target_sheet or target_sheet not in workbook_zip.namelist():
            return []

        sheet_root = ET.fromstring(workbook_zip.read(target_sheet))
        rows = sheet_root.findall("a:sheetData/a:row", XLSX_NS)
        parsed_rows: list[list[str]] = []

        for row in rows:
            values_by_col: dict[int, str] = {}
            for cell in row.findall("a:c", XLSX_NS):
                ref = cell.attrib.get("r", "")
                values_by_col[_excel_col_to_index(ref)] = _xlsx_cell_text(cell, shared_strings)

            if not values_by_col:
                continue

            max_col = max(values_by_col)
            parsed_rows.append([values_by_col.get(col, "").strip() for col in range(max_col + 1)])

        if not parsed_rows:
            return []

        data_rows = parsed_rows[1:]
        records: list[dict] = []
        for idx, row in enumerate(data_rows, start=1):
            numero = row[0].strip() if len(row) > 0 else ""
            empresa = row[1].strip() if len(row) > 1 else ""
            razon_social = row[2].strip() if len(row) > 2 else ""
            if not any([numero, empresa, razon_social]):
                continue
            records.append({
                "id": idx,
                "numero": numero,
                "empresa": empresa,
                "razon_social_poliutech": razon_social,
                "contacto": "",
                "telefono": "",
                "correo": "",
            })
        return records


def _save_provider_numbers(rows: list[dict]) -> None:
    PROVIDER_NUMBERS_JSON.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _normalize_provider_row(row: Optional[dict], idx: int) -> dict:
    row = row or {}
    return {
        "id": idx,
        "numero": str(row.get("numero", "")).strip(),
        "empresa": str(row.get("empresa", "")).strip(),
        "razon_social_poliutech": str(row.get("razon_social_poliutech", "")).strip(),
        "contacto": str(row.get("contacto", "")).strip(),
        "telefono": str(row.get("telefono", "")).strip(),
        "correo": str(row.get("correo", "")).strip(),
    }


def _load_provider_numbers() -> list[dict]:
    if PROVIDER_NUMBERS_JSON.exists():
        try:
            data = json.loads(PROVIDER_NUMBERS_JSON.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [_normalize_provider_row(row, idx) for idx, row in enumerate(data, start=1)]
        except Exception:
            pass

    seeded = _load_provider_numbers_from_xlsx()
    _save_provider_numbers(seeded)
    return seeded


def _normalize_registro_obra_row(row: Optional[dict], idx: int) -> dict:
    row = row or {}
    return {
        "id": idx,
        "numero": str(row.get("numero", "")).strip(),
        "obra": str(row.get("obra", "")).strip(),
        "ubicacion": str(row.get("ubicacion", "")).strip(),
        "encargado": str(row.get("encargado", "")).strip(),
        "puesto": str(row.get("puesto", "")).strip(),
        "telefono": str(row.get("telefono", "")).strip(),
        "correo": str(row.get("correo", "")).strip(),
        "responsable": str(row.get("responsable", "")).strip(),
    }


def _clean_registro_obra_excel_value(value: object) -> str:
    text = str(value or "").strip()
    if text in {'"', "-", "—", "N/A", "n/a"}:
        return ""
    return text


def _normalize_registro_obra_phone(value: object) -> str:
    text = _clean_registro_obra_excel_value(value)
    if not text:
        return ""
    try:
        if re.fullmatch(r"\d+(?:\.\d+)?E\d+", text, re.IGNORECASE):
            text = format(int(float(text)), "d")
    except Exception:
        pass
    return text


def _registro_obra_duplicate_key(row: dict) -> tuple[str, str, str, str, str]:
    def norm(value: object) -> str:
        raw = str(value or "").strip()
        normalized = unicodedata.normalize("NFKD", raw)
        return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower().strip()

    return (
        norm(row.get("obra")),
        norm(row.get("ubicacion")),
        norm(row.get("encargado")),
        norm(row.get("telefono")),
        norm(row.get("correo")),
    )


def _registro_obra_to_row(item: "RegistroObra", idx: Optional[int] = None) -> dict:
    position = idx if idx is not None else (item.id or 0)
    return _normalize_registro_obra_row({
        "numero": item.numero,
        "obra": item.obra,
        "ubicacion": item.ubicacion,
        "encargado": item.encargado,
        "puesto": item.puesto,
        "telefono": item.telefono,
        "correo": item.correo,
        "responsable": item.responsable,
    }, position)


def _load_registro_obras() -> list[dict]:
    items = RegistroObra.query.order_by(RegistroObra.numero.asc(), RegistroObra.id.asc()).all()
    return [_registro_obra_to_row(item, idx) for idx, item in enumerate(items, start=1)]


def _registro_obras_filters_from_request() -> dict[str, str]:
    return {
        "obra": (request.args.get("obra") or "").strip().lower(),
        "responsable": (request.args.get("responsable") or "").strip().lower(),
    }


def _registro_obra_matches_filters(row: dict, filters: dict[str, str]) -> bool:
    for field, needle in filters.items():
        if needle and needle not in str(row.get(field, "")).strip().lower():
            return False
    return True


def _filter_registro_obras(rows: list[dict], filters: dict[str, str]) -> list[dict]:
    filtered = [row for row in rows if _registro_obra_matches_filters(row, filters)]
    if is_admin():
        return filtered
    ra = (responsable_actual() or "").strip().lower()
    if not ra:
        return []
    return [row for row in filtered if (row.get("responsable") or "").strip().lower() == ra]


def _sync_cliente_from_registro_obra(row: dict) -> None:
    nombre_cliente = (row.get("encargado") or "").strip()
    empresa = (row.get("obra") or "").strip()
    if not nombre_cliente:
        return

    query = Cliente.query.filter(db.func.lower(Cliente.nombre_cliente) == nombre_cliente.lower())
    if empresa:
        query = query.filter(db.func.lower(Cliente.empresa) == empresa.lower())
    cliente = query.first()
    if not cliente:
        cliente = Cliente(nombre_cliente=nombre_cliente, empresa=empresa or None)
        db.session.add(cliente)

    responsable = (row.get("responsable") or "").strip()
    cliente.responsable = responsable or cliente.responsable
    cliente.correo = (row.get("correo") or "").strip() or cliente.correo
    cliente.telefono = (row.get("telefono") or "").strip() or cliente.telefono
    cliente.direccion = (row.get("ubicacion") or "").strip() or cliente.direccion


def _registro_obra_email_body() -> str:
    return (
        "Buen día,\n\n"
        "Con gusto de saludarlo y de acuerdo a la plática que pudimos sostener con usted o un representante de su empresa, por medio del presente, nos permitimos presentar a Corporativo Poliutech, una empresa especializada en la aplicación de recubrimientos para la construcción. Contamos con certificaciones como aplicadores en pisos epóxicos, impermeabilizantes, poliureas, pinturas y diversos recubrimientos especializados para proyectos en los sectores industrial, comercial, público y privado.\n\n"
        "Nos distinguimos por adaptarnos a los requerimientos de nuestros clientes, optimizando al máximo los recursos y espacios disponibles para garantizar soluciones eficientes y de alta calidad.\n\n"
        "Adjunto a este correo encontrará nuestro CV empresarial, donde podrá conocer más sobre nuestros servicios y proyectos.\n\n"
        "Quedamos a sus órdenes para cualquier necesidad o consulta.\n\n"
        "Atentamente Poliutech Recubrimientos Especializados"
    )


def _send_registro_obra_email(row: dict) -> None:
    recipient = (row.get("correo") or "").strip()
    if not recipient:
        raise ValueError("El registro no tiene correo destino.")
    if not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", recipient):
        raise ValueError("El correo destino no es válido.")
    if not REGISTRO_MAIL_ATTACHMENT.exists():
        raise FileNotFoundError(f"No se encontró el adjunto requerido: {REGISTRO_MAIL_ATTACHMENT.name}")

    msg = EmailMessage()
    msg["Subject"] = "Te visitamos recientemente"
    msg["From"] = f"Poliutech <{REGISTRO_MAIL_FROM}>"
    msg["To"] = recipient
    msg.set_content(_registro_obra_email_body())

    attachment_bytes = REGISTRO_MAIL_ATTACHMENT.read_bytes()
    msg.add_attachment(
        attachment_bytes,
        maintype="application",
        subtype="pdf",
        filename=REGISTRO_MAIL_ATTACHMENT.name,
    )

    with smtplib.SMTP(REGISTRO_MAIL_HOST, REGISTRO_MAIL_PORT, timeout=30) as smtp:
        smtp.ehlo()
        smtp.login(REGISTRO_MAIL_USERNAME, REGISTRO_MAIL_PASSWORD)
        smtp.send_message(msg, to_addrs=[recipient])


def _save_registro_obras(rows: list[dict]) -> None:
    db.session.query(RegistroObra).delete()
    for idx, raw_row in enumerate(rows, start=1):
        row = _normalize_registro_obra_row(raw_row, idx)
        db.session.add(RegistroObra(
            numero=idx,
            obra=row.get("obra", ""),
            ubicacion=row.get("ubicacion", ""),
            encargado=row.get("encargado", ""),
            puesto=row.get("puesto", ""),
            telefono=row.get("telefono", ""),
            correo=row.get("correo", ""),
            responsable=row.get("responsable", ""),
        ))


def _migrate_registro_obras_from_json() -> None:
    if RegistroObra.query.first() or not REGISTRO_OBRAS_JSON.exists():
        return

    try:
        data = json.loads(REGISTRO_OBRAS_JSON.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return

        rows = [_normalize_registro_obra_row(row, idx) for idx, row in enumerate(data, start=1)]
        _save_registro_obras(rows)
        for row in rows:
            _sync_cliente_from_registro_obra(row)
        db.session.commit()
        print(f"✅ Migrados {len(rows)} registros de obras desde JSON a base de datos.")
    except Exception as e:
        db.session.rollback()
        print("⚠️ ensure_schema(registro_obra.migracion_json):", e)


def _load_registro_obras_from_xlsx(file_bytes: bytes, default_responsable: str = "") -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as workbook_zip:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook_zip.namelist():
            shared_root = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("a:si", XLSX_NS):
                shared_strings.append(
                    "".join((node.text or "") for node in item.findall(".//a:t", XLSX_NS)).strip()
                )

        workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        rels_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib.get("Id"): rel.attrib.get("Target", "")
            for rel in rels_root.findall("p:Relationship", XLSX_NS)
        }

        first_sheet_path = None
        for sheet in workbook_root.findall("a:sheets/a:sheet", XLSX_NS):
            rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = rel_map.get(rel_id or "", "")
            if target:
                first_sheet_path = f"xl/{target.lstrip('/')}"
                break

        if not first_sheet_path or first_sheet_path not in workbook_zip.namelist():
            return []

        sheet_root = ET.fromstring(workbook_zip.read(first_sheet_path))
        parsed_rows: list[list[str]] = []
        for row in sheet_root.findall("a:sheetData/a:row", XLSX_NS):
            values_by_col: dict[int, str] = {}
            for cell in row.findall("a:c", XLSX_NS):
                ref = cell.attrib.get("r", "")
                values_by_col[_excel_col_to_index(ref)] = _xlsx_cell_text(cell, shared_strings)
            if not values_by_col:
                continue
            max_col = max(values_by_col)
            parsed_rows.append([values_by_col.get(col, "").strip() for col in range(max_col + 1)])

    if not parsed_rows:
        return []

    header_aliases = {
        "numero": "numero",
        "n°": "numero",
        "no": "numero",
        "obra": "obra",
        "ubicacion": "ubicacion",
        "encargado": "encargado",
        "puesto": "puesto",
        "telefono": "telefono",
        "correo": "correo",
        "responsable": "responsable",
    }
    column_map: dict[str, int] = {}
    header_row_index = -1
    for idx, row in enumerate(parsed_rows):
        current_map: dict[str, int] = {}
        for col_idx, value in enumerate(row):
            key = _normalize_text_for_match(value).replace(".", "")
            if key in header_aliases:
                current_map[header_aliases[key]] = col_idx
        if "obra" in current_map:
            column_map = current_map
            header_row_index = idx
            break

    if header_row_index < 0:
        return []

    imported_rows: list[dict] = []
    for row in parsed_rows[header_row_index + 1:]:
        obra = _clean_registro_obra_excel_value(row[column_map["obra"]]) if "obra" in column_map and len(row) > column_map["obra"] else ""
        ubicacion = _clean_registro_obra_excel_value(row[column_map["ubicacion"]]) if "ubicacion" in column_map and len(row) > column_map["ubicacion"] else ""
        encargado = _clean_registro_obra_excel_value(row[column_map["encargado"]]) if "encargado" in column_map and len(row) > column_map["encargado"] else ""
        puesto = _clean_registro_obra_excel_value(row[column_map["puesto"]]) if "puesto" in column_map and len(row) > column_map["puesto"] else ""
        telefono = _normalize_registro_obra_phone(row[column_map["telefono"]]) if "telefono" in column_map and len(row) > column_map["telefono"] else ""
        correo = _clean_registro_obra_excel_value(row[column_map["correo"]]) if "correo" in column_map and len(row) > column_map["correo"] else ""
        responsable = _clean_registro_obra_excel_value(row[column_map["responsable"]]) if "responsable" in column_map and len(row) > column_map["responsable"] else ""
        if not any([obra, ubicacion, encargado, puesto, telefono, correo, responsable]):
            continue
        imported_rows.append(_normalize_registro_obra_row({
            "numero": "",
            "obra": obra,
            "ubicacion": ubicacion,
            "encargado": encargado,
            "puesto": puesto,
            "telefono": telefono,
            "correo": correo,
            "responsable": responsable or default_responsable,
        }, len(imported_rows) + 1))

    return imported_rows


def _mobile_json_error(message: str, status: int = 400):
    return jsonify({"ok": False, "error": message}), status


def _mobile_token_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(current_app.secret_key, salt="registro-obras-mobile")


def _issue_mobile_token(user: Usuario) -> str:
    return _mobile_token_serializer().dumps({"user_id": user.id})


def _mobile_user_from_token() -> Optional[Usuario]:
    auth_header = (request.headers.get("Authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return None
    token = auth_header[7:].strip()
    if not token:
        return None
    try:
        payload = _mobile_token_serializer().loads(token, max_age=60 * 60 * 24 * 30)
    except (BadSignature, SignatureExpired):
        return None
    user_id = payload.get("user_id")
    if not user_id:
        return None
    return Usuario.query.get(int(user_id))


def _mobile_user_is_admin(user: Usuario) -> bool:
    return ((getattr(user, "rol", "") or "").upper() == "ADMIN")


def _mobile_user_responsable(user: Usuario) -> str:
    nombre = (getattr(user, "nombre", "") or "").strip()
    if not nombre:
        return ""
    first = nombre.split()[0].strip()
    return first[:1].upper() + first[1:].lower() if first else ""


def require_mobile_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = _mobile_user_from_token()
        if not user:
            return _mobile_json_error("No autorizado.", 401)
        g.mobile_user = user
        return fn(*args, **kwargs)
    return wrapper


def _mobile_user_can_access_quote(user: Usuario, cot: Cotizacion) -> bool:
    if _mobile_user_is_admin(user):
        return True
    return (cot.responsable or "").strip().lower() == _mobile_user_responsable(user).lower()


def _mobile_quote_pdf_url(cot_id: int) -> str:
    url = url_for("api_mobile_quote_pdf", cot_id=cot_id, _external=True)
    if url.startswith("http://"):
        return "https://" + url[len("http://"):]
    return url


def _firebase_is_configured() -> bool:
    return PUSH_NOTIFICATIONS_ENABLED and firebase_admin is not None and bool(FIREBASE_CREDENTIALS_FILE or FIREBASE_CREDENTIALS_JSON)


def _get_firebase_app():
    if not _firebase_is_configured():
        return None
    try:
        return firebase_admin.get_app()
    except Exception:
        pass

    try:
        if FIREBASE_CREDENTIALS_JSON:
            cred = firebase_credentials.Certificate(json.loads(FIREBASE_CREDENTIALS_JSON))
        elif FIREBASE_CREDENTIALS_FILE:
            cred = firebase_credentials.Certificate(FIREBASE_CREDENTIALS_FILE)
        else:
            return None
        return firebase_admin.initialize_app(cred)
    except Exception as exc:
        logger.warning("Firebase no se pudo inicializar: %s", exc)
        return None


def _upsert_mobile_device(user: Usuario, token: str, plataforma: str = "android", device_name: str = "", app_version: str = "") -> MobileDevice:
    existing = MobileDevice.query.filter_by(token=token).first()
    if existing:
        existing.usuario_id = user.id
        existing.plataforma = (plataforma or "android").strip().lower()[:30]
        existing.device_name = (device_name or "").strip()[:120]
        existing.app_version = (app_version or "").strip()[:40]
        existing.is_active = True
        existing.last_seen_at = now_cdmx_naive()
        db.session.add(existing)
        db.session.commit()
        return existing

    device = MobileDevice(
        usuario_id=user.id,
        token=token,
        plataforma=(plataforma or "android").strip().lower()[:30] or "android",
        device_name=(device_name or "").strip()[:120],
        app_version=(app_version or "").strip()[:40],
        is_active=True,
        last_seen_at=now_cdmx_naive(),
    )
    db.session.add(device)
    db.session.commit()
    return device


def _deactivate_mobile_device(token: str) -> None:
    if not token:
        return
    device = MobileDevice.query.filter_by(token=token).first()
    if not device:
        return
    device.is_active = False
    device.updated_at = now_cdmx_naive()
    db.session.add(device)
    db.session.commit()


def _mobile_push_tokens_for_users(user_ids: list[int]) -> list[str]:
    if not user_ids:
        return []
    rows = (
        MobileDevice.query
        .filter(MobileDevice.usuario_id.in_(user_ids), MobileDevice.is_active.is_(True))
        .all()
    )
    unique_tokens: list[str] = []
    seen: set[str] = set()
    for row in rows:
        token = (row.token or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        unique_tokens.append(token)
    return unique_tokens


def _mobile_push_user_ids_for_quote(cot: Cotizacion) -> list[int]:
    user_ids: set[int] = set()
    admins = Usuario.query.filter(db.func.upper(Usuario.rol) == "ADMIN").all()
    user_ids.update(u.id for u in admins if getattr(u, "id", None))
    responsable = (cot.responsable or "").strip().lower()
    if responsable:
        owner = Usuario.query.filter(db.func.lower(Usuario.nombre) == responsable).first()
        if owner and owner.id:
            user_ids.add(owner.id)
        else:
            users = Usuario.query.all()
            for user in users:
                first_name = _mobile_user_responsable(user).strip().lower()
            if first_name and first_name == responsable and user.id:
                    user_ids.add(user.id)
    return list(user_ids)


def _send_quote_status_push(cot: Cotizacion, previous_status: str, new_status: str) -> dict[str, int]:
    if (new_status or "").strip().upper() == "FINALIZADA":
        return {"sent": 0, "failed": 0}
    pdf_url = _mobile_quote_pdf_url(cot.id)
    tokens = _mobile_push_tokens_for_users(_mobile_push_user_ids_for_quote(cot))
    return _send_push_notification(
        tokens,
        title=f"Cotización {new_status}",
        body=f"{cot.folio or 'Sin folio'} · {money(cot.total)}",
        data={
            "type": "quote_status",
            "cotizacion_id": str(cot.id or ""),
            "folio": str(cot.folio or ""),
            "previous_status": str(previous_status or ""),
            "estatus": str(new_status or ""),
            "pdf_url": pdf_url,
        },
    )


def _send_quote_created_notification(cot: Cotizacion) -> None:
    estatus_actual = (cot.estatus or "").strip().upper()
    pdf_url = _mobile_quote_pdf_url(cot.id)
    try:
        msg = (
            "🧾 *Nueva Cotización Creada*\\n"
            f"Folio: *{cot.folio or 'Sin folio'}*\\n"
            f"Estatus: *{estatus_actual or 'SIN ESTATUS'}*\\n"
            f"Fecha (CDMX): {cot.fecha.strftime('%d/%m/%Y %H:%M') if cot.fecha else ''}\\n"
            f"Total: {money(cot.total)}"
        )
        send_whatsapp_multi(ADMIN_LIST, msg)
    except Exception as exc:
        logger.warning("WhatsApp de creación falló: %s", exc)

    try:
        tokens = _mobile_push_tokens_for_users(_mobile_push_user_ids_for_quote(cot))
        _send_push_notification(
            tokens,
            title="Nueva cotización creada",
            body=f"{cot.folio or 'Sin folio'} · {money(cot.total)} · {estatus_actual or 'SIN ESTATUS'}",
            data={
                "type": "quote_created",
                "cotizacion_id": str(cot.id or ""),
                "folio": str(cot.folio or ""),
                "estatus": str(cot.estatus or ""),
                "pdf_url": pdf_url,
            },
        )
    except Exception as exc:
        logger.warning("Push de creación falló: %s", exc)


def _send_quote_followup_push(cot: Cotizacion, seg: CotizacionSeguimiento) -> dict[str, int]:
    tokens = _mobile_push_tokens_for_users(_mobile_push_user_ids_for_quote(cot))
    preview = " ".join((seg.comentario or "").split())
    if len(preview) > 120:
        preview = preview[:117] + "..."
    return _send_push_notification(
        tokens,
        title=f"Nuevo seguimiento · {cot.folio or 'Sin folio'}",
        body=preview or f"{seg.autor} agregó un seguimiento.",
        data={
            "type": "quote_followup",
            "cotizacion_id": str(cot.id or ""),
            "seguimiento_id": str(seg.id or ""),
            "folio": str(cot.folio or ""),
            "estatus": str(cot.estatus or ""),
            "autor": str(seg.autor or ""),
            "pdf_url": _mobile_quote_pdf_url(cot.id),
        },
    )


def _send_daily_status_reminder(cot: Cotizacion, ahora: datetime) -> None:
    estatus_actual = (cot.estatus or "").strip().upper()
    if not estatus_actual or estatus_actual == "FINALIZADA":
        return

    body = (
        "🔔 *Recordatorio diario de cotización*\\n"
        f"Folio: *{cot.folio or 'Sin folio'}*\\n"
        f"Estatus: *{estatus_actual}*\\n"
        f"Fecha (CDMX): {cot.fecha.strftime('%d/%m/%Y %H:%M') if cot.fecha else ''}\\n"
        f"Total: {money(cot.total)}"
    )
    send_whatsapp_multi(ADMIN_LIST, body)
    _send_quote_status_push(cot, estatus_actual, estatus_actual)
    cot.last_whatsapp_at = ahora
    db.session.commit()


def _send_push_notification(tokens: list[str], title: str, body: str, data: Optional[dict[str, str]] = None) -> dict[str, int]:
    if not tokens:
        return {"sent": 0, "failed": 0}
    app_instance = _get_firebase_app()
    if app_instance is None or firebase_messaging is None:
        return {"sent": 0, "failed": len(tokens)}

    sent = 0
    failed = 0
    payload_data = {str(k): str(v) for k, v in (data or {}).items()}
    payload_data["title"] = str(title)
    payload_data["body"] = str(body)
    for token in tokens:
        try:
            message = firebase_messaging.Message(
                token=token,
                data=payload_data,
                android=firebase_messaging.AndroidConfig(priority="high"),
            )
            firebase_messaging.send(message, app=app_instance)
            sent += 1
        except Exception as exc:
            failed += 1
            logger.warning("Push fallido para token móvil: %s", exc)
            err = str(exc).lower()
            if any(fragment in err for fragment in ["registration-token", "not registered", "invalid argument"]):
                _deactivate_mobile_device(token)
    return {"sent": sent, "failed": failed}


def _filter_registro_obras_for_mobile(rows: list[dict], user: Usuario, obra: str = "", responsable: str = "") -> list[dict]:
    obra_filter = (obra or "").strip().lower()
    responsable_filter = (responsable or "").strip().lower()
    out = []
    for row in rows:
        obra_value = (row.get("obra") or "").strip().lower()
        responsable_value = (row.get("responsable") or "").strip().lower()
        if obra_filter and obra_filter not in obra_value:
            continue
        if responsable_filter and responsable_filter not in responsable_value:
            continue
        if not _mobile_user_is_admin(user):
            if responsable_value != _mobile_user_responsable(user).lower():
                continue
        out.append(row)
    return out


def _provider_filters_from_request() -> dict[str, str]:
    return {
        "razon_social_poliutech": (request.args.get("razon_social_poliutech") or "").strip().lower(),
    }


def _provider_row_matches_filters(row: dict, filters: dict[str, str]) -> bool:
    for field, needle in filters.items():
        if needle and needle not in str(row.get(field, "")).strip().lower():
            return False
    return True


def _filter_provider_rows(rows: list[dict], filters: dict[str, str]) -> list[dict]:
    return [row for row in rows if _provider_row_matches_filters(row, filters)]


def _normalize_prospecto_status(value: object) -> str:
    status = str(value or "").strip().upper()
    return status if status in PROSPECT_STATUS_OPTIONS else "PENDIENTE"


def _prospecto_to_row(item: "Prospecto", idx: Optional[int] = None) -> dict:
    position = idx if idx is not None else (item.id or 0)
    return {
        "id": item.id,
        "numero": position,
        "titulo": (item.titulo or "").strip(),
        "descripcion": (item.descripcion or "").strip(),
        "contacto": (item.contacto or "").strip(),
        "telefono": (item.telefono or "").strip(),
        "correo": (item.correo or "").strip(),
        "status": _normalize_prospecto_status(item.status),
        "responsable": (item.responsable or "").strip(),
        "seguimiento_count": len(item.seguimientos or []),
    }


def _load_prospectos() -> list[dict]:
    query = Prospecto.query.order_by(Prospecto.id.desc())
    items = query.all()
    return [_prospecto_to_row(item, idx) for idx, item in enumerate(items, start=1)]


def _prospectos_filters_from_request() -> dict[str, str]:
    status_raw = (request.args.get("status") or "").strip()
    return {
        "titulo": (request.args.get("titulo") or "").strip().lower(),
        "status": _normalize_prospecto_status(status_raw) if status_raw else "",
        "contacto": (request.args.get("contacto") or "").strip().lower(),
    }


def _prospecto_matches_filters(row: dict, filters: dict[str, str]) -> bool:
    titulo = filters.get("titulo") or ""
    status = filters.get("status") or ""
    contacto = filters.get("contacto") or ""

    if titulo and titulo not in str(row.get("titulo", "")).strip().lower():
        return False
    if status and status != _normalize_prospecto_status(row.get("status")):
        return False
    if contacto and contacto not in str(row.get("contacto", "")).strip().lower():
        return False
    return True


def _filter_prospectos(rows: list[dict], filters: dict[str, str]) -> list[dict]:
    return [row for row in rows if _prospecto_matches_filters(row, filters)]


def _build_simple_xls(sheet_name: str, headers: list[str], rows: list[list[str]]) -> bytes:
    def html_cell(value: object) -> str:
        text = "" if value is None else str(value)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        return escape(text).replace("\n", "<br>")

    parts = [
        "<html>",
        "<head>",
        '<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />',
        f"<title>{escape(sheet_name)}</title>",
        "</head>",
        "<body>",
        "<table border='1'>",
        "<thead><tr>",
    ]
    for header in headers:
        parts.append(f"<th>{html_cell(header)}</th>")
    parts.append("</tr></thead><tbody>")
    for row in rows:
        parts.append("<tr>")
        for cell in row:
            parts.append(f"<td>{html_cell(cell)}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table></body></html>")
    return "".join(parts).encode("utf-8")


def _normalize_import_header(value: object) -> str:
    raw = str(value or "").strip().lower()
    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return normalized


def _load_prospectos_from_xlsx(file_bytes: bytes) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as workbook_zip:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in workbook_zip.namelist():
            shared_root = ET.fromstring(workbook_zip.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("a:si", XLSX_NS):
                shared_strings.append(
                    "".join((node.text or "") for node in item.findall(".//a:t", XLSX_NS)).strip()
                )

        workbook_root = ET.fromstring(workbook_zip.read("xl/workbook.xml"))
        rels_root = ET.fromstring(workbook_zip.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib.get("Id"): rel.attrib.get("Target", "")
            for rel in rels_root.findall("p:Relationship", XLSX_NS)
        }

        target_sheet = None
        for sheet in workbook_root.findall("a:sheets/a:sheet", XLSX_NS):
            rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if rel_id:
                target = rel_map.get(rel_id, "")
                if target:
                    target_sheet = f"xl/{target.lstrip('/')}"
                    break

        if not target_sheet or target_sheet not in workbook_zip.namelist():
            return []

        sheet_root = ET.fromstring(workbook_zip.read(target_sheet))
        parsed_rows: list[list[str]] = []
        for row in sheet_root.findall("a:sheetData/a:row", XLSX_NS):
            values_by_col: dict[int, str] = {}
            for cell in row.findall("a:c", XLSX_NS):
                ref = cell.attrib.get("r", "")
                values_by_col[_excel_col_to_index(ref)] = _xlsx_cell_text(cell, shared_strings)
            if not values_by_col:
                continue
            max_col = max(values_by_col)
            parsed_rows.append([values_by_col.get(col, "").strip() for col in range(max_col + 1)])

        if len(parsed_rows) < 2:
            return []

        header_map = {
            _normalize_import_header(name): idx
            for idx, name in enumerate(parsed_rows[0])
            if str(name or "").strip()
        }

        required = ["titulo", "descripcion", "contacto", "telefono", "correo"]
        if any(col not in header_map for col in required):
            raise ValueError("El Excel debe incluir las columnas: Título, Descripción, Contacto, Teléfono y Correo.")

        rows: list[dict] = []
        for row in parsed_rows[1:]:
            titulo = row[header_map["titulo"]].strip() if len(row) > header_map["titulo"] else ""
            descripcion = row[header_map["descripcion"]].strip() if len(row) > header_map["descripcion"] else ""
            contacto = row[header_map["contacto"]].strip() if len(row) > header_map["contacto"] else ""
            telefono = row[header_map["telefono"]].strip() if len(row) > header_map["telefono"] else ""
            correo = row[header_map["correo"]].strip() if len(row) > header_map["correo"] else ""
            if not any([titulo, descripcion, contacto, telefono, correo]):
                continue
            rows.append({
                "titulo": titulo,
                "descripcion": descripcion,
                "contacto": contacto,
                "telefono": telefono,
                "correo": correo,
                "status": "PENDIENTE",
            })
        return rows


def _build_simple_xlsx(sheet_name: str, headers: list[str], rows: list[list[str]], column_widths: Optional[list[int]] = None) -> bytes:
    def cell_ref(row_idx: int, col_idx: int) -> str:
        label = ""
        num = col_idx
        while num > 0:
            num, rem = divmod(num - 1, 26)
            label = chr(65 + rem) + label
        return f"{label}{row_idx}"

    def inline_cell(row_idx: int, col_idx: int, value: object) -> str:
        text = escape("" if value is None else str(value))
        return (
            f'<c r="{cell_ref(row_idx, col_idx)}" t="inlineStr">'
            f"<is><t>{text}</t></is></c>"
        )

    cols_xml = ""
    if column_widths:
        cols_parts = []
        for idx, width in enumerate(column_widths, start=1):
            cols_parts.append(
                f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
            )
        cols_xml = f"<cols>{''.join(cols_parts)}</cols>"

    sheet_rows = []
    header_cells = "".join(inline_cell(1, idx, value) for idx, value in enumerate(headers, start=1))
    sheet_rows.append(f'<row r="1" s="1">{header_cells}</row>')

    for row_idx, row in enumerate(rows, start=2):
        cells = "".join(inline_cell(row_idx, col_idx, value) for col_idx, value in enumerate(row, start=1))
        sheet_rows.append(f'<row r="{row_idx}">{cells}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"{cols_xml}"
        f"<sheetData>{''.join(sheet_rows)}</sheetData>"
        "</worksheet>"
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )

    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )

    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2">'
        '<font><sz val="11"/><name val="Calibri"/></font>'
        '<font><b/><sz val="11"/><name val="Calibri"/></font>'
        '</fonts>'
        '<fills count="2">'
        '<fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        '</fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", root_rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("xl/styles.xml", styles_xml)
    return output.getvalue()


def _build_matrix_xlsx(sheet_name: str, rows: list[list[str]], column_widths: Optional[list[int]] = None) -> bytes:
    def cell_ref(row_idx: int, col_idx: int) -> str:
        label = ""
        num = col_idx
        while num > 0:
            num, rem = divmod(num - 1, 26)
            label = chr(65 + rem) + label
        return f"{label}{row_idx}"

    def inline_cell(row_idx: int, col_idx: int, value: object) -> str:
        text = escape("" if value is None else str(value))
        return f'<c r="{cell_ref(row_idx, col_idx)}" t="inlineStr"><is><t>{text}</t></is></c>'

    cols_xml = ""
    if column_widths:
        cols_xml = "<cols>" + "".join(
            f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
            for idx, width in enumerate(column_widths, start=1)
        ) + "</cols>"

    sheet_rows = []
    for row_idx, row in enumerate(rows, start=1):
        cells = "".join(inline_cell(row_idx, col_idx, value) for col_idx, value in enumerate(row, start=1))
        sheet_rows.append(f'<row r="{row_idx}">{cells}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"{cols_xml}<sheetData>{''.join(sheet_rows)}</sheetData></worksheet>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        "</Relationships>"
    )
    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", root_rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("xl/styles.xml", styles_xml)
    return output.getvalue()

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, abort, g, current_app
)

from sqlalchemy import text, or_, and_, case

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
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
except Exception:
    Workbook = None  # la app sigue arrancando aunque falte openpyxl
    get_column_letter = None

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
REGISTRO_MAIL_HOST = os.getenv("REGISTRO_MAIL_HOST", "servidor15.escala.net.mx").strip()
REGISTRO_MAIL_PORT = int(os.getenv("REGISTRO_MAIL_PORT", "26"))
REGISTRO_MAIL_USERNAME = os.getenv("REGISTRO_MAIL_USERNAME", "info@poliutech.com").strip()
REGISTRO_MAIL_PASSWORD = os.getenv("REGISTRO_MAIL_PASSWORD", "Info@2025?").strip()
REGISTRO_MAIL_FROM = os.getenv("REGISTRO_MAIL_FROM", REGISTRO_MAIL_USERNAME).strip()
REGISTRO_MAIL_ATTACHMENT = Path(__file__).resolve().parent / "presentacion2026OK.pdf"
FIREBASE_CREDENTIALS_FILE = os.getenv("FIREBASE_CREDENTIALS_FILE", "").strip()
FIREBASE_CREDENTIALS_JSON = os.getenv("FIREBASE_CREDENTIALS_JSON", "").strip()
PUSH_NOTIFICATIONS_ENABLED = os.getenv("PUSH_NOTIFICATIONS_ENABLED", "1").strip().lower() not in {"0", "false", "no"}

# Usa SIEMPRE los modelos desde models.py para evitar duplicados
from models import (
    db,
    Cliente,
    Concepto,
    Cotizacion,
    CotizacionDetalle,
    CotizacionSeguimiento,
    Usuario,
    MobileDevice,
    RegistroObra,
    Prospecto,
    ProspectoSeguimiento,
    ActivityLog,
    InventarioProducto,
    InventarioMovimiento,
)

# ---------------------------------------------------------
# Flask + DB + Login
# ---------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", DEFAULT_SECRET_KEY)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
logger = logging.getLogger(__name__)

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
    if request.path.startswith("/api/mobile/"):
        return
    if request.path.startswith("/cotizaciones/") and request.path.endswith("/export.pdf"):
        auth_header = (request.headers.get("Authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
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
            ("ciudad_trabajo", "ALTER TABLE cotizacion ADD COLUMN ciudad_trabajo VARCHAR(120)"),
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
        inv_cols = _table_columns("inventario_producto")
        for col, stmt in [
            ("stock_maximo", "ALTER TABLE inventario_producto ADD COLUMN stock_maximo FLOAT DEFAULT 0.0"),
        ]:
            if col not in inv_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("⚠️ ensure_schema(inventario_producto):", e)

    try:
        dcols = _table_columns("cotizacion_detalle")
        if "sistema" not in dcols:
            db.session.execute(text("ALTER TABLE cotizacion_detalle ADD COLUMN sistema VARCHAR(200)"))
        if "descripcion" not in dcols:
            db.session.execute(text("ALTER TABLE cotizacion_detalle ADD COLUMN descripcion VARCHAR(1000)"))
        for col, stmt in [
            ("capitulo", "ALTER TABLE cotizacion_detalle ADD COLUMN capitulo VARCHAR(120)"),
            ("origen", "ALTER TABLE cotizacion_detalle ADD COLUMN origen VARCHAR(50)"),
        ]:
            if col not in dcols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(detalle extras):", e)

    # --- PRECIOS UNITARIOS: columnas nativas ---
    try:
        pu_obra_cols = _table_columns("pu_obra")
        for col, stmt in [
            ("cliente", "ALTER TABLE pu_obra ADD COLUMN cliente VARCHAR(180)"),
            ("ubicacion", "ALTER TABLE pu_obra ADD COLUMN ubicacion VARCHAR(220)"),
            ("descripcion", "ALTER TABLE pu_obra ADD COLUMN descripcion TEXT"),
            ("moneda", "ALTER TABLE pu_obra ADD COLUMN moneda VARCHAR(20) DEFAULT 'MXN'"),
            ("creado_en", "ALTER TABLE pu_obra ADD COLUMN creado_en TIMESTAMP"),
            ("actualizado_en", "ALTER TABLE pu_obra ADD COLUMN actualizado_en TIMESTAMP"),
        ]:
            if col not in pu_obra_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(pu_obra):", e)

    try:
        pu_sob_cols = _table_columns("pu_sobrecosto")
        for col, stmt in [
            ("indirecto_campo_pct", "ALTER TABLE pu_sobrecosto ADD COLUMN indirecto_campo_pct FLOAT DEFAULT 0.0"),
            ("indirecto_oficina_pct", "ALTER TABLE pu_sobrecosto ADD COLUMN indirecto_oficina_pct FLOAT DEFAULT 0.0"),
            ("financiamiento_pct", "ALTER TABLE pu_sobrecosto ADD COLUMN financiamiento_pct FLOAT DEFAULT 0.0"),
            ("utilidad_pct", "ALTER TABLE pu_sobrecosto ADD COLUMN utilidad_pct FLOAT DEFAULT 10.0"),
            ("cargos_adicionales_pct", "ALTER TABLE pu_sobrecosto ADD COLUMN cargos_adicionales_pct FLOAT DEFAULT 0.0"),
            ("creado_en", "ALTER TABLE pu_sobrecosto ADD COLUMN creado_en TIMESTAMP"),
            ("actualizado_en", "ALTER TABLE pu_sobrecosto ADD COLUMN actualizado_en TIMESTAMP"),
        ]:
            if col not in pu_sob_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(pu_sobrecosto):", e)

    try:
        pu_recurso_cols = _table_columns("pu_recurso")
        for col, stmt in [
            ("tipo", "ALTER TABLE pu_recurso ADD COLUMN tipo VARCHAR(30) DEFAULT 'material'"),
            ("codigo", "ALTER TABLE pu_recurso ADD COLUMN codigo VARCHAR(60)"),
            ("descripcion", "ALTER TABLE pu_recurso ADD COLUMN descripcion VARCHAR(300)"),
            ("unidad", "ALTER TABLE pu_recurso ADD COLUMN unidad VARCHAR(50)"),
            ("costo_unitario", "ALTER TABLE pu_recurso ADD COLUMN costo_unitario FLOAT DEFAULT 0.0"),
            ("creado_en", "ALTER TABLE pu_recurso ADD COLUMN creado_en TIMESTAMP"),
            ("actualizado_en", "ALTER TABLE pu_recurso ADD COLUMN actualizado_en TIMESTAMP"),
        ]:
            if col not in pu_recurso_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(pu_recurso):", e)

    try:
        pu_partida_cols = _table_columns("pu_partida")
        for col, stmt in [
            ("capitulo", "ALTER TABLE pu_partida ADD COLUMN capitulo VARCHAR(160) DEFAULT 'General'"),
            ("clave", "ALTER TABLE pu_partida ADD COLUMN clave VARCHAR(80)"),
            ("descripcion", "ALTER TABLE pu_partida ADD COLUMN descripcion VARCHAR(600)"),
            ("unidad", "ALTER TABLE pu_partida ADD COLUMN unidad VARCHAR(50) DEFAULT 'pza'"),
            ("cantidad", "ALTER TABLE pu_partida ADD COLUMN cantidad FLOAT DEFAULT 1.0"),
            ("costo_directo", "ALTER TABLE pu_partida ADD COLUMN costo_directo FLOAT DEFAULT 0.0"),
            ("precio_unitario", "ALTER TABLE pu_partida ADD COLUMN precio_unitario FLOAT DEFAULT 0.0"),
            ("importe", "ALTER TABLE pu_partida ADD COLUMN importe FLOAT DEFAULT 0.0"),
            ("creado_en", "ALTER TABLE pu_partida ADD COLUMN creado_en TIMESTAMP"),
            ("actualizado_en", "ALTER TABLE pu_partida ADD COLUMN actualizado_en TIMESTAMP"),
        ]:
            if col not in pu_partida_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(pu_partida):", e)

    try:
        pu_insumo_cols = _table_columns("pu_partida_insumo")
        for col, stmt in [
            ("recurso_id", "ALTER TABLE pu_partida_insumo ADD COLUMN recurso_id INTEGER"),
            ("tipo", "ALTER TABLE pu_partida_insumo ADD COLUMN tipo VARCHAR(30) DEFAULT 'material'"),
            ("codigo", "ALTER TABLE pu_partida_insumo ADD COLUMN codigo VARCHAR(60)"),
            ("descripcion", "ALTER TABLE pu_partida_insumo ADD COLUMN descripcion VARCHAR(400)"),
            ("unidad", "ALTER TABLE pu_partida_insumo ADD COLUMN unidad VARCHAR(50)"),
            ("cantidad", "ALTER TABLE pu_partida_insumo ADD COLUMN cantidad FLOAT DEFAULT 0.0"),
            ("costo_unitario", "ALTER TABLE pu_partida_insumo ADD COLUMN costo_unitario FLOAT DEFAULT 0.0"),
            ("importe", "ALTER TABLE pu_partida_insumo ADD COLUMN importe FLOAT DEFAULT 0.0"),
            ("creado_en", "ALTER TABLE pu_partida_insumo ADD COLUMN creado_en TIMESTAMP"),
            ("actualizado_en", "ALTER TABLE pu_partida_insumo ADD COLUMN actualizado_en TIMESTAMP"),
        ]:
            if col not in pu_insumo_cols:
                db.session.execute(text(stmt))
        db.session.commit()
    except Exception as e:
        print("[WARN] ensure_schema(pu_partida_insumo):", e)

    _migrate_registro_obras_from_json()

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

def require_followup_author_or_admin(seg: CotizacionSeguimiento) -> None:
    if is_admin():
        return
    current_user_id = getattr(current_user, "id", None)
    if current_user_id and seg.usuario_id == current_user_id:
        return
    abort(403)

def require_cliente_owner_or_admin(cli: Cliente) -> None:
    if is_admin():
        return
    ra = responsable_actual()
    if not ra or (cli.responsable or "") != ra:
        abort(403)


def require_prospecto_owner_or_admin(prospecto: Prospecto) -> None:
    if is_admin():
        return
    ra = responsable_actual()
    if not ra or (prospecto.responsable or "") != ra:
        abort(403)


def require_prospecto_followup_author_or_admin(seg: ProspectoSeguimiento) -> None:
    if is_admin():
        return
    current_user_id = getattr(current_user, "id", None)
    if current_user_id and seg.usuario_id == current_user_id:
        return
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
    _send_quote_created_notification(cot)
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
    page = request.args.get("page", 1, type=int)
    per_page = 20

    # ADMIN: ve todo
    # USER: ve SOLO lo suyo por responsable
    if is_admin():
        quotes_query = Cotizacion.query.order_by(Cotizacion.fecha.desc())
        total_cotizaciones = quotes_query.count()
        total_importe = db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0
    else:
        ra = responsable_actual()
        quotes_query = (
            Cotizacion.query
            .filter_by(responsable=ra)
            .order_by(Cotizacion.fecha.desc())
        )
        total_cotizaciones = quotes_query.count()
        total_importe = (db.session.query(db.func.coalesce(db.func.sum(Cotizacion.total), 0))
                         .filter(Cotizacion.responsable == ra).scalar() or 0)

    pagination = quotes_query.paginate(page=page, per_page=per_page, error_out=False)
    cotizaciones = pagination.items

    total_catalogo = Concepto.query.count()

    return render_template(
        "dashboard.html",
        title="Sistema MAR",
        total_cotizaciones=total_cotizaciones,
        total_importe=float(total_importe),
        total_catalogo=total_catalogo,
        cotizaciones=cotizaciones,
        pagination=pagination,
        valid_estatus=VALID_ESTATUS,
        show_splash=True
    )

@app.route("/cotizador")
@login_required
def cotizador():
    return render_template("cotizador.html", title="Nuevo - Sistema MAR")


@app.route("/altas", methods=["GET", "POST"])
@login_required
def altas_proveedores():
    if not is_admin():
        abort(403)

    rows = _load_provider_numbers()

    if request.method == "POST":
        numeros = request.form.getlist("numero[]")
        empresas = request.form.getlist("empresa[]")
        razones = request.form.getlist("razon_social_poliutech[]")
        contactos = request.form.getlist("contacto[]")
        telefonos = request.form.getlist("telefono[]")
        correos = request.form.getlist("correo[]")

        total_rows = max(len(numeros), len(empresas), len(razones), len(contactos), len(telefonos), len(correos), 0)
        rows: list[dict] = []
        for idx in range(total_rows):
            numero = (numeros[idx] if idx < len(numeros) else "").strip()
            empresa = (empresas[idx] if idx < len(empresas) else "").strip()
            razon_social = (razones[idx] if idx < len(razones) else "").strip()
            contacto = (contactos[idx] if idx < len(contactos) else "").strip()
            telefono = (telefonos[idx] if idx < len(telefonos) else "").strip()
            correo = (correos[idx] if idx < len(correos) else "").strip()

            if not any([numero, empresa, razon_social, contacto, telefono, correo]):
                continue

            if correo and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", correo):
                flash(f"El correo '{correo}' no es valido.", "danger")
                return render_template(
                    "altas.html",
                    title="Altas de proveedores",
                    rows=[
                        _normalize_provider_row({
                            "numero": (numeros[pos] if pos < len(numeros) else "").strip(),
                            "empresa": (empresas[pos] if pos < len(empresas) else "").strip(),
                            "razon_social_poliutech": (razones[pos] if pos < len(razones) else "").strip(),
                            "contacto": (contactos[pos] if pos < len(contactos) else "").strip(),
                            "telefono": (telefonos[pos] if pos < len(telefonos) else "").strip(),
                            "correo": (correos[pos] if pos < len(correos) else "").strip(),
                        }, pos + 1)
                        for pos in range(total_rows)
                    ],
                    filtered_rows=_filter_provider_rows(
                        [
                            _normalize_provider_row({
                                "numero": (numeros[pos] if pos < len(numeros) else "").strip(),
                                "empresa": (empresas[pos] if pos < len(empresas) else "").strip(),
                                "razon_social_poliutech": (razones[pos] if pos < len(razones) else "").strip(),
                                "contacto": (contactos[pos] if pos < len(contactos) else "").strip(),
                                "telefono": (telefonos[pos] if pos < len(telefonos) else "").strip(),
                                "correo": (correos[pos] if pos < len(correos) else "").strip(),
                            }, pos + 1)
                            for pos in range(total_rows)
                        ],
                        _provider_filters_from_request(),
                    ),
                    filters=_provider_filters_from_request(),
                )

            rows.append({
                "id": len(rows) + 1,
                "numero": numero,
                "empresa": empresa,
                "razon_social_poliutech": razon_social,
                "contacto": contacto,
                "telefono": telefono,
                "correo": correo,
            })

        _save_provider_numbers(rows)
        flash("Altas actualizadas correctamente.", "success")
        return redirect(url_for("altas_proveedores"))

    filters = _provider_filters_from_request()
    return render_template(
        "altas.html",
        title="Altas de proveedores",
        rows=rows,
        filtered_rows=_filter_provider_rows(rows, filters),
        filters=filters,
    )


@app.route("/altas/export.xlsx")
@login_required
def export_altas_proveedores_xlsx():
    if not is_admin():
        abort(403)

    filters = _provider_filters_from_request()
    rows = _filter_provider_rows(_load_provider_numbers(), filters)

    headers = [
        "NUMERO",
        "EMPRESA",
        "RAZON SOCIAL POLIUTECH",
        "CONTACTO",
        "TELEFONO",
        "CORREO",
    ]
    body_rows = []
    for row in rows:
        body_rows.append([
            row.get("numero", ""),
            row.get("empresa", ""),
            row.get("razon_social_poliutech", ""),
            row.get("contacto", ""),
            row.get("telefono", ""),
            row.get("correo", ""),
        ])

    output_bytes = _build_simple_xlsx(
        "Altas",
        headers,
        body_rows,
        column_widths=[18, 28, 28, 24, 18, 32],
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        output_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="altas_proveedores_{stamp}.xlsx"'},
    )


@app.route("/altas/export.pdf")
@login_required
def export_altas_proveedores_pdf():
    if not is_admin():
        abort(403)

    filters = _provider_filters_from_request()
    rows = _filter_provider_rows(_load_provider_numbers(), filters)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=24 * mm,
        bottomMargin=38 * mm,
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="EncabezadoAltas", fontName="Helvetica", fontSize=9, leading=12, spaceAfter=4, splitLongWords=False))
    styles.add(ParagraphStyle(name="AltasCell", fontName="Helvetica", fontSize=7.5, leading=9, splitLongWords=False))
    styles.add(ParagraphStyle(name="AltasCenter", fontName="Helvetica", fontSize=7.5, leading=9, alignment=1, splitLongWords=False))

    elems = []

    def encabezado(canv, doc_):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1] - 40, A4[0], 40, stroke=0, fill=1)

        logo_path = os.path.join(app.static_folder or "static", "logo.png")
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 22.5 * mm
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
        canv.drawRightString(A4[0] - 12, A4[1] - 18, "ALTAS DE PROVEEDORES")
        canv.setFont("Helvetica", 10)
        canv.drawRightString(A4[0] - 12, A4[1] - 31, "Recubrimientos Especializados")
        canv.restoreState()

    def footer(canv, doc_):
        canv.saveState()
        division_path = os.path.join(app.static_folder or "static", "division.png")
        if os.path.exists(division_path):
            try:
                canv.drawImage(division_path, (A4[0] - 155 * mm) / 2, 45, width=155 * mm, height=3 * mm, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 9)
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.drawCentredString(A4[0] / 2, 35, "POLIUTECH - Recubrimientos Especializados")

        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#333333"))
        canv.drawCentredString(A4[0] / 2, 25, "Campos Eliseos 223 Oficina 602 - Col. Polanco V Seccion - Miguel Hidalgo, CDMX 11560")
        canv.drawCentredString(A4[0] / 2, 15, "Tel: 55 5938 6530 / 55 5938 0536 - info@poliutech.com - www.poliutech.com")

        try:
            canv.setTitle("Altas de proveedores")
        except Exception:
            pass

        canv.restoreState()

    filtro_razon = (filters.get("razon_social_poliutech") or "").strip()
    generated_at = now_cdmx_naive().strftime("%d/%m/%Y %H:%M")
    meta_data = [
        [
            Paragraph(f"<b>Fecha de exportación:</b> {generated_at}", styles["EncabezadoAltas"]),
            Paragraph(f"<b>Total de registros:</b> {len(rows)}", styles["EncabezadoAltas"]),
        ],
        [
            Paragraph("<b>Filtro aplicado:</b> Razón social Poliutech", styles["EncabezadoAltas"]),
            Paragraph(filtro_razon or "Todos", styles["EncabezadoAltas"]),
        ],
    ]
    meta_tbl = Table(meta_data, colWidths=[95 * mm, 95 * mm], hAlign="LEFT")
    meta_tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    elems.append(meta_tbl)
    elems.append(Spacer(1, 6))

    data = [[
        "NUMERO",
        "EMPRESA",
        "RAZON SOCIAL POLIUTECH",
        "CONTACTO",
        "TELEFONO",
        "CORREO",
    ]]
    for row in rows:
        data.append([
            Paragraph(_truncate_pdf_text(row.get("numero", ""), 24), styles["AltasCenter"]),
            Paragraph(_truncate_pdf_text(row.get("empresa", ""), 48), styles["AltasCell"]),
            Paragraph(_truncate_pdf_text(row.get("razon_social_poliutech", ""), 52), styles["AltasCell"]),
            Paragraph(_truncate_pdf_text(row.get("contacto", ""), 38), styles["AltasCell"]),
            Paragraph(_truncate_pdf_text(row.get("telefono", ""), 24), styles["AltasCenter"]),
            Paragraph(_truncate_pdf_text(row.get("correo", ""), 42), styles["AltasCell"]),
        ])

    if len(data) == 1:
        data.append([
            Paragraph("-", styles["AltasCenter"]),
            Paragraph("No hay registros para exportar con el filtro actual.", styles["AltasCell"]),
            Paragraph("-", styles["AltasCenter"]),
            Paragraph("-", styles["AltasCenter"]),
            Paragraph("-", styles["AltasCenter"]),
            Paragraph("-", styles["AltasCenter"]),
        ])

    tbl = Table(
        data,
        colWidths=[16 * mm, 38 * mm, 48 * mm, 28 * mm, 22 * mm, 38 * mm],
        repeatRows=1,
        hAlign="CENTER",
    )
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d47a1")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (4, 0), (4, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("WORDWRAP", (0, 0), (-1, -1), True),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    elems.append(tbl)

    doc.build(
        elems,
        onFirstPage=lambda canv, d: (draw_watermark(canv, app), encabezado(canv, d), footer(canv, d)),
        onLaterPages=lambda canv, d: (draw_watermark(canv, app), encabezado(canv, d), footer(canv, d)),
    )

    buf.seek(0)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    response = Response(
        buf.getvalue(),
        mimetype="application/pdf",
        headers={"Content-Disposition": f'inline; filename="altas_proveedores_{stamp}.pdf"'},
    )
    response.direct_passthrough = False
    return response


@app.route("/prospectos", methods=["GET", "POST"])
@login_required
def prospectos():
    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "add":
            titulo = (request.form.get("titulo") or "").strip()
            descripcion = (request.form.get("descripcion") or "").strip()
            contacto = (request.form.get("contacto") or "").strip()
            telefono = (request.form.get("telefono") or "").strip()
            correo = (request.form.get("correo") or "").strip()
            status = _normalize_prospecto_status(request.form.get("status"))
            responsable = (request.form.get("responsable") or "").strip() if is_admin() else (responsable_actual() or "").strip()

            if not titulo:
                flash("Captura el título del prospecto.", "warning")
                return redirect(url_for("prospectos"))
            if correo and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", correo):
                flash(f"El correo '{correo}' no es valido.", "danger")
                return redirect(url_for("prospectos"))

            prospecto = Prospecto(
                titulo=titulo,
                descripcion=descripcion or None,
                contacto=contacto or None,
                telefono=telefono or None,
                correo=correo or None,
                status=status,
                responsable=responsable or None,
            )
            db.session.add(prospecto)
            db.session.commit()
            flash("Prospecto agregado correctamente.", "success")
            return redirect(url_for("prospectos"))

        if action == "import":
            uploaded = request.files.get("import_file")
            if not uploaded or not (uploaded.filename or "").strip():
                flash("Selecciona un archivo Excel antes de importar.", "warning")
                return redirect(url_for("prospectos"))

            filename = (uploaded.filename or "").strip().lower()
            if not filename.endswith(".xlsx"):
                flash("Solo se permite importar archivos .xlsx.", "danger")
                return redirect(url_for("prospectos"))

            try:
                file_bytes = uploaded.read()
                if not file_bytes:
                    raise ValueError("El archivo Excel llegó vacío.")
                imported_rows = _load_prospectos_from_xlsx(file_bytes)
            except Exception as exc:
                flash(f"No pude leer el Excel: {exc}", "danger")
                return redirect(url_for("prospectos"))

            if not imported_rows:
                flash("No se encontraron prospectos válidos en el Excel.", "warning")
                return redirect(url_for("prospectos"))

            inserted = 0
            updated = 0
            for row in imported_rows:
                titulo = (row.get("titulo") or "").strip()
                descripcion = (row.get("descripcion") or "").strip()
                contacto = (row.get("contacto") or "").strip()
                telefono = (row.get("telefono") or "").strip()
                correo = (row.get("correo") or "").strip()

                if correo and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", correo):
                    flash(f"El correo '{correo}' no es valido.", "danger")
                    return redirect(url_for("prospectos"))

                existing = Prospecto.query.filter(
                    db.func.lower(Prospecto.titulo) == titulo.lower(),
                    db.func.lower(db.func.coalesce(Prospecto.correo, "")) == correo.lower(),
                ).first()

                if existing:
                    existing.descripcion = descripcion or existing.descripcion
                    existing.contacto = contacto or existing.contacto
                    existing.telefono = telefono or existing.telefono
                    existing.correo = correo or existing.correo
                    if not (existing.status or "").strip():
                        existing.status = "PENDIENTE"
                    updated += 1
                    continue

                db.session.add(Prospecto(
                    titulo=titulo,
                    descripcion=descripcion or None,
                    contacto=contacto or None,
                    telefono=telefono or None,
                    correo=correo or None,
                    status="PENDIENTE",
                ))
                inserted += 1

            db.session.commit()
            flash(f"Importación completada. Nuevos: {inserted}. Actualizados: {updated}.", "success")
            return redirect(url_for("prospectos"))

        if action == "update":
            row_ids = request.form.getlist("row_id[]")
            titulos = request.form.getlist("titulo[]")
            descripciones = request.form.getlist("descripcion[]")
            contactos = request.form.getlist("contacto[]")
            telefonos = request.form.getlist("telefono[]")
            correos = request.form.getlist("correo[]")
            statuses = request.form.getlist("status[]")

            updated = 0
            for idx, row_id in enumerate(row_ids):
                if not str(row_id).strip().isdigit():
                    continue
                prospecto = db.session.get(Prospecto, int(row_id))
                if not prospecto:
                    continue
                require_prospecto_owner_or_admin(prospecto)

                titulo = (titulos[idx] if idx < len(titulos) else "").strip()
                descripcion = (descripciones[idx] if idx < len(descripciones) else "").strip()
                contacto = (contactos[idx] if idx < len(contactos) else "").strip()
                telefono = (telefonos[idx] if idx < len(telefonos) else "").strip()
                correo = (correos[idx] if idx < len(correos) else "").strip()
                status = _normalize_prospecto_status(statuses[idx] if idx < len(statuses) else "")

                if not titulo:
                    flash("Todos los prospectos deben tener título.", "warning")
                    return redirect(url_for("prospectos"))
                if correo and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", correo):
                    flash(f"El correo '{correo}' no es valido.", "danger")
                    return redirect(url_for("prospectos"))

                prospecto.titulo = titulo
                prospecto.descripcion = descripcion or None
                prospecto.contacto = contacto or None
                prospecto.telefono = telefono or None
                prospecto.correo = correo or None
                prospecto.status = status
                if not prospecto.responsable:
                    prospecto.responsable = responsable_actual() or prospecto.responsable
                updated += 1

            db.session.commit()
            flash(f"Se actualizaron {updated} prospecto(s).", "success")
            return redirect(url_for("prospectos"))

        if action == "delete":
            selected_ids = [int(value) for value in request.form.getlist("selected_ids[]") if str(value).strip().isdigit()]
            if not selected_ids:
                flash("Selecciona al menos un prospecto para eliminar.", "warning")
                return redirect(url_for("prospectos"))

            items = Prospecto.query.filter(Prospecto.id.in_(selected_ids)).all()
            deleted = 0
            for prospecto in items:
                require_prospecto_owner_or_admin(prospecto)
                db.session.delete(prospecto)
                deleted += 1
            db.session.commit()
            flash(f"Se eliminaron {deleted} prospecto(s).", "success")
            return redirect(url_for("prospectos"))

        flash("Acción no válida para prospectos.", "danger")
        return redirect(url_for("prospectos"))

    rows = _load_prospectos()
    filters = _prospectos_filters_from_request()
    filtered_rows = _filter_prospectos(rows, filters)
    return render_template(
        "prospectos.html",
        title="Prospectos",
        rows=rows,
        filtered_rows=filtered_rows,
        filters=filters,
        status_options=PROSPECT_STATUS_OPTIONS,
        default_responsable=responsable_actual() or "",
    )


@app.route("/prospectos/export.xls")
@login_required
def export_prospectos_xls():
    rows = _filter_prospectos(_load_prospectos(), _prospectos_filters_from_request())
    headers = ["TITULO", "DESCRIPCION", "CONTACTO", "TELEFONO", "CORREO", "STATUS"]
    body_rows = [
        [
            row.get("titulo", ""),
            row.get("descripcion", ""),
            row.get("contacto", ""),
            row.get("telefono", ""),
            row.get("correo", ""),
            row.get("status", ""),
        ]
        for row in rows
    ]
    output_bytes = _build_simple_xls("Prospectos", headers, body_rows)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        output_bytes,
        mimetype="application/vnd.ms-excel",
        headers={"Content-Disposition": f'attachment; filename="prospectos_{stamp}.xls"'},
    )


@app.route("/prospectos/<int:prospecto_id>/seguimiento")
@login_required
def prospecto_seguimiento(prospecto_id: int):
    prospecto = Prospecto.query.get_or_404(prospecto_id)
    return render_template(
        "prospecto_seguimiento.html",
        prospecto=prospecto,
        seguimientos=prospecto.seguimientos,
        title=f"Seguimiento prospecto {prospecto.titulo}",
    )


@app.route("/prospectos/<int:prospecto_id>/seguimiento", methods=["POST"])
@login_required
def crear_prospecto_seguimiento(prospecto_id: int):
    prospecto = Prospecto.query.get_or_404(prospecto_id)
    comentario = (request.form.get("comentario") or "").strip()
    nuevo_status = _normalize_prospecto_status(request.form.get("status"))

    if not comentario:
        flash("Escribe un comentario de seguimiento.", "warning")
        return redirect(url_for("prospecto_seguimiento", prospecto_id=prospecto.id))

    prospecto.status = nuevo_status
    seg = ProspectoSeguimiento(
        prospecto_id=prospecto.id,
        usuario_id=getattr(current_user, "id", None),
        autor=(getattr(current_user, "nombre", None) or responsable_actual() or "Sistema").strip(),
        comentario=comentario,
        fecha_seguimiento=now_cdmx_naive(),
    )
    db.session.add(seg)
    db.session.commit()
    flash("Seguimiento guardado correctamente.", "success")
    return redirect(url_for("prospecto_seguimiento", prospecto_id=prospecto.id, _anchor=f"seguimiento-{seg.id}"))


@app.route("/prospectos/<int:prospecto_id>/seguimiento/<int:seg_id>/editar", methods=["POST"])
@login_required
def editar_prospecto_seguimiento(prospecto_id: int, seg_id: int):
    prospecto = Prospecto.query.get_or_404(prospecto_id)
    seg = ProspectoSeguimiento.query.filter_by(id=seg_id, prospecto_id=prospecto.id).first_or_404()
    require_prospecto_followup_author_or_admin(seg)

    comentario = (request.form.get("comentario") or "").strip()
    if not comentario:
        flash("El comentario no puede quedar vacío.", "warning")
        return redirect(url_for("prospecto_seguimiento", prospecto_id=prospecto.id))

    seg.comentario = comentario
    seg.actualizado_en = now_cdmx_naive()
    db.session.commit()
    flash("Seguimiento actualizado.", "success")
    return redirect(url_for("prospecto_seguimiento", prospecto_id=prospecto.id, _anchor=f"seguimiento-{seg.id}"))


@app.route("/prospectos/<int:prospecto_id>/seguimiento/<int:seg_id>/eliminar", methods=["POST"])
@login_required
def eliminar_prospecto_seguimiento(prospecto_id: int, seg_id: int):
    prospecto = Prospecto.query.get_or_404(prospecto_id)
    seg = ProspectoSeguimiento.query.filter_by(id=seg_id, prospecto_id=prospecto.id).first_or_404()
    require_prospecto_followup_author_or_admin(seg)
    db.session.delete(seg)
    db.session.commit()
    flash("Seguimiento eliminado.", "success")
    return redirect(url_for("prospecto_seguimiento", prospecto_id=prospecto.id))


@app.route("/prospectos/export.pdf")
@login_required
def export_prospectos_pdf():
    filters = _prospectos_filters_from_request()
    rows = _filter_prospectos(_load_prospectos(), filters)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=24 * mm,
        bottomMargin=38 * mm,
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="EncabezadoProspectos", fontName="Helvetica", fontSize=9, leading=12, spaceAfter=4, splitLongWords=False))
    styles.add(ParagraphStyle(name="ProspectosCell", fontName="Helvetica", fontSize=7.2, leading=8.5, splitLongWords=False))
    styles.add(ParagraphStyle(name="ProspectosCenter", fontName="Helvetica", fontSize=7.2, leading=8.5, alignment=1, splitLongWords=False))

    elems = []

    def encabezado(canv, doc_):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1] - 40, A4[0], 40, stroke=0, fill=1)

        logo_path = os.path.join(app.static_folder or "static", "logo.png")
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 22.5 * mm
                scale = max_w / iw
                canv.drawImage(img, 12, A4[1] - (ih * scale) - 8, width=max_w, height=ih * scale, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 14)
        canv.setFillColor(colors.white)
        canv.drawRightString(A4[0] - 12, A4[1] - 18, "PROSPECTOS")
        canv.setFont("Helvetica", 10)
        canv.drawRightString(A4[0] - 12, A4[1] - 31, "Recubrimientos Especializados")
        canv.restoreState()

    def footer(canv, doc_):
        canv.saveState()
        division_path = os.path.join(app.static_folder or "static", "division.png")
        if os.path.exists(division_path):
            try:
                canv.drawImage(division_path, (A4[0] - 155 * mm) / 2, 45, width=155 * mm, height=3 * mm, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 9)
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.drawCentredString(A4[0] / 2, 35, "POLIUTECH - Recubrimientos Especializados")
        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#333333"))
        canv.drawCentredString(A4[0] / 2, 25, "Campos Eliseos 223 Oficina 602 - Col. Polanco V Seccion - Miguel Hidalgo, CDMX 11560")
        canv.drawCentredString(A4[0] / 2, 15, "Tel: 55 5938 6530 / 55 5938 0536 - info@poliutech.com - www.poliutech.com")
        canv.restoreState()

    generated_at = now_cdmx_naive().strftime("%d/%m/%Y %H:%M")
    meta_data = [
        [
            Paragraph(f"<b>Fecha de exportación:</b> {generated_at}", styles["EncabezadoProspectos"]),
            Paragraph(f"<b>Total de registros:</b> {len(rows)}", styles["EncabezadoProspectos"]),
        ],
        [
            Paragraph("<b>Filtro título/contacto:</b>", styles["EncabezadoProspectos"]),
            Paragraph((filters.get("titulo") or filters.get("contacto") or "Todos").upper() if (filters.get("titulo") or filters.get("contacto")) else "Todos", styles["EncabezadoProspectos"]),
        ],
        [
            Paragraph("<b>Filtro status:</b>", styles["EncabezadoProspectos"]),
            Paragraph(filters.get("status") or "Todos", styles["EncabezadoProspectos"]),
        ],
    ]
    meta_tbl = Table(meta_data, colWidths=[95 * mm, 95 * mm], hAlign="LEFT")
    meta_tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    elems.append(meta_tbl)
    elems.append(Spacer(1, 6))

    data = [[
        "TITULO",
        "DESCRIPCION",
        "CONTACTO",
        "TELEFONO",
        "CORREO",
        "STATUS",
    ]]
    for row in rows:
        data.append([
            Paragraph(_truncate_pdf_text(row.get("titulo", ""), 38), styles["ProspectosCell"]),
            Paragraph(_truncate_pdf_text(row.get("descripcion", ""), 78), styles["ProspectosCell"]),
            Paragraph(_truncate_pdf_text(row.get("contacto", ""), 30), styles["ProspectosCell"]),
            Paragraph(_truncate_pdf_text(row.get("telefono", ""), 22), styles["ProspectosCenter"]),
            Paragraph(_truncate_pdf_text(row.get("correo", ""), 34), styles["ProspectosCell"]),
            Paragraph(_truncate_pdf_text(row.get("status", ""), 18), styles["ProspectosCenter"]),
        ])

    if len(data) == 1:
        data.append([
            Paragraph("-", styles["ProspectosCenter"]),
            Paragraph("No hay registros para exportar con el filtro actual.", styles["ProspectosCell"]),
            Paragraph("-", styles["ProspectosCenter"]),
            Paragraph("-", styles["ProspectosCenter"]),
            Paragraph("-", styles["ProspectosCenter"]),
            Paragraph("-", styles["ProspectosCenter"]),
        ])

    tbl = Table(
        data,
        colWidths=[28 * mm, 64 * mm, 26 * mm, 22 * mm, 34 * mm, 20 * mm],
        repeatRows=1,
        hAlign="CENTER",
    )
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d47a1")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (3, 0), (3, -1), "CENTER"),
        ("ALIGN", (5, 0), (5, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 7.2),
        ("WORDWRAP", (0, 0), (-1, -1), True),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    elems.append(tbl)

    doc.build(
        elems,
        onFirstPage=lambda canv, d: (draw_watermark(canv, app), encabezado(canv, d), footer(canv, d)),
        onLaterPages=lambda canv, d: (draw_watermark(canv, app), encabezado(canv, d), footer(canv, d)),
    )

    buf.seek(0)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    response = Response(
        buf.getvalue(),
        mimetype="application/pdf",
        headers={"Content-Disposition": f'inline; filename="prospectos_{stamp}.pdf"'},
    )
    response.direct_passthrough = False
    return response


@app.route("/registro-obras", methods=["GET", "POST"])
@login_required
def registro_obras():
    rows = _load_registro_obras()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()
        pending_email_rows: list[dict] = []
        if action == "add":
            send_email = (request.form.get("send_email") or "").strip().lower() in {"1", "true", "on", "yes"}
            row = _normalize_registro_obra_row({
                "numero": "",
                "obra": request.form.get("obra"),
                "ubicacion": request.form.get("ubicacion"),
                "encargado": request.form.get("encargado"),
                "puesto": request.form.get("puesto"),
                "telefono": request.form.get("telefono"),
                "correo": request.form.get("correo"),
                "responsable": request.form.get("responsable"),
            }, len(rows) + 1)
            if not any([row["obra"], row["ubicacion"], row["encargado"], row["puesto"], row["telefono"], row["correo"], row["responsable"]]):
                flash("Captura al menos un dato antes de agregar el registro.", "warning")
                return redirect(url_for("registro_obras"))
            if send_email and not row["correo"]:
                flash("Debes capturar un correo si activas ENVIAR CORREO.", "danger")
                return redirect(url_for("registro_obras"))
            if row["correo"] and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", row["correo"]):
                flash(f"El correo '{row['correo']}' no es valido.", "danger")
                return redirect(url_for("registro_obras"))
            if not is_admin():
                row["responsable"] = responsable_actual() or row["responsable"]
            row["numero"] = str(len(rows) + 1)
            rows.append(row)
            if send_email:
                pending_email_rows.append(row)
            if send_email:
                flash("Registro agregado.", "success")
            else:
                flash("Registro agregado.", "success")
        elif action == "update":
            row_ids = request.form.getlist("row_id[]")
            obras = request.form.getlist("obra[]")
            ubicaciones = request.form.getlist("ubicacion[]")
            encargados = request.form.getlist("encargado[]")
            puestos = request.form.getlist("puesto[]")
            telefonos = request.form.getlist("telefono[]")
            correos = request.form.getlist("correo[]")
            responsables = request.form.getlist("responsable[]")
            updated_rows = []
            for idx, row_id in enumerate(row_ids):
                numeric_row_id = int(row_id or idx + 1)
                row = _normalize_registro_obra_row({
                    "numero": "",
                    "obra": obras[idx] if idx < len(obras) else "",
                    "ubicacion": ubicaciones[idx] if idx < len(ubicaciones) else "",
                    "encargado": encargados[idx] if idx < len(encargados) else "",
                    "puesto": puestos[idx] if idx < len(puestos) else "",
                    "telefono": telefonos[idx] if idx < len(telefonos) else "",
                    "correo": correos[idx] if idx < len(correos) else "",
                    "responsable": responsables[idx] if idx < len(responsables) else "",
                }, numeric_row_id)
                if not any([row["obra"], row["ubicacion"], row["encargado"], row["puesto"], row["telefono"], row["correo"], row["responsable"]]):
                    continue
                if row["correo"] and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", row["correo"]):
                    flash(f"El correo '{row['correo']}' no es valido.", "danger")
                    return redirect(url_for("registro_obras"))
                if not is_admin():
                    row["responsable"] = responsable_actual() or row["responsable"]
                updated_rows.append(row)
            rows = updated_rows
            flash("Registros actualizados.", "success")
        elif action == "import":
            uploaded = request.files.get("import_file")
            if not uploaded or not (uploaded.filename or "").strip():
                flash("Selecciona un archivo Excel para importar.", "warning")
                return redirect(url_for("registro_obras"))

            import_responsable = (request.form.get("import_responsable") or "").strip()
            if not is_admin():
                import_responsable = responsable_actual() or import_responsable

            try:
                imported_rows = _load_registro_obras_from_xlsx(uploaded.read(), default_responsable=import_responsable)
            except Exception:
                imported_rows = []

            if not imported_rows:
                flash("No se encontraron registros válidos en el Excel.", "warning")
                return redirect(url_for("registro_obras"))

            existing_keys = {_registro_obra_duplicate_key(row) for row in rows}
            accepted_rows = []
            skipped_duplicates = 0
            for imported in imported_rows:
                if imported["correo"] and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", imported["correo"]):
                    flash(f"El correo '{imported['correo']}' no es valido en el archivo importado.", "danger")
                    return redirect(url_for("registro_obras"))
                if not is_admin():
                    imported["responsable"] = responsable_actual() or imported["responsable"]
                duplicate_key = _registro_obra_duplicate_key(imported)
                if duplicate_key in existing_keys:
                    skipped_duplicates += 1
                    continue
                existing_keys.add(duplicate_key)
                accepted_rows.append(imported)
                rows.append(imported)

            if not accepted_rows:
                flash("No se importaron registros nuevos; todos ya existían.", "warning")
                return redirect(url_for("registro_obras"))

            message = f"Se importaron {len(accepted_rows)} registros desde Excel."
            if skipped_duplicates:
                message += f" Se omitieron {skipped_duplicates} duplicados."
            flash(message, "success")
        elif action == "delete":
            selected_ids = {int(value) for value in request.form.getlist("selected_ids[]") if str(value).strip().isdigit()}
            rows = [row for row in rows if int(row.get("id", 0) or 0) not in selected_ids]
            flash("Registros eliminados.", "success")

        for idx, row in enumerate(rows, start=1):
            row["id"] = idx
            row["numero"] = str(idx)
        _save_registro_obras(rows)
        for row in rows:
            _sync_cliente_from_registro_obra(row)
        db.session.commit()
        if pending_email_rows:
            email_errors = []
            email_sent = 0
            for row in pending_email_rows:
                try:
                    _send_registro_obra_email(row)
                    email_sent += 1
                except Exception as e:
                    email_errors.append(f"{row.get('obra') or row.get('encargado') or row.get('correo')}: {e}")
            if email_sent:
                flash("Envío de correo exitoso.", "success")
            if email_errors:
                flash("No se pudo enviar: " + " | ".join(email_errors), "warning")
        return redirect(url_for("registro_obras"))

    filters = _registro_obras_filters_from_request()
    filtered_rows = _filter_registro_obras(rows, filters)
    return render_template(
        "registro_obras.html",
        title="Registro de obras",
        rows=rows,
        filtered_rows=filtered_rows,
        filters=filters,
        default_responsable=responsable_actual() or "",
        default_numero=str(len(rows) + 1),
    )


@app.route("/registro-obras/export.xlsx")
@login_required
def export_registro_obras_xlsx():
    filters = _registro_obras_filters_from_request()
    rows = _filter_registro_obras(_load_registro_obras(), filters)
    body_rows = [
        ["", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", ""],
        ["N°", "OBRA", "UBICACIÓN", "ENCARGADO", "PUESTO", "TELEFONO", "CORREO", "RESPONSABLE"],
    ]
    for row in rows:
        body_rows.append([
            row.get("numero", ""),
            row.get("obra", ""),
            row.get("ubicacion", ""),
            row.get("encargado", ""),
            row.get("puesto", ""),
            row.get("telefono", ""),
            row.get("correo", ""),
            row.get("responsable", ""),
        ])

    output_bytes = _build_matrix_xlsx("Registro Obras", body_rows, column_widths=[10, 34, 28, 24, 20, 18, 32, 18])
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        output_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="registro_obras_{stamp}.xlsx"'},
    )


@app.route("/api/mobile/login", methods=["POST"])
def api_mobile_login():
    payload = request.get_json(silent=True) or {}
    nombre = (payload.get("nombre") or payload.get("usuario") or "").strip()
    password = str(payload.get("password") or "").strip()
    if not nombre or not password:
        return _mobile_json_error("Faltan credenciales.", 400)

    user = Usuario.query.filter(db.func.lower(Usuario.nombre) == nombre.lower()).first()
    if not user or not user.check_password(password):
        return _mobile_json_error("Credenciales incorrectas.", 401)

    return jsonify({
        "ok": True,
        "token": _issue_mobile_token(user),
        "user": {
            "id": user.id,
            "nombre": user.nombre,
            "rol": user.rol,
            "responsable": _mobile_user_responsable(user),
        },
    })


@app.route("/api/mobile/push-token", methods=["POST"])
@require_mobile_auth
def api_mobile_push_token_register():
    payload = request.get_json(silent=True) or {}
    token = (payload.get("token") or "").strip()
    if not token:
        return _mobile_json_error("Falta el token push.", 400)

    user = g.mobile_user
    device = _upsert_mobile_device(
        user,
        token=token,
        plataforma=(payload.get("platform") or "android"),
        device_name=(payload.get("device_name") or ""),
        app_version=(payload.get("app_version") or ""),
    )
    return jsonify({
        "ok": True,
        "device": {
            "id": device.id,
            "platform": device.plataforma,
            "active": bool(device.is_active),
        },
        "firebase_configured": bool(_firebase_is_configured()),
    })


@app.route("/api/mobile/push-token", methods=["DELETE"])
@require_mobile_auth
def api_mobile_push_token_unregister():
    payload = request.get_json(silent=True) or {}
    token = (payload.get("token") or "").strip()
    if not token:
        return _mobile_json_error("Falta el token push.", 400)
    _deactivate_mobile_device(token)
    return jsonify({"ok": True})


@app.route("/api/mobile/registro-obras", methods=["GET"])
@require_mobile_auth
def api_mobile_registro_obras_list():
    user = g.mobile_user
    rows = _load_registro_obras()
    items = _filter_registro_obras_for_mobile(
        rows,
        user,
        obra=request.args.get("obra", ""),
        responsable=request.args.get("responsable", ""),
    )
    return jsonify({"ok": True, "items": items})


@app.route("/api/mobile/cotizaciones/pendientes", methods=["GET"])
@require_mobile_auth
def api_mobile_pending_quotes():
    query = Cotizacion.query.outerjoin(Cliente, Cotizacion.cliente_id == Cliente.id)
    query = query.filter(db.func.upper(Cotizacion.estatus) == "PENDIENTE")

    items = []
    for cot in query.order_by(Cotizacion.fecha.desc()).limit(100).all():
        items.append({
            "id": cot.id,
            "folio": cot.folio or "",
            "fecha": cot.fecha.isoformat() if cot.fecha else "",
            "estatus": cot.estatus or "",
            "total": cot.total or 0,
            "responsable": cot.responsable or "",
            "cliente": cot.cliente.nombre_cliente if cot.cliente else "",
            "pdf_url": _mobile_quote_pdf_url(cot.id),
        })
    return jsonify({"ok": True, "items": items})


@app.route("/api/mobile/dashboard/summary", methods=["GET"])
@require_mobile_auth
def api_mobile_dashboard_summary():
    query = Cotizacion.query

    total_cotizaciones = query.count()
    total_importe = float(
        query.with_entities(db.func.coalesce(db.func.sum(Cotizacion.total), 0)).scalar() or 0
    )
    rows = (
        query.with_entities(Cotizacion.estatus, db.func.count(Cotizacion.id))
        .group_by(Cotizacion.estatus)
        .all()
    )
    by_status = {status: 0 for status in VALID_ESTATUS}
    for status, count in rows:
        by_status[(status or "").strip().upper()] = int(count or 0)

    return jsonify({
        "ok": True,
        "kpis": {
            "total_cotizaciones": int(total_cotizaciones),
            "total_importe": total_importe,
        },
        "status_breakdown": by_status,
        "valid_estatus": VALID_ESTATUS,
    })


@app.route("/api/mobile/cotizaciones", methods=["GET"])
@require_mobile_auth
def api_mobile_quotes():
    estatus = (request.args.get("estatus") or "").strip().upper()
    query = Cotizacion.query.outerjoin(Cliente, Cotizacion.cliente_id == Cliente.id)
    if estatus:
        query = query.filter(Cotizacion.estatus == estatus)

    items = []
    for cot in query.order_by(Cotizacion.fecha.desc()).all():
        items.append({
            "id": cot.id,
            "folio": cot.folio or "",
            "fecha": cot.fecha.isoformat() if cot.fecha else "",
            "estatus": cot.estatus or "",
            "total": cot.total or 0,
            "responsable": cot.responsable or "",
            "cliente": cot.cliente.nombre_cliente if cot.cliente else "",
            "pdf_url": _mobile_quote_pdf_url(cot.id),
        })
    return jsonify({"ok": True, "items": items, "valid_estatus": VALID_ESTATUS})


@app.route("/api/mobile/cotizaciones/<int:cot_id>/estatus", methods=["POST"])
@require_mobile_auth
def api_mobile_update_quote_status(cot_id: int):
    user = g.mobile_user
    cot = Cotizacion.query.get_or_404(cot_id)
    if not _mobile_user_is_admin(user):
        if (cot.responsable or "").strip().lower() != _mobile_user_responsable(user).lower():
            return _mobile_json_error("No autorizado para esta cotización.", 403)

    payload = request.get_json(silent=True) or {}
    nuevo = (payload.get("estatus") or "").strip().upper()
    if nuevo not in VALID_ESTATUS:
        return _mobile_json_error("Estatus inválido.", 400)

    anterior = (cot.estatus or "").strip().upper()
    if nuevo == anterior:
        return jsonify({"ok": True, "folio": cot.folio or "", "estatus": nuevo, "mensaje": "Sin cambios."})

    cot.estatus = nuevo
    db.session.commit()

    try:
        _send_quote_status_push(cot, anterior, nuevo)
    except Exception as exc:
        logger.warning("Push de estatus móvil fallida: %s", exc)

    try:
        body = (
            f"🔄 *Actualización de estatus*\\n"
            f"Folio: *{cot.folio}*\\n"
            f"Anterior: {anterior}\\n"
            f"Nuevo: *{nuevo}*\\n"
            f"Total: {money(cot.total)}"
        )
        send_whatsapp_multi(ADMIN_LIST, body)
    except Exception as exc:
        logger.warning("WhatsApp de estatus móvil falló: %s", exc)

    return jsonify({
        "ok": True,
        "folio": cot.folio or "",
        "estatus": nuevo,
        "mensaje": f"Estatus de la cotización {cot.folio} actualizado a {nuevo}.",
    })


@app.route("/api/mobile/cotizaciones/<int:cot_id>/seguimiento/<int:seg_id>", methods=["GET"])
@require_mobile_auth
def api_mobile_quote_followup_detail(cot_id: int, seg_id: int):
    user = g.mobile_user
    cot = Cotizacion.query.get_or_404(cot_id)
    if not _mobile_user_can_access_quote(user, cot):
        return _mobile_json_error("No autorizado para esta cotización.", 403)

    seg = CotizacionSeguimiento.query.filter_by(id=seg_id, cotizacion_id=cot.id).first()
    if not seg:
        return _mobile_json_error("Seguimiento no encontrado.", 404)

    return jsonify({
        "ok": True,
        "cotizacion": {
            "id": cot.id,
            "folio": cot.folio or "",
            "estatus": cot.estatus or "",
            "responsable": cot.responsable or "",
            "cliente": cot.cliente.nombre_cliente if cot.cliente else "",
        },
        "seguimiento": {
            "id": seg.id,
            "autor": seg.autor or "",
            "comentario": seg.comentario or "",
            "fecha": seg.fecha_seguimiento.isoformat() if seg.fecha_seguimiento else "",
            "actualizado_en": seg.actualizado_en.isoformat() if seg.actualizado_en else "",
        },
    })


@app.route("/api/mobile/cotizaciones/<int:cot_id>/pdf", methods=["GET"])
@require_mobile_auth
def api_mobile_quote_pdf(cot_id: int):
    cot = Cotizacion.query.get_or_404(cot_id)
    if not _mobile_user_can_access_quote(g.mobile_user, cot):
        return _mobile_json_error("No autorizado para esta cotización.", 403)
    return _build_cotizacion_pdf_response(cot)


@app.route("/api/mobile/registro-obras", methods=["POST"])
@require_mobile_auth
def api_mobile_registro_obras_create():
    user = g.mobile_user
    rows = _load_registro_obras()
    payload = request.get_json(silent=True) or {}
    send_email = bool(payload.get("send_email"))
    row = _normalize_registro_obra_row({
        "numero": "",
        "obra": payload.get("obra"),
        "ubicacion": payload.get("ubicacion"),
        "encargado": payload.get("encargado"),
        "puesto": payload.get("puesto"),
        "telefono": payload.get("telefono"),
        "correo": payload.get("correo"),
        "responsable": payload.get("responsable"),
    }, len(rows) + 1)

    if not row["obra"]:
        return _mobile_json_error("El campo 'obra' es obligatorio.", 400)
    if send_email and not row["correo"]:
        return _mobile_json_error("Debes capturar un correo si activas ENVIAR CORREO.", 400)
    if row["correo"] and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", row["correo"]):
        return _mobile_json_error("Correo inválido.", 400)
    if not _mobile_user_is_admin(user):
        row["responsable"] = _mobile_user_responsable(user)

    row["numero"] = str(len(rows) + 1)
    row["id"] = len(rows) + 1
    rows.append(row)
    _save_registro_obras(rows)
    _sync_cliente_from_registro_obra(row)
    db.session.commit()
    if send_email:
        try:
            _send_registro_obra_email(row)
        except Exception as e:
            return jsonify({"ok": True, "item": row, "email_warning": str(e)}), 201
    return jsonify({"ok": True, "item": row}), 201


@app.route("/api/mobile/registro-obras/<int:item_id>", methods=["PUT"])
@require_mobile_auth
def api_mobile_registro_obras_update(item_id: int):
    user = g.mobile_user
    rows = _load_registro_obras()
    target = next((row for row in rows if int(row.get("id", 0) or 0) == item_id), None)
    if not target:
        return _mobile_json_error("Registro no encontrado.", 404)

    owner = (target.get("responsable") or "").strip().lower()
    if not _mobile_user_is_admin(user) and owner != _mobile_user_responsable(user).lower():
        return _mobile_json_error("No autorizado.", 403)

    payload = request.get_json(silent=True) or {}
    send_email = bool(payload.get("send_email"))
    updated = _normalize_registro_obra_row({
        "numero": target.get("numero"),
        "obra": payload.get("obra"),
        "ubicacion": payload.get("ubicacion"),
        "encargado": payload.get("encargado"),
        "puesto": payload.get("puesto"),
        "telefono": payload.get("telefono"),
        "correo": payload.get("correo"),
        "responsable": payload.get("responsable"),
    }, item_id)
    if not updated["obra"]:
        return _mobile_json_error("El campo 'obra' es obligatorio.", 400)
    if send_email and not updated["correo"]:
        return _mobile_json_error("Debes capturar un correo si activas ENVIAR CORREO.", 400)
    if updated["correo"] and not re.fullmatch(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", updated["correo"]):
        return _mobile_json_error("Correo inválido.", 400)
    if not _mobile_user_is_admin(user):
        updated["responsable"] = _mobile_user_responsable(user)

    target.update(updated)
    for idx, row in enumerate(rows, start=1):
        row["id"] = idx
        row["numero"] = str(idx)
    _save_registro_obras(rows)
    _sync_cliente_from_registro_obra(target)
    db.session.commit()
    if send_email:
        try:
            _send_registro_obra_email(target)
        except Exception as e:
            return jsonify({"ok": True, "item": target, "email_warning": str(e)})
    return jsonify({"ok": True, "item": target})


@app.route("/api/mobile/registro-obras/bulk-delete", methods=["POST"])
@require_mobile_auth
def api_mobile_registro_obras_bulk_delete():
    user = g.mobile_user
    rows = _load_registro_obras()
    payload = request.get_json(silent=True) or {}
    raw_ids = payload.get("ids") or []
    if not isinstance(raw_ids, list):
        return _mobile_json_error("El campo 'ids' debe ser una lista.", 400)
    selected_ids = {int(value) for value in raw_ids if str(value).strip().isdigit()}
    if not selected_ids:
        return _mobile_json_error("No se enviaron ids válidos.", 400)

    kept = []
    deleted = 0
    for row in rows:
        row_id = int(row.get("id", 0) or 0)
        owner = (row.get("responsable") or "").strip().lower()
        can_delete = _mobile_user_is_admin(user) or owner == _mobile_user_responsable(user).lower()
        if row_id in selected_ids and can_delete:
            deleted += 1
            continue
        kept.append(row)

    for idx, row in enumerate(kept, start=1):
        row["id"] = idx
        row["numero"] = str(idx)
    _save_registro_obras(kept)
    db.session.commit()
    return jsonify({"ok": True, "deleted": deleted})


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
    ciudad_trabajo = (f.get("ciudad_trabajo") or "").strip().upper() or None

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
        responsable=responsable_final,
        ciudad_trabajo=ciudad_trabajo,
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

    _send_quote_created_notification(cot)

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
        cliente = None
        if c.cliente and (c.cliente.nombre_cliente or "").strip().lower() == cliente_nombre.lower():
            cliente = c.cliente
        else:
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
        else:
            cliente.empresa = empresa or None
            cliente.responsable = responsable_final or None
            cliente.correo = correo or None
            cliente.telefono = telefono or None
            cliente.direccion = direccion or None
            cliente.rfc = rfc or None
        c.cliente_id = cliente.id

    # === ENCABEZADO ===
    c.estatus = (f.get("estatus") or c.estatus).upper()
    c.notas = (f.get("notas") or "").strip()
    c.responsable = (responsable_final or c.responsable)
    c.ciudad_trabajo = (f.get("ciudad_trabajo") or "").strip().upper() or None
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

@app.route("/cotizaciones/<int:cot_id>/seguimiento")
@login_required
def cotizacion_seguimiento(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)
    return render_template(
        "cotizacion_seguimiento.html",
        c=c,
        seguimientos=c.seguimientos,
        valid_estatus=VALID_ESTATUS,
        title=f"Seguimiento {c.folio}",
    )

@app.route("/cotizaciones/<int:cot_id>/seguimiento", methods=["POST"])
@login_required
def crear_cotizacion_seguimiento(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)

    nuevo_estatus = (request.form.get("estatus") or "").strip().upper()
    nuevo_responsable = (request.form.get("responsable") or "").strip()
    comentario = (request.form.get("comentario") or "").strip()
    hubo_cambio = False

    if nuevo_estatus:
        if nuevo_estatus not in VALID_ESTATUS:
            flash("Selecciona un estatus válido.", "danger")
            return redirect(url_for("cotizacion_seguimiento", cot_id=c.id))
        if (c.estatus or "").strip().upper() != nuevo_estatus:
            c.estatus = nuevo_estatus
            hubo_cambio = True

    if nuevo_responsable != (c.responsable or "").strip():
        c.responsable = nuevo_responsable
        hubo_cambio = True

    if not comentario and not hubo_cambio:
        flash("Haz un cambio de estatus/responsable o escribe un comentario para guardar.", "warning")
        return redirect(url_for("cotizacion_seguimiento", cot_id=c.id))

    seg = None
    if comentario:
        seg = CotizacionSeguimiento(
            cotizacion_id=c.id,
            usuario_id=getattr(current_user, "id", None),
            autor=(getattr(current_user, "nombre", None) or responsable_actual() or "Sistema").strip(),
            comentario=comentario,
            fecha_seguimiento=now_cdmx_naive(),
            actualizado_en=now_cdmx_naive(),
        )
        db.session.add(seg)

    db.session.commit()

    if seg is not None:
        try:
            _send_quote_followup_push(c, seg)
        except Exception as exc:
            logger.warning("Push de seguimiento fallida: %s", exc)

    if seg is not None and hubo_cambio:
        flash("Se guardó el seguimiento y también se actualizó la cotización.", "success")
    elif seg is not None:
        flash("Seguimiento guardado correctamente.", "success")
    else:
        flash("Cambios de la cotización guardados.", "success")

    if seg is not None:
        return redirect(url_for("cotizacion_seguimiento", cot_id=c.id, _anchor=f"seguimiento-{seg.id}"))
    return redirect(url_for("cotizacion_seguimiento", cot_id=c.id))

@app.route("/cotizaciones/<int:cot_id>/seguimiento/<int:seg_id>/editar", methods=["POST"])
@login_required
def editar_cotizacion_seguimiento(cot_id: int, seg_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)
    seg = CotizacionSeguimiento.query.filter_by(id=seg_id, cotizacion_id=c.id).first_or_404()
    require_followup_author_or_admin(seg)

    comentario = (request.form.get("comentario") or "").strip()
    if not comentario:
        flash("El comentario no puede quedar vacío.", "warning")
        return redirect(url_for("cotizacion_seguimiento", cot_id=c.id))

    seg.comentario = comentario
    seg.actualizado_en = now_cdmx_naive()
    db.session.commit()
    flash("Seguimiento actualizado.", "success")
    return redirect(url_for("cotizacion_seguimiento", cot_id=c.id))

@app.route("/cotizaciones/<int:cot_id>/seguimiento/<int:seg_id>/eliminar", methods=["POST"])
@login_required
def eliminar_cotizacion_seguimiento(cot_id: int, seg_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    require_owner_or_admin(c)
    seg = CotizacionSeguimiento.query.filter_by(id=seg_id, cotizacion_id=c.id).first_or_404()
    require_followup_author_or_admin(seg)

    db.session.delete(seg)
    db.session.commit()
    flash("Seguimiento eliminado.", "success")
    return redirect(url_for("cotizacion_seguimiento", cot_id=c.id))

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

    if nuevo not in VALID_ESTATUS:
        return jsonify({"ok": False, "error": "Estatus inválido"}), 400

    anterior = c.estatus
    if nuevo == anterior:
        return jsonify({"ok": True, "folio": c.folio, "estatus": nuevo, "mensaje": "Sin cambios."})

    c.estatus = nuevo
    db.session.commit()

    try:
        _send_quote_status_push(c, anterior, nuevo)
    except Exception as e:
        logger.warning("Push de estatus fallida: %s", e)

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

    ws.merge_cells("A1:K1")
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

    ws.merge_cells("A2:K2")
    ws["A2"] = " | ".join(filtros_texto)
    ws["A2"].alignment = left

    headers = ["Folio", "Fecha", "Cliente", "Empresa", "Telefono", "Responsable", "Estatus", "Subtotal", "IVA %", "IVA $", "Total"]
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
            c.cliente.telefono if c.cliente and c.cliente.telefono else "",
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
        for col in (8, 10, 11):
            ws.cell(row=row, column=col).number_format = '"$"#,##0.00'
        ws.cell(row=row, column=9).number_format = '0.00'
        ws.cell(row=row, column=1).alignment = left
        ws.cell(row=row, column=2).alignment = center
        ws.cell(row=row, column=3).alignment = left
        ws.cell(row=row, column=4).alignment = left
        ws.cell(row=row, column=5).alignment = left

    total_row = ws.max_row + 2
    ws.cell(row=total_row, column=10, value="Total exportado:").font = bold
    ws.cell(row=total_row, column=11, value=f"=SUM(K{header_row + 1}:K{ws.max_row})")
    ws.cell(row=total_row, column=11).font = bold
    ws.cell(row=total_row, column=11).number_format = '"$"#,##0.00'

    ws.auto_filter.ref = f"A{header_row}:K{max(header_row, ws.max_row)}"
    ws.freeze_panes = f"A{header_row + 1}"
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 28
    ws.column_dimensions["D"].width = 28
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 18
    ws.column_dimensions["G"].width = 14
    ws.column_dimensions["H"].width = 14
    ws.column_dimensions["I"].width = 10
    ws.column_dimensions["J"].width = 14
    ws.column_dimensions["K"].width = 14

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    stamp = now_cdmx_naive().strftime("%Y%m%d_%H%M%S")
    return Response(
        bio.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="cotizaciones_dashboard_{stamp}.xlsx"'}
    )


@app.route("/cotizaciones/export/seguimientos.pdf")
@login_required
def export_dashboard_followups_pdf():
    desde = (request.args.get("desde") or "").strip()
    hasta = (request.args.get("hasta") or "").strip()
    estatus = (request.args.get("estatus") or "").strip()
    cliente = (request.args.get("cliente") or "").strip()

    try:
        cotizaciones = (
            _build_dashboard_cotizaciones_query(
                desde=desde,
                hasta=hasta,
                estatus=estatus,
                cliente=cliente,
            )
            .order_by(Cotizacion.fecha.desc())
            .all()
        )
    except ValueError as exc:
        abort(400, description=str(exc))

    total_con_seguimiento = sum(1 for cot in cotizaciones if cot.seguimientos)
    total_sin_seguimiento = max(0, len(cotizaciones) - total_con_seguimiento)
    total_importe = sum(float(c.total or 0) for c in cotizaciones)
    estatus_counts = {status: 0 for status in VALID_ESTATUS}
    for cot in cotizaciones:
        status = (cot.estatus or "").strip().upper()
        if status in estatus_counts:
            estatus_counts[status] += 1

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=22 * mm,
        bottomMargin=16 * mm,
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="FollowupHeading", fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=colors.HexColor("#0d47a1"), spaceAfter=4))
    styles.add(ParagraphStyle(name="FollowupBody", fontName="Helvetica", fontSize=9, leading=12, textColor=colors.HexColor("#222222"), spaceAfter=2))
    styles.add(ParagraphStyle(name="FollowupMeta", fontName="Helvetica", fontSize=8.3, leading=10.5, textColor=colors.HexColor("#5f6b7a"), spaceAfter=2))
    styles.add(ParagraphStyle(name="FollowupComment", fontName="Helvetica", fontSize=9, leading=12, textColor=colors.HexColor("#222222"), spaceAfter=4))

    elems = []

    def _header_footer(canv, doc_):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#0d47a1"))
        canv.rect(0, A4[1] - 34, A4[0], 34, stroke=0, fill=1)

        logo_path = os.path.join(app.static_folder or "static", "logo.png")
        if os.path.exists(logo_path):
            try:
                img = ImageReader(logo_path)
                iw, ih = img.getSize()
                max_w = 18 * mm
                scale = max_w / iw
                canv.drawImage(img, 12, A4[1] - (ih * scale) - 8, width=max_w, height=ih * scale, mask="auto")
            except Exception:
                pass

        canv.setFont("Helvetica-Bold", 13)
        canv.setFillColor(colors.white)
        canv.drawRightString(A4[0] - 12, A4[1] - 14, "BITACORA DE SEGUIMIENTO")
        canv.setFont("Helvetica", 8.5)
        canv.drawRightString(A4[0] - 12, A4[1] - 25, "Comentarios de cotizaciones")

        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#555555"))
        canv.drawString(12 * mm, 8 * mm, f"Generado: {now_cdmx_naive().strftime('%d/%m/%Y %H:%M')}")
        canv.drawRightString(A4[0] - 12 * mm, 8 * mm, f"Pagina {doc_.page}")
        canv.restoreState()

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

    resumen_data = [
        [Paragraph(f"<b>Total cotizaciones:</b> {len(cotizaciones)}", styles["FollowupBody"]),
         Paragraph(f"<b>Con seguimiento:</b> {total_con_seguimiento}", styles["FollowupBody"])],
        [Paragraph(f"<b>Sin seguimiento:</b> {total_sin_seguimiento}", styles["FollowupBody"]),
         Paragraph(f"<b>Importe total:</b> {money(total_importe)}", styles["FollowupBody"])],
        [Paragraph(f"<b>Filtros:</b> {' | '.join(filtros_texto)}", styles["FollowupMeta"]),
         Paragraph(f"<b>Estatus:</b> {' | '.join(f'{k}: {v}' for k, v in estatus_counts.items())}", styles["FollowupMeta"])],
    ]
    resumen_tbl = Table(resumen_data, colWidths=[90 * mm, 90 * mm], hAlign="LEFT")
    resumen_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 1), colors.HexColor("#f3f7fb")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#cfd9e5")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d9e2ec")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    elems.append(Paragraph("Resumen", styles["FollowupHeading"]))
    elems.append(resumen_tbl)
    elems.append(Spacer(1, 8))

    if not cotizaciones:
        elems.append(Paragraph("No hay cotizaciones para los filtros seleccionados.", styles["FollowupBody"]))
    else:
        for idx, cot in enumerate(cotizaciones, start=1):
            cli = cot.cliente
            cliente_nombre = (cli.nombre_cliente if cli else "") or "Sin cliente"
            empresa_nombre = (cli.empresa if cli else "") or "-"
            fecha_cot = cot.fecha.strftime("%d/%m/%Y %H:%M") if cot.fecha else "-"

            block_items = [
                Paragraph(f"{idx}. {escape(cot.folio or '-')}", styles["FollowupHeading"]),
                Paragraph(
                    f"<b>Cliente:</b> {escape(cliente_nombre)} &nbsp;&nbsp;&nbsp; "
                    f"<b>Empresa:</b> {escape(empresa_nombre)} &nbsp;&nbsp;&nbsp; "
                    f"<b>Estatus:</b> {escape(cot.estatus or '-')}",
                    styles["FollowupBody"],
                ),
                Paragraph(
                    f"<b>Responsable:</b> {escape(cot.responsable or '-')} &nbsp;&nbsp;&nbsp; "
                    f"<b>Fecha:</b> {fecha_cot} &nbsp;&nbsp;&nbsp; "
                    f"<b>Total:</b> {money(cot.total)}",
                    styles["FollowupBody"],
                ),
                Spacer(1, 2),
            ]

            seguimientos = sorted(list(cot.seguimientos), key=lambda seg: seg.fecha_seguimiento or datetime.min)
            if seguimientos:
                for seg in seguimientos:
                    fecha_seg = seg.fecha_seguimiento.strftime("%d/%m/%Y %H:%M") if seg.fecha_seguimiento else "-"
                    editado = ""
                    if seg.actualizado_en and seg.fecha_seguimiento and seg.actualizado_en != seg.fecha_seguimiento:
                        editado = f" · Editado {seg.actualizado_en.strftime('%d/%m/%Y %H:%M')}"
                    block_items.append(Paragraph(f"<b>{fecha_seg}</b> · {escape(seg.autor or 'Sistema')}{editado}", styles["FollowupMeta"]))
                    comentario_html = escape(seg.comentario or "").replace("\n", "<br/>")
                    block_items.append(Paragraph(comentario_html, styles["FollowupComment"]))
            else:
                block_items.append(Paragraph("Sin seguimiento registrado.", styles["FollowupMeta"]))

            bloque = Table([[item] for item in block_items], colWidths=[180 * mm], hAlign="LEFT")
            bloque.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#edf4fb")),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#c7d6e6")),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            elems.append(KeepTogether([bloque, Spacer(1, 6)]))

    doc.build(elems, onFirstPage=_header_footer, onLaterPages=_header_footer)
    stamp = now_cdmx_naive().strftime("%Y%m%d_%H%M%S")
    return Response(
        buf.getvalue(),
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="seguimientos_cotizaciones_{stamp}.pdf"'}
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


def _build_cotizacion_pdf_response(c: Cotizacion):
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=10*mm, rightMargin=10*mm,
        topMargin=24*mm, bottomMargin=38*mm
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="Encabezado", fontName="Helvetica", fontSize=9, leading=12, spaceAfter=4, splitLongWords=False))
    styles.add(ParagraphStyle(name="NormalCell", fontName="Helvetica", fontSize=8, leading=10, splitLongWords=False))
    styles.add(ParagraphStyle(name="NormalRight", fontName="Helvetica", fontSize=8, leading=10, alignment=2, splitLongWords=False))
    styles.add(ParagraphStyle(name="NormalCenter", fontName="Helvetica", fontSize=8, leading=10, alignment=1, splitLongWords=False))
    styles.add(ParagraphStyle(
        name="UnitCell",
        fontName="Helvetica",
        fontSize=6.5,
        leading=7,
        alignment=1,
        splitLongWords=False,
    ))

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
                max_w = 22.5 * mm
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
    ciudad_trabajo = (getattr(c, "ciudad_trabajo", "") or "").strip()

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
            Paragraph(f"<b>Ciudad:</b> {escape(ciudad_trabajo)}", styles["Encabezado"]),
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
    data = [["Ptda", "Concepto", "Uni.", "Cant.", "Sistema", "Precio Unitario", "Subtotal"]]
    for d in c.detalles:
        data.append([
            Paragraph(_truncate_pdf_text(getattr(d, "capitulo", "") or "-", 28), styles["NormalCenter"]),
            Paragraph((d.nombre_concepto or "-").strip(), styles["NormalCell"]),
            Paragraph(" ".join(str(d.unidad or "-").strip().splitlines()), styles["UnitCell"]),
            Paragraph(f"{(d.cantidad or 0):.2f}", styles["NormalCenter"]),
            Paragraph((d.sistema or "-").strip(), styles["NormalCell"]),
            Paragraph(money(d.precio_unitario), styles["NormalRight"]),
            Paragraph(money(d.subtotal), styles["NormalRight"]),
        ])

    tbl = Table(
        data,
        colWidths=[12*mm, 78*mm, 16*mm, 16*mm, 24*mm, 22*mm, 22*mm],
        repeatRows=1,
        hAlign="CENTER"
    )
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d47a1")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 1), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 1), (1, -1), "LEFT"),
        ("ALIGN", (4, 1), (4, -1), "LEFT"),
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


@app.route("/cotizaciones/<int:cot_id>/export.pdf")
def export_cotizacion_pdf(cot_id: int):
    c = Cotizacion.query.get_or_404(cot_id)
    mobile_user = _mobile_user_from_token()
    if mobile_user:
        if not _mobile_user_can_access_quote(mobile_user, c):
            abort(403)
    elif current_user.is_authenticated:
        require_owner_or_admin(c)
    else:
        return login_manager.unauthorized()
    return _build_cotizacion_pdf_response(c)

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
    categorias = VALID_ESTATUS
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
        inicio_hoy = ahora.replace(hour=0, minute=0, second=0, microsecond=0)

        cotizaciones = (
            Cotizacion.query
            .filter(db.func.upper(Cotizacion.estatus) != "FINALIZADA")
            .all()
        )

        for cot in cotizaciones:
            if cot.last_whatsapp_at is not None and cot.last_whatsapp_at >= inicio_hoy:
                continue
            try:
                _send_daily_status_reminder(cot, ahora)
            except Exception as e:
                print(f"[Scheduler] ERROR recordatorio ({cot.folio}): {e}", file=sys.stderr)

scheduler: Optional[BackgroundScheduler] = None
try:
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        scheduler = BackgroundScheduler(timezone=TZ_CDMX, daemon=True)
        scheduler.add_job(
            enviar_notificaciones_pendientes,
            "cron",
            hour=10,
            minute=0,
            id="daily_quotes_status_reminder",
            replace_existing=True
        )
        scheduler.start()
        print("[Scheduler] Iniciado (10:00 AM CDMX).")
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


# ---------------------------------------------------------
# Inventario
# ---------------------------------------------------------
INVENTARIO_TIPOS = ("ENTRADA", "SALIDA", "AJUSTE")
INVENTARIO_MOTIVOS = {
    "ENTRADA": ("COMPRA", "DEVOLUCION", "AJUSTE POSITIVO"),
    "SALIDA": ("OBRA", "CONSUMO INTERNO", "VENTA", "AJUSTE NEGATIVO"),
    "AJUSTE": ("AJUSTE POSITIVO", "AJUSTE NEGATIVO", "CONTEO FISICO"),
}


def _inventario_stock_bajo_query():
    return InventarioProducto.query.filter(
        InventarioProducto.activo.is_(True),
        or_(
            and_(
                InventarioProducto.stock_maximo > 0,
                InventarioProducto.stock_actual < (InventarioProducto.stock_maximo * 0.15),
            ),
            and_(
                or_(InventarioProducto.stock_maximo.is_(None), InventarioProducto.stock_maximo <= 0),
                InventarioProducto.stock_actual <= InventarioProducto.stock_minimo,
            ),
        ),
    )


def _inventario_porcentaje(producto: InventarioProducto) -> float:
    stock = float(producto.stock_actual or 0)
    maximo = float(getattr(producto, "stock_maximo", 0) or 0)
    if maximo <= 0:
        maximo = max(stock, float(producto.stock_minimo or 0), 1.0)
    return max(0.0, min(100.0, (stock / maximo) * 100.0))


def _inventario_estado(producto: InventarioProducto) -> dict:
    pct = _inventario_porcentaje(producto)
    if pct >= 75:
        return {"porcentaje": pct, "color": "success", "texto": "OK"}
    if pct >= 25:
        return {"porcentaje": pct, "color": "warning", "texto": "MEDIO"}
    if pct >= 15:
        return {"porcentaje": pct, "color": "orange", "texto": "REORDEN"}
    return {"porcentaje": pct, "color": "danger", "texto": "BAJO"}


def _inventario_registrar_movimiento(
    producto: InventarioProducto,
    tipo: str,
    motivo: str,
    cantidad: float,
    costo_unitario: float = 0.0,
    proveedor: str = "",
    obra: str = "",
    referencia: str = "",
    observaciones: str = "",
) -> InventarioMovimiento:
    tipo = (tipo or "").strip().upper()
    motivo = (motivo or "").strip().upper()
    cantidad = fmt(abs(cantidad))
    costo_unitario = fmt(costo_unitario)

    if tipo not in INVENTARIO_TIPOS:
        raise ValueError("Tipo de movimiento invalido.")
    if cantidad <= 0:
        raise ValueError("La cantidad debe ser mayor a cero.")
    if motivo not in INVENTARIO_MOTIVOS.get(tipo, ()):
        raise ValueError("Motivo de movimiento invalido.")
    if tipo == "AJUSTE" and not is_admin():
        raise PermissionError("Solo el administrador puede registrar ajustes.")

    stock_antes = float(producto.stock_actual or 0)
    if tipo == "ENTRADA":
        delta = cantidad
    elif tipo == "SALIDA":
        delta = -cantidad
    else:
        delta = cantidad if "POSITIVO" in motivo else -cantidad

    stock_despues = stock_antes + delta
    if stock_despues < -0.0001:
        raise ValueError(f"Stock insuficiente. Disponible: {stock_antes:g} {producto.unidad or ''}.")

    mov = InventarioMovimiento(
        producto_id=producto.id,
        fecha=now_cdmx_naive(),
        tipo=tipo,
        motivo=motivo,
        cantidad=cantidad,
        costo_unitario=costo_unitario or float(producto.costo_unitario or 0),
        stock_antes=fmt(stock_antes),
        stock_despues=fmt(stock_despues),
        proveedor=(proveedor or "").strip() or None,
        obra=(obra or "").strip() or None,
        referencia=(referencia or "").strip() or None,
        responsable=responsable_actual() or None,
        observaciones=(observaciones or "").strip() or None,
        usuario_id=getattr(current_user, "id", None),
    )
    producto.stock_actual = fmt(stock_despues)
    if float(getattr(producto, "stock_maximo", 0) or 0) <= 0 and stock_despues > 0:
        producto.stock_maximo = fmt(max(stock_despues, float(producto.stock_minimo or 0)))
    if tipo == "ENTRADA" and costo_unitario > 0:
        producto.costo_unitario = costo_unitario
    producto.actualizado_en = now_cdmx_naive()
    db.session.add(mov)
    return mov


@app.route("/inventario")
@login_required
def inventario_index():
    q = (request.args.get("q") or "").strip()
    categoria = (request.args.get("categoria") or "").strip()
    estado = (request.args.get("estado") or "").strip()

    productos_q = InventarioProducto.query
    if q:
        like = f"%{q}%"
        productos_q = productos_q.filter(or_(
            InventarioProducto.codigo.ilike(like),
            InventarioProducto.nombre.ilike(like),
            InventarioProducto.proveedor.ilike(like),
            InventarioProducto.ubicacion.ilike(like),
        ))
    if categoria:
        productos_q = productos_q.filter(InventarioProducto.categoria == categoria)
    if estado == "bajo":
        productos_q = productos_q.filter(
            InventarioProducto.activo.is_(True),
            or_(
                and_(
                    InventarioProducto.stock_maximo > 0,
                    InventarioProducto.stock_actual < (InventarioProducto.stock_maximo * 0.15),
                ),
                and_(
                    or_(InventarioProducto.stock_maximo.is_(None), InventarioProducto.stock_maximo <= 0),
                    InventarioProducto.stock_actual <= InventarioProducto.stock_minimo,
                ),
            )
        )
    elif estado == "reorden":
        productos_q = productos_q.filter(
            InventarioProducto.activo.is_(True),
            InventarioProducto.stock_actual >= (InventarioProducto.stock_maximo * 0.15),
            InventarioProducto.stock_actual < (InventarioProducto.stock_maximo * 0.25),
        )
    elif estado == "inactivo":
        productos_q = productos_q.filter(InventarioProducto.activo.is_(False))
    else:
        productos_q = productos_q.filter(InventarioProducto.activo.is_(True))

    productos = productos_q.order_by(InventarioProducto.nombre.asc()).all()
    recientes = (
        InventarioMovimiento.query
        .join(InventarioProducto)
        .order_by(InventarioMovimiento.fecha.desc(), InventarioMovimiento.id.desc())
        .limit(12)
        .all()
    )
    categorias = [
        row[0] for row in db.session.query(InventarioProducto.categoria)
        .filter(InventarioProducto.categoria.isnot(None), InventarioProducto.categoria != "")
        .distinct()
        .order_by(InventarioProducto.categoria.asc())
        .all()
    ]
    obras = RegistroObra.query.order_by(RegistroObra.obra.asc()).limit(300).all()
    total_valor = sum(float(p.stock_actual or 0) * float(p.costo_unitario or 0) for p in productos)

    return render_template(
        "inventario.html",
        title="Inventario",
        productos=productos,
        recientes=recientes,
        categorias=categorias,
        obras=obras,
        q=q,
        categoria=categoria,
        estado=estado,
        total_valor=total_valor,
        stock_bajo_count=_inventario_stock_bajo_query().count(),
        inventario_motivos=INVENTARIO_MOTIVOS,
        inventario_estado=_inventario_estado,
    )


@app.route("/inventario/productos/crear", methods=["POST"])
@login_required
def inventario_producto_crear():
    if not is_admin():
        abort(403)

    f = request.form
    codigo = (f.get("codigo") or "").strip().upper() or None
    nombre = (f.get("nombre") or "").strip()
    if not nombre:
        flash("Captura el nombre del material.", "warning")
        return redirect(url_for("inventario_index"))
    if codigo and InventarioProducto.query.filter_by(codigo=codigo).first():
        flash("Ya existe un material con ese codigo.", "danger")
        return redirect(url_for("inventario_index"))

    producto = InventarioProducto(
        codigo=codigo,
        nombre=nombre,
        categoria=(f.get("categoria") or "").strip() or None,
        unidad=(f.get("unidad") or "").strip() or "pieza",
        stock_minimo=fmt(parse_float(f.get("stock_minimo"), 0)),
        stock_maximo=fmt(parse_float(f.get("stock_maximo"), parse_float(f.get("stock_inicial"), 0))),
        costo_unitario=fmt(parse_float(f.get("costo_unitario"), 0)),
        proveedor=(f.get("proveedor") or "").strip() or None,
        ubicacion=(f.get("ubicacion") or "").strip() or None,
        activo=True,
    )
    db.session.add(producto)
    db.session.flush()

    stock_inicial = parse_float(f.get("stock_inicial"), 0)
    if stock_inicial > 0:
        _inventario_registrar_movimiento(
            producto,
            "ENTRADA",
            "AJUSTE POSITIVO",
            stock_inicial,
            producto.costo_unitario or 0,
            proveedor=producto.proveedor or "",
            referencia="STOCK INICIAL",
            observaciones="Alta inicial de inventario.",
        )

    db.session.commit()
    flash("Material agregado al inventario.", "success")
    return redirect(url_for("inventario_index"))


@app.route("/inventario/productos/<int:producto_id>/actualizar", methods=["POST"])
@login_required
def inventario_producto_actualizar(producto_id: int):
    if not is_admin():
        abort(403)

    producto = InventarioProducto.query.get_or_404(producto_id)
    f = request.form
    codigo = (f.get("codigo") or "").strip().upper() or None
    if codigo and codigo != producto.codigo and InventarioProducto.query.filter_by(codigo=codigo).first():
        flash("Ya existe otro material con ese codigo.", "danger")
        return redirect(url_for("inventario_kardex", producto_id=producto.id))

    producto.codigo = codigo
    producto.nombre = (f.get("nombre") or producto.nombre).strip()
    producto.categoria = (f.get("categoria") or "").strip() or None
    producto.unidad = (f.get("unidad") or producto.unidad or "pieza").strip()
    producto.stock_minimo = fmt(parse_float(f.get("stock_minimo"), producto.stock_minimo or 0))
    producto.stock_maximo = fmt(parse_float(f.get("stock_maximo"), producto.stock_maximo or 0))
    producto.costo_unitario = fmt(parse_float(f.get("costo_unitario"), producto.costo_unitario or 0))
    producto.proveedor = (f.get("proveedor") or "").strip() or None
    producto.ubicacion = (f.get("ubicacion") or "").strip() or None
    producto.activo = (f.get("activo") or "1") == "1"
    producto.actualizado_en = now_cdmx_naive()
    db.session.commit()
    flash("Material actualizado.", "success")
    return redirect(url_for("inventario_kardex", producto_id=producto.id))


@app.route("/inventario/movimientos/crear", methods=["POST"])
@login_required
def inventario_movimiento_crear():
    producto = InventarioProducto.query.get_or_404(int(request.form.get("producto_id") or 0))
    try:
        _inventario_registrar_movimiento(
            producto=producto,
            tipo=request.form.get("tipo"),
            motivo=request.form.get("motivo"),
            cantidad=parse_float(request.form.get("cantidad"), 0),
            costo_unitario=parse_float(request.form.get("costo_unitario"), producto.costo_unitario or 0),
            proveedor=request.form.get("proveedor") or producto.proveedor or "",
            obra=request.form.get("obra") or "",
            referencia=request.form.get("referencia") or "",
            observaciones=request.form.get("observaciones") or "",
        )
        db.session.commit()
        flash("Movimiento registrado correctamente.", "success")
    except PermissionError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "warning")
    return redirect(request.referrer or url_for("inventario_index"))


@app.route("/inventario/productos/<int:producto_id>/kardex")
@login_required
def inventario_kardex(producto_id: int):
    producto = InventarioProducto.query.get_or_404(producto_id)
    movimientos = (
        InventarioMovimiento.query
        .filter_by(producto_id=producto.id)
        .order_by(InventarioMovimiento.fecha.desc(), InventarioMovimiento.id.desc())
        .all()
    )
    obras = RegistroObra.query.order_by(RegistroObra.obra.asc()).limit(300).all()
    return render_template(
        "inventario_kardex.html",
        title=f"Kardex {producto.nombre}",
        producto=producto,
        movimientos=movimientos,
        obras=obras,
        inventario_motivos=INVENTARIO_MOTIVOS,
        inventario_estado=_inventario_estado,
    )


@app.route("/inventario/export.xlsx")
@login_required
def inventario_export_xlsx():
    if Workbook is None:
        abort(501, description="openpyxl no instalado en el servidor.")

    productos = InventarioProducto.query.order_by(InventarioProducto.nombre.asc()).all()
    wb = Workbook()
    ws = wb.active
    ws.title = "Inventario"
    headers = [
        "Codigo", "Material", "Categoria", "Unidad", "Stock actual", "Stock minimo",
        "Stock maximo", "% inventario", "Estado", "Costo unitario", "Valor", "Proveedor", "Ubicacion", "Activo",
    ]
    ws.append(headers)
    fills_estado = {
        "OK": PatternFill("solid", fgColor="D1E7DD"),
        "MEDIO": PatternFill("solid", fgColor="FFF3CD"),
        "REORDEN": PatternFill("solid", fgColor="FED7AA"),
        "BAJO": PatternFill("solid", fgColor="F8D7DA"),
    }
    for p in productos:
        stock = float(p.stock_actual or 0)
        costo = float(p.costo_unitario or 0)
        estado_inv = _inventario_estado(p)
        ws.append([
            p.codigo or "",
            p.nombre or "",
            p.categoria or "",
            p.unidad or "",
            stock,
            float(p.stock_minimo or 0),
            float(p.stock_maximo or 0),
            estado_inv["porcentaje"] / 100.0,
            estado_inv["texto"],
            costo,
            stock * costo,
            p.proveedor or "",
            p.ubicacion or "",
            "SI" if p.activo else "NO",
        ])
        fill = fills_estado.get(estado_inv["texto"])
        if fill:
            for cell in ws[ws.max_row]:
                cell.fill = fill

    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="0D47A1")
        cell.alignment = Alignment(horizontal="center")
    for col in ("E", "F", "G"):
        for cell in ws[col][1:]:
            cell.number_format = '0.00'
    for cell in ws["H"][1:]:
        cell.number_format = '0.00%'
    for col in ("J", "K"):
        for cell in ws[col][1:]:
            cell.number_format = '"$"#,##0.00'
    widths = [16, 34, 18, 12, 14, 14, 14, 14, 14, 16, 16, 24, 20, 10]
    for idx, width in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = width

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    stamp = now_cdmx_naive().strftime("%Y%m%d_%H%M%S")
    return Response(
        bio.getvalue(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="inventario_{stamp}.xlsx"'}
    )


# Blueprints (Catálogos) — si existen en tu repo
# ---------------------------------------------------------
try:
    from catalogos_routes import bp as catalogos_bp
    app.register_blueprint(catalogos_bp, url_prefix="/catalogos")
except Exception as e:
    print(f"[WARN] No se pudo cargar blueprint catalogos_routes: {e}", file=sys.stderr)

try:
    from pu_routes import pu_bp
    app.register_blueprint(pu_bp, url_prefix="/pu")
except Exception as e:
    print(f"[WARN] No se pudo cargar blueprint pu_routes: {e}", file=sys.stderr)

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        os.makedirs(app.static_folder or "static", exist_ok=True)
    except Exception:
        pass
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
