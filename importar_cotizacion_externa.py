from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path


ZONA_PORC = {
    "Zona Norte": 10.0,
    "Zona Centro": 5.0,
    "Bajio": 10.0,
    "Bajío": 10.0,
    "Zona Sur": 15.0,
    "Frontera": 8.0,
}


def parse_float(value, default=0.0):
    try:
        if value in (None, ""):
            return float(default)
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value).replace("$", "").replace(",", "").strip())
    except Exception:
        return float(default)


def fmt(value):
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


def now_local_naive():
    return datetime.now().replace(microsecond=0)


def parse_datetime_flexible(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
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
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except Exception:
            pass
        for pattern in formats:
            try:
                return datetime.strptime(candidate, pattern)
            except Exception:
                continue
    return None


def append_note(base, extra):
    base = (base or "").strip()
    extra = (extra or "").strip()
    if not extra:
        return base or None
    return f"{base}\n{extra}".strip() if base else extra


def sample_payload():
    return {
        "folio": "COT-2026-02-026-2",
        "fecha": "2026-02-26",
        "estatus": "PENDIENTE",
        "responsable": "Rafa",
        "cliente": {
            "nombre_cliente": "Jardin Oracle Guadalajara",
            "empresa": "Oracle",
            "correo": "",
            "telefono": "",
            "direccion": "Guadalajara, Jalisco",
            "rfc": ""
        },
        "zona": "",
        "iva_porc": 16,
        "notas": "Importada desde cotizacion externa.",
        "items": [
            {
                "nombre_concepto": "Sistema Tremproof",
                "unidad": "m2",
                "cantidad": 1,
                "precio_unitario": 0,
                "sistema": "Tremproof",
                "descripcion": "Captura aqui la descripcion del concepto."
            }
        ]
    }


def normalize_payload(payload):
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
    for index, item in enumerate(items_in, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"El concepto #{index} debe ser un objeto.")
        nombre = (item.get("nombre_concepto") or item.get("concepto") or item.get("nombre") or "").strip()
        if not nombre:
            raise ValueError(f"El concepto #{index} no tiene nombre.")
        normalized_items.append({
            "nombre_concepto": nombre,
            "unidad": (item.get("unidad") or "").strip(),
            "cantidad": parse_float(item.get("cantidad"), 1.0),
            "precio_unitario": parse_float(item.get("precio_unitario", item.get("precio")), 0.0),
            "sistema": (item.get("sistema") or "").strip() or None,
            "descripcion": (item.get("descripcion") or "").strip(),
        })

    return {
        "folio": (payload.get("folio") or payload.get("folio_externo") or "").strip() or None,
        "fecha": parse_datetime_flexible(payload.get("fecha")) or now_local_naive(),
        "estatus": (payload.get("estatus") or "PENDIENTE").strip().upper(),
        "responsable": (payload.get("responsable") or "").strip() or None,
        "cliente": cliente,
        "zona": (payload.get("zona") or "").strip(),
        "iva_porc": parse_float(payload.get("iva_porc"), 16.0),
        "notas": (payload.get("notas") or "").strip() or None,
        "items": normalized_items,
    }


def table_columns(con, table):
    return {row[1] for row in con.execute(f"PRAGMA table_info({table})")}


def insert_row(con, table, data):
    filtered = {key: value for key, value in data.items() if value is not None and key in table_columns(con, table)}
    if not filtered:
        raise ValueError(f"No hay columnas compatibles para insertar en {table}.")
    cols = ", ".join(filtered.keys())
    placeholders = ", ".join([":" + key for key in filtered.keys()])
    con.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", filtered)
    return con.execute("SELECT last_insert_rowid()").fetchone()[0]


def generate_folio(con):
    max_number = 0
    rows = con.execute("SELECT folio FROM cotizacion WHERE folio LIKE 'PTCH-%'").fetchall()
    for (folio,) in rows:
        if not folio:
            continue
        folio = str(folio).strip()
        if len(folio) == 9 and folio.startswith("PTCH-") and folio[5:].isdigit():
            max_number = max(max_number, int(folio[5:]))
    for offset in range(1, 11):
        candidate = f"PTCH-{max_number + offset:04d}"
        exists = con.execute("SELECT 1 FROM cotizacion WHERE folio = ? LIMIT 1", (candidate,)).fetchone()
        if not exists:
            return candidate
    return f"PTCH-{datetime.now().strftime('%Y%m%d%H%M%S')}"


def pick_folio(con, preferred_folio):
    preferred = (preferred_folio or "").strip()
    if preferred:
        exists = con.execute("SELECT 1 FROM cotizacion WHERE folio = ? LIMIT 1", (preferred,)).fetchone()
        if not exists:
            return preferred
    return generate_folio(con)


def find_or_create_client(con, cliente_data, responsable_final):
    if cliente_data.get("empresa"):
        row = con.execute(
            "SELECT id FROM cliente WHERE lower(nombre_cliente) = lower(?) AND lower(coalesce(empresa, '')) = lower(?) LIMIT 1",
            (cliente_data["nombre_cliente"], cliente_data["empresa"]),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT id FROM cliente WHERE lower(nombre_cliente) = lower(?) LIMIT 1",
            (cliente_data["nombre_cliente"],),
        ).fetchone()
    if row:
        return row[0]

    return insert_row(con, "cliente", {
        "nombre_cliente": cliente_data["nombre_cliente"],
        "empresa": cliente_data.get("empresa"),
        "responsable": responsable_final,
        "correo": cliente_data.get("correo"),
        "telefono": cliente_data.get("telefono"),
        "direccion": cliente_data.get("direccion"),
        "rfc": cliente_data.get("rfc"),
    })


def find_or_create_concept(con, item):
    row = con.execute(
        "SELECT id FROM concepto WHERE lower(nombre_concepto) = lower(?) LIMIT 1",
        (item["nombre_concepto"],),
    ).fetchone()
    if row:
        return row[0]

    return insert_row(con, "concepto", {
        "nombre_concepto": item["nombre_concepto"],
        "unidad": item.get("unidad") or None,
        "precio_unitario": item.get("precio_unitario"),
        "descripcion": item.get("descripcion") or None,
    })


def import_payload(con, payload, source_label=None, dry_run=False):
    normalized = normalize_payload(payload)
    responsable_final = normalized["responsable"]
    cliente_id = find_or_create_client(con, normalized["cliente"], responsable_final)

    subtotal = 0.0
    detail_rows = []
    for item in normalized["items"]:
        line_subtotal = fmt(item["cantidad"] * item["precio_unitario"])
        subtotal += line_subtotal
        detail_rows.append((item, line_subtotal))

    desc_porc = float(ZONA_PORC.get(normalized["zona"], 0.0))
    descuento_total = subtotal * (desc_porc / 100.0)
    subtotal_desc = subtotal - descuento_total
    iva_monto = subtotal_desc * (normalized["iva_porc"] / 100.0)
    total = subtotal_desc + iva_monto

    notas = normalized["notas"]
    if source_label:
        notas = append_note(notas, f"Importada desde: {source_label}")
    if normalized["folio"]:
        notas = append_note(notas, f"Folio externo original: {normalized['folio']}")
    if normalized["zona"] and desc_porc > 0:
        notas = append_note(notas, f"Zona: {normalized['zona']} ({int(desc_porc)}% descuento)")

    cotizacion_id = insert_row(con, "cotizacion", {
        "folio": pick_folio(con, normalized["folio"]),
        "cliente_id": cliente_id,
        "fecha": normalized["fecha"].isoformat(sep=" "),
        "estatus": normalized["estatus"],
        "subtotal": fmt(subtotal),
        "descuento_total": fmt(descuento_total),
        "iva_porc": fmt(normalized["iva_porc"]),
        "iva_monto": fmt(iva_monto),
        "total": fmt(total),
        "notas": notas,
        "last_whatsapp_at": None,
        "responsable": responsable_final,
    })

    folio = con.execute("SELECT folio FROM cotizacion WHERE id = ?", (cotizacion_id,)).fetchone()[0]

    for item, line_subtotal in detail_rows:
        concepto_id = find_or_create_concept(con, item)
        insert_row(con, "cotizacion_detalle", {
            "cotizacion_id": cotizacion_id,
            "concepto_id": concepto_id,
            "nombre_concepto": item["nombre_concepto"],
            "unidad": item["unidad"],
            "cantidad": item["cantidad"],
            "precio_unitario": item["precio_unitario"],
            "descuento": 0.0,
            "sistema": item["sistema"],
            "descripcion": item["descripcion"],
            "subtotal": line_subtotal,
        })

    result = {
        "folio": folio,
        "cliente_id": cliente_id,
        "cotizacion_id": cotizacion_id,
        "subtotal": fmt(subtotal),
        "descuento_total": fmt(descuento_total),
        "iva_monto": fmt(iva_monto),
        "total": fmt(total),
        "items": len(detail_rows),
    }

    if dry_run:
        con.rollback()
    else:
        con.commit()
    return result


def load_payload(args):
    if args.print_sample:
        print(json.dumps(sample_payload(), ensure_ascii=False, indent=2))
        raise SystemExit(0)
    if args.stdin:
        import sys
        return json.load(sys.stdin)
    if not args.json_path:
        raise SystemExit("Debes indicar --json PATH o usar --stdin.")
    return json.loads(Path(args.json_path).read_text(encoding="utf-8"))


def build_parser():
    parser = argparse.ArgumentParser(description="Importa una cotizacion externa a una base SQLite del sistema")
    parser.add_argument("--json", dest="json_path", help="Ruta al JSON de importacion")
    parser.add_argument("--stdin", action="store_true", help="Leer el JSON desde stdin")
    parser.add_argument("--db", default="mar3.db", help="Ruta a la base SQLite. Default: mar3.db")
    parser.add_argument("--source-label", default="", help="Ruta o referencia del PDF original")
    parser.add_argument("--dry-run", action="store_true", help="Valida y calcula sin escribir en BD")
    parser.add_argument("--print-sample", action="store_true", help="Imprime una plantilla JSON y termina")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    payload = load_payload(args)
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"No existe la base de datos: {db_path}")
    con = sqlite3.connect(str(db_path))
    try:
        result = import_payload(con, payload, source_label=args.source_label, dry_run=args.dry_run)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
