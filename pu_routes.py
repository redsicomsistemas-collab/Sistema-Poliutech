from __future__ import annotations

import json
import shutil
from pathlib import Path
from threading import Lock
from typing import Any

from flask import Blueprint, flash, redirect, render_template, request, send_file, url_for
from flask_login import login_required
from openpyxl.utils import column_index_from_string, coordinate_to_tuple, get_column_letter


pu_bp = Blueprint("pu", __name__, template_folder="templates")

PROJECT_ROOT = Path(__file__).resolve().parent
PU_XLSM_PATH = PROJECT_ROOT / "APU General (1).xlsm"
PU_MANIFEST_PATH = PROJECT_ROOT / "apu_manifest.json"
PU_SHEETS_DIR = PROJECT_ROOT / "apu_sheets"
PAYLOAD_LOCK = Lock()
DEFAULT_ROWS = 20
DEFAULT_COLS = 12
MANIFEST_CACHE: dict[str, Any] = {"mtime": None, "payload": None}
SHEET_CACHE: dict[str, tuple[float, dict]] = {}


def _pesos(cantidad: Any) -> str:
    try:
        valor = float(cantidad or 0)
    except Exception:
        valor = 0.0

    decimales = round(100 * (valor - int(valor)))
    entero_txt = f"{valor:.2f}".split(".")[0]
    places = {2: " MIL ", 3: " MILLONES ", 4: " BILLONES ", 5: " TRILLONES "}

    def get_digit(digit: str) -> str:
        return {"1": "UN", "2": "DOS", "3": "TRES", "4": "CUATRO", "5": "CINCO", "6": "SEIS", "7": "SIETE", "8": "OCHO", "9": "NUEVE"}.get(digit, "")

    def get_tens(text: str) -> str:
        if len(text) == 1:
            return get_digit(text)
        if text[0] == "1":
            return {"10": "DIEZ", "11": "ONCE", "12": "DOCE", "13": "TRECE", "14": "CATORCE", "15": "QUINCE", "16": "DIECISEIS", "17": "DIECISIETE", "18": "DIECIOCHO", "19": "DIECINUEVE"}.get(text, "")
        if text[1] == "0":
            return {"2": "VEINTE", "3": "TREINTA", "4": "CUARENTA", "5": "CINCUENTA", "6": "SESENTA", "7": "SETENTA", "8": "OCHENTA", "9": "NOVENTA"}.get(text[0], "")
        prefix = {"2": "VEINTI", "3": "TREINTA Y ", "4": "CUARENTA Y ", "5": "CINCUENTA Y ", "6": "SESENTA Y ", "7": "SETENTA Y ", "8": "OCHENTA Y ", "9": "NOVENTA Y "}.get(text[0], "")
        return prefix + get_digit(text[1])

    def get_hundreds(text: str) -> str:
        if int(text) == 0:
            return ""
        text = text.zfill(3)
        result = ""
        if text[0] != "0":
            if text == "100":
                result = "CIEN "
            else:
                result = {"1": "CIENTO ", "2": "DOSCIENTOS ", "3": "TRESCIENTOS ", "4": "CUATROCIENTOS ", "5": "QUINIENTOS ", "6": "SEISCIENTOS ", "7": "SETECIENTOS ", "8": "OCHOCIENTOS ", "9": "NOVECIENTOS "}.get(text[0], "")
        if text[1] != "0":
            result += get_tens(text[1:])
        else:
            result += get_digit(text[2])
        return result

    count = 1
    pesotes = ""
    original_num = int(entero_txt or 0)
    original_len = len(entero_txt)
    while entero_txt:
        temp = get_hundreds(entero_txt[-3:])
        if temp:
            if temp == "UN" and count > 2:
                pesotes = temp + places[count][:-3] + " " + pesotes
            else:
                pesotes = temp + places.get(count, "") + pesotes
        entero_txt = entero_txt[:-3]
        count += 1

    if not pesotes:
        pesotes = " (CERO PESOS"
    elif pesotes == "UN":
        pesotes = " (UN PESO"
    elif original_len > 6 and original_num % 1000000 == 0:
        pesotes = f" ({pesotes} DE PESOS"
    else:
        pesotes = f" ({pesotes} PESOS"
    return f"{pesotes} {int(decimales):02d}/100 M.N.)".upper().strip()


def _load_manifest() -> dict:
    mtime = PU_MANIFEST_PATH.stat().st_mtime
    if MANIFEST_CACHE["payload"] is not None and MANIFEST_CACHE["mtime"] == mtime:
        return MANIFEST_CACHE["payload"]
    payload = json.loads(PU_MANIFEST_PATH.read_text(encoding="utf-8"))
    MANIFEST_CACHE["mtime"] = mtime
    MANIFEST_CACHE["payload"] = payload
    return payload


def _sheet_meta(sheet_name: str) -> dict | None:
    manifest = _load_manifest()
    for item in manifest.get("sheets", []):
        if item.get("name") == sheet_name:
            return item
    return None


def _load_sheet(sheet_name: str) -> dict:
    meta = _sheet_meta(sheet_name)
    if not meta:
        raise KeyError(sheet_name)
    path = PU_SHEETS_DIR / meta["file"]
    mtime = path.stat().st_mtime
    cached = SHEET_CACHE.get(sheet_name)
    if cached and cached[0] == mtime:
        return cached[1]
    payload = json.loads(path.read_text(encoding="utf-8"))
    SHEET_CACHE[sheet_name] = (mtime, payload)
    return payload


def _write_sheet(sheet_name: str, payload: dict) -> None:
    meta = _sheet_meta(sheet_name)
    path = PU_SHEETS_DIR / meta["file"]
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    SHEET_CACHE[sheet_name] = (path.stat().st_mtime, payload)


def _window(sheet: dict, start_row: int, start_col: int, rows: int, cols: int):
    max_row = int(sheet.get("max_row") or 1)
    max_col = int(sheet.get("max_col") or 1)
    end_row = min(max_row, start_row + rows - 1)
    end_col = min(max_col, start_col + cols - 1)
    cells = sheet.get("cells", {})

    header_cols = [{"label": get_column_letter(col), "width": 140} for col in range(start_col, end_col + 1)]
    grid_rows = []
    for row_idx in range(start_row, end_row + 1):
        row_cells = []
        for col_idx in range(start_col, end_col + 1):
            coord = f"{get_column_letter(col_idx)}{row_idx}"
            cell = cells.get(coord, {})
            row_cells.append({
                "coord": coord,
                "value": cell.get("value", ""),
                "is_formula": bool(cell.get("formula")),
                "formula": cell.get("formula", ""),
                "rowspan": 1,
                "colspan": 1,
                "style": "",
                "width": 140,
                "height": 26,
            })
        grid_rows.append({"row_number": row_idx, "height": 26, "cells": row_cells})
    return header_cols, grid_rows, end_row, end_col


def _set_cell(sheet: dict, row: int, col: int, value: str, raw: str = "", formula: str = "") -> None:
    coord = f"{get_column_letter(col)}{row}"
    if value in (None, "") and not raw and not formula:
        sheet.get("cells", {}).pop(coord, None)
        return
    sheet.setdefault("cells", {})[coord] = {
        "value": "" if value is None else str(value),
        "formula": formula,
        "raw": raw if raw else ("" if value is None else str(value)),
        "row": row,
        "col": col,
        "col_letter": get_column_letter(col),
    }


def _copy_range_values(sheet: dict, source_start: str, source_end: str, target_start: str) -> None:
    start_row, start_col = coordinate_to_tuple(source_start)
    end_row, end_col = coordinate_to_tuple(source_end)
    target_row, target_col = coordinate_to_tuple(target_start)
    for row_idx in range(start_row, end_row + 1):
        for col_idx in range(start_col, end_col + 1):
            source = sheet.get("cells", {}).get(f"{get_column_letter(col_idx)}{row_idx}", {})
            _set_cell(
                sheet,
                target_row + (row_idx - start_row),
                target_col + (col_idx - start_col),
                source.get("value", ""),
                raw=source.get("raw", ""),
                formula="",
            )


def _clear_range(sheet: dict, start: str, end: str) -> None:
    start_row, start_col = coordinate_to_tuple(start)
    end_row, end_col = coordinate_to_tuple(end)
    for row_idx in range(start_row, end_row + 1):
        for col_idx in range(start_col, end_col + 1):
            sheet.get("cells", {}).pop(f"{get_column_letter(col_idx)}{row_idx}", None)


def _sort_rows(sheet: dict, start_row: int, end_row: int, start_col: str, end_col: str, key_col: str) -> None:
    start_col_idx = column_index_from_string(start_col)
    end_col_idx = column_index_from_string(end_col)
    key_col_idx = column_index_from_string(key_col)
    rows = []
    for row_idx in range(start_row, end_row + 1):
        pack = []
        for col_idx in range(start_col_idx, end_col_idx + 1):
            pack.append(sheet.get("cells", {}).get(f"{get_column_letter(col_idx)}{row_idx}", {}).copy())
        rows.append(pack)
    key_offset = key_col_idx - start_col_idx
    rows.sort(key=lambda row: (row[key_offset].get("value", "") == "", row[key_offset].get("value", "")))
    for row_idx in range(start_row, end_row + 1):
        for col_idx in range(start_col_idx, end_col_idx + 1):
            sheet.get("cells", {}).pop(f"{get_column_letter(col_idx)}{row_idx}", None)
    for row_offset, pack in enumerate(rows):
        target_row = start_row + row_offset
        for col_offset, cell in enumerate(pack):
            if not cell:
                continue
            _set_cell(
                sheet,
                target_row,
                start_col_idx + col_offset,
                cell.get("value", ""),
                raw=cell.get("raw", ""),
                formula=cell.get("formula", ""),
            )


def _macro_buttons(sheet_name: str) -> list[dict[str, str]]:
    buttons = []
    if sheet_name == "VOLUMETRIA":
        buttons.extend([{"action": "volumen", "label": "Volumen"}, {"action": "borrar", "label": "Borrar"}])
    if sheet_name == "RESUMEN":
        buttons.extend([{"action": "resumen", "label": "Resumen"}, {"action": "borra_res", "label": "Borra_Res"}])
    return buttons


@pu_bp.route("/")
@login_required
def obras_index():
    manifest = _load_manifest()
    return render_template(
        "pu_index.html",
        title="Precios Unitarios",
        workbook_name="Datos nativos APU",
        sheets=manifest.get("sheets", []),
    )


@pu_bp.route("/sheet/<sheet_name>")
@login_required
def sheet_view(sheet_name: str):
    with PAYLOAD_LOCK:
        manifest = _load_manifest()
        meta = _sheet_meta(sheet_name)
        if not meta:
            flash("La hoja solicitada no existe en Precios Unitarios.", "warning")
            return redirect(url_for("pu.obras_index"))
        sheet = _load_sheet(sheet_name)
        default_row = 1 if sheet_name != "MANO DE OBRA" else 80
        start_row = max(1, int(request.args.get("row", default_row)))
        start_col = max(1, int(request.args.get("col", 1)))
        rows = max(5, min(40, int(request.args.get("rows", DEFAULT_ROWS))))
        cols = max(5, min(18, int(request.args.get("cols", DEFAULT_COLS))))
        header_cols, grid_rows, end_row, end_col = _window(sheet, start_row, start_col, rows, cols)
        pesos_input = (request.args.get("pesos") or "").strip()
        pesos_result = _pesos(pesos_input) if pesos_input else ""

    return render_template(
        "pu_sheet.html",
        title=f"Precios Unitarios - {sheet_name}",
        workbook_name="Datos nativos APU",
        sheets=[s["name"] for s in manifest.get("sheets", [])],
        hidden_sheets={s["name"] for s in manifest.get("sheets", []) if s.get("hidden")},
        freeze_panes=meta.get("freeze_panes") or None,
        sheet_name=sheet_name,
        header_cols=header_cols,
        grid_rows=grid_rows,
        min_row=1,
        min_col=1,
        max_row=meta.get("max_row", 1),
        max_col=meta.get("max_col", 1),
        start_row=start_row,
        start_col=start_col,
        end_row=end_row,
        end_col=end_col,
        rows=rows,
        cols=cols,
        macro_buttons=_macro_buttons(sheet_name),
        pesos_input=pesos_input,
        pesos_result=pesos_result,
    )


@pu_bp.route("/action/<action_name>", methods=["POST"])
@login_required
def run_action(action_name: str):
    with PAYLOAD_LOCK:
        sheet_name = (request.form.get("sheet_name") or "PORTADA").strip() or "PORTADA"
        if action_name == "reset":
            for item in _load_manifest().get("sheets", []):
                source = PU_SHEETS_DIR / item["file"]
                # La carpeta apu_sheets es ahora la fuente runtime ligera. En Render se restaura redeployando.
                if not source.exists():
                    continue
            SHEET_CACHE.clear()
            flash("Cache de Precios Unitarios limpiado.", "success")
            return redirect(url_for("pu.sheet_view", sheet_name=sheet_name))

        if action_name == "volumen":
            target_sheet_name = "VOLUMETRIA"
            sheet = _load_sheet(target_sheet_name)
            _copy_range_values(sheet, "Z21", "AP1020", "BK21")
            _sort_rows(sheet, 21, 1020, "BK", "CA", "BK")
            _write_sheet(target_sheet_name, sheet)
            flash("Macro Volumen aplicada.", "success")
        elif action_name == "borrar":
            target_sheet_name = "VOLUMETRIA"
            sheet = _load_sheet(target_sheet_name)
            _clear_range(sheet, "AA21", "AP1020")
            _clear_range(sheet, "BK21", "CA1020")
            _write_sheet(target_sheet_name, sheet)
            flash("Macro Borrar aplicada.", "success")
        elif action_name == "resumen":
            target_sheet_name = "RESUMEN"
            sheet = _load_sheet(target_sheet_name)
            _copy_range_values(sheet, "AA21", "AH1020", "AQ21")
            _sort_rows(sheet, 21, 1020, "AQ", "AX", "AX")
            _write_sheet(target_sheet_name, sheet)
            flash("Macro Resumen aplicada.", "success")
        elif action_name == "borra_res":
            target_sheet_name = "RESUMEN"
            sheet = _load_sheet(target_sheet_name)
            _clear_range(sheet, "AQ21", "AX1020")
            _write_sheet(target_sheet_name, sheet)
            flash("Macro Borra_Res aplicada.", "success")
        else:
            flash("Acción no reconocida.", "warning")
            return redirect(url_for("pu.sheet_view", sheet_name=sheet_name))

    return redirect(url_for("pu.sheet_view", sheet_name=sheet_name))


@pu_bp.route("/update-cell", methods=["POST"])
@login_required
def update_cell():
    with PAYLOAD_LOCK:
        sheet_name = (request.form.get("sheet_name") or "").strip()
        cell_ref = (request.form.get("cell_ref") or "").strip().upper()
        cell_value = request.form.get("cell_value", "")
        if not sheet_name or not cell_ref:
            flash("Indica hoja y celda para actualizar.", "warning")
            return redirect(url_for("pu.obras_index"))
        sheet = _load_sheet(sheet_name)
        row_idx, col_idx = coordinate_to_tuple(cell_ref)
        _set_cell(sheet, row_idx, col_idx, cell_value, raw=cell_value, formula=cell_value if cell_value.startswith("=") else "")
        _write_sheet(sheet_name, sheet)
        flash(f"Celda {cell_ref} actualizada.", "success")
    return redirect(url_for("pu.sheet_view", sheet_name=sheet_name))


@pu_bp.route("/download-xlsm")
@login_required
def download_template_xlsm():
    return send_file(PU_XLSM_PATH, as_attachment=True, download_name=PU_XLSM_PATH.name)
