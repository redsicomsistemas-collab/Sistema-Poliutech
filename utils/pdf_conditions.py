from html import escape
import unicodedata


def _normalize_heading(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return normalized.lower().strip().rstrip(":").strip()


def format_pdf_condition_lines(values: list[object]) -> str:
    """Formatea condiciones, dejando CLÁUSULAS como título sin viñeta."""
    lines = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            lines.append("")
        elif _normalize_heading(text) == "clausulas":
            lines.append(escape(text))
        else:
            lines.append(f"• {escape(text)}")
    return "<br/>".join(lines)
