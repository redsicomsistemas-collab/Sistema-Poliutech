from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.config import settings
from app.models import GeneratedContent


OPENAI_API_URL = "https://api.openai.com/v1/responses"


def generate_content(image_path: Path, user_guide: str) -> GeneratedContent:
    if settings.use_real_ai and settings.openai_api_key:
        try:
            content = _generate_with_openai(image_path, user_guide)
            content.image_summary = (
                f"Fuente IA: OpenAI ({settings.openai_model}). "
                f"{content.image_summary}"
            )
            return content
        except (HTTPError, URLError, TimeoutError, ValueError, KeyError, json.JSONDecodeError) as exc:
            return _generate_demo_content(image_path, user_guide, reason=_format_error(exc))

    return _generate_demo_content(
        image_path,
        user_guide,
        reason="No hay API key activa o USE_REAL_AI esta desactivado.",
    )


def _generate_with_openai(image_path: Path, user_guide: str) -> GeneratedContent:
    mime_type = _detect_mime_type(image_path)
    image_b64 = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    prompt = _build_prompt(user_guide)

    payload = {
        "model": settings.openai_model,
        "instructions": (
            "Eres un asistente que interpreta imagenes y redacta copy en espanol. "
            "Obedeces la guia del usuario con precision y devuelves solo JSON valido sin markdown."
        ),
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{image_b64}",
                        "detail": "high",
                    },
                ],
            }
        ],
    }

    request = Request(
        OPENAI_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urlopen(request, timeout=90) as response:
        body = json.loads(response.read().decode("utf-8"))

    output_text = _extract_output_text(body)
    content = json.loads(output_text)
    return GeneratedContent(
        image_summary=content["image_summary"].strip(),
        facebook_copy=content["facebook_copy"].strip(),
        linkedin_copy=content["linkedin_copy"].strip(),
        hashtags=content["hashtags"].strip(),
        cta=content["cta"].strip(),
        alt_text=content["alt_text"].strip(),
    )


def _extract_output_text(body: dict) -> str:
    if body.get("output_text"):
        return body["output_text"]

    for item in body.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                return content["text"]

    raise ValueError("No se encontro texto de salida en la respuesta de OpenAI.")


def _build_prompt(user_guide: str) -> str:
    guide = user_guide.strip() or "describe la imagen con precision y redacta un copy neutro"
    return f"""
Analiza la imagen y redacta copy basado principalmente en lo que realmente se ve y en la guia del usuario.

Guia del usuario:
{guide}

Instrucciones clave:
- Responde en espanol.
- Interpreta primero la imagen con precision.
- La guia del usuario manda sobre el tono, angulo y nivel de intensidad.
- Si la guia pide un tono incisivo, duro, critico, directo o agresivo, usalo.
- Si la guia pide un tono neutro, institucional, serio o informativo, respetalo.
- No conviertas todo en marketing ni en venta a menos que la guia lo pida.
- No inventes hechos, nombres, lugares, cifras o contexto que no aparezcan en la imagen o en la guia.
- No metas hashtags ni CTA si no aportan valor; puedes dejarlos vacios.
- Facebook y LinkedIn pueden tener tonos distintos, pero ambos deben seguir la misma intencion base de la guia.

Devuelve exactamente un objeto JSON con estas llaves:
- image_summary: descripcion precisa y breve de lo que muestra la imagen
- facebook_copy: copy final para Facebook
- linkedin_copy: copy final para LinkedIn
- hashtags: una sola cadena; puede ir vacia si no se necesitan hashtags
- cta: una sola cadena; puede ir vacia si no hace falta llamada a la accion
- alt_text: descripcion util y concreta de la imagen para accesibilidad
""".strip()


def _detect_mime_type(image_path: Path) -> str:
    suffix = image_path.suffix.lower()
    mapping = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
    }
    return mapping.get(suffix, "image/jpeg")


def _format_error(exc: Exception) -> str:
    message = str(exc).strip().replace("\n", " ")
    return message[:220] if message else exc.__class__.__name__


def _generate_demo_content(image_path: Path, user_guide: str, reason: str) -> GeneratedContent:
    guide = user_guide.strip() or "describe la imagen con precision y redacta un copy neutro"
    file_name = image_path.stem.replace("_", " ").strip() or "la imagen"

    summary = (
        f"Fuente IA: DEMO local. Motivo del fallback: {reason}. "
        f"La imagen muestra contenido visual relacionado con {file_name}. "
        f"La guia del usuario enfatiza: {guide}."
    )
    hashtags = ""
    cta = ""
    facebook_copy = (
        f"Interpretacion demo basada en la guia: {guide}. "
        f"Referencia visual detectada: {file_name}."
    )
    linkedin_copy = (
        f"Borrador demo basado en la guia: {guide}. "
        f"Referencia visual detectada: {file_name}."
    )
    alt_text = f"Imagen relacionada con: {guide}."

    return GeneratedContent(
        image_summary=summary,
        facebook_copy=facebook_copy,
        linkedin_copy=linkedin_copy,
        hashtags=hashtags,
        cta=cta,
        alt_text=alt_text,
    )
