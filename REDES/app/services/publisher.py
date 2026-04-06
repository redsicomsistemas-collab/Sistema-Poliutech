from __future__ import annotations

from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import mimetypes
import uuid

from app.config import settings


FACEBOOK_GRAPH_BASE = "https://graph.facebook.com/v25.0"


def publish_to_selected_networks(
    *,
    publish_facebook: bool,
    publish_linkedin: bool,
    facebook_copy: str,
    linkedin_copy: str,
    image_path: str,
) -> str:
    results: list[str] = []

    if publish_facebook:
        if settings.facebook_page_id and settings.facebook_page_access_token:
            try:
                facebook_result = _publish_facebook_feed_post(
                    page_id=settings.facebook_page_id,
                    access_token=settings.facebook_page_access_token,
                    caption=facebook_copy,
                    image_path=Path(image_path),
                )
                results.append(facebook_result)
            except (HTTPError, URLError, OSError, ValueError) as exc:
                results.append(f"Facebook fallo: {_format_error(exc)}")
        else:
            results.append("Facebook en simulacion: faltan FACEBOOK_PAGE_ID o FACEBOOK_PAGE_ACCESS_TOKEN.")

    if publish_linkedin:
        if settings.linkedin_author_urn and settings.linkedin_access_token:
            results.append(
                f"LinkedIn listo para integracion real con la imagen {image_path}."
            )
        else:
            results.append("LinkedIn en simulacion: faltan credenciales reales.")

    if not results:
        results.append("No se selecciono ninguna red.")

    return " ".join(results)


def _publish_facebook_feed_post(
    *,
    page_id: str,
    access_token: str,
    caption: str,
    image_path: Path,
) -> str:
    if not image_path.exists():
        raise ValueError(f"No se encontro la imagen en {image_path}")

    photo_id = _upload_facebook_photo_unpublished(
        page_id=page_id,
        access_token=access_token,
        image_path=image_path,
        caption=caption,
    )

    feed_payload = urlencode(
        {
            "message": caption,
            "attached_media": json.dumps([{"media_fbid": photo_id}]),
            "access_token": access_token,
        }
    )
    request = Request(
        f"{FACEBOOK_GRAPH_BASE}/{page_id}/feed",
        data=feed_payload.encode("utf-8"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    post_id = payload.get("post_id") or payload.get("id")
    if not post_id:
        raise ValueError(f"Respuesta inesperada de Facebook al crear el feed post: {payload}")
    return (
        "Facebook publicado correctamente por /feed. "
        f"Post ID: {post_id}. "
        f"Photo ID usada: {photo_id}."
    )



def _upload_facebook_photo_unpublished(
    *,
    page_id: str,
    access_token: str,
    image_path: Path,
    caption: str,
) -> str:
    boundary = f"----SocialCopyPilot{uuid.uuid4().hex}"
    mime_type = mimetypes.guess_type(str(image_path))[0] or "application/octet-stream"
    image_bytes = image_path.read_bytes()

    parts: list[bytes] = []
    parts.append(_multipart_field(boundary, "caption", caption))
    parts.append(_multipart_field(boundary, "published", "false"))
    parts.append(_multipart_field(boundary, "access_token", access_token))
    parts.append(
        _multipart_file(
            boundary,
            field_name="source",
            filename=image_path.name,
            mime_type=mime_type,
            content=image_bytes,
        )
    )
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)

    request = Request(
        f"{FACEBOOK_GRAPH_BASE}/{page_id}/photos",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )

    with urlopen(request, timeout=90) as response:
        payload = json.loads(response.read().decode("utf-8"))

    photo_id = payload.get("id")
    if not photo_id:
        raise ValueError(f"Respuesta inesperada al subir foto a Facebook: {payload}")
    return str(photo_id)



def _multipart_field(boundary: str, name: str, value: str) -> bytes:
    return (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
        f"{value}\r\n"
    ).encode("utf-8")



def _multipart_file(
    boundary: str,
    *,
    field_name: str,
    filename: str,
    mime_type: str,
    content: bytes,
) -> bytes:
    header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8")
    return header + content + b"\r\n"



def _format_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        try:
            payload = exc.read().decode("utf-8")
        except Exception:
            payload = str(exc)
        lowered = payload.lower()
        if "publish_actions" in lowered or "permission(s) publish_actions" in lowered:
            return (
                f"HTTP {exc.code}: Facebook rechazo el token actual para publicar. "
                f"Actualiza FACEBOOK_PAGE_ACCESS_TOKEN en Render con el access_token real de la pagina. "
                f"Detalle: {payload}"
            )
        if "session has expired" in lowered or '"error_subcode":463' in lowered:
            return (
                f"HTTP {exc.code}: El token de Facebook vencio. "
                f"Genera uno nuevo para la pagina y actualizalo en Render. "
                f"Detalle: {payload}"
            )
        return f"HTTP {exc.code}: {payload}"
    return str(exc)
