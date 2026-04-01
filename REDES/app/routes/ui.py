from pathlib import Path
import shutil
import uuid
from urllib.parse import quote_plus, unquote_plus

from fastapi import APIRouter, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.db import get_connection, init_db
from app.runtime import TEMPLATES_DIR, UPLOADS_DIR
from app.services.ai import generate_content
from app.services.publisher import publish_to_selected_networks


router = APIRouter()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
init_db()


def _compose_user_guide(
    *,
    focus: str,
    tone: str,
    intention: str,
    extra_notes: str,
) -> str:
    parts: list[str] = []
    if focus.strip():
        parts.append(f"Enfoque: {focus.strip()}")
    if tone.strip():
        parts.append(f"Tono: {tone.strip()}")
    if intention.strip():
        parts.append(f"Intencion: {intention.strip()}")
    if extra_notes.strip():
        parts.append(f"Indicaciones extra: {extra_notes.strip()}")
    return " | ".join(parts)


@router.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    conn = get_connection()
    drafts = conn.execute(
        "SELECT * FROM drafts ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()
    message = request.query_params.get("message", "")
    result_status = request.query_params.get("result_status", "")
    result_text = request.query_params.get("result_text", "")
    result_text = unquote_plus(result_text) if result_text else ""
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "drafts": drafts,
            "detail": None,
            "message": message,
            "result_status": result_status,
            "result_text": result_text,
        },
    )


@router.post("/generate", response_class=HTMLResponse)
async def generate(
    request: Request,
    image: UploadFile,
    focus: str = Form(""),
    tone: str = Form(""),
    intention: str = Form(""),
    extra_notes: str = Form(""),
) -> HTMLResponse:
    user_guide = _compose_user_guide(
        focus=focus,
        tone=tone,
        intention=intention,
        extra_notes=extra_notes,
    )

    file_suffix = Path(image.filename or "image.jpg").suffix or ".jpg"
    saved_name = f"{uuid.uuid4().hex}{file_suffix}"
    saved_path = UPLOADS_DIR / saved_name
    with saved_path.open("wb") as buffer:
        shutil.copyfileobj(image.file, buffer)

    generated = generate_content(saved_path, user_guide)

    conn = get_connection()
    cur = conn.execute(
        """
        INSERT INTO drafts (
            image_path, image_summary, user_guide,
            facebook_copy, linkedin_copy, hashtags, cta, alt_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            saved_name,
            generated.image_summary,
            user_guide,
            generated.facebook_copy,
            generated.linkedin_copy,
            generated.hashtags,
            generated.cta,
            generated.alt_text,
        ),
    )
    draft_id = cur.lastrowid
    conn.commit()
    drafts = conn.execute(
        "SELECT * FROM drafts ORDER BY id DESC LIMIT 20"
    ).fetchall()
    detail = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    conn.close()

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "drafts": drafts,
            "detail": detail,
            "message": "Borrador generado correctamente.",
            "result_status": "",
            "result_text": "",
        },
    )


@router.get("/drafts/{draft_id}", response_class=HTMLResponse)
def draft_detail(request: Request, draft_id: int) -> HTMLResponse:
    conn = get_connection()
    drafts = conn.execute(
        "SELECT * FROM drafts ORDER BY id DESC LIMIT 20"
    ).fetchall()
    detail = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    conn.close()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "drafts": drafts,
            "detail": detail,
            "message": "",
            "result_status": "",
            "result_text": "",
        },
    )


@router.post("/drafts/{draft_id}/update")
def update_draft(
    draft_id: int,
    facebook_copy: str = Form(...),
    linkedin_copy: str = Form(...),
) -> RedirectResponse:
    conn = get_connection()
    conn.execute(
        "UPDATE drafts SET facebook_copy = ?, linkedin_copy = ? WHERE id = ?",
        (facebook_copy, linkedin_copy, draft_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/drafts/{draft_id}", status_code=303)


@router.post("/drafts/{draft_id}/publish")
def publish_draft(
    draft_id: int,
    publish_facebook: str | None = Form(default=None),
    publish_linkedin: str | None = Form(default=None),
) -> RedirectResponse:
    conn = get_connection()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    result = publish_to_selected_networks(
        publish_facebook=publish_facebook == "on",
        publish_linkedin=publish_linkedin == "on",
        facebook_copy=draft["facebook_copy"],
        linkedin_copy=draft["linkedin_copy"],
        image_path=str(UPLOADS_DIR / draft["image_path"]),
    )
    status = "published" if "fallo" not in result.lower() else "error"
    conn.execute(
        "UPDATE drafts SET status = ?, publish_result = ? WHERE id = ?",
        (status, result, draft_id),
    )
    conn.commit()
    conn.close()
    encoded_result = quote_plus(result)
    return RedirectResponse(
        url=f"/?message=Listo%20para%20crear%20otro%20borrador.&result_status={status}&result_text={encoded_result}",
        status_code=303,
    )


@router.post("/drafts/{draft_id}/delete")
def delete_draft(draft_id: int) -> RedirectResponse:
    conn = get_connection()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    if draft:
        image_path = UPLOADS_DIR / draft["image_path"]
        if image_path.exists():
            image_path.unlink()
        conn.execute("DELETE FROM drafts WHERE id = ?", (draft_id,))
        conn.commit()
    conn.close()
    return RedirectResponse(url="/?message=Borrador%20eliminado.", status_code=303)
