from pathlib import Path
import shutil
import uuid

from fastapi import APIRouter, Form, HTTPException, UploadFile
from pydantic import BaseModel

from app.db import get_connection, init_db
from app.runtime import UPLOADS_DIR
from app.services.ai import generate_content
from app.services.publisher import publish_to_selected_networks


router = APIRouter(prefix="/api/mobile", tags=["mobile"])
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
init_db()


class DraftPayload(BaseModel):
    id: int
    image_path: str
    image_url: str
    image_summary: str
    user_guide: str
    facebook_copy: str
    linkedin_copy: str
    hashtags: str
    cta: str
    alt_text: str
    status: str
    publish_result: str
    created_at: str


class GeneratedDirectPayload(BaseModel):
    user_guide: str
    image_summary: str
    facebook_copy: str
    linkedin_copy: str
    hashtags: str
    cta: str
    alt_text: str


class UpdateDraftRequest(BaseModel):
    facebook_copy: str
    linkedin_copy: str


class PublishDraftRequest(BaseModel):
    publish_facebook: bool = False
    publish_linkedin: bool = False


class PublishDraftResponse(BaseModel):
    status: str
    result: str


class HealthResponse(BaseModel):
    status: str


class DeleteResponse(BaseModel):
    deleted: bool


class GenerateResponse(BaseModel):
    message: str
    draft: DraftPayload


class DraftListResponse(BaseModel):
    drafts: list[DraftPayload]


class DraftDetailResponse(BaseModel):
    draft: DraftPayload


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/drafts", response_model=DraftListResponse)
def list_drafts() -> DraftListResponse:
    conn = get_connection()
    drafts = conn.execute("SELECT * FROM drafts ORDER BY id DESC LIMIT 50").fetchall()
    conn.close()
    return DraftListResponse(drafts=[_serialize_draft(draft) for draft in drafts])


@router.get("/drafts/{draft_id}", response_model=DraftDetailResponse)
def get_draft(draft_id: int) -> DraftDetailResponse:
    conn = get_connection()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    conn.close()
    if not draft:
        raise HTTPException(status_code=404, detail="Borrador no encontrado")
    return DraftDetailResponse(draft=_serialize_draft(draft))


@router.post("/generate", response_model=GenerateResponse)
async def generate_draft(
    image: UploadFile,
    focus: str = Form(""),
    tone: str = Form(""),
    intention: str = Form(""),
    extra_notes: str = Form(""),
) -> GenerateResponse:
    user_guide = _compose_user_guide(
        focus=focus,
        tone=tone,
        intention=intention,
        extra_notes=extra_notes,
    )

    saved_name, saved_path = _save_upload(image)
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
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    conn.close()

    if not draft:
        raise HTTPException(status_code=500, detail="No fue posible recuperar el borrador generado")

    return GenerateResponse(
        message="Borrador generado correctamente.",
        draft=_serialize_draft(draft),
    )


@router.post("/generate-direct", response_model=GeneratedDirectPayload)
async def generate_direct(
    image: UploadFile,
    focus: str = Form(""),
    tone: str = Form(""),
    intention: str = Form(""),
    extra_notes: str = Form(""),
) -> GeneratedDirectPayload:
    user_guide = _compose_user_guide(
        focus=focus,
        tone=tone,
        intention=intention,
        extra_notes=extra_notes,
    )
    _, saved_path = _save_upload(image)
    generated = generate_content(saved_path, user_guide)

    return GeneratedDirectPayload(
        user_guide=user_guide,
        image_summary=generated.image_summary,
        facebook_copy=generated.facebook_copy,
        linkedin_copy=generated.linkedin_copy,
        hashtags=generated.hashtags,
        cta=generated.cta,
        alt_text=generated.alt_text,
    )


@router.put("/drafts/{draft_id}", response_model=DraftDetailResponse)
def update_draft(draft_id: int, payload: UpdateDraftRequest) -> DraftDetailResponse:
    conn = get_connection()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    if not draft:
        conn.close()
        raise HTTPException(status_code=404, detail="Borrador no encontrado")

    conn.execute(
        "UPDATE drafts SET facebook_copy = ?, linkedin_copy = ? WHERE id = ?",
        (payload.facebook_copy, payload.linkedin_copy, draft_id),
    )
    conn.commit()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    conn.close()
    return DraftDetailResponse(draft=_serialize_draft(draft))


@router.post("/drafts/{draft_id}/publish", response_model=PublishDraftResponse)
def publish_draft(draft_id: int, payload: PublishDraftRequest) -> PublishDraftResponse:
    conn = get_connection()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    if not draft:
        conn.close()
        raise HTTPException(status_code=404, detail="Borrador no encontrado")

    result = publish_to_selected_networks(
        publish_facebook=payload.publish_facebook,
        publish_linkedin=payload.publish_linkedin,
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
    return PublishDraftResponse(status=status, result=result)


@router.post("/publish-direct", response_model=PublishDraftResponse)
async def publish_direct(
    image: UploadFile,
    facebook_copy: str = Form(""),
    linkedin_copy: str = Form(""),
    publish_facebook: bool = Form(False),
    publish_linkedin: bool = Form(False),
) -> PublishDraftResponse:
    _, saved_path = _save_upload(image)
    result = publish_to_selected_networks(
        publish_facebook=publish_facebook,
        publish_linkedin=publish_linkedin,
        facebook_copy=facebook_copy,
        linkedin_copy=linkedin_copy,
        image_path=str(saved_path),
    )
    status = "published" if "fallo" not in result.lower() else "error"
    return PublishDraftResponse(status=status, result=result)


@router.delete("/drafts/{draft_id}", response_model=DeleteResponse)
def delete_draft(draft_id: int) -> DeleteResponse:
    conn = get_connection()
    draft = conn.execute("SELECT * FROM drafts WHERE id = ?", (draft_id,)).fetchone()
    if not draft:
        conn.close()
        return DeleteResponse(deleted=False)

    image_path = UPLOADS_DIR / draft["image_path"]
    if image_path.exists():
        image_path.unlink()
    conn.execute("DELETE FROM drafts WHERE id = ?", (draft_id,))
    conn.commit()
    conn.close()
    return DeleteResponse(deleted=True)


def _compose_user_guide(*, focus: str, tone: str, intention: str, extra_notes: str) -> str:
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


def _save_upload(image: UploadFile) -> tuple[str, Path]:
    file_suffix = Path(image.filename or "image.jpg").suffix or ".jpg"
    saved_name = f"{uuid.uuid4().hex}{file_suffix}"
    saved_path = UPLOADS_DIR / saved_name
    with saved_path.open("wb") as buffer:
        shutil.copyfileobj(image.file, buffer)
    return saved_name, saved_path


def _serialize_draft(draft) -> DraftPayload:
    image_name = draft["image_path"]
    return DraftPayload(
        id=draft["id"],
        image_path=image_name,
        image_url=f"/uploads/{image_name}",
        image_summary=draft["image_summary"],
        user_guide=draft["user_guide"],
        facebook_copy=draft["facebook_copy"],
        linkedin_copy=draft["linkedin_copy"],
        hashtags=draft["hashtags"],
        cta=draft["cta"],
        alt_text=draft["alt_text"],
        status=draft["status"],
        publish_result=draft["publish_result"],
        created_at=draft["created_at"],
    )
