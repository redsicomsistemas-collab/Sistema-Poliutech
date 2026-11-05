from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi import Request
from fastapi.responses import RedirectResponse
import os

from app.routes import api, ui
from app.runtime import STATIC_DIR, UPLOADS_DIR


app = FastAPI(title="Social Copy Pilot")
MIGRATION_TARGET_URL = os.getenv("MIGRATION_TARGET_URL", "").strip().rstrip("/")
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")
app.include_router(ui.router)
app.include_router(api.router)

@app.middleware("http")
async def redirect_migrated_service(request: Request, call_next):
    if MIGRATION_TARGET_URL:
        suffix = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        return RedirectResponse(f"{MIGRATION_TARGET_URL}{suffix}", status_code=308)
    return await call_next(request)
