from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes import api, ui
from app.runtime import STATIC_DIR, UPLOADS_DIR


app = FastAPI(title="Social Copy Pilot")
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")
app.include_router(ui.router)
app.include_router(api.router)
