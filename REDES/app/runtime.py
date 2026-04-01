from __future__ import annotations

import sys
from pathlib import Path


if getattr(sys, "frozen", False):
    APP_DIR = Path(sys.executable).resolve().parent
    RESOURCE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))
else:
    APP_DIR = Path.cwd()
    RESOURCE_DIR = Path(__file__).resolve().parent.parent


UPLOADS_DIR = APP_DIR / "uploads"
DB_PATH = APP_DIR / "social_copy.db"
ENV_PATH = APP_DIR / ".env"
STATIC_DIR = RESOURCE_DIR / "static"
TEMPLATES_DIR = RESOURCE_DIR / "templates"
