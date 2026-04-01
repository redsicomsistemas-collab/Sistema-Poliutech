import sqlite3

from app.runtime import DB_PATH


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_path TEXT NOT NULL,
            image_summary TEXT NOT NULL,
            user_guide TEXT NOT NULL,
            facebook_copy TEXT NOT NULL,
            linkedin_copy TEXT NOT NULL,
            hashtags TEXT NOT NULL,
            cta TEXT NOT NULL,
            alt_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            publish_result TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()
    conn.close()
