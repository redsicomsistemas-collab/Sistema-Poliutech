from dataclasses import dataclass
import os

from app.runtime import ENV_PATH


def load_dotenv() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv()


@dataclass
class Settings:
    app_env: str = os.getenv("APP_ENV", "development")
    secret_key: str = os.getenv("SECRET_KEY", "change-me")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    use_real_ai: bool = os.getenv("USE_REAL_AI", "0") == "1"
    facebook_page_id: str = os.getenv("FACEBOOK_PAGE_ID", "")
    facebook_page_access_token: str = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "")
    linkedin_author_urn: str = os.getenv("LINKEDIN_AUTHOR_URN", "")
    linkedin_access_token: str = os.getenv("LINKEDIN_ACCESS_TOKEN", "")


settings = Settings()
