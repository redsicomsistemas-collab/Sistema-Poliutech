# REDES para Render

Sube este paquete a Render como `Web Service`.

Archivos importantes:
- `app/`
- `static/`
- `templates/`
- `requirements.txt`
- `render.yaml`
- `.env.example`

Variables de entorno en Render:
- `APP_ENV=production`
- `SECRET_KEY=un_valor_seguro`
- `OPENAI_API_KEY=...`
- `OPENAI_MODEL=gpt-5.4-mini`
- `USE_REAL_AI=1`
- `FACEBOOK_PAGE_ID=676415215561622`
- `FACEBOOK_PAGE_ACCESS_TOKEN=...`

Start command:
- `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

No subas:
- `.venv/`
- `android/`
- `dist/`
- `build/`
- APKs
- instaladores
- `.env` real
- bases locales si no quieres reutilizarlas
