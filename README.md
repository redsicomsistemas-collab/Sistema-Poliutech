# M.A.R. – Sistema Poliutech (Flask)

## Instalación rápida (Windows PowerShell)
```powershell
cd MAR_web
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
$env:FLASK_APP="app.py"
flask run
```

- DB fija: `instance/mar.db` (incluida vacía).
- Exportación: PDF (reportlab) y Excel (pandas+openpyxl). Si no están, se crean archivos de fallback.

## Push móvil para pendientes
- Backend: instalar `firebase-admin` desde `requirements.txt`.
- Configurar `FIREBASE_CREDENTIALS_FILE` con la ruta al JSON de service account de Firebase o `FIREBASE_CREDENTIALS_JSON` con el contenido JSON.
- App Android: agregar `google-services.json` dentro de `android-registro-obras/app/` para que Firebase pueda emitir el token del dispositivo.
- Endpoint móvil para registrar el dispositivo: `POST /api/mobile/push-token` con bearer token del login móvil.
