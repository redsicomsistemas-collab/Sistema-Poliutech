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
