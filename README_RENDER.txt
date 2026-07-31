Sistema Poliutech — Restaurado (MARWHATS checkpoint)

Deploy en Render:
Build: pip install -r requirements.txt
Start: python app.py

Python: definido en runtime.txt (python-3.11.9)

Variables de WhatsApp Cloud API:
META_WHATSAPP_ACCESS_TOKEN
META_WHATSAPP_PHONE_NUMBER_ID
META_WHATSAPP_TEMPLATE_NAME=mar_notificacion
META_WHATSAPP_TEMPLATE_LANGUAGE=es_MX
META_GRAPH_API_VERSION=v23.0
ADMIN_WHATSAPP_RECIPIENTS

Respaldo temporal por SMS:
TWILIO_ACCOUNT_SID
TWILIO_AUTH_TOKEN
TWILIO_SMS_FROM
# Alternativa a TWILIO_SMS_FROM:
TWILIO_MESSAGING_SERVICE_SID
