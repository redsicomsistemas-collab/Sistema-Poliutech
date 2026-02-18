import os
from datetime import datetime, timedelta
from twilio.rest import Client
from app import app, db, Cotizacion, Cliente

# =========================
# CONFIGURACIÓN TWILIO
# =========================
TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID") or "TU_SID_AQUI"
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN") or "TU_TOKEN_AQUI"
TWILIO_FROM = os.environ.get("TWILIO_WHATSAPP_FROM") or "whatsapp:+14155238886"  # número Twilio Sandbox

client = Client(TWILIO_SID, TWILIO_TOKEN)

# =========================
# FUNCIÓN DE ENVÍO
# =========================
def enviar_notificacion(cliente_nombre, telefono, folio):
    if not telefono:
        print(f"[⚠️] {cliente_nombre} no tiene teléfono registrado, se omite.")
        return
    
    mensaje = (
        f"Hola {cliente_nombre}, te recordamos que tu cotización {folio} "
        "sigue en estado PENDIENTE. Si necesitas seguimiento, contáctanos. "
        "— Sistema Poliutech MAR5."
    )

    try:
        client.messages.create(
            from_=TWILIO_FROM,
            to=f"whatsapp:+52{telefono}",
            body=mensaje
        )
        print(f"[✅] Notificación enviada a {cliente_nombre} ({telefono}) — Cotización {folio}")
    except Exception as e:
        print(f"[❌] Error enviando mensaje a {cliente_nombre}: {e}")

# =========================
# LÓGICA PRINCIPAL
# =========================
def revisar_pendientes():
    with app.app_context():
        limite = datetime.utcnow() - timedelta(days=2)
        cotizaciones = Cotizacion.query.filter(
            Cotizacion.estatus == "PENDIENTE",
            Cotizacion.fecha <= limite
        ).all()

        print(f"[⏰] Revisando cotizaciones pendientes ({len(cotizaciones)} encontradas)...")

        for cot in cotizaciones:
            if cot.cliente and cot.cliente.telefono:
                enviar_notificacion(
                    cliente_nombre=cot.cliente.nombre_cliente,
                    telefono=cot.cliente.telefono,
                    folio=cot.folio
                )

if __name__ == "__main__":
    revisar_pendientes()
