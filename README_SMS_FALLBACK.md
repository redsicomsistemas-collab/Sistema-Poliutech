# Respaldo temporal por SMS

Mientras Meta termina la revisión del remitente de WhatsApp, MAR intenta enviar
cada aviso por Meta y, si ese envío falla, utiliza automáticamente Twilio SMS.
Las notificaciones push existentes siguen funcionando en los módulos que ya
cuentan con destinatarios móviles.

## Variables de Render

```text
TWILIO_ACCOUNT_SID=<Account SID>
TWILIO_AUTH_TOKEN=<Auth Token>
TWILIO_SMS_FROM=+1XXXXXXXXXX
```

`TWILIO_SMS_FROM` debe ser un número comprado en la cuenta de Twilio con
capacidad SMS. El número compartido del Sandbox de WhatsApp no funciona como
remitente SMS.

Si la cuenta usa un Messaging Service, se puede configurar en su lugar:

```text
TWILIO_MESSAGING_SERVICE_SID=MGXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

No se deben configurar simultáneamente `TWILIO_SMS_FROM` y
`TWILIO_MESSAGING_SERVICE_SID`; si existen ambos, MAR utiliza el Messaging
Service.

Cuando Meta apruebe el número no hay que desplegar otro cambio: MAR comenzará a
usar WhatsApp y conservará SMS únicamente como respaldo ante errores.
