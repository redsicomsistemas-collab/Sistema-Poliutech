# WhatsApp Cloud API de Meta

MAR envía las notificaciones directamente mediante la Graph API de Meta. Twilio
ya no participa en el envío.

## Plantilla requerida

En WhatsApp Manager crea una plantilla con estos datos:

- Nombre: `mar_notificacion`
- Categoría: `UTILITY`
- Idioma: `Spanish (MEX)`
- Cuerpo:

```text
Sistema MAR

Tipo de aviso: {{1}}

Detalle:
{{2}}

Este mensaje fue generado automáticamente.
```

Agrega ejemplos realistas para ambas variables al enviarla a aprobación. El
nombre y el idioma configurados en Render deben coincidir exactamente con los
de la plantilla aprobada.

## Variables de Render

```text
META_WHATSAPP_ACCESS_TOKEN=<token permanente del usuario del sistema>
META_WHATSAPP_PHONE_NUMBER_ID=<identificador numérico del remitente>
META_WHATSAPP_TEMPLATE_NAME=mar_notificacion
META_WHATSAPP_TEMPLATE_LANGUAGE=es_MX
META_GRAPH_API_VERSION=v23.0
```

`META_WHATSAPP_ACCESS_TOKEN` debe ser un token permanente de un usuario del
sistema de Meta con permisos `whatsapp_business_messaging` y
`whatsapp_business_management`. No se debe usar el token temporal de la pantalla
de inicio rápido.

Después de guardar las variables, ejecuta **Manual Deploy → Deploy latest
commit** en Render. Los logs correctos muestran:

```text
[Meta WhatsApp] Cloud API configurada.
[Meta WhatsApp] Enviado a 52XXXXXXXXXX; id=wamid...
```

Si Meta rechaza un mensaje, MAR registra y muestra el error HTTP de Graph API
para poder diagnosticarlo.
