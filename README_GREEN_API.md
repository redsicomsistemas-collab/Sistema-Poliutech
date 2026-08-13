# WhatsApp interno mediante GREEN-API

Esta integración opcional conserva correo, Firebase push, Meta y SMS. Utiliza
una sesión vinculada de WhatsApp Business para avisos internos de bajo volumen.

## Variables de Render

```text
WHATSAPP_PROVIDER=green_api
GREEN_API_ID_INSTANCE=<idInstance>
GREEN_API_TOKEN_INSTANCE=<apiTokenInstance>
WHATSAPP_DAILY_LIMIT=20
```

El número dedicado debe aparecer como `authorized` en
https://console.green-api.com/. Los secretos se guardan únicamente en Render.

El plan Developer admite hasta tres chats. No debe utilizarse para campañas,
clientes ni envíos masivos. El servidor aplica además un límite de envíos por
24 horas para proteger la cuenta frente a procesos repetitivos.

## Prueba

Después del deploy, iniciar sesión como Admin en Sistema MAR y abrir
`/debug/send_test`. El destinatario debe estar configurado en Administración >
Notificaciones o en `ADMIN_WHATSAPP_RECIPIENTS`.

GREEN-API utiliza una sesión vinculada de WhatsApp Web y no la API oficial de
Meta; WhatsApp puede cerrar la sesión o restringir el número. Se deben conservar
correo y push como respaldo.
