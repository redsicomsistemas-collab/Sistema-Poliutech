# MAR Productivy Analytics

Aplicación local y centralizada de analítica laboral para Windows 10 y 11. Incluye servidor/panel, agente Windows e instaladores Inno Setup.

## Render

El servicio usa `PORT` automáticamente. Configure estos secretos en Render:

- `MAR_ADMIN_PASSWORD`: contraseña inicial del administrador.
- `MAR_PUBLIC_URL`: URL HTTPS completa, por ejemplo `https://mar-productivy-analytics.onrender.com`.
- `MAR_DATA_PATH=/var/data`.

El `render.yaml` declara un disco persistente en `/var/data`. No ejecute el servidor sin PostgreSQL o disco persistente: el sistema de archivos normal de Render es efímero.

## Desarrollo local

```powershell
dotnet run --project server/MAR.Productivy.Analytics.Server
```

Panel: `http://localhost:5080`. El agente se compila desde `agent/WorkPulse.Agent` y los instaladores desde `installer` con Inno Setup 6.

No confirme contraseñas, tokens, `agent.json`, datos de empleados, respaldos ni archivos de `C:\ProgramData` al repositorio.
