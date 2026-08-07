# MAR Productivy Analytics Agent para Windows

Agente visible y auditable para Windows 10/11. Registra la aplicación en primer plano, título de ventana (opcional), duración e inactividad. No registra teclas, texto escrito, archivos, audio, cámara ni contenido de pantalla.

Publicación recomendada:

```powershell
dotnet publish .\WorkPulse.Agent\WorkPulse.Agent.csproj -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true
```

Copie `agent.example.json`, coloque las credenciales emitidas por el servidor y ejecute `install.ps1` como administrador.
