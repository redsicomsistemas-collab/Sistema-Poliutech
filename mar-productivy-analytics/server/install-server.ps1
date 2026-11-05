$ErrorActionPreference = 'Stop'
$source = $PSScriptRoot
$target = Join-Path $env:ProgramFiles 'MAR Productivy Analytics Server'
if (-not (Test-Path (Join-Path $source 'MAR.Productivy.Analytics.Server.exe'))) { throw 'Primero publique el servidor o use el paquete preparado.' }
New-Item -ItemType Directory -Path $target -Force | Out-Null
Copy-Item -Path (Join-Path $source '*') -Destination $target -Recurse -Force
$exe = Join-Path $target 'MAR.Productivy.Analytics.Server.exe'
New-NetFirewallRule -DisplayName 'MAR Productivy Analytics Server' -Direction Inbound -Protocol TCP -LocalPort 5080 -Action Allow -ErrorAction SilentlyContinue | Out-Null
$action = New-ScheduledTaskAction -Execute $exe
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName 'MAR Productivy Analytics Server' -Action $action -Trigger $trigger -Principal $principal -Description 'Servidor local de MAR Productivy Analytics.' -Force | Out-Null
Start-ScheduledTask -TaskName 'MAR Productivy Analytics Server'
Write-Host 'Servidor instalado. Abra http://localhost:5080'
