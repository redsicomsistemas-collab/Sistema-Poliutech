param(
  [Parameter(Mandatory=$true)][string]$PackagePath,
  [Parameter(Mandatory=$true)][string]$ConfigPath
)
$ErrorActionPreference = 'Stop'
$installPath = Join-Path $env:ProgramFiles 'MAR Productivy Analytics'
if (-not (Test-Path -LiteralPath $PackagePath)) { throw "No existe el paquete: $PackagePath" }
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "No existe la configuración: $ConfigPath" }
New-Item -ItemType Directory -Path $installPath -Force | Out-Null
Copy-Item -LiteralPath $PackagePath -Destination (Join-Path $installPath 'MAR.Productivy.Analytics.Agent.exe') -Force
Copy-Item -LiteralPath $ConfigPath -Destination (Join-Path $installPath 'agent.json') -Force
$action = New-ScheduledTaskAction -Execute (Join-Path $installPath 'MAR.Productivy.Analytics.Agent.exe')
$trigger = New-ScheduledTaskTrigger -AtLogOn
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName 'MAR Productivy Analytics Agent' -Action $action -Trigger $trigger -Principal $principal -Description 'Analítica transparente de uso de aplicaciones para MAR Productivy Analytics.' -Force | Out-Null
Write-Host "MAR Productivy Analytics Agent instalado. Se iniciará al entrar a Windows."
