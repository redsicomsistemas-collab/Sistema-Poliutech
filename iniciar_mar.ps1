$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venvPython = Join-Path $root ".venv\Scripts\python.exe"
if (Test-Path $venvPython) {
    $python = $venvPython
} else {
    $python = "python"
}

$env:FLASK_APP = "app.py"
$env:FLASK_RUN_HOST = "127.0.0.1"
$env:FLASK_RUN_PORT = "5000"

$url = "http://127.0.0.1:5000"

try {
    $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
    if ($response.StatusCode -ge 200) {
        Start-Process $url
        exit 0
    }
} catch {
    # MAR is not running yet, so start Flask below.
}

Start-Job -ScriptBlock {
    param($targetUrl)
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $response = Invoke-WebRequest -Uri $targetUrl -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -ge 200) {
                Start-Process $targetUrl
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
} -ArgumentList $url | Out-Null

Write-Host ""
Write-Host "MAR esta iniciando en $url"
Write-Host "Deja esta ventana abierta mientras uses el sistema."
Write-Host ""

& $python -m flask run --host 127.0.0.1 --port 5000
