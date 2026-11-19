
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\Scripts\python.exe"
$logs = Join-Path $root "logs"
$pidFile = Join-Path $logs "streamlit.pid"

New-Item -ItemType Directory -Force -Path $logs |
    Out-Null

if (-not (Test-Path $python)) {
    throw "Environnement Python .venv introuvable."
}

Write-Host "Demarrage de Kafka et Spark..." -ForegroundColor Cyan

docker compose `
    -f docker-compose.yml `
    -f docker-compose.demo.yml `
    up -d --remove-orphans

if ($LASTEXITCODE -ne 0) {
    throw "Echec du demarrage Docker."
}

Write-Host "Verification de Streamlit..." -ForegroundColor Cyan

$alreadyRunning = $false

if (Test-Path $pidFile) {
    $savedPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($savedPid -match '^\d+$') {
        $existing = Get-Process -Id ([int]$savedPid) `
            -ErrorAction SilentlyContinue
        if ($existing -and $existing.ProcessName -match "python") {
            $alreadyRunning = $true
        }
    }
}

if (-not $alreadyRunning) {
    $process = Start-Process `
        -FilePath $python `
        -ArgumentList @(
            "-m", "streamlit", "run",
            "dashboard/app.py",
            "--server.headless=true"
        ) `
        -WorkingDirectory $root `
        -RedirectStandardOutput "$logs\streamlit.out.log" `
        -RedirectStandardError "$logs\streamlit.err.log" `
        -PassThru

    Set-Content -Path $pidFile -Value $process.Id
    Write-Host "Streamlit demarre (PID $($process.Id))"
}
else {
    Write-Host "Streamlit est deja demarre."
}

Write-Host ""
Write-Host "Infrastructure lancee !" -ForegroundColor Green
Write-Host "Dashboard : http://localhost:8501"
Write-Host "Les traitements Spark demarrent en arriere-plan."
Write-Host "Ils peuvent prendre un peu de temps a s'initialiser."
