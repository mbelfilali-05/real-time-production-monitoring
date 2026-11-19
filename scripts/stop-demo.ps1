
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$pidFile = Join-Path $root "logs\streamlit.pid"

Write-Host "Arret des services Docker..." -ForegroundColor Cyan

docker compose `
    -f docker-compose.yml `
    -f docker-compose.demo.yml `
    stop

if ($LASTEXITCODE -ne 0) {
    Write-Warning "Certains services Docker n'ont pas pu etre arretes."
}

if (Test-Path $pidFile) {
    $savedPid = Get-Content $pidFile

    if ($savedPid -match '^\d+$') {
        $process = Get-Process -Id ([int]$savedPid) `
            -ErrorAction SilentlyContinue

        if ($process -and $process.ProcessName -match "python") {
            Stop-Process -Id $process.Id -Force
            Write-Host "Streamlit arrete."
        }
    }

    Remove-Item $pidFile -Force
}

Write-Host "Arret termine." -ForegroundColor Green
