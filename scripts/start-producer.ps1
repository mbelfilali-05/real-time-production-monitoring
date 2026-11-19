
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\Scripts\python.exe"

Write-Host "Demarrage du simulateur..." -ForegroundColor Cyan
Write-Host "Appuyez sur Ctrl+C pour arreter la production."
Write-Host ""

& $python "simulator/producer.py"
