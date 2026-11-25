# Gera dist\Arquipelago.exe (executável único, não precisa de Python nos laboratórios).
# Uso (na raiz do projeto):  powershell -ExecutionPolicy Bypass -File empacotamento\gerar_exe.ps1

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    python -m venv .venv
}
.\.venv\Scripts\python.exe -m pip install -q -r requirements.txt pyinstaller

.\.venv\Scripts\python.exe -m PyInstaller --noconfirm --clean --onefile --console `
    --name Arquipelago `
    --paths src `
    --collect-submodules arquipelago `
    --exclude-module matplotlib --exclude-module tkinter --exclude-module pytest `
    --distpath dist --workpath build\pyinstaller --specpath build `
    empacotamento\arquipelago_exe.py

Copy-Item empacotamento\liberar_firewall.bat dist\ -Force
Copy-Item empacotamento\simular_3_nos_neste_pc.bat dist\ -Force
Copy-Item empacotamento\peers_exemplo.txt dist\ -Force
Copy-Item empacotamento\LEIA-ME.txt dist\ -Force
Write-Host "Pronto: dist\Arquipelago.exe"
