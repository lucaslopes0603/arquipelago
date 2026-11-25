@echo off
REM Libera o Arquipelago no Firewall do Windows (precisa ser executado como ADMINISTRADOR).
REM Clique com o botao direito neste arquivo -> "Executar como administrador".

net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Este script precisa ser executado como administrador.
    echo Clique com o botao direito -^> "Executar como administrador".
    pause
    exit /b 1
)

netsh advfirewall firewall delete rule name="Arquipelago" >nul 2>&1
netsh advfirewall firewall add rule name="Arquipelago" dir=in action=allow program="%~dp0Arquipelago.exe" enable=yes profile=any
netsh advfirewall firewall add rule name="Arquipelago" dir=in action=allow protocol=TCP localport=50051-50100 enable=yes profile=any
netsh advfirewall firewall add rule name="Arquipelago" dir=in action=allow protocol=UDP localport=50050 enable=yes profile=any
echo.
echo Regras criadas: TCP 50051-50100 (gRPC) e UDP 50050 (descoberta).
pause
