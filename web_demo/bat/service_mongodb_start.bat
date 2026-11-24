@echo off
setlocal
title Start MongoDB service

rem ============================================================================
rem  Chay service MongoDB (mongod) tren node nay.
rem  Ten service mac dinh: MongoDB. Doi bien SERVICE ben duoi neu may dat ten khac.
rem  Tu xin quyen Administrator (net start/stop can quyen nay).
rem ============================================================================

net session >nul 2>&1
if errorlevel 1 (
    echo Dang yeu cau quyen Administrator...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

set "SERVICE=MongoDB"

sc query "%SERVICE%" >nul 2>&1
if errorlevel 1 (
    echo [LOI] Khong tim thay service "%SERVICE%" tren may nay.
    echo Xem ten that bang lenh: sc query state= all ^| findstr /i "mongo"
    goto end
)

sc query "%SERVICE%" | find "RUNNING" >nul
if not errorlevel 1 (
    echo [OK] Service %SERVICE% dang chay san.
    goto end
)

echo Dang chay service %SERVICE%...
net start "%SERVICE%"
sc query "%SERVICE%" | find "RUNNING" >nul
if errorlevel 1 (
    echo [LOI] Service %SERVICE% khong chay duoc.
    echo Xem log: C:\Program Files\MongoDB\Server\9.0\log\mongod.log
) else (
    echo [OK] Service %SERVICE% da chay.
)

:end
echo.
sc query "%SERVICE%" 2>nul | findstr /c:"STATE"
echo.
pause
endlocal
