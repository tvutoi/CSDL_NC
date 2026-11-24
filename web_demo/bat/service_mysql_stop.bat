@echo off
setlocal
title Stop MySQL service

rem ============================================================================
rem  Dung service MySQL (MySQL Server 8.0) tren node nay.
rem  Ten service mac dinh: MySQL80. Doi bien SERVICE ben duoi neu may dat ten khac.
rem  Tu xin quyen Administrator (net start/stop can quyen nay).
rem ============================================================================

net session >nul 2>&1
if errorlevel 1 (
    echo Dang yeu cau quyen Administrator...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

set "SERVICE=MySQL80"

sc query "%SERVICE%" >nul 2>&1
if errorlevel 1 (
    echo [LOI] Khong tim thay service "%SERVICE%" tren may nay.
    echo Xem ten that bang lenh: sc query state= all ^| findstr /i "mysql"
    goto end
)

sc query "%SERVICE%" | find "STOPPED" >nul
if not errorlevel 1 (
    echo [OK] Service %SERVICE% da dung san.
    goto end
)
echo Luu y: dung MySQL tren node Primary se lam nhom MGR bau Primary moi.
echo Neu chi con 1 node trong nhom thi cum mat da so va khong ghi duoc.

echo Dang dung service %SERVICE%...
net stop "%SERVICE%"
sc query "%SERVICE%" | find "STOPPED" >nul
if errorlevel 1 (
    echo [LOI] Service %SERVICE% chua dung han. Kiem tra lai: sc query %SERVICE%
) else (
    echo [OK] Service %SERVICE% da dung.
)

:end
echo.
sc query "%SERVICE%" 2>nul | findstr /c:"STATE"
echo.
pause
endlocal
