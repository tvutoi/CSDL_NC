@echo off
setlocal
title Start MySQL service

rem ============================================================================
rem  Chay service MySQL (MySQL Server 8.0) tren node nay.
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
    echo Xem log: C:\ProgramData\MySQL\MySQL Server 8.0\Data\*.err
) else (
    echo [OK] Service %SERVICE% da chay.
    echo Luu y: my.ini cua cum dat group_replication_start_on_boot = OFF, nen node CHUA vao nhom MGR.
    echo Chay setup_node_secondary.bat ^(node phu^) hoac setup_node_primary.bat ^(dung cum lan dau^) de join lai.
)

:end
echo.
sc query "%SERVICE%" 2>nul | findstr /c:"STATE"
echo.
pause
endlocal
