@echo off
setlocal enabledelayedexpansion
title [NODE CHINH] Setup MySQL Group Replication - Primary (192.168.1.221)
color 0b

echo =====================================================================
echo    HE THONG PHAN TAN MYSQL GROUP REPLICATION - NODE CHINH (PRIMARY)
echo =====================================================================
echo.

:: 1. Kiem tra quyen Administrator
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [CANH BAO] Tap lenh can quyen Administrator de cau hinh Firewall.
    echo Dang yeu cau cap quyen Administrator...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd -ArgumentList '/k cd /d \"\"%~dp0\"\" ^&^& \"\"%~f0\"\"' -Verb RunAs"
    exit /b
)

cd /d "%~dp0"

:: 2. Nhap mat khau MySQL
set "MYSQL_PWD=root"
set /p INPUT_PWD="Nhap mat khau MySQL root [Mac dinh: root - Nhan Enter neu dung]: "
if not "!INPUT_PWD!"=="" set "MYSQL_PWD=!INPUT_PWD!"

:: 3. Tim duong dan mysql.exe
set "MYSQL_BIN="
where mysql.exe >nul 2>&1
if %errorlevel% equ 0 (
    set "MYSQL_BIN=mysql.exe"
) else (
    if exist "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" (
        set "MYSQL_BIN=C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"
    ) else if exist "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe" (
        set "MYSQL_BIN=C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe"
    ) else if exist "C:\Program Files\MySQL\MySQL Server 9.0\bin\mysql.exe" (
        set "MYSQL_BIN=C:\Program Files\MySQL\MySQL Server 9.0\bin\mysql.exe"
    )
)

if "%MYSQL_BIN%"=="" (
    color 0c
    echo [LOI] Khong tim thay mysql.exe! Vui long kiem tra lai cai dat MySQL.
    goto end_script
)

echo.
echo [1/3] Dang cau hinh Windows Firewall va Che do mang Private...
powershell -NoProfile -Command "Get-NetConnectionProfile | Set-NetConnectionProfile -NetworkCategory Private -ErrorAction SilentlyContinue"
netsh advfirewall firewall delete rule name="CTH612_DB_Ports" >nul 2>&1
netsh advfirewall firewall add rule name="CTH612_DB_Ports" dir=in action=allow protocol=TCP localport=3306,33061,27017,8000 profile=any >nul
netsh advfirewall firewall delete rule name="CTH612_ICMP_Ping" >nul 2>&1
netsh advfirewall firewall add rule name="CTH612_ICMP_Ping" dir=in action=allow protocol=ICMPv4:8,any profile=any >nul
echo   - Da mo thong port TCP 3306, 33061, 27017, 8000 va ICMP Ping.

echo.
echo [2/3] Dang khoi tao Group Replication tren Node Chinh (Bootstrap Group)...
"%MYSQL_BIN%" -u root -p!MYSQL_PWD! -e "STOP GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group = ON; START GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group = OFF;"

echo.
echo [3/3] Kiem tra trang thai Member hien tai:
echo ---------------------------------------------------------------------
"%MYSQL_BIN%" -u root -p!MYSQL_PWD! -t -e "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema.replication_group_members;"

echo.
echo =====================================================================
color 0a
echo   [HOAN TAT] Node chinh da ONLINE voi vai tro PRIMARY!
echo   Bay gio hay chay file setup_node_secondary.bat tren cac Node phu.
echo =====================================================================
echo.

:end_script
echo Nhan phim bat ky de dong cua so nay...
pause >nul
