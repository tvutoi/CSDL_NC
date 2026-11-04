@echo off
setlocal enabledelayedexpansion
title [NODE PHU] Join MySQL Group Replication - Secondary (192.168.1.222 / 223 / 224)
color 0b

echo =====================================================================
echo    HE THONG PHAN TAN MYSQL GROUP REPLICATION - NODE PHU (SECONDARY)
echo =====================================================================
echo.

:: 1. Kiem tra quyen Administrator
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [CANH BAO] Tap lenh can quyen Administrator de cau hinh Firewall.
    echo Dang yeu cau cap quyen Administrator...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd -ArgumentList '/c \"\"%~f0\"\"' -Verb RunAs"
    exit /b
)

cd /d "%~dp0"

:: 2. Nhap thong tin cau hinh
set "PRIMARY_IP=192.168.1.221"
set /p INPUT_IP="Nhap dia chi IP cua Node Chinh [Mac dinh: 192.168.1.221 - Nhan Enter neu dung]: "
if not "!INPUT_IP!"=="" set "PRIMARY_IP=!INPUT_IP!"

set "MYSQL_PWD=root"
set /p INPUT_PWD="Nhap mat khau MySQL root tren may nay [Mac dinh: root - Nhan Enter neu dung]: "
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
    echo [LOI] Khong tim thay mysql.exe tren may nay! Vui long kiem tra lai cai dat MySQL.
    pause
    exit /b 1
)

echo.
echo [1/4] Dang cau hinh Windows Firewall va Che do mang Private...
powershell -NoProfile -Command "Get-NetConnectionProfile | Set-NetConnectionProfile -NetworkCategory Private -ErrorAction SilentlyContinue"
netsh advfirewall firewall delete rule name="CTH612_DB_Ports" >nul 2>&1
netsh advfirewall firewall add rule name="CTH612_DB_Ports" dir=in action=allow protocol=TCP localport=3306,33061,27017,8000 profile=any >nul
netsh advfirewall firewall delete rule name="CTH612_ICMP_Ping" >nul 2>&1
netsh advfirewall firewall add rule name="CTH612_ICMP_Ping" dir=in action=allow protocol=ICMPv4:8,any profile=any >nul
echo   - Da mo thong port TCP 3306, 33061, 27017, 8000 va ICMP Ping.

echo.
echo [2/4] Kiem tra ket noi mang LAN den Node Chinh (!PRIMARY_IP!)...
ping -n 2 !PRIMARY_IP! >nul 2>&1
if %errorlevel% neq 0 (
    color 0e
    echo [CANH BAO] Khong the ping den Node Chinh (!PRIMARY_IP!).
    echo Kiem tra:
    echo   1. Hai may da bat chung mang Wi-Fi chua?
    echo   2. Tren Node Chinh da chay setup_node_primary.bat chua?
    echo.
    set /p CONT="Ban co muon tiep tuc thu ket noi khong? (Y/N) [Mac dinh: Y]: "
    if /i "!CONT!"=="N" exit /b 1
) else (
    echo   - Ket noi mang LAN den !PRIMARY_IP! thanh cong.
)

echo.
echo [3/4] Dang dong bo GTID va tham gia Group Replication...
"%MYSQL_BIN%" -u root -p%MYSQL_PWD% -e "STOP GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group = OFF; RESET REPLICA FOR CHANNEL 'group_replication_recovery'; RESET MASTER; START GROUP_REPLICATION;" >nul 2>&1

if %errorlevel% neq 0 (
    color 0c
    echo [LOI] Lenh START GROUP_REPLICATION that bai!
    echo Nguyen nhan pho bien: Sai mat khau MySQL root, hoac Node Chinh chua bootstrap xong.
    pause
    exit /b 1
)

echo Dang doi nhan dong bo du lieu tu Node Chinh (3 giay)...
timeout /t 3 /nobreak >nul

echo.
echo [4/4] Trang thai Member trong nhom:
echo ---------------------------------------------------------------------
"%MYSQL_BIN%" -u root -p%MYSQL_PWD% -t -e "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members;"

echo.
echo =====================================================================
color 0a
echo   [HOAN TAT] Node nay da gui yeu cau tham gia Group thanh cong!
echo   Kiem tra cot MEMBER_STATE:
echo     + ONLINE     : Da tham gia hoan tat va san sang nhan du lieu!
echo     + RECOVERING : Dang keo du lieu tu dong tu Node Chinh.
echo =====================================================================
echo.
pause
