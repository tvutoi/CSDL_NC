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
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd -ArgumentList '/k cd /d \"\"%~dp0\"\" ^&^& \"\"%~f0\"\"' -Verb RunAs"
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
    goto end_script
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
    echo.
    echo =====================================================================
    echo [CANH BAO] KHONG THE PING DEN NODE CHINH (!PRIMARY_IP!)!
    echo =====================================================================
    echo Cac nguyen nhan pho bien:
    echo   1. May nay va Node Chinh KHONG chung mot mang Wi-Fi / Router.
    echo      (Kiem tra router co bat AP/Client Isolation ngan cac laptop nhin thay nhau).
    echo   2. May Node Chinh chua chay setup_node_primary.bat de mo Firewall.
    echo   3. Card mang cua may nay hoac Node Chinh bi thay doi IP.
    echo.
    echo Giai phap tot nhat: 1 ban phat Hotspot tu dien thoai hoac laptop va
    echo ca 4 may cung ket noi vao Hotspot do!
    echo =====================================================================
    echo.
    set "CONT=Y"
    set /p CONT="Ban co muon thu ket noi MySQL den Node Chinh khong? (Y/N) [Mac dinh: Y]: "
    if /i "!CONT!"=="N" goto end_script
) else (
    echo   - Ket noi mang LAN den !PRIMARY_IP! thanh cong.
)

echo.
echo [3/4] Dang ket noi Node Chinh de lay tap GTID chuan...
set "PRIMARY_GTID="
for /f "usebackq tokens=*" %%g in (`"%MYSQL_BIN%" -h !PRIMARY_IP! -u root -p!MYSQL_PWD! -s -N -e "SELECT REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '');" 2^>nul`) do (
    set "PRIMARY_GTID=%%g"
)

if not "!PRIMARY_GTID!"=="" (
    echo   - Da doc thanh cong GTID goc tu Node Chinh:
    echo     !PRIMARY_GTID:~0,60!...
    echo   - Dang dong bo GTID sach tren Node nay...
    "%MYSQL_BIN%" -u root -p!MYSQL_PWD! -e "STOP GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group = OFF; RESET REPLICA FOR CHANNEL 'group_replication_recovery'; RESET MASTER; SET @@GLOBAL.gtid_purged = '!PRIMARY_GTID!'; START GROUP_REPLICATION;"
) else (
    echo   - [Luu y] Khong the doc truc tiep tu !PRIMARY_IP!:3306. Thu dong bo co ban...
    "%MYSQL_BIN%" -u root -p!MYSQL_PWD! -e "STOP GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group = OFF; RESET REPLICA FOR CHANNEL 'group_replication_recovery'; RESET MASTER; START GROUP_REPLICATION;"
)

echo.
echo Dang doi dong bo trang thai thanh vien (4 giay)...
timeout /t 4 /nobreak >nul

echo.
echo [4/4] Trang thai Member trong Replication Group:
echo ---------------------------------------------------------------------
"%MYSQL_BIN%" -u root -p!MYSQL_PWD! -t -e "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members;"

echo.
echo =====================================================================
echo   Kiem tra cot MEMBER_STATE o tren:
echo     + ONLINE     : Da tham gia hoan tat va san sang nhan du lieu!
echo     + RECOVERING : Dang tu dong sao chep du lieu tu Node Chinh.
echo     + OFFLINE    : Ket noi den Node Chinh bi chan (kiem tra Firewall/Mang).
echo =====================================================================
echo.

:end_script
echo Nhan phim bat ky de dong cua so nay...
pause >nul
