@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title Dong bo lai MongoDB tu dau (initial sync) cho node qua cu

rem ============================================================================
rem  Dung khi node MongoDB ket o RECOVERING vi "too stale": optime cua node cu hon
rem  ban ghi dau tien con trong oplog cua Primary, node khong tu bat kip duoc nua.
rem  Script: dung service MongoDB -> doi ten thu muc data (KHONG xoa) -> tao thu muc
rem  data rong -> chay lai service. Node van nam trong rs.conf() nen tu initial sync
rem  toan bo du lieu tu Primary/Secondary dang khoe.
rem  CHI chay tren node phu qua cu. KHONG chay tren Primary.
rem ============================================================================

net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Dang yeu cau quyen Administrator...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd -ArgumentList '/k cd /d \"\"%~dp0\"\" ^&^& \"\"%~f0\"\"' -Verb RunAs"
    exit /b
)

set "SERVICE=MongoDB"
set "DATA_DIR=C:\Program Files\MongoDB\Server\9.0\data"
set /p INPUT_DIR="Thu muc dbPath cua MongoDB [Mac dinh: %DATA_DIR%]: "
if not "!INPUT_DIR!"=="" set "DATA_DIR=!INPUT_DIR!"
if not exist "!DATA_DIR!" (
    echo [LOI] Khong thay thu muc "!DATA_DIR!". Xem storage.dbPath trong mongod.cfg.
    goto end_script
)

rem ---- Tim mongosh de kiem tra an toan ----
set "MONGOSH="
where mongosh >nul 2>&1 && set "MONGOSH=mongosh"
if "!MONGOSH!"=="" if exist "C:\Program Files\mongosh\mongosh.exe" set "MONGOSH=C:\Program Files\mongosh\mongosh.exe"
if "!MONGOSH!"=="" for /d %%d in ("%LOCALAPPDATA%\Programs\mongosh*") do if exist "%%d\mongosh.exe" set "MONGOSH=%%d\mongosh.exe"

echo.
echo [1/4] Kiem tra trang thai node nay...
if "!MONGOSH!"=="" (
    echo   [CANH BAO] Khong tim thay mongosh nen khong tu kiem tra duoc.
    echo   Hay tu xac nhan tren giao dien: node nay KHONG phai Node chinh,
    echo   va cum dang co Node chinh o may khac.
) else (
    set "IS_PRIMARY="
    for /f "usebackq tokens=*" %%r in (`"!MONGOSH!" --quiet --host 127.0.0.1 --port 27017 --eval "print(db.hello().isWritablePrimary)" 2^>nul`) do set "IS_PRIMARY=%%r"
    set "OTHER_PRIMARY="
    for /f "usebackq tokens=*" %%r in (`"!MONGOSH!" --quiet --host 127.0.0.1 --port 27017 --eval "print(db.hello().primary || '')" 2^>nul`) do set "OTHER_PRIMARY=%%r"
    for /f "usebackq tokens=*" %%r in (`"!MONGOSH!" --quiet --host 127.0.0.1 --port 27017 --eval "var s=rs.status().members.filter(m=>m.self)[0]; print(s.stateStr+' optime '+s.optimeDate.toISOString())" 2^>nul`) do echo   - Node nay: %%r
    if /i "!IS_PRIMARY!"=="true" (
        color 0c
        echo   [DUNG] Node nay dang la PRIMARY. Khong duoc resync Primary.
        goto end_script
    )
    if "!OTHER_PRIMARY!"=="" (
        color 0c
        echo   [DUNG] Node nay khong thay Primary nao trong cum.
        echo   Initial sync can mot Primary/Secondary khoe de chep du lieu. Xu ly Primary truoc.
        goto end_script
    )
    echo   - Primary hien tai: !OTHER_PRIMARY!
)

echo.
echo Thu muc "!DATA_DIR!" se duoc DOI TEN (khong xoa) thanh data_stale_^<thoi gian^>.
echo Du lieu tren node nay se duoc chep lai tu dau tu cac node khac.
set "CONFIRM="
set /p CONFIRM="Go YES (chu hoa) de tiep tuc: "
if not "!CONFIRM!"=="YES" (
    echo Da huy.
    goto end_script
)

for /f "usebackq tokens=*" %%t in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "TS=%%t"
for %%p in ("!DATA_DIR!") do set "PARENT=%%~dpp" & set "BASENAME=%%~nxp"
set "BACKUP_NAME=!BASENAME!_stale_!TS!"

echo.
echo [2/4] Dung service !SERVICE!...
net stop "!SERVICE!"
if !errorlevel! neq 0 (
    sc query "!SERVICE!" | find "STOPPED" >nul
    if !errorlevel! neq 0 (
        color 0c
        echo [LOI] Khong dung duoc service !SERVICE!.
        goto end_script
    )
)

echo.
echo [3/4] Doi ten thu muc data cu thanh !BACKUP_NAME! va tao thu muc moi...
ren "!DATA_DIR!" "!BACKUP_NAME!"
if !errorlevel! neq 0 (
    color 0c
    echo [LOI] Khong doi ten duoc thu muc data. Chay lai service cu...
    net start "!SERVICE!"
    goto end_script
)
mkdir "!DATA_DIR!"
rem Quyen ghi cho tai khoan chay service (mac dinh NETWORK SERVICE)
icacls "!DATA_DIR!" /grant "NT AUTHORITY\NETWORK SERVICE:(OI)(CI)F" >nul 2>&1

echo.
echo [4/4] Chay lai service !SERVICE!...
net start "!SERVICE!"
if !errorlevel! neq 0 (
    color 0c
    echo [LOI] Service khong chay. Xem log: C:\Program Files\MongoDB\Server\9.0\log\mongod.log
    echo Muon quay lai du lieu cu: dung service, xoa thu muc data moi, doi ten !BACKUP_NAME! ve !BASENAME!.
    goto end_script
)

echo.
color 0a
echo =====================================================================
echo   Da khoi dong lai. Node se o STARTUP2 (initial sync) roi len SECONDARY.
echo   Kiem tra tren giao dien hoac: mongosh --eval "rs.status().members.map(m=>m.name+' '+m.stateStr)"
echo   Khi node thanh SECONDARY, app tu cap lai phieu bau (moi luot mot node).
echo   Thu muc cu: !PARENT!!BACKUP_NAME! (xoa thu cong khi da on dinh).
echo =====================================================================

:end_script
echo.
echo Nhan phim bat ky de dong cua so nay...
pause >nul
