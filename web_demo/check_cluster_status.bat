@echo off
setlocal enabledelayedexpansion
title Kiem Tra Trang Thai MySQL Group Replication
color 0b

echo =====================================================================
echo    KIEM TRA TRANG THAI MYSQL GROUP REPLICATION VA SO LUONG BAN GHI
echo =====================================================================
echo.

cd /d "%~dp0"

set "MYSQL_PWD=root"
set /p INPUT_PWD="Nhap mat khau MySQL root [Mac dinh: root - Nhan Enter neu dung]: "
if not "!INPUT_PWD!"=="" set "MYSQL_PWD=!INPUT_PWD!"

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
    echo [LOI] Khong tim thay mysql.exe!
    pause
    exit /b 1
)

echo.
echo [1] Danh sach cac Member trong Replication Group:
echo ---------------------------------------------------------------------
"%MYSQL_BIN%" -u root -p%MYSQL_PWD% -t -e "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE, MEMBER_VERSION FROM performance_schema.replication_group_members;"

echo.
echo [2] So luong ban ghi hien tai trong database 'ecommerce_db':
echo ---------------------------------------------------------------------
"%MYSQL_BIN%" -u root -p%MYSQL_PWD% -t -e "SELECT 'users' AS `Bang`, COUNT(*) AS `So luong` FROM ecommerce_db.users UNION ALL SELECT 'products', COUNT(*) FROM ecommerce_db.products UNION ALL SELECT 'orders', COUNT(*) FROM ecommerce_db.orders UNION ALL SELECT 'order_items', COUNT(*) FROM ecommerce_db.order_items UNION ALL SELECT 'payments', COUNT(*) FROM ecommerce_db.payments;"

echo.
pause
