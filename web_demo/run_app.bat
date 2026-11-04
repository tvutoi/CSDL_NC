@echo off
title Web Demo Server - Polyglot Ecommerce
cd /d "%~dp0"
echo ===================================================
echo KHOI DONG SERVER WEB DEMO (FASTAPI)
echo ===================================================
echo Truy cap Local tai: http://localhost:8000

REM 1. Thu dung py launcher
py --version >nul 2>&1
if %errorlevel% equ 0 (
    py app.py
    goto end
)

REM 2. Thu dung python trong PATH
python --version >nul 2>&1
if %errorlevel% equ 0 (
    python app.py
    goto end
)

REM 3. Fallback duong dan tuyet doi Python da cai dat tren may
if exist "C:\Users\uttoi\AppData\Local\Python\pythoncore-3.15-64\python.exe" (
    "C:\Users\uttoi\AppData\Local\Python\pythoncore-3.15-64\python.exe" app.py
    goto end
)

if exist "C:\Users\uttoi\AppData\Local\Python\bin\python.exe" (
    "C:\Users\uttoi\AppData\Local\Python\bin\python.exe" app.py
    goto end
)

echo [LOI] Khong tim thay Python tren he thong!
pause

:end
pause
