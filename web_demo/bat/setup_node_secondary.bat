@echo off
setlocal enabledelayedexpansion
title [NODE PHU] Join / Rejoin MySQL Group Replication - Secondary
color 0b

echo =====================================================================
echo    MYSQL GROUP REPLICATION - NODE PHU ^(192.168.1.222 / 223 / 224^)
echo    Tu nhan biet: join lan dau  hoac  vao lai nhom sau khi bat tat service
echo =====================================================================
echo.

:: 1. Quyen Administrator
net session >nul 2>&1
if errorlevel 1 goto need_admin
goto admin_ok
:need_admin
echo [CANH BAO] Can quyen Administrator de cau hinh Firewall.
echo Dang yeu cau cap quyen Administrator...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd -ArgumentList '/k cd /d \"\"%~dp0\"\" ^&^& \"\"%~f0\"\"' -Verb RunAs"
exit /b
:admin_ok
cd /d "%~dp0"

:: 2. Thong tin cau hinh
set "PRIMARY_IP=192.168.1.221"
set /p INPUT_IP="Nhap IP cua mot node DANG ONLINE trong nhom [Mac dinh 192.168.1.221, Enter de dung]: "
if not "!INPUT_IP!"=="" set "PRIMARY_IP=!INPUT_IP!"

set "MYSQL_PWD=root"
set /p INPUT_PWD="Nhap mat khau MySQL root tren cac node [Mac dinh root, Enter de dung]: "
if not "!INPUT_PWD!"=="" set "MYSQL_PWD=!INPUT_PWD!"

:: 3. Tim mysql.exe
set "MYSQL_BIN="
where mysql.exe >nul 2>&1
if not errorlevel 1 set "MYSQL_BIN=mysql.exe"
if "!MYSQL_BIN!"=="" if exist "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" set "MYSQL_BIN=C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"
if "!MYSQL_BIN!"=="" if exist "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe" set "MYSQL_BIN=C:\Program Files\MySQL\MySQL Server 8.4\bin\mysql.exe"
if "!MYSQL_BIN!"=="" if exist "C:\Program Files\MySQL\MySQL Server 9.0\bin\mysql.exe" set "MYSQL_BIN=C:\Program Files\MySQL\MySQL Server 9.0\bin\mysql.exe"
if "!MYSQL_BIN!"=="" goto no_mysql

echo.
echo [1/5] Cau hinh Windows Firewall va che do mang Private...
powershell -NoProfile -Command "Get-NetConnectionProfile | Set-NetConnectionProfile -NetworkCategory Private -ErrorAction SilentlyContinue"
netsh advfirewall firewall delete rule name="CTH612_DB_Ports" >nul 2>&1
netsh advfirewall firewall add rule name="CTH612_DB_Ports" dir=in action=allow protocol=TCP localport=3306,33061,27017,8000 profile=any >nul
netsh advfirewall firewall delete rule name="CTH612_ICMP_Ping" >nul 2>&1
netsh advfirewall firewall add rule name="CTH612_ICMP_Ping" dir=in action=allow protocol=ICMPv4:8,any profile=any >nul
echo   - Da mo port TCP 3306, 33061, 27017, 8000 va ICMP Ping.

echo.
echo [2/5] Kiem tra mang LAN den !PRIMARY_IP! ...
ping -n 2 !PRIMARY_IP! >nul 2>&1
if errorlevel 1 goto ping_fail
echo   - Ket noi mang LAN den !PRIMARY_IP! thanh cong.
goto ping_done
:ping_fail
color 0e
echo [CANH BAO] Khong ping duoc den !PRIMARY_IP!.
echo Nguyen nhan pho bien: khac mang Wi-Fi hoac Router, router bat AP Isolation,
echo node kia chua mo Firewall, hoac node kia da doi IP.
set "CONT=Y"
set /p CONT="Van thu ket noi MySQL? Y hoac N [Mac dinh Y]: "
if /i "!CONT!"=="N" goto end_script
color 0b
:ping_done

echo.
echo [3/5] Kiem tra trang thai node nay...
call :qlocal LSTATE "SELECT IFNULL((SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid),'NOT_MEMBER');"
if "!LSTATE!"=="" goto err_local

set /a WAITED=0
:wait_auto
if /i "!LSTATE!"=="ONLINE" goto already_online
call :qlocal AUTOREJ "SELECT COUNT(*) FROM performance_schema.threads WHERE LOCATE('THD_autorejoin', NAME) > 0;"
set "BUSY=0"
if /i "!LSTATE!"=="RECOVERING" set "BUSY=1"
if not "!AUTOREJ!"=="0" if not "!AUTOREJ!"=="" set "BUSY=1"
if "!BUSY!"=="0" goto detect_done
if !WAITED! geq 45 goto detect_done
echo   - Node dang tu vao nhom, trang thai !LSTATE!. Cho them 3 giay...
timeout /t 3 /nobreak >nul
set /a WAITED+=3
call :qlocal LSTATE "SELECT IFNULL((SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid),'NOT_MEMBER');"
goto wait_auto
:detect_done

:: Node van dang RECOVERING sau khi cho: khong chen STOP/START, chi theo doi tiep
if /i "!LSTATE!"=="RECOVERING" goto poll_online

call :qlocal HASDATA "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='ecommerce_db';"
if "!HASDATA!"=="" set "HASDATA=0"
call :qlocal LGTID "SELECT REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '');"
call :qlocal REPLUSER "SELECT COUNT(*) FROM mysql.user WHERE user='repl';"
if "!REPLUSER!"=="" set "REPLUSER=0"
echo   - Trang thai hien tai: !LSTATE!. So bang ecommerce_db tren node nay: !HASDATA!.
if "!HASDATA!"=="0" echo   - Node chua co du lieu: day la lan join DAU TIEN.
if not "!HASDATA!"=="0" echo   - Node da co du lieu: day la truong hop VAO LAI nhom.

echo.
echo [4/5] Kiem tra nhom tren !PRIMARY_IP! va tinh tuong thich GTID...
call :qremote PONLINE "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE='ONLINE';"
if "!PONLINE!"=="" goto primary_unreachable
if "!PONLINE!"=="0" goto no_group
echo   - Nhom dang hoat dong, co !PONLINE! node ONLINE.
set "REMOTE_OK=1"
goto gtid_check
:primary_unreachable
color 0e
echo [CANH BAO] Khong doc duoc trang thai nhom tu !PRIMARY_IP!.
echo Kiem tra mat khau root, cong 3306 va Firewall cua node do.
set "CONT=N"
set /p CONT="Van thu START GROUP_REPLICATION dua tren group_seeds? Y hoac N [Mac dinh N]: "
if /i not "!CONT!"=="Y" goto end_script
color 0b
set "REMOTE_OK=0"
goto gtid_ok

:gtid_check
set "DO_RESET=N"
if "!LGTID!"=="" goto gtid_ok
call :qremote SUBSET "SELECT GTID_SUBSET('!LGTID!', @@GLOBAL.gtid_executed);"
if "!SUBSET!"=="1" goto gtid_ok
if "!SUBSET!"=="" goto gtid_ok
:: SUBSET = 0: node co giao dich rieng ma nhom khong co
if not "!HASDATA!"=="0" goto errant_with_data
echo   - Node moi nhung co GTID cuc bo khong thuoc nhom, thuong do tao user khi bat binlog.
set "ANS=Y"
set /p ANS="Xoa GTID cuc bo bang RESET MASTER de join? Y hoac N [Mac dinh Y]: "
if /i "!ANS!"=="Y" set "DO_RESET=Y"
if /i not "!ANS!"=="Y" goto end_script
:gtid_ok

echo.
echo [5/5] Vao nhom...
:: Dung phien cu neu co, khong bao loi neu chua chay
"!MYSQL_BIN!" -u root -e "STOP GROUP_REPLICATION;" >nul 2>&1

if "!DO_RESET!"=="Y" echo   - Chay RESET MASTER ^(chi cho node moi, chua co du lieu^).
if "!DO_RESET!"=="Y" "!MYSQL_BIN!" -u root -e "RESET MASTER;"
if "!DO_RESET!"=="Y" if errorlevel 1 goto join_failed

:: Node moi: tao user repl cuc bo, khong ghi binlog
if not "!REPLUSER!"=="0" goto repl_ready
echo   - Tao user repl tren node nay ^(khong ghi binlog^).
"!MYSQL_BIN!" -u root -e "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; SET SQL_LOG_BIN=0; CREATE USER IF NOT EXISTS 'repl'@'%%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026'; ALTER USER 'repl'@'%%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026'; GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%%'; GRANT CONNECTION_ADMIN, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO 'repl'@'%%'; FLUSH PRIVILEGES; SET SQL_LOG_BIN=1;"
if errorlevel 1 goto join_failed
:repl_ready

:: Kenh recovery: dat lai moi lan, chay lai nhieu lan van an toan
"!MYSQL_BIN!" -u root -e "CHANGE REPLICATION SOURCE TO SOURCE_USER='repl', SOURCE_PASSWORD='ReplPass_Nhom2@2026' FOR CHANNEL 'group_replication_recovery';"
if errorlevel 1 goto join_failed

"!MYSQL_BIN!" -u root -e "SET GLOBAL group_replication_bootstrap_group=OFF; START GROUP_REPLICATION;"
if errorlevel 1 goto join_failed

:poll_online
set /a POLLED=0
:poll_loop
call :qlocal LSTATE "SELECT IFNULL((SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid),'NOT_MEMBER');"
if /i "!LSTATE!"=="ONLINE" goto joined
if /i "!LSTATE!"=="ERROR" goto join_failed
if !POLLED! geq 90 goto poll_timeout
echo   - Trang thai !LSTATE!, dang doi dong bo du lieu...
timeout /t 3 /nobreak >nul
set /a POLLED+=3
goto poll_loop

:joined
color 0a
echo.
echo   [OK] Node nay da ONLINE trong nhom.
goto show_members

:already_online
color 0a
echo.
echo   [OK] Node nay DA ONLINE trong nhom, khong can lam gi them.

:show_members
echo.
echo Cac thanh vien trong nhom:
echo ---------------------------------------------------------------------
"!MYSQL_BIN!" -u root -t -e "SELECT MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members;"
call :qlocal LORD "SELECT COUNT(*) FROM ecommerce_db.orders;"
call :qremote ROORD "SELECT COUNT(*) FROM ecommerce_db.orders;"
echo.
echo So don hang tren node nay: !LORD!  -  tren !PRIMARY_IP!: !ROORD!
echo Neu hai so lech nhau ngay sau khi vao nhom, doi vai giay roi kiem tra lai,
echo vi node phu ap dung giao dich tre hon Primary mot chut.
goto end_script

:poll_timeout
color 0e
echo.
echo [CANH BAO] Sau 90 giay node van o trang thai !LSTATE!. Co the dang chep nhieu du lieu.
echo Chay lai file nay sau it phut hoac xem cot MEMBER_STATE ben duoi.
goto show_members

:join_failed
color 0c
echo.
echo [LOI] Khong vao duoc nhom. 6 dong log Group Replication gan nhat cua MySQL:
echo ---------------------------------------------------------------------
"!MYSQL_BIN!" -u root -t -e "SELECT LOGGED, LEFT(DATA, 180) AS MESSAGE FROM performance_schema.error_log WHERE SUBSYSTEM='Repl' ORDER BY LOGGED DESC LIMIT 6;"
echo ---------------------------------------------------------------------
echo Nguyen nhan hay gap:
echo   - Nhom chua chay tren node nao: chay setup_node_primary.bat tren Node 1 truoc.
echo   - Cong 33061 bi Firewall chan, hoac sai IP trong group_seeds.
echo   - Node co giao dich rieng ma nhom khong co ^(errant GTID^): xem error log MySQL.
echo   - Sai mat khau root.
goto end_script

:errant_with_data
color 0c
echo.
echo [LOI] Node nay co du lieu va co giao dich rieng khong thuoc nhom.
echo Script KHONG tu dong RESET MASTER vi se mat lich su GTID cua node dang co du lieu.
echo Can xu ly bang tay, vi du dong bo lai tu mot node khac, roi chay lai.
goto end_script

:no_group
color 0c
echo.
echo [LOI] Khong co node nao ONLINE trong nhom o !PRIMARY_IP!.
echo Script nay chi vao nhom DANG HOAT DONG, khong tu tao nhom moi.
echo Hay chay setup_node_primary.bat tren Node 1 de dung nhom truoc.
goto end_script

:err_local
color 0c
echo.
echo [LOI] Khong doc duoc MySQL tren may nay.
echo Kiem tra: service MySQL dang chay, mat khau root dung, my.ini co dong
echo plugin-load-add = group_replication.dll.
goto end_script

:no_mysql
color 0c
echo [LOI] Khong tim thay mysql.exe tren may nay. Kiem tra lai cai dat MySQL.
goto end_script

:end_script
echo.
echo Nhan phim bat ky de dong cua so nay...
pause >nul
exit /b

:: --- Ham: chay truy van tren MySQL cuc bo, ket qua vao bien ten %1 ---
:: Ghi ra file tam roi doc lai bang set /p. Khong dung for /f vi cmd /c bo cap nhay
:: ngoai cua lenh co nhieu cap nhay, va dau ngoac trong cau SQL lam hong cu phap for.
:qlocal
set "%~1="
del "%TEMP%\gr_q.txt" >nul 2>&1
"%MYSQL_BIN%" -u root -s -N -e %2 >"%TEMP%\gr_q.txt" 2>nul
if exist "%TEMP%\gr_q.txt" set /p %~1=<"%TEMP%\gr_q.txt"
exit /b

:: --- Ham: chay truy van tren node PRIMARY_IP, ket qua vao bien ten %1 ---
:qremote
set "%~1="
del "%TEMP%\gr_q.txt" >nul 2>&1
"%MYSQL_BIN%" -h %PRIMARY_IP% -u root -s -N -e %2 >"%TEMP%\gr_q.txt" 2>nul
if exist "%TEMP%\gr_q.txt" set /p %~1=<"%TEMP%\gr_q.txt"
exit /b
