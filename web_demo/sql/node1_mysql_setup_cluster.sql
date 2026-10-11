-- =====================================================================================
-- FILE DÀNH CHO NODE 1 (192.168.1.221) - BƯỚC 1: KHỞI TẠO CỤM MYSQL GROUP REPLICATION
-- Người phụ trách: Thành viên 1 (Trưởng nhóm - Máy Primary ban đầu)
-- Hướng dẫn chạy: Mở MySQL Command Line hoặc MySQL Workbench trên Node 1 và thực thi
-- =====================================================================================

-- 1. Tạo tài khoản Replication chuyên dụng (Không ghi vào binlog để tránh xung đột GTID)
SET SQL_LOG_BIN = 0;
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026';
ALTER USER 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
GRANT CONNECTION_ADMIN, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO 'repl'@'%';

-- Tạo tài khoản root cho phép kết nối từ các máy trong mạng LAN (Node 2, 3, 4)
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'RootPassword@2026';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SET SQL_LOG_BIN = 1;

-- 2. Cấu hình kênh phục hồi dữ liệu phân tán (Recovery Channel)
CHANGE REPLICATION SOURCE TO 
    SOURCE_USER = 'repl', 
    SOURCE_PASSWORD = 'ReplPass_Nhom2@2026' 
    FOR CHANNEL 'group_replication_recovery';

-- 3. BOOTSTRAP KHỞI TẠO NHÓM (CHỈ CHẠY TRÊN NODE 1 DUY NHẤT)
SET GLOBAL group_replication_bootstrap_group = ON;
START GROUP_REPLICATION;
SET GLOBAL group_replication_bootstrap_group = OFF;

-- 4. Kiểm tra trạng thái Node 1 (Kết quả chuẩn: MEMBER_STATE = 'ONLINE', MEMBER_ROLE = 'PRIMARY')
SELECT 
    MEMBER_ID, 
    MEMBER_HOST, 
    MEMBER_PORT, 
    MEMBER_STATE, 
    MEMBER_ROLE, 
    MEMBER_VERSION
FROM performance_schema.replication_group_members;

