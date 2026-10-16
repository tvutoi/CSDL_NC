
-- 1. tao user replication
SET SQL_LOG_BIN = 0;
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026';
ALTER USER 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
GRANT CONNECTION_ADMIN, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO 'repl'@'%';

-- allow root tu lan
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'RootPassword@2026';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SET SQL_LOG_BIN = 1;

-- 2. recovery channel
CHANGE REPLICATION SOURCE TO 
    SOURCE_USER = 'repl', 
    SOURCE_PASSWORD = 'ReplPass_Nhom2@2026' 
    FOR CHANNEL 'group_replication_recovery';

-- 3. bootstrap group tren node 1
SET GLOBAL group_replication_bootstrap_group = ON;
START GROUP_REPLICATION;
SET GLOBAL group_replication_bootstrap_group = OFF;

-- 4. check trang thai
SELECT 
    MEMBER_ID, 
    MEMBER_HOST, 
    MEMBER_PORT, 
    MEMBER_STATE, 
    MEMBER_ROLE, 
    MEMBER_VERSION
FROM performance_schema.replication_group_members;

