
-- 1. tao user replication
SET SQL_LOG_BIN = 0;
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026';
ALTER USER 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'ReplPass_Nhom2@2026';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
GRANT CONNECTION_ADMIN, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO 'repl'@'%';

CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'RootPassword@2026';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SET SQL_LOG_BIN = 1;

CHANGE REPLICATION SOURCE TO 
    SOURCE_USER = 'repl', 
    SOURCE_PASSWORD = 'ReplPass_Nhom2@2026' 
    FOR CHANNEL 'group_replication_recovery';

-- start group replication
START GROUP_REPLICATION;

-- 2. check trang thai
SELECT 
    @@hostname AS Node3_Hostname,
    @@server_id AS Node3_ServerID,
    @@global.read_only AS Is_Read_Only,
    @@global.gtid_executed AS GTID_Synced;

SELECT 
    MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE
FROM performance_schema.replication_group_members;

USE ecommerce_db;
SELECT 'Node 3 - users' AS Table_Check, COUNT(*) AS Total_Rows FROM users
UNION ALL
SELECT 'Node 3 - products', COUNT(*) FROM products
UNION ALL
SELECT 'Node 3 - orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Node 3 - order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'Node 3 - payments', COUNT(*) FROM payments;

-- 3. query thong ke khach hang
SELECT 
    u.city,
    pm.payment_method,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(pm.amount) AS total_paid_amount
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN payments pm ON o.order_id = pm.order_id
WHERE o.shipping_region = 'MIEN_BAC'
GROUP BY u.city, pm.payment_method
ORDER BY total_paid_amount DESC
LIMIT 15;

