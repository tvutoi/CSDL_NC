
-- 1. join group replication
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

START GROUP_REPLICATION;

-- 2. check trang thai
USE ecommerce_db;

SELECT 
    @@hostname AS Executed_On_Host,
    @@server_id AS Server_ID,
    IF(@@global.read_only = 0, 'PRIMARY (Read/Write)', 'SECONDARY (Read-Only)') AS Node_Role,
    (SELECT COUNT(*) FROM users) AS Total_Users,
    (SELECT COUNT(*) FROM products) AS Total_Products,
    (SELECT COUNT(*) FROM orders) AS Total_Orders,
    (SELECT COUNT(*) FROM order_items) AS Total_Order_Items,
    (SELECT COUNT(*) FROM payments) AS Total_Payments;

-- 3. test query lon voi profiling
SET profiling = 1;

SELECT 
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.brand,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.quantity * oi.unit_price) AS gross_revenue_vnd
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status = 'COMPLETED'
GROUP BY p.product_id, p.sku, p.product_name, p.category, p.brand
ORDER BY gross_revenue_vnd DESC
LIMIT 10;

SHOW PROFILES;

-- 4. test chan ghi tren secondary
-- INSERT INTO orders (user_id, total_amount, status) VALUES (1, 1000000, 'PENDING');
