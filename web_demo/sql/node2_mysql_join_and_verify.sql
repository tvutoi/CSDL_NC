
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

-- 2. check trang thai va dem dong
SELECT 
    @@hostname AS Node2_Hostname,
    @@server_id AS Node2_ServerID,
    @@global.read_only AS Is_Read_Only,
    @@global.gtid_executed AS GTID_Synced;

SELECT 
    MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE
FROM performance_schema.replication_group_members;

USE ecommerce_db;
SELECT 'Node 2 - users' AS Table_Check, COUNT(*) AS Total_Rows FROM users
UNION ALL
SELECT 'Node 2 - products', COUNT(*) FROM products
UNION ALL
SELECT 'Node 2 - orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Node 2 - order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'Node 2 - payments', COUNT(*) FROM payments;

-- 3. query thong ke doanh thu mien trung
SELECT 
    p.category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_items_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue_vnd
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.shipping_region = 'MIEN_TRUNG' AND o.status = 'COMPLETED'
GROUP BY p.category
ORDER BY total_revenue_vnd DESC;

-- 4. test failover neu len primary
/*
START TRANSACTION;
UPDATE products SET stock_quantity = stock_quantity - 1 WHERE product_id = 1;
INSERT INTO orders (user_id, total_amount, status, shipping_region) VALUES (2, 28000000.00, 'COMPLETED', 'MIEN_TRUNG');
SET @new_order_id = LAST_INSERT_ID();
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (@new_order_id, 1, 1, 28000000.00);
INSERT INTO payments (order_id, amount, payment_method, transaction_status) VALUES (@new_order_id, 28000000.00, 'E-WALLET', 'SUCCESS');
COMMIT;
SELECT * FROM orders WHERE order_id = @new_order_id;
*/

