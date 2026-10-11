-- =====================================================================================
-- FILE DÀNH CHO NODE 4 (192.168.1.224) - GIA NHẬP CỤM 4 NÚT & KIỂM THỬ BENCHMARK
-- Người phụ trách: Thành viên 4 (Máy Node 4 - Secondary 3 / Client Benchmark)
-- =====================================================================================

-- =====================================================================================
-- PHẦN 1: GIA NHẬP CỤM GROUP REPLICATION (NẾU NHÓM CHẠY CẢ 4 MÁY LÀM NODE CSDL)
-- =====================================================================================
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

-- Gia nhập cụm để tạo thành hệ thống 4 nút hoạt động độc lập:
START GROUP_REPLICATION;

-- =====================================================================================
-- PHẦN 2: KIỂM TRA TRẠNG THÁI 4 NODE & CHỨNG MINH ĐỒNG BỘ DỮ LIỆU TỨC THỜI
-- =====================================================================================
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

-- =====================================================================================
-- PHẦN 3: SO SÁNH TRUY VẤN PHỨC TẠP TRÊN TẬP DỮ LIỆU LỚN (212,000 BẢN GHI)
-- (Bật SET profiling = 1 để đo chính xác thời gian thực thi bằng mili-giây)
-- =====================================================================================
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

-- Xem thời gian thực thi chi tiết của câu truy vấn vừa chạy:
SHOW PROFILES;

-- =====================================================================================
-- PHẦN 4: MINH HỌA RÀNG BUỘC NHẤT QUÁN TRÊN SECONDARY (CHỐNG GHI TRÁI PHÉP)
-- (Chạy câu lệnh dưới đây trên Node Secondary khi Node 1 đang là Primary)
-- -> Hệ thống sẽ lập tức chặn và báo lỗi: "The MySQL server is running with the --super-read-only option"
-- =====================================================================================
-- INSERT INTO orders (user_id, total_amount, status) VALUES (1, 1000000, 'PENDING');
