-- =====================================================================================
-- SCRIPT KHỞI TẠO CƠ SỞ DỮ LIỆU & NẠP DỮ LIỆU LỚN (LARGE DATASET) CHO MYSQL 8.0
-- ĐỀ TÀI 2: HỆ THỐNG THƯƠNG MẠI ĐIỆN TỬ PHÂN TÁN (MYSQL GROUP REPLICATION + MONGODB)
-- Quy mô dữ liệu tạo tự động: ~212,000 bản ghi:
--   + users:       10,000 dòng
--   + products:     2,000 dòng
--   + orders:      50,000 dòng
--   + order_items: 100,000 dòng
--   + payments:    50,000 dòng
-- Kỹ thuật sử dụng: Set-based Bulk Insert (Tally Table) giúp chạy cực nhanh (chỉ ~5-10 giây)
-- và tương thích hoàn toàn với MySQL Group Replication (tránh tràn bộ nhớ giao dịch).
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ecommerce_db;

-- Tắt kiểm tra khóa ngoại tạm thời để xóa bảng cũ (nếu có)
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS seq_numbers;
SET FOREIGN_KEY_CHECKS = 1;

-- =====================================================================================
-- 1. TẠO CẤU TRÚC CÁC BẢNG GIAO DỊCH (CHUẨN INNODB & BẮT BUỘC CÓ PRIMARY KEY CHO MGR)
-- =====================================================================================

-- 1.1. Bảng khách hàng (10,000 dòng)
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    city VARCHAR(50) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_users_city (city),
    INDEX idx_users_created (created_at)
) ENGINE=InnoDB;

-- 1.2. Bảng sản phẩm & tồn kho (2,000 dòng)
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(50) NOT NULL,
    brand VARCHAR(50) NOT NULL,
    price DECIMAL(15, 2) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 100,
    status VARCHAR(20) DEFAULT 'AVAILABLE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_products_category (category),
    INDEX idx_products_brand (brand),
    INDEX idx_products_price (price)
) ENGINE=InnoDB;

-- 1.3. Bảng đơn hàng (50,000 dòng)
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    total_amount DECIMAL(15, 2) NOT NULL,
    status VARCHAR(30) DEFAULT 'COMPLETED', -- PENDING, COMPLETED, CANCELLED
    shipping_region VARCHAR(50) DEFAULT 'MIEN_NAM',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    INDEX idx_orders_user (user_id),
    INDEX idx_orders_status_date (status, created_at),
    INDEX idx_orders_region (shipping_region)
) ENGINE=InnoDB;

-- 1.4. Bảng chi tiết đơn hàng (100,000 dòng)
CREATE TABLE order_items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(15, 2) NOT NULL,
    CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    INDEX idx_items_order (order_id),
    INDEX idx_items_product (product_id)
) ENGINE=InnoDB;

-- 1.5. Bảng thanh toán giao dịch (50,000 dòng)
CREATE TABLE payments (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    payment_method VARCHAR(50) DEFAULT 'E-WALLET', -- E-WALLET, CREDIT_CARD, BANK_TRANSFER, COD
    transaction_status VARCHAR(30) DEFAULT 'SUCCESS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    INDEX idx_payments_order (order_id),
    INDEX idx_payments_method (payment_method)
) ENGINE=InnoDB;

-- =====================================================================================
-- 2. TẠO BẢNG SỐ HỌC PHỤ TRỢ (TALLY TABLE) TỪ 1 ĐẾN 100,000 ĐỂ BULK INSERT CỰC NHANH
-- =====================================================================================
CREATE TABLE seq_numbers (
    n INT PRIMARY KEY
) ENGINE=InnoDB;

-- Sinh 100,000 số tự nhiên chỉ trong 1 giây bằng Cross Join 5 tập (10^5 = 100,000)
INSERT INTO seq_numbers (n)
SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 + e.N * 10000 + 1
FROM (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
CROSS JOIN (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e;

-- =====================================================================================
-- 3. NẠP 10,000 DÒNG VÀO BẢNG USERS (TRUYỀN TRỰC TIẾP user_id = n TRÁNH LỖ HỔNG AUTO_INCREMENT CỦA MGR)
-- =====================================================================================
INSERT INTO users (user_id, username, full_name, email, phone, city, balance, created_at)
SELECT 
    n,
    CONCAT('user_', n),
    CONCAT(
        ELT((n % 8) + 1, 'Nguyễn ', 'Trần ', 'Lê ', 'Phạm ', 'Hoàng ', 'Huỳnh ', 'Phan ', 'Vũ '),
        ELT((n % 6) + 1, 'Văn ', 'Thị ', 'Minh ', 'Quốc ', 'Gia ', 'Thanh '),
        ELT((n % 10) + 1, 'An', 'Bình', 'Châu', 'Dũng', 'Hạnh', 'Khoa', 'Linh', 'Nam', 'Phúc', 'Tâm'),
        ' #', n
    ),
    CONCAT('user', n, '@ecommerce.vn'),
    CONCAT('09', LPAD(n % 100000000, 8, '0')),
    ELT((n % 6) + 1, 'Cần Thơ', 'TP. Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng', 'Hải Phòng', 'An Giang'),
    ROUND(50000000 + (n % 50) * 2000000, 2),
    TIMESTAMPADD(MINUTE, -(10000 - n), NOW())
FROM seq_numbers
WHERE n <= 10000;

-- =====================================================================================
-- 4. NẠP 2,000 DÒNG VÀO BẢNG PRODUCTS (GỒM CÁC SẢN PHẨM CHỦ LỰC & DỮ LIỆU MỞ RỘNG)
-- =====================================================================================
INSERT INTO products (product_id, sku, product_name, category, brand, price, stock_quantity, status, created_at)
SELECT 
    n,
    CONCAT('SKU-', LPAD(n, 6, '0')),
    CASE 
        WHEN n = 1 THEN 'iPhone 15 Pro Max 256GB Titan Tự Nhiên'
        WHEN n = 2 THEN 'Laptop Dell XPS 15 9530 Core i9 OLED'
        WHEN n = 3 THEN 'Tai nghe chống ồn Sony WH-1000XM5'
        WHEN n = 4 THEN 'MacBook Pro 16 M3 Max 64GB RAM'
        WHEN n = 5 THEN 'Samsung Galaxy S24 Ultra 5G 512GB'
        ELSE CONCAT(
            ELT((n % 6) + 1, 'Điện thoại ', 'Laptop ', 'Máy tính bảng ', 'Tai nghe ', 'Đồng hồ thông minh ', 'Màn hình đồ họa '),
            ELT((n % 7) + 1, 'Apple ', 'Samsung ', 'Dell ', 'Sony ', 'Asus ', 'LG ', 'Xiaomi '),
            'Series Pro Gen ', (n % 5) + 1, ' - Model #', n
        )
    END,
    ELT((n % 6) + 1, 'Smartphone', 'Laptop', 'Tablet', 'Audio', 'Smartwatch', 'Monitor'),
    ELT((n % 7) + 1, 'Apple', 'Samsung', 'Dell', 'Sony', 'Asus', 'LG', 'Xiaomi'),
    CASE 
        WHEN n = 1 THEN 28000000.00
        WHEN n = 2 THEN 38000000.00
        WHEN n = 3 THEN 6800000.00
        WHEN n = 4 THEN 65000000.00
        WHEN n = 5 THEN 29500000.00
        ELSE ROUND(1500000 + (n % 40) * 1000000, 2)
    END,
    500 + (n % 500),
    'AVAILABLE',
    TIMESTAMPADD(HOUR, -(2000 - n), NOW())
FROM seq_numbers
WHERE n <= 2000;

-- =====================================================================================
-- 5. NẠP 50,000 DÒNG VÀO BẢNG ORDERS (CHIA 2 LÔ ĐỂ ĐỒNG BỘ MGR MƯỢT MÀ)
-- =====================================================================================
-- Lô 1: Đơn hàng 1 -> 25,000
INSERT INTO orders (order_id, user_id, total_amount, status, shipping_region, created_at)
SELECT 
    n,
    ((n - 1) % 10000) + 1,
    ROUND(3000000 + (n % 30) * 1500000, 2),
    ELT((n % 5) + 1, 'COMPLETED', 'COMPLETED', 'COMPLETED', 'PENDING', 'COMPLETED'),
    ELT((n % 3) + 1, 'MIEN_NAM', 'MIEN_TRUNG', 'MIEN_BAC'),
    TIMESTAMPADD(MINUTE, -(50000 - n), NOW())
FROM seq_numbers
WHERE n BETWEEN 1 AND 25000;

-- Lô 2: Đơn hàng 25,001 -> 50,000
INSERT INTO orders (order_id, user_id, total_amount, status, shipping_region, created_at)
SELECT 
    n,
    ((n - 1) % 10000) + 1,
    ROUND(3000000 + (n % 30) * 1500000, 2),
    ELT((n % 5) + 1, 'COMPLETED', 'COMPLETED', 'COMPLETED', 'CANCELLED', 'COMPLETED'),
    ELT((n % 3) + 1, 'MIEN_NAM', 'MIEN_TRUNG', 'MIEN_BAC'),
    TIMESTAMPADD(MINUTE, -(50000 - n), NOW())
FROM seq_numbers
WHERE n BETWEEN 25001 AND 50000;

-- =====================================================================================
-- 6. NẠP 100,000 DÒNG VÀO BẢNG ORDER_ITEMS (CHIA 4 LÔ X 25,000 DÒNG)
-- =====================================================================================
INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
SELECT n, ((n - 1) % 50000) + 1, ((n - 1) % 2000) + 1, (n % 3) + 1, ROUND(1500000 + (n % 25) * 1000000, 2)
FROM seq_numbers WHERE n BETWEEN 1 AND 25000;

INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
SELECT n, ((n - 1) % 50000) + 1, ((n + 500) % 2000) + 1, (n % 3) + 1, ROUND(1500000 + (n % 25) * 1000000, 2)
FROM seq_numbers WHERE n BETWEEN 25001 AND 50000;

INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
SELECT n, ((n - 1) % 50000) + 1, ((n + 1000) % 2000) + 1, (n % 2) + 1, ROUND(1500000 + (n % 25) * 1000000, 2)
FROM seq_numbers WHERE n BETWEEN 50001 AND 75000;

INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
SELECT n, ((n - 1) % 50000) + 1, ((n + 1500) % 2000) + 1, (n % 4) + 1, ROUND(1500000 + (n % 25) * 1000000, 2)
FROM seq_numbers WHERE n BETWEEN 75001 AND 100000;

-- =====================================================================================
-- 7. NẠP 50,000 DÒNG VÀO BẢNG PAYMENTS (CHIA 2 LÔ X 25,000 DÒNG)
-- =====================================================================================
INSERT INTO payments (payment_id, order_id, amount, payment_method, transaction_status, created_at)
SELECT 
    n,
    n,
    ROUND(3000000 + (n % 30) * 1500000, 2),
    ELT((n % 4) + 1, 'E-WALLET', 'BANK_TRANSFER', 'CREDIT_CARD', 'COD'),
    'SUCCESS',
    TIMESTAMPADD(MINUTE, -(50000 - n), NOW())
FROM seq_numbers
WHERE n BETWEEN 1 AND 25000;

INSERT INTO payments (payment_id, order_id, amount, payment_method, transaction_status, created_at)
SELECT 
    n,
    n,
    ROUND(3000000 + (n % 30) * 1500000, 2),
    ELT((n % 4) + 1, 'E-WALLET', 'BANK_TRANSFER', 'CREDIT_CARD', 'COD'),
    'SUCCESS',
    TIMESTAMPADD(MINUTE, -(50000 - n), NOW())
FROM seq_numbers
WHERE n BETWEEN 25001 AND 50000;

-- Xóa bảng phụ trợ sau khi hoàn tất
DROP TABLE IF EXISTS seq_numbers;

-- Kiểm tra tổng số dòng dữ liệu đã nạp:
SELECT 'users' AS Table_Name, COUNT(*) AS Total_Rows FROM users
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM payments;
