-- =====================================================================================
-- SỬA brand / category CỦA 5 SẢN PHẨM CHỦ LỰC (product_id 1..5)
-- -------------------------------------------------------------------------------------
-- Lỗi: bản seed cũ đặt tên riêng cho 5 sản phẩm này nhưng brand/category vẫn sinh theo
-- công thức chung, nên bị lệch (vd iPhone 15 = Samsung/Laptop). Tìm "samsung" ra iPhone 15.
-- Seed mới (node1_mysql_seed_large_data.sql) đã sửa; script này sửa DB đã seed từ trước.
--
-- CHẠY TRÊN NODE PRIMARY, khi nhóm MGR có ĐỦ ĐA SỐ (ít nhất 3/4 node ONLINE), ví dụ:
--   Get-Content .\sql\fix_products_1_5_brand_category.sql -Raw | & "C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe" -h 192.168.1.222 -u root -p -t
-- Thiếu đa số thì câu UPDATE sẽ bị treo; khi đó nhấn Ctrl+C, giao dịch tự hủy, không mất dữ liệu.
-- Chạy lại nhiều lần vẫn an toàn (giá trị đích cố định).
-- =====================================================================================

-- 1. KIỂM TRA TRƯỚC: node đang kết nối phải là PRIMARY, nhóm phải có >= 3 node ONLINE
SELECT MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE
FROM performance_schema.replication_group_members
ORDER BY MEMBER_HOST;

SELECT
    @@hostname AS node_dang_ket_noi,
    (@@server_uuid = (SELECT VARIABLE_VALUE FROM performance_schema.global_status
                      WHERE VARIABLE_NAME = 'group_replication_primary_member')) AS la_primary,
    @@global.super_read_only AS super_read_only,
    (SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_STATE = 'ONLINE') AS so_node_online,
    (SELECT COUNT(*) FROM performance_schema.replication_group_members) AS tong_node_trong_nhom;
-- Kết quả hợp lệ: la_primary = 1, super_read_only = 0, so_node_online * 2 > tong_node_trong_nhom.
-- Nếu không đúng: DỪNG, không chạy tiếp phần dưới.

-- 2. DỮ LIỆU TRƯỚC KHI SỬA
SELECT product_id, product_name, brand, category
FROM ecommerce_db.products
WHERE product_id BETWEEN 1 AND 5
ORDER BY product_id;

-- 3. SỬA (giới hạn chờ khóa 10 s để không treo vô hạn vì khóa dòng)
SET SESSION innodb_lock_wait_timeout = 10;

UPDATE ecommerce_db.products
SET category = ELT(product_id, 'Smartphone', 'Laptop', 'Audio', 'Laptop', 'Smartphone'),
    brand    = ELT(product_id, 'Apple', 'Dell', 'Sony', 'Apple', 'Samsung')
WHERE product_id BETWEEN 1 AND 5;

-- 4. KIỂM TRA SAU KHI SỬA (mong đợi:
--    1 Apple/Smartphone, 2 Dell/Laptop, 3 Sony/Audio, 4 Apple/Laptop, 5 Samsung/Smartphone)
SELECT product_id, product_name, brand, category
FROM ecommerce_db.products
WHERE product_id BETWEEN 1 AND 5
ORDER BY product_id;

-- 5. Không còn sản phẩm brand Samsung mà tên không chứa "Samsung" (mong đợi 0)
SELECT COUNT(*) AS sp_brand_samsung_ten_khong_co_samsung
FROM ecommerce_db.products
WHERE brand = 'Samsung' AND product_name NOT LIKE '%Samsung%';

-- 6. Kiểm tra trên các node phụ (chạy riêng trên từng node, sau vài giây):
--    SELECT product_id, brand, category FROM ecommerce_db.products WHERE product_id BETWEEN 1 AND 5;
