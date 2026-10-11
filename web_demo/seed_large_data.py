"""
Script khởi tạo & nạp dữ liệu lớn (Large Dataset) tự động cho cả MySQL và MongoDB.
Chạy lệnh: python seed_large_data.py
Quy mô nạp:
  - MySQL (ecommerce_db):      212,000 bản ghi (10k users, 2k products, 50k orders, 100k order_items, 50k payments)
  - MongoDB (ecommerce_nosql): 100,000 documents (50k product_reviews + 50k user_activity_logs)
"""
from app import seed_large_database_api

if __name__ == "__main__":
    print("=================================================================")
    print("BẮT ĐẦU KHỞI TẠO & NẠP DỮ LIỆU LỚN (MYSQL 212K + MONGODB 100K)")
    print("=================================================================")
    res = seed_large_database_api(scale="standard")
    print("-> Hoàn tất thành công!")
    print(f"   + Thời gian thực thi: {res['elapsed_seconds']} giây")
    print(f"   + MySQL Node ({res['mysql_node']}): {res['mysql_rows_seeded']:,} dòng")
    print(f"   + MongoDB Node ({res['mongo_node']}): {res['mongodb_docs_seeded']:,} documents")

