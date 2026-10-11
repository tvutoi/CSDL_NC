// =====================================================================================
// FILE DÀNH CHO NODE 4 (192.168.1.224) - KIỂM TRA ĐỒNG BỘ & ĐO HIỆU NĂNG TRUY VẤN MONGODB
// Chạy bằng lệnh: mongosh "mongodb://192.168.1.224:27017/ecommerce_nosql" --file node4_mongodb_verify_secondary.js
// =====================================================================================

db = db.getSiblingDB('ecommerce_nosql');
db.getMongo().setReadPref('secondaryPreferred');

print("=== KIỂM TRA TRẠNG THÁI REPLICA SET TỪ NODE 4 (192.168.1.224) ===");
try {
    let status = rs.status();
    status.members.forEach(m => {
        print(" - Node: " + m.name + " | Vai trò: " + m.stateStr + " | Health: " + m.health);
    });
} catch (e) {
    print("Lưu ý: Đang chạy ở chế độ độc lập hoặc kết nối từ Client.");
}

print("\n=== ĐO THỜI GIAN THỰC THI AGGREGATION TRÊN 100,000 DOCUMENTS (EXPLAIN EXECUTION STATS) ===");
let t0 = Date.now();
let explainRes = db.product_reviews.explain("executionStats").aggregate([
    { $match: { product_id: { $lte: 50 } } },
    { $group: { _id: "$product_id", avgRating: { $avg: "$rating" }, reviewCount: { $sum: 1 } } },
    { $sort: { reviewCount: -1 } },
    { $limit: 10 }
]);
let elapsed = Date.now() - t0;
print(" + Thời gian thực thi Aggregation trên tập dữ liệu lớn: " + elapsed + " ms");

