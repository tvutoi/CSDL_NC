
db = db.getSiblingDB('ecommerce_nosql');

// cho phep doc secondary
db.getMongo().setReadPref('secondaryPreferred');

print("=== KIỂM TRA TRẠNG THÁI ĐỒNG BỘ REPLICA SET TẠI NODE 2 (192.168.1.222) ===");
try {
    let status = rs.status();
    status.members.forEach(m => {
        print(" - Node: " + m.name + " | Vai trò: " + m.stateStr + " | Health: " + m.health);
    });
} catch (e) {
    print("Lưu ý: Đang chạy ở chế độ độc lập hoặc chưa khởi tạo Replica Set.");
}

print("\n=== ĐỐI CHIẾU SỐ LƯỢNG DOCUMENT ĐÃ ĐỒNG BỘ SANG NODE 2 ===");
print(" + Tổng số product_reviews tại Node 2:    " + db.product_reviews.countDocuments());
print(" + Tổng số user_activity_logs tại Node 2: " + db.user_activity_logs.countDocuments());

print("\n=== TRUY VẤN TỔNG HỢP (AGGREGATION) TOP 5 SẢN PHẨM ĐƯỢC ĐÁNH GIÁ CAO NHẤT TẠI NODE 2 ===");
let topRated = db.product_reviews.aggregate([
    { $group: { _id: "$product_id", avgRating: { $avg: "$rating" }, totalReviews: { $sum: 1 } } },
    { $sort: { totalReviews: -1, avgRating: -1 } },
    { $limit: 5 }
]).toArray();
printjson(topRated);

