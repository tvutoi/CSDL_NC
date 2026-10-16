
db = db.getSiblingDB('ecommerce_nosql');
db.getMongo().setReadPref('secondaryPreferred');

print("=== KIỂM TRA TRẠNG THÁI REPLICA SET TẠI NODE 3 (192.168.1.223) ===");
try {
    let status = rs.status();
    status.members.forEach(m => {
        print(" - Node: " + m.name + " | Vai trò: " + m.stateStr + " | Health: " + m.health);
    });
} catch (e) {
    print("Lưu ý: Đang chạy ở chế độ độc lập hoặc chưa khởi tạo Replica Set.");
}

print("\n=== ĐỐI CHIẾU SỐ LƯỢNG DOCUMENT TẠI NODE 3 ===");
print(" + Tổng số product_reviews:    " + db.product_reviews.countDocuments());
print(" + Tổng số user_activity_logs: " + db.user_activity_logs.countDocuments());

print("\n=== THỐNG KÊ HÀNH VI NGƯỜI DÙNG THEO LOẠI TƯƠNG TÁC TẠI NODE 3 ===");
let actionStats = db.user_activity_logs.aggregate([
    { $group: { _id: "$action", count: { $sum: 1 }, avgDurationSec: { $avg: "$metadata.duration_seconds" } } },
    { $sort: { count: -1 } }
]).toArray();
printjson(actionStats);

