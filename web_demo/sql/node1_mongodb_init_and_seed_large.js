// =====================================================================================
// SCRIPT KHỞI TẠO & NẠP DỮ LIỆU LỚN (LARGE DATASET) CHO MONGODB REPLICA SET
// Chạy bằng lệnh: mongosh "mongodb://192.168.1.221:27017/ecommerce_nosql" --file seed_mongodb_large.js
// Hoặc chạy local: mongosh "mongodb://127.0.0.1:27017/ecommerce_nosql" --file seed_mongodb_large.js
// Quy mô dữ liệu tạo tự động: 100,000 Documents:
//   + Collection product_reviews:    50,000 documents (Đánh giá, bình luận, hình ảnh, JSON động)
//   + Collection user_activity_logs: 50,000 documents (Nhật ký hành vi, thiết bị, clickstream)
// =====================================================================================

db = db.getSiblingDB('ecommerce_nosql');

print("=== Bắt đầu làm sạch và khởi tạo dữ liệu lớn trên MongoDB (ecommerce_nosql) ===");
db.product_reviews.drop();
db.user_activity_logs.drop();

const titles = [
    "Chất lượng tuyệt vời, rất đáng tiền!",
    "Giao hàng siêu nhanh, đóng gói kỹ",
    "Sản phẩm dùng ổn trong tầm giá",
    "Máy chạy mượt, pin trâu, màn hình sắc nét",
    "Hàng chính hãng nguyên seal, rất hài lòng",
    "Thiết kế sang trọng, hiệu năng vượt trội",
    "Dịch vụ chăm sóc khách hàng nhiệt tình",
    "Camera chụp đêm xuất sắc, âm thanh sống động"
];

const comments = [
    "Mình đã sử dụng được 2 tuần, máy hoạt động rất mát và ổn định. Shop tư vấn nhiệt tình.",
    "Đóng gói 3 lớp chống sốc cực kỳ cẩn thận. Giao tới Cần Thơ chỉ mất đúng 24h.",
    "Hiệu năng xử lý đồ họa và đa nhiệm rất mượt. Sẽ tiếp tục ủng hộ hệ thống trong tương lai.",
    "Mức giá cạnh tranh so với thị trường, đầy đủ phụ kiện đi kèm và bảo hành điện tử chính hãng.",
    "Trải nghiệm thực tế vượt kỳ vọng, kết nối nhanh, pin dùng liên tục cả ngày không lo hết."
];

const packagings = ["Nguyên seal, hộp cứng cáp", "Bọc chống sốc 3 lớp", "Đóng gói tiêu chuẩn", "Rất cẩn thận, đẹp mắt"];
const speeds = ["Giao hỏa tốc 2h", "Giao nhanh 24h", "Đúng hẹn", "Giao sớm hơn dự kiến"];
const devices = ["iOS App", "Android App", "Desktop Chrome", "Mobile Safari", "Edge Windows"];
const actions = ["VIEW_PRODUCT", "ADD_TO_CART", "SEARCH_KEYWORD", "FILTER_CATEGORY", "VIEW_REVIEWS"];

// 1. NẠP 50,000 DOCUMENTS VÀO COLLECTION product_reviews (10 Batch x 5,000 docs)
print("-> Đang nạp 50,000 documents vào collection 'product_reviews'...");
const totalReviews = 50000;
const batchSize = 5000;

for (let b = 0; b < totalReviews / batchSize; b++) {
    let batch = [];
    for (let i = 1; i <= batchSize; i++) {
        let idx = b * batchSize + i;
        let prodId = ((idx - 1) % 2000) + 1;
        let userId = ((idx - 1) % 10000) + 1;
        let rating = (idx % 10 === 0) ? 3 : ((idx % 4 === 0) ? 4 : 5);
        
        batch.push({
            product_id: prodId,
            user_id: userId,
            rating: rating,
            title: titles[idx % titles.length],
            comment: comments[idx % comments.length] + " (Mã đánh giá #" + idx + ")",
            images: [
                "https://picsum.photos/seed/rev_" + (idx % 500) + "/400/300",
                "https://picsum.photos/seed/sub_" + (idx % 300) + "/400/300"
            ],
            sentiment_attributes: {
                packaging: packagings[idx % packagings.length],
                delivery_speed: speeds[idx % speeds.length],
                verified_purchase: true,
                helpful_votes: (idx % 45)
            },
            device_info: {
                platform: devices[idx % devices.length],
                ip_subnet: "192.168.1." + ((idx % 200) + 10)
            },
            created_at: new Date(Date.now() - (totalReviews - idx) * 60000)
        });
    }
    db.product_reviews.insertMany(batch, { ordered: false });
    print("   + Đã nạp " + ((b + 1) * batchSize) + " / " + totalReviews + " product_reviews");
}

// Tạo Index tối ưu cho truy vấn phân tán và tổng hợp Polyglot
print("-> Đang tạo Index cho 'product_reviews'...");
db.product_reviews.createIndex({ product_id: 1, created_at: -1 });
db.product_reviews.createIndex({ user_id: 1 });
db.product_reviews.createIndex({ rating: 1 });

// 2. NẠP 50,000 DOCUMENTS VÀO COLLECTION user_activity_logs (10 Batch x 5,000 docs)
print("-> Đang nạp 50,000 documents vào collection 'user_activity_logs'...");
const totalLogs = 50000;

for (let b = 0; b < totalLogs / batchSize; b++) {
    let batch = [];
    for (let i = 1; i <= batchSize; i++) {
        let idx = b * batchSize + i;
        batch.push({
            user_id: ((idx - 1) % 10000) + 1,
            product_id: ((idx - 1) % 2000) + 1,
            action: actions[idx % actions.length],
            session_id: "sess_" + ((idx % 5000) + 1000),
            metadata: {
                device: devices[idx % devices.length],
                duration_seconds: (idx % 180) + 5,
                referrer: (idx % 2 === 0) ? "Google Search" : "Direct App"
            },
            created_at: new Date(Date.now() - (totalLogs - idx) * 30000)
        });
    }
    db.user_activity_logs.insertMany(batch, { ordered: false });
    print("   + Đã nạp " + ((b + 1) * batchSize) + " / " + totalLogs + " user_activity_logs");
}

db.user_activity_logs.createIndex({ product_id: 1, action: 1 });
db.user_activity_logs.createIndex({ user_id: 1, created_at: -1 });

print("=== HOÀN TẤT NẠP DỮ LIỆU LỚN TRÊN MONGODB ===");
print("Tổng số product_reviews:    " + db.product_reviews.countDocuments());
print("Tổng số user_activity_logs: " + db.user_activity_logs.countDocuments());
