// =====================================================================================
// TẠO COLLECTION NHẬT KÝ ĐỒNG BỘ DÙNG CHUNG: ecommerce_nosql.sync_audit_logs
// Chạy trên node PRIMARY của replica set rsEcomNhom2 (chạy lại nhiều lần vẫn an toàn):
//   mongosh "mongodb://192.168.1.221:27017/ecommerce_nosql?directConnection=true" --file sql/node1_mongodb_create_sync_audit_logs.js
// App (sync_log_store.py) cũng tự tạo các index này ở lần ghi đầu tiên; script dùng khi dựng cụm mới
// hoặc khi muốn kiểm tra. Tên và tham số index PHẢI khớp _ensure_indexes trong sync_log_store.py,
// nếu không create_index của app sẽ báo lỗi trùng tên index khác tham số.
//
// Mỗi document là một sự kiện đồng bộ (cùng cấu trúc bản ghi trong sync_audit_logs.json cũ):
//   _id               = id của sự kiện (SYNC-xxxxxxxx)
//   ts                = thời điểm (UTC, Date) lấy từ thoi_diem: sắp xếp + TTL 7 ngày
//   ma_nhom_giao_dich = khóa nghiệp vụ (DH-<order>, REV-<id>, CATCHUP-..., RECOVERY-...), duy nhất
//   app_host          = tên máy chạy app đã ghi sự kiện
// =====================================================================================

db = db.getSiblingDB('ecommerce_nosql');

if (!db.getCollectionNames().includes('sync_audit_logs')) {
    db.createCollection('sync_audit_logs');
    print('+ Đã tạo collection sync_audit_logs');
} else {
    print('= Collection sync_audit_logs đã có');
}

// TTL: tự xóa sự kiện cũ hơn 7 ngày (MAX_RETENTION_DAYS)
db.sync_audit_logs.createIndex({ ts: -1 }, { name: 'ts_ttl', expireAfterSeconds: 7 * 86400 });
// Chống trùng: nhiều máy chạy app cùng ghi một sự kiện thì chỉ giữ một bản
db.sync_audit_logs.createIndex(
    { ma_nhom_giao_dich: 1 },
    { name: 'uq_ma_nhom_giao_dich', unique: true, partialFilterExpression: { ma_nhom_giao_dich: { $type: 'string' } } }
);
// Lọc theo hệ CSDL (MYSQL_GR / MONGO_RS) trên tab nhật ký
db.sync_audit_logs.createIndex({ he_csdl: 1, ts: -1 }, { name: 'he_csdl_ts' });

print('Index hiện có:');
db.sync_audit_logs.getIndexes().forEach(function (ix) { print('  - ' + ix.name + ' ' + JSON.stringify(ix.key)); });
print('Số sự kiện: ' + db.sync_audit_logs.countDocuments());
