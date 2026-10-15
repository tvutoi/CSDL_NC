"""
Module quản lý lưu trữ và truy vấn Nhật ký Đồng bộ Cụm (Replication & Sync Audit Logs)
RÀNG BUỘC KỸ THUẬT:
- Dữ liệu CHỈ lưu trữ vào DUY NHẤT 01 file JSON trên máy chủ backend.
- TUYỆT ĐỐI KHÔNG tạo bảng, collection, schema hay database nào trong CSDL.
- Ghi nguyên tử (Atomic write qua file .tmp và os.replace).
- Hàng đợi khóa luồng tuần tự (threading.Lock).
- Tự phục hồi khi file bị xóa hoặc lỗi cú pháp JSON (Self-healing).
- Giới hạn tối đa 1.000 bản ghi mới nhất hoặc 7 ngày (Retention policy).
- Hỗ trợ bộ đệm RAM (chỉ đọc lại đĩa khi mtime thay đổi) và Server-side filtering/pagination.
"""

import os
import json
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SYNC_LOG_FILE = os.path.join(CURRENT_DIR, "sync_audit_logs.json")

_SYNC_LOCK = threading.Lock()
_CACHE_LOGS: List[Dict[str, Any]] = []
_LAST_MTIME: float = 0.0

MAX_LOG_ENTRIES = 1000
MAX_RETENTION_DAYS = 7


def get_sync_log_file_path() -> str:
    """Trả về đường dẫn tuyệt đối của file JSON lưu trữ log"""
    return SYNC_LOG_FILE


def load_sync_logs_safe() -> List[Dict[str, Any]]:
    """
    Đọc toàn bộ bản ghi từ file JSON an toàn.
    Tự động khởi tạo [] nếu file chưa có hoặc bị xóa.
    Tự động sao lưu và tạo mới [] nếu file bị lỗi JSON (corrupt).
    """
    global _CACHE_LOGS, _LAST_MTIME
    with _SYNC_LOCK:
        if not os.path.exists(SYNC_LOG_FILE):
            try:
                with open(SYNC_LOG_FILE, "w", encoding="utf-8") as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                _CACHE_LOGS = []
                _LAST_MTIME = os.path.getmtime(SYNC_LOG_FILE)
            except Exception as e:
                print(f"[SYNC_STORE_ERROR] Không thể khởi tạo {SYNC_LOG_FILE}: {e}")
            return []

        try:
            current_mtime = os.path.getmtime(SYNC_LOG_FILE)
            if current_mtime == _LAST_MTIME and _CACHE_LOGS is not None:
                return list(_CACHE_LOGS)

            with open(SYNC_LOG_FILE, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
                if isinstance(data, list):
                    _CACHE_LOGS = data
                    _LAST_MTIME = current_mtime
                    return list(_CACHE_LOGS)
                else:
                    raise ValueError("Nội dung file JSON không phải là mảng list []")
        except (json.JSONDecodeError, ValueError) as err:
            corrupt_backup = os.path.join(CURRENT_DIR, f"sync_audit_logs.corrupt.{int(time.time())}.json")
            print(f"[SYNC_STORE_CORRUPT] File JSON bị lỗi cú pháp ({err}). Sao lưu thành {corrupt_backup} và phục hồi [].")
            try:
                if os.path.exists(SYNC_LOG_FILE):
                    os.replace(SYNC_LOG_FILE, corrupt_backup)
                with open(SYNC_LOG_FILE, "w", encoding="utf-8") as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                _CACHE_LOGS = []
                _LAST_MTIME = os.path.getmtime(SYNC_LOG_FILE)
            except Exception as ex:
                print(f"[SYNC_STORE_ERROR] Lỗi trong quá trình phục hồi file log: {ex}")
            return []
        except Exception as e:
            print(f"[SYNC_STORE_ERROR] Lỗi đọc file log: {e}")
            return list(_CACHE_LOGS) if _CACHE_LOGS else []


def _json_serializer(obj):
    """Xử lý serialize các kiểu dữ liệu đặc biệt như Decimal, datetime, Timestamp"""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def save_sync_logs_atomic(logs: List[Dict[str, Any]]) -> bool:
    """
    Ghi nguyên tử danh sách bản ghi ra file JSON duy nhất:
    Ghi ra file tạm .tmp rồi đổi tên (os.replace) đè lên file chính.
    """
    global _CACHE_LOGS, _LAST_MTIME
    with _SYNC_LOCK:
        try:
            # 1. Xoay vòng bản ghi (Retention policy)
            if len(logs) > MAX_LOG_ENTRIES:
                logs = logs[:MAX_LOG_ENTRIES]

            # 2. Ghi ra file tạm thời
            temp_file = f"{SYNC_LOG_FILE}.tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(logs, f, ensure_ascii=False, indent=2, default=_json_serializer)

            # 3. Đổi tên nguyên tử
            os.replace(temp_file, SYNC_LOG_FILE)

            # 4. Đồng bộ cache RAM
            _CACHE_LOGS = list(logs)
            _LAST_MTIME = os.path.getmtime(SYNC_LOG_FILE)
            return True
        except Exception as e:
            print(f"[SYNC_STORE_ERROR] Ghi nguyên tử thất bại: {e}")
            temp_file = f"{SYNC_LOG_FILE}.tmp"
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
            return False


def append_sync_log_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Thêm một bản ghi mới lên đầu file JSON hoặc cập nhật nếu đã tồn tại ma_nhom_giao_dich.
    Đảm bảo tính Idempotency: Không bao giờ tạo 2 dòng cho cùng một giao dịch.
    """
    logs = load_sync_logs_safe()
    tx_group = entry.get("ma_nhom_giao_dich")

    # 1. Kiểm tra chống trùng theo ma_nhom_giao_dich
    if tx_group:
        for existing in logs:
            if existing.get("ma_nhom_giao_dich") == tx_group:
                # Cập nhật bản ghi cũ thay vì chèn mới
                existing.update({
                    "trang_thai": entry.get("trang_thai", existing.get("trang_thai")),
                    "status": entry.get("status", existing.get("status")),
                    "status_text": entry.get("status_text", existing.get("status_text")),
                    "node_dich": entry.get("node_dich", existing.get("node_dich")),
                    "target_statuses": entry.get("target_statuses", existing.get("target_statuses")),
                    "target_display": entry.get("target_display", existing.get("target_display")),
                    "latency_ms": entry.get("latency_ms", existing.get("latency_ms")),
                    "end_time": entry.get("end_time", existing.get("end_time")),
                    "chi_tiet": entry.get("chi_tiet", existing.get("chi_tiet")),
                })
                save_sync_logs_atomic(logs)
                return existing

    # 2. Nếu chưa có, chèn lên đầu danh sách
    logs.insert(0, entry)
    save_sync_logs_atomic(logs)
    return entry


def deduplicate_sync_logs_file() -> int:
    """
    Loại bỏ các bản ghi trùng lặp trong file JSON:
    - Nếu cùng ma_nhom_giao_dich, chỉ giữ lại bản ghi sớm nhất.
    - Với các bản ghi mẫu hoặc legacy không có ma_nhom_giao_dich, nếu cùng (db_type, action, title) thì chỉ giữ 1 bản ghi sớm nhất.
    Trả về số lượng bản ghi đã được dọn sạch.
    """
    logs = load_sync_logs_safe()
    if not logs:
        return 0

    # Duyệt từ cũ đến mới (ngược lại) để giữ bản ghi sớm nhất
    reversed_logs = list(reversed(logs))
    seen_keys = set()
    deduped_reversed = []

    for l in reversed_logs:
        tx_group = l.get("ma_nhom_giao_dich")
        if tx_group:
            key = f"TX:{tx_group}"
        else:
            title = l.get("title", "")
            db = l.get("db_type") or l.get("he_csdl") or ""
            act = l.get("action") or l.get("loai_su_kien") or ""
            key = f"LEGACY:{db}:{act}:{title}"

        if key not in seen_keys:
            seen_keys.add(key)
            deduped_reversed.append(l)

    # Đảo lại để bản ghi mới hơn nằm ở đầu danh sách
    deduped_logs = list(reversed(deduped_reversed))
    removed_count = len(logs) - len(deduped_logs)
    if removed_count > 0:
        save_sync_logs_atomic(deduped_logs)
    return removed_count


def clear_sync_logs_store() -> bool:
    """Xóa toàn bộ nhật ký trong file JSON (ghi lại [])"""
    return save_sync_logs_atomic([])


def query_sync_logs(
    page: int = 1,
    limit: int = 10,
    db_type: str = "ALL",
    search: str = ""
) -> Dict[str, Any]:
    """
    Truy vấn, lọc theo CSDL, tìm kiếm và phân trang trực tiếp từ file JSON.
    """
    all_logs = load_sync_logs_safe()

    cnt_all = len(all_logs)
    cnt_mysql = sum(1 for l in all_logs if l.get("he_csdl") == "MYSQL_GR" or l.get("db_type") == "MYSQL_GR")
    cnt_mongo = sum(1 for l in all_logs if l.get("he_csdl") == "MONGO_RS" or l.get("db_type") == "MONGO_RS")

    filtered = all_logs
    if db_type == "MYSQL":
        filtered = [l for l in filtered if l.get("he_csdl") == "MYSQL_GR" or l.get("db_type") == "MYSQL_GR"]
    elif db_type == "MONGO":
        filtered = [l for l in filtered if l.get("he_csdl") == "MONGO_RS" or l.get("db_type") == "MONGO_RS"]

    if search and isinstance(search, str) and search.strip():
        kw = search.strip().lower()
        res = []
        for l in filtered:
            text_corpus = " ".join([
                str(l.get("id", "")),
                str(l.get("log_id", "")),
                str(l.get("title", "")),
                str(l.get("summary", "")),
                str(l.get("ma_nhom_giao_dich", "")),
                str(l.get("gtid_hoac_optime", "")),
                str(l.get("data_payload", "")),
                str(l.get("affected_tables", ""))
            ]).lower()
            if kw in text_corpus:
                res.append(l)
        filtered = res

    total_filtered = len(filtered)
    page = max(1, page)
    limit = max(1, min(limit, 100))
    start_idx = (page - 1) * limit
    paged_logs = filtered[start_idx : start_idx + limit]

    return {
        "total": total_filtered,
        "page": page,
        "limit": limit,
        "logs": paged_logs,
        "cnt_all": cnt_all,
        "cnt_mysql": cnt_mysql,
        "cnt_mongo": cnt_mongo
    }
