# Nhat ky dong bo: nguon chinh la MongoDB (collection sync_audit_logs cua replica set),
# moi may chay app deu doc/ghi chung nen thay cung mot nhat ky.
# File sync_audit_logs.json KHONG con la noi luu: chi dung lam bo dem khi cum Mongo khong co Primary
# (id cho day nam trong sync_audit_logs.pending.json). Day bu len Mongo xong thi file duoc lam rong.
#
# Giao dien ham giu nguyen nhu ban luu file (load / save / append / clear / dedup / query) de app.py
# gan nhu khong phai doi. save_sync_logs_atomic(logs) chi day len cac ban ghi THAY DOI so voi luc
# load (so theo doi tuong dict duoc load tra ve), khong bao gio xoa ngam; xoa phai goi delete_sync_logs.

import os
import json
import copy
import socket
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Callable, Iterable

try:
    from pymongo import MongoClient, DESCENDING
    from pymongo.errors import DuplicateKeyError, BulkWriteError, PyMongoError
    from bson.errors import BSONError
except Exception:  # pymongo luon co trong requirements; phong khi thieu thi chay che do file
    MongoClient = None
    DuplicateKeyError = BulkWriteError = PyMongoError = BSONError = Exception
    DESCENDING = -1

# Loi ghi Mongo -> chuyen sang file. BSONError (vd InvalidDocument) KHONG phai PyMongoError.
_MONGO_ERRORS = (PyMongoError, BSONError)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SYNC_LOG_FILE = os.path.join(CURRENT_DIR, "sync_audit_logs.json")
PENDING_FILE = os.path.join(CURRENT_DIR, "sync_audit_logs.pending.json")

COLLECTION_NAME = "sync_audit_logs"
MAX_LOG_ENTRIES = 1000
MAX_RETENTION_DAYS = 7          # TTL index tren truong ts
READ_CACHE_SEC = 1.0            # gom cac lan load lien tiep trong 1 s thanh 1 truy van
RETRY_BACKOFF_SEC = 5.0         # Mongo loi thi 5 s sau moi thu ket noi lai (tranh treo request)
CLIENT_REVALIDATE_SEC = 15.0    # dinh ky hoi lai hello de bat Primary doi

_SYNC_LOCK = threading.RLock()

# --- cau hinh do app.py truyen vao ---
_get_nodes: Callable[[], List[str]] = lambda: []
_get_db_name: Callable[[], str] = lambda: "ecommerce_nosql"
APP_HOST = socket.gethostname()

# --- trang thai ket noi ---
_client = None
_client_host: Optional[str] = None
_client_is_primary = False
_client_checked_at = 0.0
_last_fail_at = 0.0
_no_primary_at = 0.0
_indexes_ready_for: Optional[str] = None
_insert_counter = 0

# --- dong ho chung cua cum: gio cua Primary MongoDB (hello.localTime) ---
_clock_offset_sec = 0.0         # gio Primary - gio may chay app
_clock_checked_at = 0.0
CLOCK_REFRESH_SEC = 60.0

# --- cache doc + dang ky doi tuong da tra ra (de save biet ban ghi nao thay doi) ---
_CACHE_LOGS: List[Dict[str, Any]] = []
_CACHE_AT = 0.0
_CACHE_SOURCE = "file"          # "mongo" | "file"
_ISSUED = deque(maxlen=8)       # moi phan tu: {id(obj): (obj, serialized)}


def configure(get_nodes: Callable[[], List[str]], get_db_name: Callable[[], str]) -> None:
    global _get_nodes, _get_db_name
    _get_nodes = get_nodes
    _get_db_name = get_db_name


def get_sync_log_file_path() -> str:
    return SYNC_LOG_FILE


def get_store_status() -> Dict[str, Any]:
    with _SYNC_LOCK:
        pend = _read_pending()
        return {
            "source": _CACHE_SOURCE,
            "mongo_host": _client_host,
            "mongo_writable": bool(_client is not None and _client_is_primary),
            "collection": f"{_get_db_name()}.{COLLECTION_NAME}",
            "pending_upserts": len(pend.get("upsert", [])),
            "pending_deletes": len(pend.get("delete", [])),
            "pending_clear": bool(pend.get("clear_before")),
            "app_host": APP_HOST,
        }


# ============================================================================
# Tien ich
# ============================================================================

def _json_serializer(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def _ser(doc: Dict[str, Any]) -> str:
    return json.dumps(doc, ensure_ascii=False, sort_keys=True, default=_json_serializer)


def _doc_id(doc: Dict[str, Any]) -> Optional[str]:
    return doc.get("id") or doc.get("log_id")


def _ts_of(doc: Dict[str, Any]) -> datetime:
    # thoi_diem la gio dia phuong cua may ghi ("%Y-%m-%d %H:%M:%S.%f"); doi sang UTC cho TTL va sap xep
    raw = doc.get("thoi_diem")
    if isinstance(raw, str):
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt).astimezone(timezone.utc)
            except ValueError:
                continue
    return datetime.now(timezone.utc)


def _clean(obj: Any) -> Any:
    # Di qua JSON nhu ban luu file cu: Decimal (gia tu MySQL) -> chuoi, datetime -> ISO.
    # BSON khong ma hoa duoc Decimal: insert/update se nem InvalidDocument va mat su kien.
    return json.loads(json.dumps(obj, ensure_ascii=False, default=_json_serializer))


def _to_mongo(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = _clean(doc)
    d["_id"] = _doc_id(doc)
    d["ts"] = _ts_of(doc)
    d.setdefault("app_host", APP_HOST)
    return d


def _from_mongo(d: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(d)
    d.pop("_id", None)
    d.pop("ts", None)
    return d


def _is_port_open(host: str, port: int, timeout: float = 0.45) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _split_host(h: str):
    if ":" in h:
        a, b = h.rsplit(":", 1)
        try:
            return a, int(b)
        except ValueError:
            return a, 27017
    return h, 27017


# ============================================================================
# File local (ban sao + hang doi cho day)
# ============================================================================

def _read_file_logs() -> List[Dict[str, Any]]:
    if not os.path.exists(SYNC_LOG_FILE):
        return []
    try:
        with open(SYNC_LOG_FILE, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        raise ValueError("Nội dung file JSON không phải là mảng list []")
    except (json.JSONDecodeError, ValueError) as err:
        corrupt_backup = os.path.join(CURRENT_DIR, f"sync_audit_logs.corrupt.{int(time.time())}.json")
        print(f"[SYNC_STORE_CORRUPT] File JSON bị lỗi cú pháp ({err}). Sao lưu thành {corrupt_backup}.")
        try:
            os.replace(SYNC_LOG_FILE, corrupt_backup)
        except Exception:
            pass
        return []
    except Exception as e:
        print(f"[SYNC_STORE_ERROR] Lỗi đọc file log: {e}")
        return []


def _write_json_atomic(path: str, data: Any) -> bool:
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=_json_serializer)
        os.replace(tmp, path)
        return True
    except Exception as e:
        print(f"[SYNC_STORE_ERROR] Ghi {os.path.basename(path)} thất bại: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False


def _write_file_logs(logs: List[Dict[str, Any]]) -> bool:
    return _write_json_atomic(SYNC_LOG_FILE, logs[:MAX_LOG_ENTRIES])


def _read_pending() -> Dict[str, Any]:
    if not os.path.exists(PENDING_FILE):
        return {"upsert": [], "delete": [], "clear_before": None, "migrated": False}
    try:
        with open(PENDING_FILE, "r", encoding="utf-8") as f:
            p = json.load(f)
        p.setdefault("upsert", [])
        p.setdefault("delete", [])
        p.setdefault("clear_before", None)
        p.setdefault("migrated", False)
        return p
    except Exception:
        return {"upsert": [], "delete": [], "clear_before": None, "migrated": False}


def _write_pending(p: Dict[str, Any]) -> None:
    _write_json_atomic(PENDING_FILE, p)


def _mark_pending(upsert_ids: Iterable[str] = (), delete_ids: Iterable[str] = (), clear: bool = False) -> None:
    p = _read_pending()
    up = list(dict.fromkeys(list(p["upsert"]) + [i for i in upsert_ids if i]))
    de = list(dict.fromkeys(list(p["delete"]) + [i for i in delete_ids if i]))
    if clear:
        p["clear_before"] = datetime.now(timezone.utc).isoformat()
        up, de = [], []
    p["upsert"] = [i for i in up if i not in de]
    p["delete"] = de
    _write_pending(p)


# ============================================================================
# Ket noi Mongo (khong dung get_mongo_client cua app de tranh tac dung phu reconfig)
# ============================================================================

def _drop_client() -> None:
    global _client, _client_host, _client_is_primary
    try:
        if _client is not None:
            _client.close()
    except Exception:
        pass
    _client, _client_host, _client_is_primary = None, None, False


def _connect(need_primary: bool):
    """Tra ve collection hoac None. Uu tien Primary; doc thi chap nhan Secondary."""
    global _client, _client_host, _client_is_primary, _client_checked_at, _last_fail_at, _no_primary_at
    if MongoClient is None:
        return None
    now = time.time()

    if _client is not None and (now - _client_checked_at) < CLIENT_REVALIDATE_SEC:
        if _client_is_primary or not need_primary:
            return _client[_get_db_name()][COLLECTION_NAME]
        if (now - _no_primary_at) < RETRY_BACKOFF_SEC:
            return None  # vua quet: cum chua co Primary, khong quet lai ngay

    if _client is None and (now - _last_fail_at) < RETRY_BACKOFF_SEC:
        return None

    _drop_client()
    secondary = None
    for host in list(_get_nodes() or []):
        h, p = _split_host(host)
        if not _is_port_open(h, p):
            continue
        c = None
        try:
            c = MongoClient(
                f"mongodb://{h}:{p}/?directConnection=true",
                serverSelectionTimeoutMS=800, connectTimeoutMS=800, socketTimeoutMS=3000, w=1,
            )
            hello = c.admin.command("hello")
            if hello.get("isWritablePrimary"):
                _update_clock(hello)
                if secondary is not None:
                    secondary[0].close()
                _client, _client_host, _client_is_primary = c, f"{h}:{p}", True
                _client_checked_at = now
                return _client[_get_db_name()][COLLECTION_NAME]
            if hello.get("secondary") and secondary is None:
                secondary = (c, f"{h}:{p}")
                continue
            c.close()
        except Exception:
            try:
                if c is not None:
                    c.close()
            except Exception:
                pass

    if secondary is not None:
        _client, _client_host, _client_is_primary = secondary[0], secondary[1], False
        _client_checked_at = now
        _no_primary_at = now
        if not need_primary:
            return _client[_get_db_name()][COLLECTION_NAME]
        return None

    _last_fail_at = now
    return None


def _update_clock(hello: Dict[str, Any]) -> None:
    global _clock_offset_sec, _clock_checked_at
    lt = hello.get("localTime")
    if isinstance(lt, datetime):
        if lt.tzinfo is None:
            lt = lt.replace(tzinfo=timezone.utc)
        _clock_offset_sec = (lt - datetime.now(timezone.utc)).total_seconds()
        _clock_checked_at = time.time()


def cluster_now() -> datetime:
    """Gio dia phuong (naive) theo dong ho Primary MongoDB, dung chung cho moi may chay app.
    Dong ho tung may co the lech (vd .223 nhanh ~7 phut); ghi created_at/thoi_diem theo gio nay thi
    moi may hien thi giong nhau. Khong lien lac duoc Primary thi giu do lech lan gan nhat (mac dinh 0)."""
    with _SYNC_LOCK:
        if (time.time() - _clock_checked_at) >= CLOCK_REFRESH_SEC:
            col = _connect(need_primary=True)
            if col is not None and _client is not None:
                try:
                    _update_clock(_client.admin.command("hello"))
                except _MONGO_ERRORS as e:
                    _on_mongo_error(e, "đọc giờ Primary")
        return datetime.now() + timedelta(seconds=_clock_offset_sec)


def get_clock_offset_sec() -> float:
    return _clock_offset_sec


def _on_mongo_error(err: Exception, where: str) -> None:
    global _last_fail_at
    print(f"[SYNC_STORE_MONGO] {where}: {err}")
    _drop_client()
    _last_fail_at = time.time()


def _ensure_indexes(col) -> None:
    global _indexes_ready_for
    key = f"{_client_host}/{_get_db_name()}"
    if _indexes_ready_for == key:
        return
    col.create_index([("ts", DESCENDING)], name="ts_ttl",
                     expireAfterSeconds=MAX_RETENTION_DAYS * 86400)
    col.create_index("ma_nhom_giao_dich", name="uq_ma_nhom_giao_dich", unique=True,
                     partialFilterExpression={"ma_nhom_giao_dich": {"$type": "string"}})
    col.create_index([("he_csdl", 1), ("ts", DESCENDING)], name="he_csdl_ts")
    _indexes_ready_for = key


def _upsert_docs(col, docs: List[Dict[str, Any]]) -> List[str]:
    """Ghi de theo _id. Trung ma_nhom_giao_dich voi ban ghi khac (may khac da ghi cung su kien)
    thi bo qua ban nay. Tra ve danh sach id da xu ly xong (ke ca bi bo qua vi trung)."""
    done = []
    for d in docs:
        md = _to_mongo(d)
        try:
            col.replace_one({"_id": md["_id"]}, md, upsert=True)
        except DuplicateKeyError:
            pass
        done.append(md["_id"])
    return done


def _insert_if_absent(col, docs: List[Dict[str, Any]]) -> None:
    """Chen ban ghi chua co; trung _id hoac ma_nhom_giao_dich thi bo qua, KHONG ghi de."""
    md = [_to_mongo(d) for d in docs if _doc_id(d)]
    if not md:
        return
    try:
        col.insert_many(md, ordered=False)
    except BulkWriteError:
        pass  # cac loi con lai deu la trung khoa


def _flush_pending(col) -> None:
    """Day len Mongo nhung thay doi luc mat ket noi. Goi khi da co collection ghi duoc."""
    p = _read_pending()
    changed = False

    if p.get("clear_before"):
        try:
            cb = datetime.fromisoformat(p["clear_before"])
        except Exception:
            cb = datetime.now(timezone.utc)
        col.delete_many({"ts": {"$lte": cb}})
        p["clear_before"] = None
        changed = True

    file_logs = None
    if not p.get("migrated"):
        # Lan dau may nay ket noi duoc: chen nhat ky file cu len (gop nhat ky cua moi may), khong ghi de.
        # Giu ban sao file cu o sync_audit_logs.json.bak (*.bak da nam trong .gitignore).
        file_logs = _read_file_logs()
        if file_logs:
            _write_json_atomic(SYNC_LOG_FILE + ".bak", file_logs)
        _insert_if_absent(col, file_logs)
        p["migrated"] = True
        changed = True

    if p.get("upsert"):
        if file_logs is None:
            file_logs = _read_file_logs()
        want = set(p["upsert"])
        _upsert_docs(col, [l for l in file_logs if _doc_id(l) in want])
        p["upsert"] = []
        changed = True

    if p.get("delete"):
        col.delete_many({"_id": {"$in": list(p["delete"])}})
        p["delete"] = []
        changed = True

    if changed:
        _write_pending(p)
        # Moi thu trong file da len Mongo: lam rong bo dem
        if os.path.exists(SYNC_LOG_FILE):
            _write_file_logs([])


def _trim(col) -> None:
    # Giu toi da MAX_LOG_ENTRIES ban ghi moi nhat (TTL lo phan cu hon 7 ngay)
    edge = list(col.find({}, {"ts": 1}).sort("ts", DESCENDING).skip(MAX_LOG_ENTRIES).limit(1))
    if edge:
        col.delete_many({"ts": {"$lte": edge[0]["ts"]}})


def _writable_collection():
    col = _connect(need_primary=True)
    if col is None:
        return None
    try:
        _ensure_indexes(col)
        _flush_pending(col)
        return col
    except _MONGO_ERRORS as e:
        _on_mongo_error(e, "chuẩn bị ghi")
        return None


# ============================================================================
# API dung boi app.py
# ============================================================================

def _issue(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Tra ban sao sau cho nguoi goi va ghi nho noi dung luc tra, de save biet cai nao bi sua."""
    out = copy.deepcopy(logs)
    _ISSUED.append({id(o): (o, _ser(o)) for o in out})
    return out


def _lookup_issued(obj: Dict[str, Any]):
    oid = id(obj)
    for reg in reversed(_ISSUED):
        hit = reg.get(oid)
        if hit is not None and hit[0] is obj:
            return hit
    return None


def _invalidate_cache() -> None:
    global _CACHE_AT
    _CACHE_AT = 0.0


def load_sync_logs_safe() -> List[Dict[str, Any]]:
    """Danh sach nhat ky moi nhat truoc (toi da 1000). Doc Mongo; khong duoc thi doc file local."""
    global _CACHE_LOGS, _CACHE_AT, _CACHE_SOURCE
    with _SYNC_LOCK:
        if (time.time() - _CACHE_AT) < READ_CACHE_SEC:
            return _issue(_CACHE_LOGS)

        logs = None
        col = _writable_collection()
        if col is None:
            col = _connect(need_primary=False)
        if col is not None:
            try:
                cur = col.find({}).sort("ts", DESCENDING).limit(MAX_LOG_ENTRIES)
                logs = [_from_mongo(d) for d in cur]
                pend = _read_pending()
                if not pend.get("migrated"):
                    # Chua chen duoc nhat ky file cu len Mongo (dang doc tu Secondary): giu file, ghep de hien thi
                    have = {_doc_id(l) for l in logs}
                    extra = [l for l in _read_file_logs() if _doc_id(l) not in have]
                    if extra:
                        logs = sorted(extra + logs, key=_ts_of, reverse=True)[:MAX_LOG_ENTRIES]
                elif pend.get("upsert"):
                    # Doc tu Secondary khi chua day duoc: ghep cac ban ghi local dang cho
                    want = set(pend["upsert"])
                    have = {_doc_id(l) for l in logs}
                    extra = [l for l in _read_file_logs() if _doc_id(l) in want and _doc_id(l) not in have]
                    if extra:
                        logs = sorted(extra + logs, key=_ts_of, reverse=True)[:MAX_LOG_ENTRIES]
                # Khong ghi ban sao ra file: file chi dung lam bo dem khi Mongo khong ghi duoc
                _CACHE_SOURCE = "mongo"
            except _MONGO_ERRORS as e:
                _on_mongo_error(e, "đọc nhật ký")
                logs = None

        if logs is None:
            logs = _read_file_logs()
            _CACHE_SOURCE = "file"

        _CACHE_LOGS = logs
        _CACHE_AT = time.time()
        return _issue(logs)


def save_sync_logs_atomic(logs: List[Dict[str, Any]]) -> bool:
    """Day len cac ban ghi moi hoac bi sua so voi luc load. Khong xoa ban ghi nao."""
    with _SYNC_LOCK:
        logs = list(logs[:MAX_LOG_ENTRIES])
        changed = []
        for d in logs:
            if not _doc_id(d):
                continue
            hit = _lookup_issued(d)
            if hit is not None and hit[1] == _ser(d):
                continue
            changed.append(d)

        ok = True
        if changed:
            col = _writable_collection()
            pushed = False
            if col is not None:
                try:
                    _upsert_docs(col, changed)
                    pushed = True
                except _MONGO_ERRORS as e:
                    _on_mongo_error(e, "ghi nhật ký")
            if not pushed:
                ok = _write_file_logs(logs)
                _mark_pending(upsert_ids=[_doc_id(d) for d in changed])

        # cac doi tuong nay da khop voi kho, lan save sau khong day lai neu khong sua tiep
        _ISSUED.append({id(o): (o, _ser(o)) for o in logs})
        _invalidate_cache()
        return ok


def append_sync_log_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Them mot su kien. Trung ma_nhom_giao_dich thi cap nhat ban ghi da co (ke ca do may khac ghi)."""
    global _insert_counter
    fields = ("trang_thai", "status", "status_text", "node_dich", "target_statuses",
              "target_display", "latency_ms", "end_time", "chi_tiet")
    with _SYNC_LOCK:
        tx_group = entry.get("ma_nhom_giao_dich")
        col = _writable_collection()
        if col is not None:
            try:
                existing = col.find_one({"ma_nhom_giao_dich": tx_group}) if tx_group else None
                if existing is None:
                    try:
                        col.insert_one(_to_mongo(entry))
                        _invalidate_cache()
                        _insert_counter += 1
                        if _insert_counter % 50 == 0:
                            _trim(col)
                        return entry
                    except DuplicateKeyError:
                        existing = col.find_one({"ma_nhom_giao_dich": tx_group}) if tx_group else None
                        if existing is None:
                            return entry
                upd = _clean({k: entry.get(k, existing.get(k)) for k in fields})
                col.update_one({"_id": existing["_id"]}, {"$set": upd})
                existing.update(upd)
                _invalidate_cache()
                return _from_mongo(existing)
            except _MONGO_ERRORS as e:
                _on_mongo_error(e, "thêm nhật ký")

        # Mat ket noi: ghi vao file local, danh dau cho day
        logs = _read_file_logs()
        result = entry
        merged = False
        if tx_group:
            for existing in logs:
                if existing.get("ma_nhom_giao_dich") == tx_group:
                    existing.update({k: entry.get(k, existing.get(k)) for k in fields})
                    result = existing
                    merged = True
                    break
        if not merged:
            logs.insert(0, entry)
        _write_file_logs(logs)
        _mark_pending(upsert_ids=[_doc_id(result)])
        _invalidate_cache()
        return result


def delete_sync_logs(ids: Iterable[str]) -> int:
    ids = [i for i in ids if i]
    if not ids:
        return 0
    with _SYNC_LOCK:
        col = _writable_collection()
        if col is not None:
            try:
                n = col.delete_many({"_id": {"$in": ids}}).deleted_count
                _invalidate_cache()
                return n
            except _MONGO_ERRORS as e:
                _on_mongo_error(e, "xóa nhật ký")
        logs = [l for l in _read_file_logs() if _doc_id(l) not in set(ids)]
        _write_file_logs(logs)
        _mark_pending(delete_ids=ids)
        _invalidate_cache()
        return len(ids)


def deduplicate_sync_logs_file() -> int:
    # Ban ghi trung ma_nhom_giao_dich da bi unique index chan; con lai ban ghi cu khong co ma nhom
    logs = load_sync_logs_safe()
    if not logs:
        return 0
    seen = set()
    drop_ids = []
    for l in reversed(logs):
        tx_group = l.get("ma_nhom_giao_dich")
        if tx_group:
            key = f"TX:{tx_group}"
        else:
            key = f"LEGACY:{l.get('db_type') or l.get('he_csdl') or ''}:" \
                  f"{l.get('action') or l.get('loai_su_kien') or ''}:{l.get('title', '')}"
        if key in seen:
            drop_ids.append(_doc_id(l))
        else:
            seen.add(key)
    return delete_sync_logs(drop_ids) if drop_ids else 0


def clear_sync_logs_store() -> bool:
    global _CACHE_LOGS
    with _SYNC_LOCK:
        ok = True
        col = _writable_collection()
        if col is not None:
            try:
                col.delete_many({})
            except _MONGO_ERRORS as e:
                _on_mongo_error(e, "xóa toàn bộ nhật ký")
                col = None
        if col is None:
            _mark_pending(clear=True)
            ok = False
        if col is None or os.path.exists(SYNC_LOG_FILE):
            _write_file_logs([])
        _CACHE_LOGS = []
        _invalidate_cache()
        return ok


def trim_sync_logs() -> None:
    with _SYNC_LOCK:
        col = _writable_collection()
        if col is not None:
            try:
                _trim(col)
            except _MONGO_ERRORS as e:
                _on_mongo_error(e, "cắt bớt nhật ký")


def query_sync_logs(
    page: int = 1,
    limit: int = 10,
    db_type: str = "ALL",
    search: str = ""
) -> Dict[str, Any]:
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
                str(l.get("affected_tables", "")),
                str(l.get("app_host", "")),
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
        "cnt_mongo": cnt_mongo,
        "store": get_store_status(),
    }
