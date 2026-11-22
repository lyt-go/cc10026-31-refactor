"""调用审计事件的数据契约：统一编码、编号、原子落盘和确认边界。

普通调用事件（call_audit）与导出事件（export_task）原先各写一套事件字典和
编号逻辑。这里把两类事件共用的信封字段、编号分配、落盘方式和确认判定收拢到
一个契约中，同时保留各自事件原有业务字段及其含义：

- 调用事件：``record_no`` / ``request_params`` / ``response_code`` /
  ``latency_ms`` / ``result_rows``
- 导出事件：``export_id`` / ``task_no`` / ``query_params`` / ``export_format`` /
  ``rows_count`` / ``file_name`` / ``file_path``

落盘采用“同目录暂存文件 + fsync + os.replace”：只有完整写完的记录才会出现
在 ``data/audit/*.json`` 中；中途失败的残片留在 ``data/audit/.staging/``，
不会进入按 API/调用方/时间检索的结果。旧数据没有 ``write_state`` 标记时按
已确认记录兼容读取，保证旧审计引用仍然可查。
"""
from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

CONTRACT_VERSION = 1

# 事件类型：普通调用与导出完成。旧数据没有 event 字段时仍按调用记录兼容读取。
CALL_EVENT = "api_call"
EXPORT_EVENT = "export_completed"
EVENT_CALL = CALL_EVENT
EVENT_EXPORT = EXPORT_EVENT

# 写入状态 / 确认边界。
CONFIRMED = "confirmed"
PENDING = "pending"

# 公共信封字段。业务字段由各事件编码函数补齐，不改写原有字段含义。
COMMON_FIELDS = (
    "audit_id",
    "event",
    "api_name",
    "caller",
    "timestamp",
    "_logged_at",
    "contract_version",
    "write_state",
    "confirmed",
)
CALL_EVENT_FIELDS = (
    "record_no",
    "request_params",
    "response_code",
    "latency_ms",
    "result_rows",
)
EXPORT_EVENT_FIELDS = (
    "export_id",
    "task_no",
    "query_params",
    "export_format",
    "rows_count",
    "file_name",
    "file_path",
)

TZ = timezone(timedelta(hours=8))
AUDIT_DIR = "audit"
STAGING_DIR = ".staging"

# 编号分配和写入共用一把进程内可重入锁，避免两类事件同时取到同一个数字引用。
_LOCK = threading.RLock()
# 写入失败但已被分配过的编号按 DATA_ROOT 记住，避免同一进程内再次复用造成混淆。
_RESERVED_IDS: dict[str, set[int]] = {}


def now_iso() -> str:
    """契约统一使用东八区 ISO 时间。"""
    return datetime.now(TZ).isoformat()


def _root_key(root: str | os.PathLike[str]) -> str:
    return str(Path(root).resolve())


def _audit_dir(root: str | os.PathLike[str]) -> Path:
    return Path(root) / AUDIT_DIR


def _staging_dir(root: str | os.PathLike[str]) -> Path:
    return _audit_dir(root) / STAGING_DIR


def encode_event(
    event: str,
    *,
    audit_id: int,
    api_name: str,
    caller: str,
    timestamp: str | None = None,
    **payload: Any,
) -> dict[str, Any]:
    """编码公共信封；业务字段原样展开，保证现有字段含义不变。"""
    record: dict[str, Any] = {
        "audit_id": int(audit_id),
        "event": event,
        "api_name": api_name,
        "caller": caller,
        "timestamp": timestamp or now_iso(),
    }
    record.update(payload)
    return record


def encode_call_event(
    *,
    audit_id: int,
    record_no: str,
    caller: str,
    api_name: str,
    request_params: str = "",
    response_code: str = "200",
    latency_ms: float = 0.0,
    result_rows: int = 0,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """普通调用审计事件：保留原调用审计字段，补公共信封。"""
    return encode_event(
        CALL_EVENT,
        audit_id=audit_id,
        api_name=api_name,
        caller=caller,
        timestamp=timestamp,
        record_no=record_no,
        request_params=request_params,
        response_code=response_code,
        latency_ms=latency_ms,
        result_rows=result_rows,
    )


def encode_export_event(
    *,
    audit_id: int,
    export_id: int,
    api_name: str,
    caller: str = "export_task",
    task_no: str = "",
    query_params: str = "",
    export_format: str = "json",
    rows_count: int = 0,
    file_name: str = "",
    file_path: str = "",
    timestamp: str | None = None,
) -> dict[str, Any]:
    """导出审计事件：保留原导出字段，并写入可回查的导出文件信息。"""
    return encode_event(
        EXPORT_EVENT,
        audit_id=audit_id,
        api_name=api_name,
        caller=caller,
        timestamp=timestamp,
        export_id=export_id,
        task_no=task_no,
        query_params=query_params,
        export_format=export_format,
        rows_count=rows_count,
        file_name=file_name,
        file_path=file_path,
    )


def normalize_event(
    audit_id: int,
    audit_data: dict[str, Any],
    *,
    event: str | None = None,
) -> dict[str, Any]:
    """把任意审计事件收敛到统一契约，不丢弃调用方已有的业务字段。"""
    record = dict(audit_data)
    record["audit_id"] = int(audit_id)
    if event is not None:
        record["event"] = event
    elif not record.get("event"):
        # 兼容没有 event 字段的旧记录：按业务字段补齐为调用或导出事件，
        # 其余旧事件的 event 保持为空，检索时不会被误认为调用事件。
        if record.get("record_no"):
            record["event"] = CALL_EVENT
        elif record.get("export_id") is not None:
            record["event"] = EXPORT_EVENT
    if not record.get("timestamp"):
        record["timestamp"] = now_iso()
    record["contract_version"] = CONTRACT_VERSION
    record["write_state"] = CONFIRMED
    record["confirmed"] = True
    return record


def is_confirmed(record: Any) -> bool:
    """确认边界：只有完整落盘且没有失败标记的记录才算已确认。

    旧记录没有 ``write_state`` / ``confirmed`` 字段，按已确认兼容处理。
    """
    if not isinstance(record, dict):
        return False
    if record.get("confirmed") is False:
        return False
    state = record.get("write_state")
    if state is not None and state != CONFIRMED:
        return False
    return True


def is_call_event(record: Any) -> bool:
    """判定普通调用事件；兼容没有 ``event`` 字段的旧调用审计记录。"""
    if not isinstance(record, dict):
        return False
    event = record.get("event")
    if event == CALL_EVENT:
        return True
    # 旧调用记录只写 record_no，没有 event；仍按调用事件认领。
    return event is None and bool(record.get("record_no"))


def is_export_event(record: Any) -> bool:
    """判定导出事件；兼容旧导出审计记录。"""
    if not isinstance(record, dict):
        return False
    return record.get("event") == EXPORT_EVENT or (
        record.get("event") is None and record.get("export_id") is not None)


def row_count(record: dict[str, Any]) -> int | None:
    """跨两类事件读取行数：调用事件用 result_rows，导出事件用 rows_count。"""
    value = record.get("result_rows", record.get("rows_count"))
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def request_info(record: dict[str, Any]) -> dict[str, Any]:
    """从已确认的调用事件中取回请求侧字段。"""
    return {field: record[field] for field in CALL_EVENT_FIELDS if field in record}


def file_info(record: dict[str, Any]) -> dict[str, Any]:
    """从已确认的导出事件中取回文件侧字段。"""
    return {field: record[field] for field in EXPORT_EVENT_FIELDS if field in record}


def _visible_ids(root: str | os.PathLike[str]) -> set[int]:
    ids: set[int] = set()
    for path in _audit_dir(root).glob("[0-9]*.json"):
        try:
            ids.add(int(path.stem))
        except ValueError:
            continue
    return ids


def query_events(
    root: str | os.PathLike[str],
    *,
    event: str | None = None,
    api_name: str | None = None,
    caller: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    include_pending: bool = False,
) -> list[dict[str, Any]]:
    """按 API / 调用方 / 时间 / 事件类型检索审计事件。

    这是审计核对共用的取数口径：默认只看已确认事件，``event`` 用于把普通
    调用与导出事件分开；``include_pending=True`` 时额外带上未完成写入残片。
    """
    audit_dir = _audit_dir(root)
    records: list[dict[str, Any]] = []
    if audit_dir.is_dir():
        for path in sorted(audit_dir.glob("[0-9]*.json")):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(record, dict):
                records.append(record)
    if include_pending:
        records.extend(pending_records(root))

    matched: list[dict[str, Any]] = []
    for record in records:
        if not include_pending and not is_confirmed(record):
            continue
        if event is not None:
            if event == CALL_EVENT:
                if not is_call_event(record):
                    continue
            elif event == EXPORT_EVENT:
                if not is_export_event(record):
                    continue
            elif record.get("event") != event:
                continue
        if api_name and record.get("api_name") != api_name:
            continue
        if caller and record.get("caller") != caller:
            continue
        if start_time and record.get("_logged_at", "") < start_time:
            continue
        if end_time and record.get("_logged_at", "") > end_time:
            continue
        matched.append(record)
    return matched


def _staged_ids(root: str | os.PathLike[str]) -> set[int]:
    """残片文件名以数字编号开头，用于避免中断写入的编号被下一笔复用。"""
    ids: set[int] = set()
    staging = _staging_dir(root)
    if not staging.is_dir():
        return ids
    for path in staging.iterdir():
        if not path.is_file():
            continue
        match = re.match(r"(\d+)", path.name)
        if match:
            ids.add(int(match.group(1)))
    return ids


def allocate_audit_id(
    root: str | os.PathLike[str],
    extra_ids: Iterable[int | str | None] | None = None,
    preferred: int | None = None,
) -> int:
    """分配下一个数字审计编号。

    同时参考已确认记录、暂存残片、进程内失败预留以及调用方内存中的编号，
    保证两类事件不会因为各自只扫描自己那半边数据而拿到同一个编号。

    ``preferred`` 用于保留旧的数字引用（例如调用记录沿用模块行号）：
    该编号未被占用时原样返回，已占用时改用共享序列的下一个可用编号，
    绝不覆盖已确认事件。
    """
    with _LOCK:
        ids = _visible_ids(root) | _staged_ids(root) | _RESERVED_IDS.get(_root_key(root), set())
        for value in extra_ids or ():
            if value is None:
                continue
            try:
                ids.add(int(value))
            except (TypeError, ValueError):
                continue
        if preferred is not None:
            try:
                preferred_id = int(preferred)
            except (TypeError, ValueError):
                preferred_id = 0
            if preferred_id > 0 and preferred_id not in ids:
                return preferred_id
        return max(ids, default=0) + 1


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def lock() -> "threading.RLock":
    """取号与落盘共用的进程内可重入锁。"""
    return _LOCK


def reserve_audit_id(root: str | os.PathLike[str], audit_id: int) -> None:
    """写入失败后预留编号，避免下一笔事件复用同一数字引用。"""
    with _LOCK:
        _RESERVED_IDS.setdefault(_root_key(root), set()).add(int(audit_id))


def _staging_for(path: Path) -> Path:
    """暂存文件目录：目标已在暂存目录内时不再嵌套。"""
    if path.parent.name == STAGING_DIR:
        return path.parent
    return path.parent / STAGING_DIR


def _atomic_write_json(path: Path, data: Any) -> None:
    """暂存 + 原子改名；失败时清理暂存文件，不留下可查询的半条记录。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = _staging_for(path)
    staging.mkdir(parents=True, exist_ok=True)
    temp = staging / f"{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            temp.unlink()
        except OSError:
            pass
        raise


def _write_pending_marker(
    root: str | os.PathLike[str],
    audit_id: int,
    record: dict[str, Any],
    error: BaseException,
) -> None:
    """写入失败时留下核对用的未确认标记，避免半条记录混入正常查询。

    标记放在 ``audit/.staging/``，正常审计查询不会命中；审计核对时通过
    ``include_pending`` / ``pending_records`` 才能看到这笔写入没有完成。
    """
    marker = dict(record)
    marker["audit_id"] = int(audit_id)
    marker["write_state"] = PENDING
    marker["confirmed"] = False
    marker["_failed_at"] = now_iso()
    marker["_error"] = f"{type(error).__name__}: {error}"
    path = _staging_dir(root) / f"{int(audit_id):06d}.failed.json"
    try:
        _atomic_write_json(path, marker)
    except BaseException:
        # 连标记都写不下去时，编号仍由 _RESERVED_IDS 预留，下一笔不会复用。
        pass


def write_event(
    root: str | os.PathLike[str],
    audit_id: int,
    audit_data: dict[str, Any],
    *,
    event: str | None = None,
) -> dict[str, Any]:
    """按统一契约写入一条审计事件，返回实际落盘的记录。"""
    record = normalize_event(audit_id, audit_data, event=event)
    path = _audit_dir(root) / f"{int(audit_id):06d}.json"
    with _LOCK:
        # 与原实现一致：_logged_at 以真正写入时间为准，供按时间检索使用。
        record["_logged_at"] = now_iso()
        try:
            _atomic_write_json(path, record)
        except BaseException as exc:
            _write_pending_marker(root, audit_id, record, exc)
            raise
    return record


def append_event(
    root: str | os.PathLike[str],
    build_event: Callable[[int], dict[str, Any]],
    *,
    extra_ids: Iterable[int | str | None] | None = None,
    preferred: int | None = None,
) -> dict[str, Any]:
    """在同一把锁内完成“取号 + 编码 + 落盘”。

    ``build_event`` 接收分配到的数字编号，返回事件字典。若落盘中途失败，
    该编号会预留，残片留在暂存目录且不会进入正常查询。
    """
    with _LOCK:
        audit_id = allocate_audit_id(root, extra_ids=extra_ids, preferred=preferred)
        record = build_event(audit_id)
        if not isinstance(record, dict):
            raise TypeError("审计事件编码结果必须是 dict")
        record = dict(record)
        record["audit_id"] = int(audit_id)
        try:
            return write_event(root, audit_id, record)
        except BaseException:
            reserve_audit_id(root, audit_id)
            raise


def pending_records(root: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """读取暂存目录中的未完成残片，供核对时确认哪些写入没有走完。

    正常审计查询不会返回这些记录；只有在明确要求查看未完成事件时才使用。
    中断写入可能留下两种残片：失败标记 ``*.failed.json``，以及未完成改名、
    但已写满内容的 ``*.tmp``。两者都标记为未确认。
    """
    staging = _staging_dir(root)
    if not staging.is_dir():
        return []
    records: list[dict[str, Any]] = []
    paths = sorted(set(staging.glob("*.failed.json")) | set(staging.glob("*.tmp")))
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(record, dict):
            continue
        record = dict(record)
        record["confirmed"] = False
        record["write_state"] = PENDING
        record["_pending_file"] = path.name
        records.append(record)
    return records
