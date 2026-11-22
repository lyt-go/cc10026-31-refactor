"""跨模块共享数据管理器：读写 data/ 目录下的业务数据产物。

职责：
- 每个服务产出 domain_snapshot 后序列化到 data/{module}/snapshot.json
- 下游服务从 data/ 目录读取上游的标准化数据产物
- 审计记录写入 data/audit/
- 用量记录写入 data/usage/
- 导出任务结果写入 data/exports/
- API 注册快照写入 data/api/
- 订阅审批记录写入 data/subscriptions/
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.services import audit_contract

# 数据目录相对于仓库根
DATA_ROOT = Path(os.environ.get("DATA_ROOT", str(
    Path(__file__).resolve().parents[3] / "data"
)))

# 东八区
TZ = timezone(timedelta(hours=8))


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _write_json(path: Path, data: Any) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


# ========== API 注册快照 ==========


def write_api_snapshot(api_id: int, api_data: dict[str, Any]) -> None:
    """将单个 API 注册数据写入共享快照，供路由、缓存、限流等模块消费。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    key = str(api_id)
    api_data["_synced_at"] = _now_iso()
    snapshot[key] = api_data
    _write_json(DATA_ROOT / "api" / "registry.json", snapshot)


def read_api_snapshot(api_id: int | None = None) -> dict[str, Any] | dict[str, dict[str, Any]]:
    """读取 API 注册快照。返回单个或全部。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    if api_id is not None:
        return snapshot.get(str(api_id), {})
    return snapshot


def remove_api_snapshot(api_id: int) -> None:
    """标记 API 为下线。"""
    snapshot = _read_json(DATA_ROOT / "api" / "registry.json", {})
    key = str(api_id)
    if key in snapshot:
        snapshot[key]["status"] = "已下线"
        snapshot[key]["_deleted_at"] = _now_iso()
        _write_json(DATA_ROOT / "api" / "registry.json", snapshot)


# ========== 订阅审批 ==========


def write_subscription(sub_id: int, sub_data: dict[str, Any]) -> None:
    """写入订阅申请记录到共享目录。"""
    record = {
        "subscription_id": sub_id,
        **sub_data,
        "_created_at": sub_data.get("_created_at", _now_iso()),
        "_updated_at": _now_iso(),
    }
    _write_json(DATA_ROOT / "subscriptions" / f"{sub_id:06d}.json", record)


def read_subscription(sub_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "subscriptions" / f"{sub_id:06d}.json", {})


def list_subscriptions() -> list[dict[str, Any]]:
    """列出所有订阅记录。"""
    _ensure_dir(DATA_ROOT / "subscriptions")
    records = []
    for f in sorted(DATA_ROOT.glob("subscriptions/*.json")):
        record = _read_json(f)
        if record:
            records.append(record)
    return records


def approve_subscription(sub_id: int, approved: bool, approver: str = "system") -> dict[str, Any]:
    """审批订阅，记录审批流程。"""
    record = read_subscription(sub_id)
    if not record:
        return {}
    record["approval_status"] = "已通过" if approved else "已驳回"
    record["approver"] = approver
    record["_approved_at"] = _now_iso()
    write_subscription(sub_id, record)
    return record


# ========== 用量计费 ==========


def write_usage(usage_id: int, usage_data: dict[str, Any]) -> None:
    """写入用量记录到共享目录。"""
    usage_data["_created_at"] = usage_data.get("_created_at", _now_iso())
    usage_data["_updated_at"] = _now_iso()
    _write_json(DATA_ROOT / "usage" / f"{usage_id:06d}.json", usage_data)


def read_usage(usage_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "usage" / f"{usage_id:06d}.json", {})


def list_usage(billing_cycle: str | None = None) -> list[dict[str, Any]]:
    """列出用量记录，支持按计费周期过滤。"""
    _ensure_dir(DATA_ROOT / "usage")
    records = []
    for f in sorted(DATA_ROOT.glob("usage/*.json")):
        record = _read_json(f)
        if record:
            if billing_cycle is None or record.get("billing_cycle") == billing_cycle:
                records.append(record)
    return records


def settle_billing(billing_cycle: str, use_auto_settlement: bool = True) -> dict[str, Any]:
    """结算指定计费周期，生成结算汇总。"""
    records = list_usage(billing_cycle)
    total_amount = sum(float(r.get("amount", 0)) for r in records)
    total_calls = sum(int(r.get("call_count", 0)) for r in records)
    settlement = {
        "billing_cycle": billing_cycle,
        "total_amount": total_amount,
        "total_calls": total_calls,
        "record_count": len(records),
        "settled_at": _now_iso(),
        "auto_settlement": use_auto_settlement,
    }
    _write_json(DATA_ROOT / "usage" / f"_settlement_{billing_cycle}.json", settlement)

    for r in records:
        r["settlement_status"] = "已结算"
        r["_settled_at"] = _now_iso()
        write_usage(r.get("subscription_id", 0), r)
    return settlement


# ========== 调用审计 ==========


def allocate_audit_id(extra_ids: list[int | str | None] | None = None,
                      preferred: int | None = None) -> int:
    """按审计数据契约分配下一个数字审计编号。

    ``preferred`` 用于保留旧引用：该编号未被占用时沿用，否则顺延到下一个可用编号。
    """
    return audit_contract.allocate_audit_id(DATA_ROOT, extra_ids=extra_ids, preferred=preferred)


def append_audit_event(build_event, *, extra_ids=None, preferred=None) -> dict[str, Any]:
    """在同一把锁内完成取号、编码和落盘，返回实际写入的记录。

    落盘仍经过 :func:`write_audit`，保留该写入入口作为唯一审计落盘点。
    """
    with audit_contract.lock():
        audit_id = allocate_audit_id(extra_ids=extra_ids, preferred=preferred)
        record = build_event(audit_id)
        if not isinstance(record, dict):
            raise TypeError("审计事件编码结果必须是 dict")
        record = dict(record)
        record["audit_id"] = int(audit_id)
        try:
            return write_audit(audit_id, record)
        except BaseException:
            audit_contract.reserve_audit_id(DATA_ROOT, audit_id)
            raise


def write_audit(audit_id: int, audit_data: dict[str, Any]) -> dict[str, Any]:
    """按审计数据契约写入审计记录，返回实际落盘的记录。

    旧调用方只需传 ``audit_id`` 和事件字典；公共信封字段与确认标记由契约补齐，
    原有业务字段原样保留。写入使用暂存文件 + 原子改名，中途失败不会留下
    可被正常查询命中的半条记录。
    """
    return audit_contract.write_event(DATA_ROOT, audit_id, audit_data)


def read_audit(audit_id: int) -> dict[str, Any]:
    """读取单条已确认审计记录；未完成写入不会出现在这里。"""
    record = _read_json(DATA_ROOT / "audit" / f"{audit_id:06d}.json", {})
    return record if audit_contract.is_confirmed(record) else {}


def list_audit(api_name: str | None = None, caller: str | None = None,
               start_time: str | None = None, end_time: str | None = None,
               include_pending: bool = False, event: str | None = None) -> list[dict[str, Any]]:
    """按条件查询审计记录（两类事件共用的取数口径）。

    默认只返回已确认落盘的事件，写入中断的残片留在 ``audit/.staging/``，
    不会与已确认事件混作同一笔。``include_pending=True`` 时才额外读取残片，
    用于审计核对。旧数据没有确认标记，按已确认兼容读取。
    ``event`` 可按事件类型隔离普通调用与导出事件。
    """
    return audit_contract.query_events(
        DATA_ROOT,
        event=event,
        api_name=api_name,
        caller=caller,
        start_time=start_time,
        end_time=end_time,
        include_pending=include_pending,
    )


def archive_audit_records(before_date: str) -> dict[str, Any]:
    """归档指定日期之前的审计记录。"""
    records = list_audit()
    archived = [
        r for r in records
        if r.get("_logged_at", "")[:10] < before_date and str(r.get("audit_id", "")).isdigit()
    ]
    archive_data = {
        "archive_date": _now_iso(),
        "before_date": before_date,
        "archived_count": len(archived),
        "records": archived,
    }
    _write_json(DATA_ROOT / "audit" / f"_archive_{before_date}.json", archive_data)
    # 标记已归档
    for r in archived:
        rid = r.get("audit_id", 0)
        r["status"] = "已归档"
        write_audit(rid, r)
    return archive_data


# ========== 导出任务 ==========


def write_export(export_id: int, export_data: dict[str, Any]) -> dict[str, Any]:
    """写入导出任务结果，返回可写入审计事件的文件信息。"""
    export_data["_created_at"] = export_data.get("_created_at", _now_iso())
    export_data["_updated_at"] = _now_iso()
    _write_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", export_data)
    # 同时生成一份导出 CSV
    return _write_export_csv(export_id, export_data)


def _write_export_csv(export_id: int, export_data: dict[str, Any]) -> dict[str, Any]:
    """根据导出数据生成 CSV 文件，返回供审计事件回查的文件信息。"""
    columns = export_data.get("columns", [])
    rows = export_data.get("rows", [])
    csv_path = DATA_ROOT / "exports" / f"{export_id:06d}.csv"
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "file_name": csv_path.name,
        "file_path": str(csv_path.relative_to(DATA_ROOT)),
        "json_path": f"exports/{export_id:06d}.json",
    }


def read_export(export_id: int) -> dict[str, Any]:
    return _read_json(DATA_ROOT / "exports" / f"{export_id:06d}.json", {})


def list_exports() -> list[dict[str, Any]]:
    _ensure_dir(DATA_ROOT / "exports")
    records = []
    for f in sorted(DATA_ROOT.glob("exports/[0-9]*.json")):
        record = _read_json(f)
        if record:
            records.append(record)
    return records


# ========== 模块数据快照 ==========


def write_module_snapshot(module: str, snapshot: dict[str, Any]) -> None:
    """各服务将自己的 domain_snapshot 写入共享目录，供跨模块消费。"""
    snapshot["_snapshot_at"] = _now_iso()
    _write_json(DATA_ROOT / module / "snapshot.json", snapshot)


def read_module_snapshot(module: str) -> dict[str, Any]:
    return _read_json(DATA_ROOT / module / "snapshot.json", {})


def list_module_snapshots() -> list[dict[str, Any]]:
    """读取所有模块的快照汇总。"""
    snapshots = []
    for module_dir in DATA_ROOT.iterdir():
        if module_dir.is_dir() and not module_dir.name.startswith("_"):
            snap = _read_json(module_dir / "snapshot.json")
            if snap:
                snapshots.append(snap)
    return snapshots