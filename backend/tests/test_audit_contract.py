"""审计数据契约测试：普通调用与导出事件共用编码、编号与确认边界。

覆盖：
1. 旧数据没有契约字段时仍按已确认读取（旧引用可查）
2. 普通调用事件保留原字段，且只被调用审计认领
3. 导出事件带回导出文件信息，按 API/调用方原口径可查
4. 审计写盘中断：文件或事件只剩一侧时，未确认事件不进入正常查询
5. 两类事件共用编号序列，不互相覆盖
6. 限流计数只统计已确认的普通调用事件
"""
from __future__ import annotations

import json

import pytest


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    from app.services import audit_contract, data_manager
    from app.store import store
    from app.seed import SEED_ROWS

    monkeypatch.setattr(data_manager, "DATA_ROOT", tmp_path)
    audit_contract._RESERVED_IDS.clear()
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    return tmp_path


def _audit_files(root):
    return sorted((root / "audit").glob("[0-9]*.json"))


def test_legacy_audit_record_still_readable(data_root):
    """旧数据没有 event/write_state 字段，仍按已确认事件返回。"""
    from app.services.data_manager import list_audit, read_audit

    legacy = {
        "audit_id": 1,
        "api_name": "旧API",
        "caller": "旧调用方",
        "event": "route_degraded",
        "reason": "历史事件",
        "_logged_at": "2025-01-01T00:00:00+08:00",
    }
    (data_root / "audit").mkdir(parents=True)
    (data_root / "audit" / "000001.json").write_text(
        json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

    records = list_audit(api_name="旧API", caller="旧调用方")
    assert [r["audit_id"] for r in records] == [1]
    assert records[0]["reason"] == "历史事件"
    assert read_audit(1)["event"] == "route_degraded"


def test_call_event_keeps_fields_and_is_verified(data_root):
    from app.services.call_audit import CallAuditService
    from app.services.data_manager import write_api_snapshot, write_subscription

    write_api_snapshot(1, {"api_name": "API-A", "status": "已上线"})
    write_subscription(1, {"api_name": "API-A", "applicant": "调用方A",
                           "approval_status": "已通过"})
    svc = CallAuditService()
    entry = svc.record_api_call("调用方A", "API-A", params="id=1",
                                response_code="200", latency_ms=12.5, result_rows=7)

    stored = json.loads((data_root / "audit" / f"{entry['id']:06d}.json").read_text(encoding="utf-8"))
    # 旧字段含义不变
    assert stored["record_no"] == entry["记录编号"]
    assert stored["request_params"] == "id=1"
    assert stored["response_code"] == "200"
    assert stored["latency_ms"] == 12.5
    assert stored["result_rows"] == 7
    # 契约字段
    assert stored["event"] == "api_call"
    assert stored["confirmed"] is True

    verified = svc.verify_audits("调用方A", "API-A")
    assert [r["audit_id"] for r in verified] == [entry["id"]]
    assert verified[0]["verified"] is True


def test_export_event_returns_file_info(data_root):
    from app.services.export_task import ExportTaskService
    from app.services.data_manager import list_audit, read_export

    svc = ExportTaskService()
    result = svc.execute_export(1)
    assert result["ok"] and result["confirmed"] is True

    records = list_audit(api_name="导出任务样例1", caller="export_task")
    assert len(records) == 1
    record = records[0]
    assert record["event"] == "export_completed"
    assert record["export_id"] == 1
    assert record["rows_count"] == 3
    assert record["task_no"] == "EXPO-0001"
    assert record["file_name"] == "000001.csv"
    assert (data_root / record["file_path"]).exists()
    assert read_export(1)["rows_count"] == 3


def test_interrupted_audit_write_is_not_confirmed(data_root, monkeypatch):
    """导出文件已写盘但审计事件中断：正常查询看不到这笔事件。"""
    from app.services import audit_contract
    from app.services.data_manager import list_audit
    from app.services.export_task import ExportTaskService
    from app.store import store

    original = audit_contract._atomic_write_json

    def fail_audit(path, data):
        if path.parent.name == "audit" and path.suffix == ".json" and path.stem.isdigit():
            raise OSError("simulated interruption")
        return original(path, data)

    monkeypatch.setattr(audit_contract, "_atomic_write_json", fail_audit)
    svc = ExportTaskService()
    result = svc.execute_export(2)

    assert result["ok"] is False and result["confirmed"] is False
    # 导出文件仍在，但任务没有确认完成
    assert (data_root / "exports" / "000002.json").exists()
    assert store.find("export_task", 2)["status"] == "导出中"
    # 正常查询为空，未完成事件只在核对时出现
    assert list_audit(api_name="导出任务样例2") == []
    pending = list_audit(api_name="导出任务样例2", include_pending=True)
    assert len(pending) == 1
    assert pending[0]["confirmed"] is False
    assert pending[0]["event"] == "export_completed"


def test_confirmed_call_survives_interrupted_export(data_root, monkeypatch):
    """同一 API/调用方下，导出中途失败不影响已确认的调用事件。"""
    from app.services import audit_contract
    from app.services.call_audit import CallAuditService
    from app.services.data_manager import list_audit, write_api_snapshot, write_subscription
    from app.services.export_task import ExportTaskService

    write_api_snapshot(1, {"api_name": "API-B", "status": "已上线"})
    write_subscription(1, {"api_name": "API-B", "applicant": "调用方B",
                           "approval_status": "已通过"})
    call_svc = CallAuditService()
    call = call_svc.record_api_call("调用方B", "API-B", result_rows=4)

    original = audit_contract._atomic_write_json

    def fail_audit(path, data):
        if path.parent.name == "audit" and path.suffix == ".json" and path.stem.isdigit():
            raise OSError("simulated interruption")
        return original(path, data)

    # 让导出任务也挂到同一 API/调用方口径上
    from app.store import store
    store.find("export_task", 2)["关联API"] = "API-B"
    monkeypatch.setattr(audit_contract, "_atomic_write_json", fail_audit)
    ExportTaskService().execute_export(2)

    confirmed = list_audit(api_name="API-B")
    assert [r["event"] for r in confirmed] == ["api_call"]
    assert [r["audit_id"] for r in call_svc.verify_audits("调用方B", "API-B")] == [call["id"]]
    # 未确认的导出事件只在核对视角可见，且编号不与已确认调用事件相同
    pending = list_audit(api_name="API-B", include_pending=True)
    assert [r["confirmed"] for r in pending] == [True, False]
    assert pending[1]["audit_id"] != call["id"]


def test_call_and_export_share_id_sequence(data_root):
    """两类事件共用编号序列，不会互相覆盖。"""
    from app.services.call_audit import CallAuditService
    from app.services.data_manager import list_audit
    from app.services.export_task import ExportTaskService

    call = CallAuditService().record_api_call("调用方D", "API-D")
    export = ExportTaskService().execute_export(1)
    assert export["ok"]

    records = list_audit()
    ids = [r["audit_id"] for r in records]
    assert len(ids) == len(set(ids))
    assert call["id"] in ids
    assert all((data_root / "audit" / f"{i:06d}.json").exists() for i in ids)


def test_rate_limit_counts_only_confirmed_calls(data_root):
    """限流按原口径查调用事件，导出与未完成事件不计入。"""
    from app.services.call_audit import CallAuditService
    from app.services.rate_limiter import RateLimiterService
    from app.services.export_task import ExportTaskService
    from app.store import store

    rows = store.rows("rate_limiter")
    rows.append({
        "id": 99, "策略编号": "RATE-TEST", "目标API": "API-R", "调用方": "调用方R",
        "每秒上限": "2", "每分钟上限": "2", "每日上限": "2", "超限动作": "拒绝",
        "策略版本": "v1", "status": "已生效", "pending": False, "abnormal": False,
    })
    ExportTaskService().execute_export(1)  # 调用方 export_task 的导出事件
    CallAuditService().record_api_call("调用方R", "API-R")

    # 若把导出事件或未确认事件算进来，计数会达到 2 而误判超限。
    result = RateLimiterService().check_rate_limit("API-R", "调用方R")
    assert result["allowed"] is True
    assert result["current_count"] == 1


def test_event_views_isolate_call_and_export(data_root):
    """调用审计与导出任务各自按原口径取数，互不混入对方事件。"""
    from app.services.call_audit import CallAuditService
    from app.services.export_task import ExportTaskService
    from app.services.data_manager import write_api_snapshot, write_subscription

    write_api_snapshot(1, {"api_name": "API-S", "status": "已上线"})
    write_subscription(1, {"api_name": "API-S", "applicant": "调用方S",
                           "approval_status": "已通过"})
    call_svc = CallAuditService()
    call = call_svc.record_api_call("调用方S", "API-S", result_rows=3)
    ExportTaskService().execute_export(1)

    calls = call_svc.list_confirmed_calls(api_name="API-S", caller="调用方S")
    assert [r["audit_id"] for r in calls] == [call["id"]]
    assert calls[0]["result_rows"] == 3

    exports = ExportTaskService().list_confirmed_exports()
    assert len(exports) == 1
    assert exports[0]["export_id"] == 1
    assert exports[0]["file_name"] == "000001.csv"

    # 调用审计验证不会把导出事件误当调用记录
    verified = call_svc.verify_audits("调用方S", "API-S")
    assert [r["audit_id"] for r in verified] == [call["id"]]


def test_legacy_records_keep_old_references(data_root):
    """旧调用记录没有 event 字段时仍可按原口径查到并保留原字段。"""
    from app.services.call_audit import CallAuditService

    legacy = {
        "audit_id": 1,
        "record_no": "CALL-0001",
        "caller": "旧调用方",
        "api_name": "旧API",
        "request_params": "id=1",
        "response_code": "200",
        "latency_ms": 5.0,
        "result_rows": 2,
        "_logged_at": "2025-01-02T00:00:00+08:00",
    }
    (data_root / "audit").mkdir(parents=True, exist_ok=True)
    (data_root / "audit" / "000001.json").write_text(
        json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

    records = CallAuditService().list_confirmed_calls(api_name="旧API", caller="旧调用方")
    assert len(records) == 1
    assert records[0]["record_no"] == "CALL-0001"
    assert records[0]["result_rows"] == 2
