"""调用审计领域服务：管理API调用的审计记录、归档和查询追溯。

跨模块数据流：
- 消费 usage_billing 快照 → 用量异常触发审计核查
- 消费 subscription 共享记录 → 审批通过的订阅记录审计
- 消费 export_task 快照 → 导出操作记录审计
- 审计记录定期归档 → 写入 data/audit/ 共享目录
- 被 error_classify 消费 → 异常调用触发错误归类
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CallAuditService(BaseModuleService):
    MODULE = "call_audit"
    ENTITY = "调用记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "调用方", "目标API", "请求参数", "响应码", "响应耗时", "返回行数", "时间戳"]
    REQUIRED_FIELDS = ["记录编号", "调用方", "目标API"]
    STATUS_ORDER = ["已记录", "已校验", "已归档", "已过期"]
    ACTION_RULES = {"确认校验": "已校验", "提交归档": "已归档", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def record_api_call(self, caller: str, api_name: str, params: str = "",
                        response_code: str = "200", latency_ms: float = 0.0,
                        result_rows: int = 0) -> dict[str, Any]:
        """记录一次API调用到审计日志。

        事件编码走统一审计数据契约；数字引用沿用模块行号（未被占用时），
        与旧数据、导出事件共用同一编号序列，避免撞号。
        """
        from app.services import audit_contract
        from app.services.data_manager import append_audit_event
        from app.store import store as s

        timestamp = self._now_iso()
        audit_rows = s.rows(self.MODULE)
        preferred_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1

        record = append_audit_event(
            lambda audit_id: audit_contract.encode_call_event(
                audit_id=audit_id,
                record_no=f"CALL-{audit_id:04d}",
                caller=caller,
                api_name=api_name,
                request_params=params,
                response_code=response_code,
                latency_ms=latency_ms,
                result_rows=result_rows,
                timestamp=timestamp,
            ),
            extra_ids=[row.get("id") for row in audit_rows],
            preferred=preferred_id,
        )
        new_id = int(record["audit_id"])
        record_no = record["record_no"]

        # 同时记录到内存
        entry = {
            "id": new_id,
            "记录编号": record_no,
            "调用方": caller,
            "目标API": api_name,
            "请求参数": params,
            "响应码": response_code,
            "响应耗时": str(latency_ms),
            "返回行数": str(result_rows),
            "时间戳": timestamp,
            "status": "已记录",
            "pending": True,
            "abnormal": response_code != "200",
        }
        audit_rows.append(entry)
        return entry

    def _now_iso(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).isoformat()

    def list_confirmed_calls(self, caller: str | None = None, api_name: str | None = None,
                             start_time: str | None = None, end_time: str | None = None) -> list[dict[str, Any]]:
        """按 API/调用方/时间取已确认的普通调用事件（不含导出事件与未完成写入）。"""
        from app.services import audit_contract
        from app.services.data_manager import list_audit
        return list_audit(
            api_name=api_name,
            caller=caller,
            start_time=start_time,
            end_time=end_time,
            event=audit_contract.CALL_EVENT,
        )

    def verify_audits(self, caller: str, api_name: str) -> list[dict[str, Any]]:
        """校验调用方对指定API的已确认审计记录。

        只取已确认落盘的调用事件；导出事件和写入未完成的事件不参与调用审计，
        按原口径（API + 调用方）检索时不会与调用记录混作一笔。
        """
        from app.services.data_manager import read_api_snapshot

        # 检查API是否已注册
        registry = read_api_snapshot()
        api_exists = any(
            isinstance(v, dict) and v.get("api_name") == api_name
            for v in (registry.values() if isinstance(registry, dict) else []))

        if not api_exists:
            return []

        audits = self.list_confirmed_calls(caller=caller, api_name=api_name)
        # 校验订阅状态
        from app.services.data_manager import list_subscriptions
        subs = list_subscriptions()
        valid_sub = any(
            s.get("api_name") == api_name and s.get("applicant") == caller and s.get("approval_status") == "已通过"
            for s in subs
        )

        for audit in audits:
            audit["subscription_valid"] = valid_sub
            audit["verified"] = valid_sub
        return audits

    def archive_old_records(self, before_date: str) -> dict[str, Any]:
        """归档指定日期之前的审计记录。"""
        from app.services.data_manager import archive_audit_records
        return archive_audit_records(before_date)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "提交归档":
            self.archive_old_records(self._today_str())

        result = super().run_action(entry_id, action)
        from app.services.data_manager import write_module_snapshot
        write_module_snapshot(self.MODULE, self.domain_snapshot())
        return result

    def _today_str(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        audit_list = []
        for row in rows:
            audit_list.append({
                "id": row.get("id"),
                "record_no": row.get("记录编号"),
                "caller": row.get("调用方"),
                "api": row.get("目标API"),
                "params": row.get("请求参数"),
                "response_code": row.get("响应码"),
                "latency": row.get("响应耗时"),
                "result_rows": row.get("返回行数"),
                "timestamp": row.get("时间戳"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(audit_list),
            "audits": audit_list,
            "archived_count": sum(1 for r in audit_list if r.get("status") == "已归档"),
            "abnormal_count": sum(1 for r in rows if r.get("abnormal")),
        }