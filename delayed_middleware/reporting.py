"""把区域实时查询结果格式化为值班可读文本。"""

from __future__ import annotations

import json
from typing import Any


PHASE_LABELS = {
    "scheduled": "已排期/未到点",
    "ready": "到点且前置满足，可领取",
    "held": "合作端已领取，尚无实际执行回执",
    "held_catch_up": "合作端已领取；已过窗口但允许 catch_up 迟到回报",
    "catch_up_ready": "已错过窗口，可按 catch_up 补发",
    "waiting_precondition": "到点但前置事件未满足",
    "missed_waiting_precondition": "已错过窗口且前置仍未满足",
    "blocked_old_disposition": "被旧通知处置/撤回确认阻塞",
    "withdraw_requested": "已请求合作方撤回，等待确认",
    "withdrawal_unconfirmed_expired": "撤回请求已过期但仍无结论",
    "expired_unconfirmed": "窗口已过且执行情况无人确认",
    "skipped_missed": "严格窗口已错过，按 skip 不投递",
    "sent": "已有实际 sent 回执",
    "withdrawn": "合作方确认撤回/未执行",
    "superseded": "已被新编排替代（未派发）",
    "superseded_after_send": "旧项实际发出后被改期替代",
    "cancelled_by_revision": "已在新编排中取消",
}


def phase_label(phase: str | None) -> str:
    return PHASE_LABELS.get(phase or "", phase or "未知")


def format_inspect(report: dict) -> str:
    lines: list[str] = []
    lines.append(f"区域：{report['show']} / {report['region']}")
    lines.append(
        f"当前 UTC：{report.get('queried_utc')}；IANA：{report.get('tz')}；"
        f"编排版本：r{report.get('current_revision')}；"
        f"地区正式开放：{'是' if report.get('region_open') else '否'}"
    )

    lines.append("\n动作：")
    if not report.get("actions"):
        lines.append("  （当前版本没有动作）")
    for action in report.get("actions", []):
        lines.append(
            f"- {action['action']}@r{action['revision']}：{phase_label(action.get('state'))}"
        )
        lines.append(
            f"  应执行：{action.get('should_execute_at_utc')} UTC；"
            f"本地 {action.get('scheduled_local')} fold={action.get('fold')}；"
            f"窗口 {action.get('window_seconds')}s，截止 {action.get('deadline_utc')}；"
            f"错过策略 {action.get('miss_policy')}"
        )
        actual = action.get("actual_execution")
        if actual:
            lines.append(
                f"  实际执行：{actual.get('first_executed_utc')} UTC；"
                f"接收者 {', '.join(actual.get('recipients', [])) or '（未提供）'}；"
                f"SLA={action.get('timeliness') or 'unknown'}"
            )
        else:
            lines.append("  实际执行：尚无 sent 回执；领取/请求记录不算执行")

        missing = action.get("missing_preconditions") or []
        if missing:
            descriptions = "；".join(m.get("message", m.get("precondition", "?")) for m in missing)
            lines.append(f"  未满足前置：{descriptions}")
        else:
            lines.append("  前置事件：已满足或无依赖")

        gate = action.get("redelivery_gate") or {}
        if gate.get("state") and gate.get("state") != "new_schedule":
            lines.append(f"  重排闸门：{gate.get('state')} blocked={gate.get('blocked')}")

        dispatch = action.get("active_dispatch")
        if dispatch:
            lines.append(f"  当前合作端单据：{dispatch.get('id')} / state 见区域风险列表")

        previous = action.get("previous_attempts") or []
        for old in previous:
            old_actual = old.get("actual") or {}
            recipients = ",".join(old_actual.get("recipients", []))
            lines.append(
                f"  旧版本 r{old.get('revision')}：{phase_label(old.get('phase'))}"
                + (f"；实际发出 {old_actual.get('first_executed_utc')} 给 {recipients}" if old_actual else "")
            )

    corrections = report.get("corrections") or []
    lines.append("\n观众更正：")
    if not corrections:
        lines.append("  无")
    for correction in corrections:
        lines.append(
            f"- {correction['id']}：{correction.get('status')}；"
            f"需看更正的观众：{', '.join(correction.get('recipients_who_need_correction', [])) or '无'}"
        )
        for decision in correction.get("recipient_decisions", []):
            lines.append(
                f"  · {decision['recipient']}：已收到原提醒={decision.get('known_original_receipt')}，"
                f"需纠正={decision.get('needs_correction')}，仿真到达={decision.get('simulated_arrival_state')}"
            )

    unresolved = report.get("unresolved") or []
    lines.append("\n值班需处理的未决事项：")
    if not unresolved:
        lines.append("  无")
    for item in unresolved:
        lines.append(f"- [{item.get('code')}] {item.get('message')}")
        if item.get("action"):
            lines.append(f"  {item.get('action')}@r{item.get('revision')} dispatch={item.get('dispatch_id')}")

    issues = report.get("planning_issues") or []
    open_issues = [i for i in issues if i.get("status") == "needs_editor_change"]
    if open_issues:
        lines.append("\n无法解析、需退回编排修改：")
        for issue in open_issues:
            for error in issue.get("errors", []):
                lines.append(f"- r{issue.get('revision')} {error.get('path')}: {error.get('message')}")

    return "\n".join(lines)


def print_inspect(report: dict, *, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_inspect(report))
