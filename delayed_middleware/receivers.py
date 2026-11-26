"""少量本地仿真接收者，用来表达不同的回执/到达状态。

核心中间件只把合作端 ``sent`` 回执当作事实；接收者模拟器是演示和重算人群时的
辅助组件，不会把“已请求发送”改写成“观众已收到”。
"""

from __future__ import annotations

from typing import Iterable


# a/c 与仓库示例回调中的接收者一致；较长时间后都会到达。
DEFAULT_RECEIVERS: dict[str, dict] = {
    "a": {"label": "甲", "profile": "instant", "note": "在线，立即送达"},
    "b": {"label": "乙", "profile": "delayed", "delay_seconds": 45, "note": "弱网，延迟后送达"},
    "c": {"label": "丙", "profile": "retry", "first_delay_seconds": 20, "retry_delay_seconds": 120, "note": "首次应答不确定，重试后送达"},
    "d": {"label": "丁", "profile": "unreachable", "note": "当前渠道不可达"},
}

DEFAULT_AUDIENCE = ["a", "b", "c", "d"]


class ReceiverSimulator:
    def __init__(self, receivers: dict | None = None):
        self.receivers = receivers if receivers is not None else {k: dict(v) for k, v in DEFAULT_RECEIVERS.items()}

    def ensure(self, recipient: str) -> dict:
        if recipient not in self.receivers:
            # 合作方回执中出现的陌生接收者：不臆造失败，按已确认接收处理。
            self.receivers[recipient] = {"label": recipient, "profile": "confirmed", "note": "合作方回执确认的接收者"}
        return self.receivers[recipient]

    def receipt_state(self, recipient: str, sent_epoch: int | None, now_epoch: int) -> str:
        profile = self.ensure(recipient).get("profile", "confirmed")
        if profile in ("confirmed", "instant"):
            return "delivered"
        if sent_epoch is None:
            return "unknown"
        elapsed = max(0, now_epoch - sent_epoch)
        if profile == "delayed":
            delay = int(self.receivers[recipient].get("delay_seconds", 45))
            return "delivered" if elapsed >= delay else "in_flight"
        if profile == "retry":
            first = int(self.receivers[recipient].get("first_delay_seconds", 20))
            retry = int(self.receivers[recipient].get("retry_delay_seconds", 120))
            if elapsed >= retry:
                return "delivered_after_retry"
            if elapsed >= first:
                return "delivered"
            return "deferred"
        if profile == "unreachable":
            return "failed"
        return "unknown"

    def snapshot(self) -> dict:
        return {rid: dict(info) for rid, info in sorted(self.receivers.items())}

    def reachable_audience(self, audience: Iterable[str], sent_at: dict[str, int | None], now: int) -> dict:
        result = {}
        for recipient in audience:
            state = self.receipt_state(recipient, sent_at.get(recipient), now)
            result[recipient] = state
        return result
