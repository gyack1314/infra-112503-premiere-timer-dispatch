"""核心延时消息中间件：预约、领取、回执、改期、事实与区域查询。

该模块不依赖 Web 框架，也不把“合作方已领取/中间件已发出请求”视为执行完成。
只有合作端 ``sent`` 回执中的 ``executed_at`` 才形成实际执行事实。
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .receivers import DEFAULT_AUDIENCE, ReceiverSimulator
from .timeutil import TimePointError, iso_utc, parse_iso_utc, resolve_local_time


UNIT = "\u001f"

# 没有显式声明时，按常见动作名识别“观众互动入口”。
INTERACTION_ACTION_NAMES = {
    "join",
    "enter",
    "entry",
    "interact",
    "interaction",
    "chat_entry",
    "open_interaction",
    "interaction_entry",
}
REGION_OPEN_FACT_NAMES = {
    "region_open",
    "open_region",
    "publish_region",
    "region_published",
    "room",
    "room_open",
    "open_room",
    "chat_room_open",
}


class MiddlewareError(RuntimeError):
    """中间件操作基础异常。"""


class NotFoundError(MiddlewareError):
    pass


class PlanValidationError(MiddlewareError):
    def __init__(self, errors: list[dict]):
        self.errors = errors
        super().__init__("；".join(e["message"] for e in errors))


class RevisionConflict(MiddlewareError):
    pass


class CallbackConflict(MiddlewareError):
    pass


def region_key(show: str, region: str) -> str:
    return f"{show}{UNIT}{region}"


def _is_int(value: Any) -> bool:
    return type(value) is int


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Middleware:
    def __init__(
        self,
        state_path: str | Path | None = None,
        *,
        now_provider=time.time,
        withdrawal_grace_seconds: int = 300,
        receiver_profiles: dict | None = None,
        wall_clock_fallback: bool = False,
    ):
        self.lock = threading.RLock()
        self.state_path = Path(state_path) if state_path else None
        self.now_provider = now_provider
        self.withdrawal_grace_seconds = withdrawal_grace_seconds
        self.wall_clock_fallback = wall_clock_fallback
        self.simulator = ReceiverSimulator(receiver_profiles)

        if self.state_path and self.state_path.exists():
            with self.state_path.open("r", encoding="utf-8") as source:
                self.state = json.load(source)
            profiles = self.state.get("receiver_profiles")
            if profiles:
                self.simulator = ReceiverSimulator(profiles)
        else:
            self.state = self._fresh_state()
            if self.state_path:
                self._save_locked()

    def _fresh_state(self) -> dict:
        return {
            "version": 1,
            "seq": 0,
            "clock": None,
            "default_audience": list(DEFAULT_AUDIENCE),
            "receiver_profiles": {k: dict(v) for k, v in self.simulator.snapshot().items()},
            "regions": {},
            "items": {},
            "dispatches": {},
            "corrections": {},
            "exposures": [],
            "events": [],
            "replan_requests": [],
        }

    # ------------------------------------------------------------------
    # 持久化、时钟与审计
    # ------------------------------------------------------------------
    def _save_locked(self) -> None:
        if not self.state_path:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name(f".{self.state_path.name}.tmp-{os.getpid()}")
        with temporary.open("w", encoding="utf-8") as target:
            json.dump(self.state, target, ensure_ascii=False, indent=2, sort_keys=False)
        os.replace(temporary, self.state_path)

    def _next_seq(self) -> int:
        self.state["seq"] = int(self.state.get("seq", 0)) + 1
        return self.state["seq"]

    def _now(self, now: int | float | None = None) -> int:
        if now is not None:
            return int(now)
        clock = self.state.get("clock")
        if clock and _is_int(clock.get("epoch")):
            return int(clock["epoch"])
        if self.wall_clock_fallback:
            return int(self.now_provider())
        # 演练记录尚未给出第一个 clock 时，不能用宿主机当前时间把场景误判为过期。
        return 0

    def _event(self, event_type: str, **fields: Any) -> dict:
        event = {"seq": self._next_seq(), "type": event_type}
        event.update(fields)
        self.state.setdefault("events", []).append(event)
        # 状态文件只保留最近 1000 条审计，完整业务事实仍在各实体中。
        if len(self.state["events"]) > 1000:
            self.state["events"] = self.state["events"][-1000:]
        return event

    def set_clock(self, utc: str, *, logical_at: int | None = None) -> dict:
        with self.lock:
            epoch = parse_iso_utc(utc)
            self.state["clock"] = {"utc": iso_utc(epoch), "epoch": epoch}
            event = self._event("clock", logical_at=logical_at, utc=iso_utc(epoch), epoch=epoch)
            self._evaluate_all_locked(epoch)
            self._save_locked()
            return dict(event)

    def restart(self, *, logical_at: int | None = None) -> dict:
        """模拟进程重启；配置 state_path 时会真正从磁盘重新加载。"""
        with self.lock:
            self._save_locked()
            durable = False
            if self.state_path and self.state_path.exists():
                with self.state_path.open("r", encoding="utf-8") as source:
                    self.state = json.load(source)
                profiles = self.state.get("receiver_profiles")
                if profiles:
                    self.simulator = ReceiverSimulator(profiles)
                durable = True
            now = self._now()
            event = self._event("restart", logical_at=logical_at, durable_state=durable, at_utc=iso_utc(now))
            self._evaluate_all_locked(now)
            self._save_locked()
            return {"restarted": True, "durable_state": durable, "at": iso_utc(now), "event": event}

    # ------------------------------------------------------------------
    # 预约与改期
    # ------------------------------------------------------------------
    def schedule_plan(
        self,
        record: dict,
        *,
        now: int | float | None = None,
        logical_at: int | None = None,
    ) -> dict:
        if not isinstance(record, dict):
            raise PlanValidationError([{"code": "record_not_object", "message": "plan record 必须是对象"}])

        with self.lock:
            current_time = self._now(now)
            normalized, errors = self._normalize_plan(record)
            show = record.get("show") if isinstance(record.get("show"), str) else "<unknown>"
            region = record.get("region") if isinstance(record.get("region"), str) else "<unknown>"

            if errors:
                request_id = _new_id("replan")
                ticket = {
                    "id": request_id,
                    "show": show,
                    "region": region,
                    "rk": region_key(show, region) if show != "<unknown>" and region != "<unknown>" else None,
                    "revision": record.get("revision"),
                    "status": "needs_editor_change",
                    "errors": errors,
                    "logical_at": logical_at,
                    "created_epoch": current_time,
                    "created_utc": iso_utc(current_time),
                }
                self.state.setdefault("replan_requests", []).append(ticket)
                self._event("plan_rejected", logical_at=logical_at, show=show, region=region, errors=errors)
                self._save_locked()
                raise PlanValidationError(errors)

            assert normalized is not None
            show = normalized["show"]
            region = normalized["region"]
            revision = normalized["revision"]
            rk = region_key(show, region)
            region_state = self.state["regions"].get(rk)

            canonical = json.dumps(
                {"tz": normalized["tz"], "actions": normalized["raw_actions"]},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )

            if region_state and str(revision) in region_state.get("revisions", {}):
                existing = region_state["revisions"][str(revision)]
                if existing.get("canonical") == canonical:
                    return {
                        "accepted": True,
                        "idempotent": True,
                        "show": show,
                        "region": region,
                        "revision": revision,
                    }
                raise RevisionConflict(f"{show}/{region} revision {revision} 已存在且内容不同")

            if region_state and _is_int(region_state.get("current_revision")) and revision <= int(region_state["current_revision"]):
                raise RevisionConflict(
                    f"revision {revision} 不能覆盖当前 revision {region_state['current_revision']}"
                )

            if not region_state:
                region_state = {
                    "show": show,
                    "region": region,
                    "current_revision": None,
                    "tz": normalized["tz"],
                    "revisions": {},
                    "item_order": [],
                    "audience": normalized.get("audience"),
                    "publication": {"open": False, "status": "unknown", "history": []},
                    "facts": {},
                }
                self.state["regions"][rk] = region_state

            old_revision = region_state.get("current_revision")
            old_items: dict[str, dict] = {}
            if old_revision is not None:
                for item_id in region_state["item_order"]:
                    item = self.state["items"][item_id]
                    if item["revision"] == old_revision:
                        old_items[item["action"]] = item

            region_state["tz"] = normalized["tz"]
            if normalized.get("audience") is not None:
                region_state["audience"] = normalized["audience"]
            region_state["revisions"][str(revision)] = {
                "revision": revision,
                "tz": normalized["tz"],
                "canonical": canonical,
                "actions": normalized["actions"],
                "raw_actions": normalized["raw_actions"],
                "received_seq": self.state["seq"] + 1,
                "logical_at": logical_at,
                "received_epoch": current_time,
                "received_utc": iso_utc(current_time),
            }

            new_items: dict[str, dict] = {}
            for action in normalized["actions"]:
                item_id = f"{rk}{UNIT}{action['action']}{UNIT}r{revision}"
                item = {
                    "id": item_id,
                    "rk": rk,
                    "show": show,
                    "region": region,
                    "action": action["action"],
                    "revision": revision,
                    "kind": action.get("kind"),
                    "scheduled_epoch": action["resolved"]["utc_epoch"],
                    "scheduled_utc": action["resolved"]["utc"],
                    "local": action["local"],
                    "fold": action["resolved"]["fold"],
                    "ambiguous": action["resolved"]["ambiguous"],
                    "utc_offset_seconds": action["resolved"]["utc_offset_seconds"],
                    "window_seconds": action["window"],
                    "deadline_epoch": action["resolved"]["utc_epoch"] + action["window"],
                    "deadline_utc": iso_utc(action["resolved"]["utc_epoch"] + action["window"]),
                    "miss": action["miss"],
                    "needs": list(action.get("needs", [])),
                    "requires_region_open": action["requires_region_open"],
                    "publishes_region_open": action["publishes_region_open"],
                    "allow_replacement_after_send": action["allow_replacement_after_send"],
                    "audience": action.get("audience"),
                    "redelivery_gate": {"state": "new_schedule", "blocked": False},
                    "missing_preconditions": [],
                    "phase": "scheduled",
                    "terminal_state": None,
                    "lifecycle": "current",
                    "actual": None,
                    "disposition": None,
                    "active_dispatch_id": None,
                    "pull_count": 0,
                    "unknown_reports": [],
                    "created_seq": self.state["seq"] + 1,
                    "logical_at": logical_at,
                }
                self.state["items"][item_id] = item
                region_state["item_order"].append(item_id)
                new_items[action["action"]] = item

            # 旧安排：未派项直接重算；合作方持有的项必须先请求撤回/等待结论。
            for action_name, old_item in old_items.items():
                self._supersede_or_withdraw_locked(old_item, new_items.get(action_name), current_time)
            for action_name, old_item in old_items.items():
                if action_name not in new_items:
                    # 已在上面处理；保留显式分支便于阅读“删除动作”的情况。
                    pass

            region_state["current_revision"] = revision
            event = self._event(
                "plan_accepted",
                logical_at=logical_at,
                rk=rk,
                show=show,
                region=region,
                revision=revision,
                previous_revision=old_revision,
                actions=[a["action"] for a in normalized["actions"]],
            )
            self._evaluate_region_locked(rk, current_time)
            rechecked = self._recheck_summary_locked(rk)
            self._save_locked()
            return {
                "accepted": True,
                "idempotent": False,
                "show": show,
                "region": region,
                "revision": revision,
                "previous_revision": old_revision,
                "rechecked": rechecked,
                "event": event,
            }

    def reserve(self, record: dict, **kwargs) -> dict:
        """首次节目预约的语义化别名。"""
        return self.schedule_plan(record, **kwargs)

    def reschedule(self, record: dict, **kwargs) -> dict:
        """改期入口：必须针对已有 show/region，且提交更高 revision。"""
        show = record.get("show") if isinstance(record, dict) else None
        region = record.get("region") if isinstance(record, dict) else None
        revision = record.get("revision") if isinstance(record, dict) else None
        if not isinstance(show, str) or not isinstance(region, str):
            raise NotFoundError("改期记录需要 show 与 region")
        rk = region_key(show, region)
        with self.lock:
            region_state = self.state["regions"].get(rk)
            if not region_state or region_state.get("current_revision") is None:
                raise NotFoundError(f"未找到 {show}/{region} 的原预约，不能改期")
            current = int(region_state["current_revision"])
            if not _is_int(revision) or revision <= current:
                raise RevisionConflict(f"改期 revision 必须大于当前 r{current}")
        return self.schedule_plan(record, **kwargs)

    def _normalize_plan(self, record: dict) -> tuple[dict | None, list[dict]]:
        errors: list[dict] = []

        show = record.get("show")
        region = record.get("region")
        tz_name = record.get("tz")
        revision = record.get("revision")
        actions = record.get("actions")

        if not isinstance(show, str) or not show:
            errors.append({"code": "bad_show", "path": "show", "message": "show 必须是非空字符串"})
        if not isinstance(region, str) or not region:
            errors.append({"code": "bad_region", "path": "region", "message": "region 必须是非空字符串"})
        if not _is_int(revision) or revision <= 0:
            errors.append({"code": "bad_revision", "path": "revision", "message": "revision 必须是正整数"})

        tz_error = None
        if not isinstance(tz_name, str) or not tz_name:
            errors.append({"code": "bad_tz", "path": "tz", "message": "tz 必须是非空 IANA 时区名"})
        else:
            try:
                from zoneinfo import ZoneInfo

                ZoneInfo(tz_name)
            except Exception as exc:  # noqa: BLE001 - 转换为编排错误
                tz_error = str(exc)
                errors.append({"code": "bad_tz", "path": "tz", "message": f"未知 IANA 时区 {tz_name!r}: {exc}"})

        if not isinstance(actions, list):
            errors.append({"code": "bad_actions", "path": "actions", "message": "actions 必须是数组"})
            return None, errors

        seen: set[str] = set()
        normalized_actions: list[dict] = []
        raw_actions: list[dict] = []

        for index, raw_action in enumerate(actions):
            path = f"actions[{index}]"
            if not isinstance(raw_action, dict):
                errors.append({"code": "bad_action", "path": path, "message": "每个 action 必须是对象"})
                continue

            name = raw_action.get("action")
            if not isinstance(name, str) or not name:
                errors.append({"code": "bad_action_name", "path": f"{path}.action", "message": "action 名称必须是非空字符串"})
                continue
            if name in seen:
                errors.append({"code": "duplicate_action", "path": f"{path}.action", "message": f"action {name!r} 在同一预约中重复"})
            seen.add(name)

            window = raw_action.get("window")
            if not _is_int(window) or window < 0:
                errors.append({"code": "bad_window", "path": f"{path}.window", "message": f"action {name}: window 必须是非负整数秒"})

            miss = raw_action.get("miss")
            if miss not in ("skip", "catch_up"):
                errors.append({"code": "bad_miss", "path": f"{path}.miss", "message": f"action {name}: miss 必须是 skip 或 catch_up"})

            needs = raw_action.get("needs", [])
            if needs is None:
                needs = []
            if not isinstance(needs, list) or not all(isinstance(n, str) and n for n in needs):
                errors.append({"code": "bad_needs", "path": f"{path}.needs", "message": f"action {name}: needs 必须是字符串数组"})
                needs = []
            elif name in needs:
                errors.append({"code": "self_dependency", "path": f"{path}.needs", "message": f"action {name} 不能依赖自身"})

            audience = raw_action.get("audience")
            if audience is not None and (not isinstance(audience, list) or not all(isinstance(x, str) for x in audience)):
                errors.append({"code": "bad_audience", "path": f"{path}.audience", "message": f"action {name}: audience 必须是字符串数组"})
                audience = None

            resolved = None
            if isinstance(tz_name, str) and not tz_error:
                try:
                    resolved = resolve_local_time(raw_action.get("local"), tz_name, raw_action.get("fold"))
                except TimePointError as exc:
                    errors.append({"code": "bad_local_time", "path": f"{path}.local", "message": f"action {name}: {exc}"})

            kind = raw_action.get("kind") or raw_action.get("type")
            explicit_gate = raw_action.get("requires_region_open")
            if explicit_gate is None:
                requires_region_open = (
                    isinstance(kind, str) and kind.lower() in {"interaction", "entry", "chat_entry"}
                ) or name.lower() in INTERACTION_ACTION_NAMES
            else:
                requires_region_open = bool(explicit_gate)

            action_data = {
                "action": name,
                "local": raw_action.get("local"),
                "fold": raw_action.get("fold"),
                "window": window if _is_int(window) else 0,
                "miss": miss if miss in ("skip", "catch_up") else "skip",
                "needs": list(needs),
                "kind": kind,
                "audience": audience,
                "requires_region_open": requires_region_open,
                "publishes_region_open": bool(raw_action.get("publishes_region_open", False))
                or name.lower() in REGION_OPEN_FACT_NAMES,
                "allow_replacement_after_send": bool(raw_action.get("allow_replacement_after_send", False)),
                "resolved": resolved.as_dict() if resolved else None,
            }
            normalized_actions.append(action_data)
            raw_actions.append(dict(raw_action))

        # needs 可以指向同地区 action，也可以指向稍后到达的同名外部公开事实；
        # 因此这里只对已知 action 做环检查，未知名在运行期显示为未满足前置。
        names = {a["action"] for a in normalized_actions}
        dependency_map: dict[str, list[str]] = {}
        for action in normalized_actions:
            dependency_map[action["action"]] = [n for n in action["needs"] if n in names]

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node: str, chain: list[str]) -> None:
            if node in visiting:
                errors.append({"code": "dependency_cycle", "path": "actions[].needs", "message": "依赖存在环: " + " -> ".join(chain + [node])})
                return
            if node in visited:
                return
            visiting.add(node)
            for nxt in dependency_map.get(node, []):
                visit(nxt, chain + [node])
            visiting.remove(node)
            visited.add(node)

        for name in names:
            visit(name, [])

        if errors:
            return None, errors

        return {
            "show": show,
            "region": region,
            "revision": revision,
            "tz": tz_name,
            "actions": normalized_actions,
            "raw_actions": raw_actions,
            "audience": record.get("audience") if isinstance(record.get("audience"), list) else None,
        }, []

    def _active_dispatch_locked(self, item: dict) -> dict | None:
        dispatch_id = item.get("active_dispatch_id")
        dispatch = self.state["dispatches"].get(dispatch_id) if dispatch_id else None
        if dispatch and dispatch.get("state") in {"leased", "withdraw_requested"}:
            return dispatch

        # 兼容 active id 丢失或重复领取的情况。
        candidates = [
            d
            for d in self.state["dispatches"].values()
            if d.get("item_id") == item["id"] and d.get("state") in {"leased", "withdraw_requested"}
        ]
        if candidates:
            latest = max(candidates, key=lambda d: d.get("seq", 0))
            item["active_dispatch_id"] = latest["id"]
            return latest
        item["active_dispatch_id"] = None
        return None

    def _supersede_or_withdraw_locked(self, old_item: dict, new_item: dict | None, now: int) -> None:
        old_item["superseded_at_epoch"] = now
        old_item["superseded_at_utc"] = iso_utc(now)
        if new_item:
            old_item["superseded_by_revision"] = new_item["revision"]
        active = self._active_dispatch_locked(old_item)
        if active:
            active["state"] = "withdraw_requested"
            active["withdraw_requested_epoch"] = now
            active["withdraw_requested_utc"] = iso_utc(now)
            active["respond_by_epoch"] = now + self.withdrawal_grace_seconds
            active["respond_by_utc"] = iso_utc(now + self.withdrawal_grace_seconds)
            active["withdraw_reason"] = "plan_revised"
            old_item["lifecycle"] = "withdraw_requested"
            old_item["phase"] = "withdraw_requested"
            if new_item:
                new_item["redelivery_gate"] = {
                    "state": "awaiting_old_disposition",
                    "blocked": True,
                    "dispatch_id": active["id"],
                    "old_revision": old_item["revision"],
                    "message": "合作通知方仍持有旧安排，必须先确认撤回或未执行",
                }
            self._event(
                "withdrawal_requested",
                rk=old_item["rk"],
                show=old_item["show"],
                region=old_item["region"],
                action=old_item["action"],
                revision=old_item["revision"],
                dispatch_id=active["id"],
            )
            return

        if old_item.get("actual"):
            old_item["lifecycle"] = "superseded_after_send"
            if new_item:
                blocked = not new_item.get("allow_replacement_after_send", False)
                new_item["redelivery_gate"] = {
                    "state": "prior_sent_correction_required",
                    "blocked": blocked,
                    "old_revision": old_item["revision"],
                    "first_executed_epoch": old_item["actual"].get("first_executed_epoch"),
                    "message": "旧安排已确认实际发出；必须保留更正事实，不能把新通知当作旧通知未出现的证明",
                }
        elif (old_item.get("disposition") or {}).get("state") == "withdrawn":
            old_item["lifecycle"] = "withdrawn"
            if new_item:
                new_item["redelivery_gate"] = {
                    "state": "released_after_withdraw",
                    "blocked": False,
                    "old_revision": old_item["revision"],
                }
        else:
            old_item["lifecycle"] = "superseded"
            old_item["superseded_epoch"] = now
            if new_item:
                new_item["redelivery_gate"] = {
                    "state": "unsent_old_item_recomputed",
                    "blocked": False,
                    "old_revision": old_item["revision"],
                }

    # ------------------------------------------------------------------
    # 地区发布事实
    # ------------------------------------------------------------------
    def receive_publication(
        self,
        record: dict,
        *,
        now: int | float | None = None,
        logical_at: int | None = None,
    ) -> dict:
        if not isinstance(record, dict):
            raise PlanValidationError([{"code": "record_not_object", "message": "publication record 必须是对象"}])
        show = record.get("show")
        region = record.get("region")
        if not isinstance(show, str) or not show or not isinstance(region, str) or not region:
            raise NotFoundError("publication 需要 show 与 region")

        with self.lock:
            current_time = self._now(now)
            rk = region_key(show, region)
            region_state = self.state["regions"].get(rk)
            if not region_state:
                region_state = {
                    "show": show,
                    "region": region,
                    "current_revision": None,
                    "tz": record.get("tz") if isinstance(record.get("tz"), str) else None,
                    "revisions": {},
                    "item_order": [],
                    "audience": None,
                    "publication": {"open": False, "status": "unknown", "history": []},
                    "facts": {},
                }
                self.state["regions"][rk] = region_state

            occurred = record.get("published_at") or record.get("occurred_at") or record.get("opened_at")
            occurred_epoch = parse_iso_utc(occurred) if isinstance(occurred, str) else current_time
            status = str(record.get("status", "open")).lower()
            is_open = bool(record.get("open", status in {"open", "live", "published", "online"}))

            fact = {
                "status": status,
                "open": is_open,
                "at_epoch": occurred_epoch,
                "at_utc": iso_utc(occurred_epoch),
                "source": record.get("source", "region_publication_api"),
            }
            region_state["publication"]["open"] = is_open
            region_state["publication"]["status"] = status
            region_state["publication"]["opened_at"] = iso_utc(occurred_epoch) if is_open else region_state["publication"].get("opened_at")
            region_state["publication"]["history"].append({**fact, "logical_at": logical_at})
            if is_open:
                region_state["facts"]["region_open"] = {
                    "type": "publication",
                    "at_epoch": occurred_epoch,
                    "at_utc": iso_utc(occurred_epoch),
                    "source": fact["source"],
                }
            else:
                region_state["facts"].pop("region_open", None)

            self._event(
                "publication_received",
                logical_at=logical_at,
                rk=rk,
                show=show,
                region=region,
                status=status,
                open=is_open,
                at_epoch=occurred_epoch,
                at_utc=iso_utc(occurred_epoch),
                source=fact["source"],
            )
            self._evaluate_region_locked(rk, current_time)
            self._save_locked()
            return {"accepted": True, "region_open": is_open, "opened_at": iso_utc(occurred_epoch)}

    def record_fact(
        self,
        record: dict,
        *,
        now: int | float | None = None,
        logical_at: int | None = None,
    ) -> dict:
        """记录“上游已经公开”的事实，可供 needs 按名称依赖。"""
        show = record.get("show")
        region = record.get("region")
        name = record.get("fact") or record.get("name")
        if not isinstance(show, str) or not show or not isinstance(region, str) or not region:
            raise NotFoundError("fact 需要 show 与 region")
        if not isinstance(name, str) or not name:
            raise PlanValidationError([{"code": "bad_fact_name", "message": "fact/name 必须是非空字符串"}])

        with self.lock:
            current_time = self._now(now)
            rk = region_key(show, region)
            region_state = self.state["regions"].get(rk)
            if not region_state:
                region_state = {
                    "show": show,
                    "region": region,
                    "current_revision": None,
                    "tz": None,
                    "revisions": {},
                    "item_order": [],
                    "audience": None,
                    "publication": {"open": False, "status": "unknown", "history": []},
                    "facts": {},
                }
                self.state["regions"][rk] = region_state

            occurred_raw = record.get("occurred_at") or record.get("published_at") or record.get("at_utc")
            occurred_epoch = parse_iso_utc(occurred_raw) if isinstance(occurred_raw, str) else current_time
            fact = {
                "type": record.get("type", "publication"),
                "at_epoch": occurred_epoch,
                "at_utc": iso_utc(occurred_epoch),
                "source": record.get("source", "external_publication_api"),
                "detail": record.get("detail"),
            }
            region_state["facts"][name] = fact
            if name == "region_open":
                region_state["publication"]["open"] = bool(record.get("open", True))
                region_state["publication"]["status"] = record.get("status", "open")
                region_state["publication"]["opened_at"] = fact["at_utc"]

            self._event(
                "fact_received",
                logical_at=logical_at,
                rk=rk,
                show=show,
                region=region,
                fact=name,
                fact_type=fact["type"],
                at_epoch=occurred_epoch,
                at_utc=fact["at_utc"],
                source=fact["source"],
            )
            self._evaluate_region_locked(rk, current_time)
            self._save_locked()
            return {"accepted": True, "fact": name, "at_utc": fact["at_utc"]}

    def set_audience(
        self,
        show: str,
        region: str,
        audience: list[str],
        *,
        now: int | float | None = None,
    ) -> dict:
        if not isinstance(audience, list) or not all(isinstance(x, str) and x for x in audience):
            raise PlanValidationError([{"code": "bad_audience", "message": "audience 必须是非空字符串数组"}])
        with self.lock:
            rk = region_key(show, region)
            region_state = self.state["regions"].get(rk)
            if not region_state:
                raise NotFoundError(f"未找到 {show}/{region}")
            region_state["audience"] = list(audience)
            current_time = self._now(now)
            self._evaluate_region_locked(rk, current_time)
            self._save_locked()
            return {"accepted": True, "show": show, "region": region, "audience": audience}

    # ------------------------------------------------------------------
    # 依赖、窗口和状态重算
    # ------------------------------------------------------------------
    def _fact_for_need_locked(self, rk: str, need: str, item: dict) -> dict | None:
        region_state = self.state["regions"][rk]
        fact = region_state.get("facts", {}).get(need)
        if fact:
            return {"source": "region_fact", "name": need, **fact}

        matches = [
            candidate
            for candidate_id in region_state["item_order"]
            if (candidate := self.state["items"].get(candidate_id))
            and candidate.get("action") == need
            and candidate.get("actual")
        ]
        if not matches:
            return None
        chosen = min(matches, key=lambda x: x["actual"].get("first_executed_epoch", 10**20))
        actual = chosen["actual"]
        return {
            "source": "partner_sent_receipt",
            "name": need,
            "item_id": chosen["id"],
            "revision": chosen["revision"],
            "at_epoch": actual.get("first_executed_epoch"),
            "at_utc": iso_utc(actual.get("first_executed_epoch")),
            "revision_matches": chosen["revision"] == item["revision"],
        }

    def _gate_status_locked(self, item: dict) -> tuple[bool, str | None]:
        gate = item.get("redelivery_gate") or {}
        state = gate.get("state")
        if state == "awaiting_old_disposition":
            dispatch = self.state["dispatches"].get(gate.get("dispatch_id"))
            if not dispatch or dispatch.get("state") == "withdrawn":
                gate["state"] = "released_after_withdraw"
                gate["blocked"] = False
                return False, None
            if dispatch.get("state") == "sent":
                gate["state"] = "prior_sent_correction_required"
                gate["blocked"] = not item.get("allow_replacement_after_send", False)
            else:
                gate["blocked"] = True
                return True, "waiting_old_disposition"

        if (item.get("redelivery_gate") or {}).get("state") == "prior_sent_correction_required":
            blocked = not item.get("allow_replacement_after_send", False)
            gate["blocked"] = blocked
            return blocked, "prior_sent_requires_correction" if blocked else None

        return False, None

    def _preconditions_locked(self, rk: str, item: dict, region_state: dict) -> tuple[list[dict], list[dict]]:
        missing: list[dict] = []
        satisfied: list[dict] = []
        for need in item.get("needs", []):
            fact = self._fact_for_need_locked(rk, need, item)
            if not fact:
                missing.append(
                    {
                        "code": "upstream_not_actual",
                        "precondition": need,
                        "message": f"等待 {need} 已公开或回执确认的实际完成",
                    }
                )
            else:
                satisfied.append(fact)

        if item.get("requires_region_open") and not region_state.get("publication", {}).get("open"):
            missing.append(
                {
                    "code": "region_open",
                    "precondition": "region_open",
                    "message": "地区尚未正式发布/开放，不能投递互动入口",
                }
            )

        item["missing_preconditions"] = missing
        item["satisfied_preconditions"] = satisfied
        return missing, satisfied

    def _recheck_summary_locked(self, rk: str) -> list[dict]:
        region_state = self.state["regions"][rk]
        current_revision = region_state.get("current_revision")
        result = []
        for item_id in region_state["item_order"]:
            item = self.state["items"][item_id]
            if item.get("revision") != current_revision:
                continue
            result.append(
                {
                    "action": item.get("action"),
                    "revision": item.get("revision"),
                    "state": item.get("phase"),
                    "missing_preconditions": item.get("missing_preconditions", []),
                    "redelivery_gate_state": (item.get("redelivery_gate") or {}).get("state"),
                }
            )
        return result

    def _evaluate_all_locked(self, now: int) -> None:
        for rk in list(self.state["regions"].keys()):
            self._evaluate_region_locked(rk, now)

    def _evaluate_region_locked(self, rk: str, now: int) -> None:
        region_state = self.state["regions"].get(rk)
        if not region_state:
            return

        for item_id in list(region_state["item_order"]):
            item = self.state["items"].get(item_id)
            if not item:
                continue

            active = self._active_dispatch_locked(item)
            item["evaluated_epoch"] = now
            item["evaluated_utc"] = iso_utc(now)

            # 已确认的事实不可被时钟或重启改写。
            if item.get("actual"):
                executed = item["actual"].get("first_executed_epoch")
                if executed is not None and executed < item["scheduled_epoch"]:
                    item["timeliness"] = "early"
                elif executed is not None and executed <= item["deadline_epoch"]:
                    item["timeliness"] = "on_time"
                else:
                    item["timeliness"] = "late"
                item["phase"] = "sent"
                continue

            if (item.get("disposition") or {}).get("state") == "withdrawn":
                item["phase"] = "withdrawn"
                continue

            if item.get("terminal_state") == "skipped_missed":
                # 已跳过的动作不再投递，但若前置事实或旧派发结论在跳过之后到达，
                # 仍刷新值班看到的原因，不能一直显示旧状态。
                missing, _satisfied = self._preconditions_locked(rk, item, region_state)
                gate_blocked, gate_reason = self._gate_status_locked(item)
                if gate_blocked:
                    gate = item.get("redelivery_gate") or {}
                    missing.append(
                        {
                            "code": gate_reason or "old_disposition",
                            "precondition": "old_disposition",
                            "message": gate.get("message")
                            or ("旧通知已实际发出，必须先完成更正" if gate_reason == "prior_sent_requires_correction" else "旧合作通知项尚未确认不会执行"),
                        }
                    )
                item["phase"] = "skipped_missed"
                continue

            missing, satisfied = self._preconditions_locked(rk, item, region_state)

            if active:
                # 严格窗口已过且合作方仍持有未确认项：请求撤回，但绝不盖章完成。
                if (
                    active.get("state") == "leased"
                    and now > item["deadline_epoch"]
                    and item.get("miss") == "skip"
                    and not active.get("window_close_withdrawal_requested")
                ):
                    active["state"] = "withdraw_requested"
                    active["window_close_withdrawal_requested"] = True
                    active["withdraw_reason"] = "strict_window_closed"
                    active["withdraw_requested_epoch"] = now
                    active["withdraw_requested_utc"] = iso_utc(now)
                    active["respond_by_epoch"] = now + self.withdrawal_grace_seconds
                    active["respond_by_utc"] = iso_utc(now + self.withdrawal_grace_seconds)
                    self._event(
                        "withdrawal_requested",
                        rk=rk,
                        show=item["show"],
                        region=item["region"],
                        action=item["action"],
                        revision=item["revision"],
                        dispatch_id=active["id"],
                        reason="strict_window_closed",
                    )

                if active.get("state") == "withdraw_requested":
                    if now > item["deadline_epoch"]:
                        item["phase"] = "withdrawal_unconfirmed_expired"
                    else:
                        item["phase"] = "withdraw_requested"
                elif now > item["deadline_epoch"]:
                    # catch_up 已被领取后允许合作方迟到回报；它仍不是完成事实。
                    item["phase"] = "held_catch_up" if item.get("miss") == "catch_up" else "expired_unconfirmed"
                else:
                    item["phase"] = "held"
                continue

            if item.get("lifecycle") in {"superseded", "cancelled_by_revision"}:
                item["phase"] = item["lifecycle"]
                continue
            if item.get("lifecycle") == "superseded_after_send":
                item["phase"] = "sent" if item.get("actual") else "superseded"
                continue
            if region_state.get("current_revision") != item.get("revision"):
                item["phase"] = "superseded"
                continue

            gate_blocked, gate_reason = self._gate_status_locked(item)
            if gate_blocked:
                gate = item.get("redelivery_gate") or {}
                item.setdefault("missing_preconditions", []).append(
                    {
                        "code": gate_reason or "old_disposition",
                        "precondition": "old_disposition",
                        "message": gate.get("message")
                        or ("旧通知已实际发出，必须先完成更正" if gate_reason == "prior_sent_requires_correction" else "旧合作通知项尚未确认不会执行"),
                    }
                )

            if now < item["scheduled_epoch"]:
                item["phase"] = "scheduled"
            elif now <= item["deadline_epoch"]:
                if gate_blocked:
                    item["phase"] = "blocked_old_disposition"
                elif missing:
                    item["phase"] = "waiting_precondition"
                else:
                    item["phase"] = "ready"
            else:
                if item.get("miss") == "skip":
                    item["terminal_state"] = "skipped_missed"
                    item["decided_epoch"] = now
                    item["decided_utc"] = iso_utc(now)
                    item["phase"] = "skipped_missed"
                    self._event(
                        "action_skipped_missed",
                        rk=rk,
                        show=item["show"],
                        region=item["region"],
                        action=item["action"],
                        revision=item["revision"],
                        deadline_utc=item["deadline_utc"],
                        decided_utc=iso_utc(now),
                    )
                elif gate_blocked:
                    item["phase"] = "blocked_old_disposition"
                elif missing:
                    item["phase"] = "missed_waiting_precondition"
                else:
                    item["missed"] = True
                    item["phase"] = "catch_up_ready"

    # ------------------------------------------------------------------
    # 人群重算
    # ------------------------------------------------------------------
    def _audience_for_item_locked(self, region_state: dict, item: dict, now: int | float | None = None) -> dict:
        if item.get("audience"):
            target = list(item["audience"])
        elif region_state.get("audience"):
            target = list(region_state["audience"])
        else:
            target = list(self.state.get("default_audience", DEFAULT_AUDIENCE))

        actual = item.get("actual") or {}
        already = list(actual.get("recipients", []))
        already_set = set(already)
        eligible = [recipient for recipient in target if recipient not in already_set]
        sent_at = {}
        for batch in actual.get("batches", []):
            for recipient in batch.get("recipients", []):
                sent_at.setdefault(recipient, batch.get("executed_epoch"))
        receipt_states = self.simulator.reachable_audience(target, sent_at, self._now(now))
        return {
            "target": target,
            "already_confirmed_reached": already,
            "eligible": eligible,
            "simulated_arrival_states": receipt_states,
        }

    # ------------------------------------------------------------------
    # 合作端领取
    # ------------------------------------------------------------------
    def provider_pull(
        self,
        provider: str,
        show: str,
        region: str,
        *,
        now: int | float | None = None,
        logical_at: int | None = None,
    ) -> dict:
        if not isinstance(provider, str) or not provider:
            raise NotFoundError("provider 不能为空")
        with self.lock:
            current_time = self._now(now)
            rk = region_key(show, region)
            region_state = self.state["regions"].get(rk)
            if not region_state:
                raise NotFoundError(f"未找到 {show}/{region} 的预约")

            self._evaluate_region_locked(rk, current_time)

            withdrawals: list[dict] = []
            held: list[dict] = []
            for dispatch in self.state["dispatches"].values():
                if dispatch.get("rk") != rk or dispatch.get("provider") != provider:
                    continue
                if dispatch.get("state") == "withdraw_requested":
                    withdrawals.append(self._dispatch_payload_locked(dispatch))
                elif dispatch.get("state") == "leased":
                    held.append(self._dispatch_payload_locked(dispatch))

            deliveries: list[dict] = []
            current_revision = region_state.get("current_revision")
            for item_id in region_state["item_order"]:
                item = self.state["items"][item_id]
                if item.get("revision") != current_revision:
                    continue
                if item.get("phase") not in {"ready", "catch_up_ready"}:
                    continue
                if self._active_dispatch_locked(item):
                    continue

                seq = self._next_seq()
                dispatch_id = f"dispatch_{seq}"
                audience = self._audience_for_item_locked(region_state, item, current_time)
                dispatch = {
                    "id": dispatch_id,
                    "seq": seq,
                    "kind": "action",
                    "item_id": item["id"],
                    "rk": rk,
                    "show": show,
                    "region": region,
                    "action": item["action"],
                    "revision": item["revision"],
                    "provider": provider,
                    "state": "leased",
                    "leased_epoch": current_time,
                    "leased_utc": iso_utc(current_time),
                    "logical_at": logical_at,
                    "scheduled_epoch": item["scheduled_epoch"],
                    "deadline_epoch": item["deadline_epoch"],
                    "late": item["phase"] == "catch_up_ready",
                    "audience": audience["eligible"],
                    "audience_report": audience,
                    "reports": [],
                }
                self.state["dispatches"][dispatch_id] = dispatch
                item["active_dispatch_id"] = dispatch_id
                item["pull_count"] = int(item.get("pull_count", 0)) + 1
                item["phase"] = "held"
                deliveries.append(self._dispatch_payload_locked(dispatch))

            correction_deliveries = self._pull_corrections_locked(rk, provider, current_time, logical_at)

            self._event(
                "pull",
                logical_at=logical_at,
                rk=rk,
                show=show,
                region=region,
                provider=provider,
                delivery_ids=[d["id"] for d in deliveries],
                withdrawal_ids=[d["id"] for d in withdrawals],
                correction_ids=[d["id"] for d in correction_deliveries],
            )
            self._save_locked()
            return {
                "provider": provider,
                "show": show,
                "region": region,
                "server_utc": iso_utc(current_time),
                "deliveries": deliveries,
                "withdrawals": withdrawals,
                "held": held,
                "correction_deliveries": correction_deliveries,
            }

    def _dispatch_payload_locked(self, dispatch: dict) -> dict:
        if dispatch.get("kind") == "correction":
            correction = self.state["corrections"].get(dispatch.get("correction_id"), {})
            return {
                "id": dispatch["id"],
                "kind": "correction",
                "correction_id": dispatch.get("correction_id"),
                "show": dispatch.get("show"),
                "region": dispatch.get("region"),
                "recipients": correction.get("recipients", dispatch.get("recipients", [])),
                "regarding_action": correction.get("regarding_action"),
                "old_revision": correction.get("old_revision"),
                "original_executed_utc": correction.get("first_original_sent_utc"),
                "message": correction.get("message"),
                "leased_utc": dispatch.get("leased_utc"),
            }
        return {
            "id": dispatch["id"],
            "kind": "action",
            "show": dispatch.get("show"),
            "region": dispatch.get("region"),
            "action": dispatch.get("action"),
            "revision": dispatch.get("revision"),
            "scheduled_utc": iso_utc(dispatch.get("scheduled_epoch")),
            "deadline_utc": iso_utc(dispatch.get("deadline_epoch")),
            "late": dispatch.get("late", False),
            "audience": dispatch.get("audience", []),
            "fencing_token": f"{dispatch.get('action')}@r{dispatch.get('revision')}:{dispatch['id']}",
        }

    def _pull_corrections_locked(self, rk: str, provider: str, now: int, logical_at: int | None) -> list[dict]:
        result: list[dict] = []
        for correction in self.state["corrections"].values():
            if correction.get("rk") != rk:
                continue
            if correction.get("status") not in {"required", "supplement_required"}:
                continue
            if correction.get("active_dispatch_id"):
                existing = self.state["dispatches"].get(correction["active_dispatch_id"])
                if existing and existing.get("state") in {"leased", "withdraw_requested"}:
                    continue

            seq = self._next_seq()
            dispatch_id = f"dispatch_{seq}"
            recipients = list(correction.get("recipients", []))
            dispatch = {
                "id": dispatch_id,
                "seq": seq,
                "kind": "correction",
                "correction_id": correction["id"],
                "rk": rk,
                "show": correction["show"],
                "region": correction["region"],
                "provider": provider,
                "state": "leased",
                "recipients": recipients,
                "leased_epoch": now,
                "leased_utc": iso_utc(now),
                "logical_at": logical_at,
                "reports": [],
            }
            self.state["dispatches"][dispatch_id] = dispatch
            correction["active_dispatch_id"] = dispatch_id
            correction["status"] = "offered"
            correction["offered_epoch"] = now
            result.append(self._dispatch_payload_locked(dispatch))
        return result

    # ------------------------------------------------------------------
    # 合作端回执
    # ------------------------------------------------------------------
    def provider_callback(
        self,
        record: dict,
        *,
        now: int | float | None = None,
        logical_at: int | None = None,
    ) -> dict:
        if not isinstance(record, dict):
            raise CallbackConflict("callback record 必须是对象")
        if record.get("correction_id"):
            return self._correction_callback(record, now=now, logical_at=logical_at)

        provider = record.get("provider")
        show = record.get("show")
        region = record.get("region")
        action = record.get("action")
        revision = record.get("revision")
        state_value = record.get("state")

        with self.lock:
            current_time = self._now(now)
            if state_value not in {"sent", "withdrawn", "unknown"}:
                raise CallbackConflict("state 必须是 sent、withdrawn 或 unknown")
            if not all(isinstance(x, str) and x for x in [provider, show, region, action]):
                raise NotFoundError("callback 缺少 provider/show/region/action")
            if not _is_int(revision):
                raise NotFoundError("callback revision 必须是整数")

            rk = region_key(show, region)
            item_id = f"{rk}{UNIT}{action}{UNIT}r{revision}"
            item = self.state["items"].get(item_id)
            if not item:
                raise NotFoundError(f"未找到回执对应的预约动作 {show}/{region}/{action}@r{revision}")

            dispatch = self._dispatch_for_callback_locked(item, provider, record.get("dispatch_id"))
            report = {
                "state": state_value,
                "seq": self._next_seq(),
                "logical_at": logical_at,
                "reported_utc": iso_utc(current_time),
                "record": {k: v for k, v in record.items() if k != "record"},
            }
            dispatch.setdefault("reports", []).append(report)
            dispatch["last_report"] = state_value

            if state_value == "unknown":
                item.setdefault("unknown_reports", []).append(report)
                self._event(
                    "callback_unknown",
                    logical_at=logical_at,
                    rk=rk,
                    show=show,
                    region=region,
                    action=action,
                    revision=revision,
                    dispatch_id=dispatch["id"],
                )
                self._evaluate_region_locked(rk, current_time)
                self._save_locked()
                return {"accepted": True, "state": "unknown", "dispatch_id": dispatch["id"], "completed": False}

            if state_value == "withdrawn":
                if dispatch.get("state") == "sent" or item.get("actual"):
                    conflict = {
                        "code": "withdraw_after_sent_conflict",
                        "message": "已有实际 sent 回执，不能再把旧项改成 withdrawn；应走更正",
                        "callback": record,
                    }
                    self.state.setdefault("callback_conflicts", []).append(conflict)
                    self._save_locked()
                    raise CallbackConflict(conflict["message"])

                dispatch["state"] = "withdrawn"
                dispatch["terminal_epoch"] = current_time
                dispatch["terminal_utc"] = iso_utc(current_time)
                dispatch["withdrawn_by_callback"] = True
                item["active_dispatch_id"] = None
                item["disposition"] = {"state": "withdrawn", "at_epoch": current_time, "at_utc": iso_utc(current_time)}
                self._release_gates_locked(rk, action, revision, "released_after_withdraw")
                self._event(
                    "callback_withdrawn",
                    logical_at=logical_at,
                    rk=rk,
                    show=show,
                    region=region,
                    action=action,
                    revision=revision,
                    dispatch_id=dispatch["id"],
                )
                self._evaluate_region_locked(rk, current_time)
                self._save_locked()
                return {"accepted": True, "state": "withdrawn", "dispatch_id": dispatch["id"], "completed": False}

            # state == sent
            executed_raw = record.get("executed_at")
            if isinstance(executed_raw, str) and executed_raw:
                executed_epoch = parse_iso_utc(executed_raw)
                executed_source = "callback.executed_at"
            elif self.state.get("clock"):
                executed_epoch = int(self.state["clock"]["epoch"])
                executed_source = "last_clock"
            else:
                executed_epoch = current_time
                executed_source = "server_time"

            recipients = record.get("recipients", [])
            if recipients is None:
                recipients = []
            if not isinstance(recipients, list) or not all(isinstance(x, str) and x for x in recipients):
                raise CallbackConflict("sent 回执的 recipients 必须是非空字符串数组")

            # 同一派发的完全重复回执保持幂等，不重复生成更正/暴露记录。
            if (
                dispatch.get("state") == "sent"
                and dispatch.get("executed_epoch") == executed_epoch
                and list(dispatch.get("recipients", [])) == recipients
            ):
                return {
                    "accepted": True,
                    "idempotent": True,
                    "state": "sent",
                    "dispatch_id": dispatch["id"],
                    "actual_executed_utc": iso_utc(executed_epoch),
                }

            dispatch["state"] = "sent"
            dispatch["executed_epoch"] = executed_epoch
            dispatch["executed_utc"] = iso_utc(executed_epoch)
            dispatch["executed_at_source"] = executed_source
            dispatch["recipients"] = recipients
            dispatch["terminal_epoch"] = current_time
            item["active_dispatch_id"] = None

            self._append_actual_locked(item, dispatch, executed_epoch, recipients, provider, current_time)
            self._record_exposures_locked(item, dispatch, recipients, executed_epoch, current_time)

            current_revision = self.state["regions"][rk].get("current_revision")
            requires_correction = revision < current_revision or item.get("lifecycle") in {
                "withdraw_requested",
                "superseded",
                "superseded_after_send",
            } or dispatch.get("state") == "withdraw_requested" or bool(dispatch.get("withdraw_requested_epoch"))

            correction_id = None
            if requires_correction:
                correction_id = self._create_or_update_correction_locked(
                    item, dispatch, recipients, executed_epoch, current_time, logical_at
                )
                self._mark_new_item_gate_sent_locked(rk, action, revision, executed_epoch)

            if item.get("publishes_region_open"):
                region_state = self.state["regions"][rk]
                region_state["publication"]["open"] = True
                region_state["publication"]["status"] = "open_by_action_receipt"
                region_state["publication"]["opened_at"] = iso_utc(executed_epoch)
                region_state["facts"]["region_open"] = {
                    "type": "partner_sent_receipt",
                    "at_epoch": executed_epoch,
                    "at_utc": iso_utc(executed_epoch),
                    "source_action": action,
                    "source_revision": revision,
                }

            self._event(
                "callback_sent",
                logical_at=logical_at,
                rk=rk,
                show=show,
                region=region,
                action=action,
                revision=revision,
                dispatch_id=dispatch["id"],
                executed_utc=iso_utc(executed_epoch),
                recipients=recipients,
                correction_id=correction_id,
            )
            self._evaluate_region_locked(rk, current_time)
            self._save_locked()
            return {
                "accepted": True,
                "state": "sent",
                "dispatch_id": dispatch["id"],
                "actual_executed_utc": iso_utc(executed_epoch),
                "recipients": recipients,
                "correction_id": correction_id,
                "completed": True,
            }

    def _dispatch_for_callback_locked(self, item: dict, provider: str, dispatch_id: str | None) -> dict:
        if dispatch_id:
            dispatch = self.state["dispatches"].get(dispatch_id)
            if not dispatch:
                raise NotFoundError(f"未知 dispatch_id {dispatch_id}")
            return dispatch

        active = self._active_dispatch_locked(item)
        if active and active.get("provider") == provider:
            return active

        candidates = [
            d
            for d in self.state["dispatches"].values()
            if d.get("item_id") == item["id"] and d.get("provider") == provider
        ]
        if candidates:
            return max(candidates, key=lambda d: d.get("seq", 0))

        # 合作方可能在状态丢失/主动回报时带来事实；保留为未预约派发，而不是拒绝事实。
        seq = self._next_seq()
        dispatch = {
            "id": f"dispatch_{seq}",
            "seq": seq,
            "kind": "action",
            "item_id": item["id"],
            "rk": item["rk"],
            "show": item["show"],
            "region": item["region"],
            "action": item["action"],
            "revision": item["revision"],
            "provider": provider,
            "state": "leased",
            "unsolicited_callback": True,
            "reports": [],
        }
        self.state["dispatches"][dispatch["id"]] = dispatch
        item["active_dispatch_id"] = dispatch["id"]
        return dispatch

    def _append_actual_locked(
        self, item: dict, dispatch: dict, executed_epoch: int, recipients: list[str], provider: str, now: int
    ) -> None:
        actual = item.get("actual")
        if not actual:
            actual = {
                "state": "sent",
                "provider": provider,
                "first_executed_epoch": executed_epoch,
                "first_executed_utc": iso_utc(executed_epoch),
                "last_executed_epoch": executed_epoch,
                "last_executed_utc": iso_utc(executed_epoch),
                "recipients": [],
                "batches": [],
                "confirmed_at_utc": iso_utc(now),
            }
            item["actual"] = actual
        actual["first_executed_epoch"] = min(actual["first_executed_epoch"], executed_epoch)
        actual["first_executed_utc"] = iso_utc(actual["first_executed_epoch"])
        actual["last_executed_epoch"] = max(actual["last_executed_epoch"], executed_epoch)
        actual["last_executed_utc"] = iso_utc(actual["last_executed_epoch"])
        actual["provider"] = provider
        for recipient in recipients:
            if recipient not in actual["recipients"]:
                actual["recipients"].append(recipient)
        actual["batches"].append(
            {
                "dispatch_id": dispatch["id"],
                "provider": provider,
                "executed_epoch": executed_epoch,
                "executed_utc": iso_utc(executed_epoch),
                "recipients": list(recipients),
                "confirmed_epoch": now,
                "confirmed_utc": iso_utc(now),
            }
        )

    def _record_exposures_locked(
        self, item: dict, dispatch: dict, recipients: list[str], executed_epoch: int, now: int
    ) -> None:
        existing = {(e.get("dispatch_id"), e.get("recipient")) for e in self.state.get("exposures", [])}
        for recipient in recipients:
            key = (dispatch["id"], recipient)
            if key in existing:
                continue
            self.simulator.ensure(recipient)
            arrival = self.simulator.receipt_state(recipient, executed_epoch, now)
            exposure = {
                "id": _new_id("exposure"),
                "rk": item["rk"],
                "show": item["show"],
                "region": item["region"],
                "meaning": item["action"],
                "action": item["action"],
                "revision": item["revision"],
                "dispatch_id": dispatch["id"],
                "recipient": recipient,
                "sent_epoch": executed_epoch,
                "sent_utc": iso_utc(executed_epoch),
                "recorded_epoch": now,
                "recorded_utc": iso_utc(now),
                "simulated_arrival_state": arrival,
                "partner_confirmed_recipient": True,
            }
            self.state.setdefault("exposures", []).append(exposure)

    def _create_or_update_correction_locked(
        self,
        item: dict,
        dispatch: dict,
        recipients: list[str],
        executed_epoch: int,
        now: int,
        logical_at: int | None,
    ) -> str:
        reschedule_epoch = item.get("superseded_at_epoch")
        sent_before_reschedule = (
            reschedule_epoch is not None and executed_epoch < int(reschedule_epoch)
        )
        cid = f"correction:{item['show']}:{item['region']}:{item['action']}:r{item['revision']}"
        correction = self.state["corrections"].get(cid)
        if not correction:
            correction = {
                "id": cid,
                "rk": item["rk"],
                "show": item["show"],
                "region": item["region"],
                "regarding_action": item["action"],
                "old_revision": item["revision"],
                "status": "required",
                "reason": "old_revision_confirmed_sent",
                "send_timing": (
                    "sent_before_reschedule"
                    if sent_before_reschedule
                    else "sent_for_old_revision"
                ),
                "reschedule_recorded_utc": iso_utc(reschedule_epoch),
                "original_dispatch_ids": [],
                "recipients": [],
                "recipient_decisions": [],
                "first_original_sent_epoch": executed_epoch,
                "first_original_sent_utc": iso_utc(executed_epoch),
                "created_epoch": now,
                "created_utc": iso_utc(now),
                "logical_at": logical_at,
                "message": (
                    f"更正：{item['show']}/{item['region']} 的 {item['action']} 旧提醒"
                    f"(revision {item['revision']}) 涉及的安排已变更；请勿按旧时间行动，以最新编排和后续更正为准。"
                ),
                "active_dispatch_id": None,
            }
            self.state["corrections"][cid] = correction

        correction["first_original_sent_epoch"] = min(correction["first_original_sent_epoch"], executed_epoch)
        correction["first_original_sent_utc"] = iso_utc(correction["first_original_sent_epoch"])
        if dispatch["id"] not in correction["original_dispatch_ids"]:
            correction["original_dispatch_ids"].append(dispatch["id"])

        decisions = {d["recipient"]: d for d in correction["recipient_decisions"]}
        for recipient in recipients:
            if recipient not in correction["recipients"]:
                correction["recipients"].append(recipient)
            arrival = self.simulator.receipt_state(recipient, executed_epoch, now)
            decisions[recipient] = {
                "recipient": recipient,
                "partner_confirmed_original_send": True,
                # 合作方 sent.recipients 已证明该观众在实际发送名单中；
                # 仿真到达态只用于值班观察，不用请求记录替代事实。
                "known_original_receipt": True,
                "needs_correction": True,
                "basis": "partner_sent_callback_recipients",
                "simulated_arrival_state": arrival,
                "original_sent_utc": iso_utc(executed_epoch),
            }
        correction["recipient_decisions"] = [decisions[r] for r in correction["recipients"] if r in decisions]

        if correction.get("status") == "offered":
            correction["supplement_required"] = True
        return cid

    def _mark_new_item_gate_sent_locked(self, rk: str, action: str, old_revision: int, executed_epoch: int) -> None:
        region_state = self.state["regions"][rk]
        current_revision = region_state.get("current_revision")
        for item_id in region_state["item_order"]:
            candidate = self.state["items"][item_id]
            if candidate.get("action") != action or candidate.get("revision") != current_revision:
                continue
            blocked = not candidate.get("allow_replacement_after_send", False)
            candidate["redelivery_gate"] = {
                "state": "prior_sent_correction_required",
                "blocked": blocked,
                "old_revision": old_revision,
                "actual_epoch": executed_epoch,
                "actual_utc": iso_utc(executed_epoch),
                "message": "旧项已确认实际发出；已生成观众更正，不能把新公告当作旧公告未出现的证明",
            }

    def _release_gates_locked(self, rk: str, action: str, old_revision: int, new_state: str) -> None:
        region_state = self.state["regions"][rk]
        for item_id in region_state["item_order"]:
            candidate = self.state["items"][item_id]
            gate = candidate.get("redelivery_gate") or {}
            if candidate.get("action") == action and gate.get("old_revision") == old_revision:
                candidate["redelivery_gate"] = {"state": new_state, "blocked": False, "old_revision": old_revision}

    def _correction_callback(self, record: dict, *, now: int | float | None, logical_at: int | None) -> dict:
        cid = record.get("correction_id")
        correction = self.state["corrections"].get(cid)
        if not correction:
            raise NotFoundError(f"未找到更正 {cid}")
        with self.lock:
            current_time = self._now(now)
            state_value = record.get("state")
            dispatch = None
            if record.get("dispatch_id"):
                dispatch = self.state["dispatches"].get(record["dispatch_id"])
            elif correction.get("active_dispatch_id"):
                dispatch = self.state["dispatches"][correction["active_dispatch_id"]]

            if dispatch:
                dispatch.setdefault("reports", []).append({"state": state_value, "logical_at": logical_at})
                dispatch["last_report"] = state_value
            if state_value == "sent":
                executed_raw = record.get("executed_at")
                executed_epoch = parse_iso_utc(executed_raw) if isinstance(executed_raw, str) else current_time
                correction["status"] = "sent"
                correction["sent_epoch"] = executed_epoch
                correction["sent_utc"] = iso_utc(executed_epoch)
                correction["sent_recipients"] = record.get("recipients", correction.get("recipients", []))
                if dispatch:
                    dispatch["state"] = "sent"
                    dispatch["executed_epoch"] = executed_epoch
            elif state_value == "withdrawn":
                correction["status"] = "withdrawn"
                if dispatch:
                    dispatch["state"] = "withdrawn"
            else:
                correction["status"] = "unknown"
            self._event("correction_callback", correction_id=cid, state=state_value, logical_at=logical_at)
            self._save_locked()
            return {"accepted": True, "correction_id": cid, "state": state_value}

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_regions(self, *, now: int | float | None = None) -> dict:
        with self.lock:
            current_time = self._now(now)
            self._evaluate_all_locked(current_time)
            items = []
            for rk, region in sorted(self.state["regions"].items(), key=lambda kv: (kv[1].get("show", ""), kv[1].get("region", ""))):
                current = region.get("current_revision")
                counts: dict[str, int] = {}
                for item_id in region["item_order"]:
                    item = self.state["items"][item_id]
                    if item.get("revision") != current:
                        continue
                    phase = item.get("phase", "unknown")
                    counts[phase] = counts.get(phase, 0) + 1
                items.append(
                    {
                        "show": region["show"],
                        "region": region["region"],
                        "current_revision": current,
                        "tz": region.get("tz"),
                        "region_open": region.get("publication", {}).get("open", False),
                        "phase_counts": counts,
                    }
                )
            self._save_locked()
            return {"now_utc": iso_utc(current_time), "regions": items}

    def inspect(self, show: str, region: str, *, now: int | float | None = None) -> dict:
        rk = region_key(show, region)
        with self.lock:
            region_state = self.state["regions"].get(rk)
            if not region_state:
                raise NotFoundError(f"未找到 {show}/{region}")
            current_time = self._now(now)
            self._evaluate_region_locked(rk, current_time)

            current_revision = region_state.get("current_revision")
            actions: list[dict] = []
            history: list[dict] = []
            for item_id in region_state["item_order"]:
                item = self.state["items"][item_id]
                view = self._item_view_locked(item, current_time)
                if item.get("revision") == current_revision:
                    actions.append(view)
                else:
                    history.append(view)

            corrections = [
                self._correction_view_locked(c)
                for c in self.state["corrections"].values()
                if c.get("rk") == rk
            ]

            unresolved = self._unresolved_locked(rk)
            planning_issues = [
                ticket
                for ticket in self.state.get("replan_requests", [])
                if ticket.get("rk") == rk or (ticket.get("show") == show and ticket.get("region") == region)
            ]
            events = [e for e in self.state["events"] if e.get("rk") == rk or (e.get("show") == show and e.get("region") == region)]
            self._save_locked()
            return {
                "show": show,
                "region": region,
                "queried_utc": iso_utc(current_time),
                "tz": region_state.get("tz"),
                "current_revision": current_revision,
                "region_open": region_state.get("publication", {}).get("open", False),
                "publication": region_state.get("publication", {}),
                "actions": sorted(actions, key=lambda x: x.get("scheduled_epoch", 0)),
                "previous_attempts": sorted(history, key=lambda x: (x.get("action", ""), x.get("revision", 0))),
                "corrections": corrections,
                "unresolved": unresolved,
                "planning_issues": planning_issues,
                "recent_events": events[-30:],
                "receiver_directory": self.simulator.snapshot(),
            }

    def _item_view_locked(self, item: dict, now: int | float | None = None) -> dict:
        dispatch = self._active_dispatch_locked(item)
        previous = []
        if item.get("revision") == self.state["regions"][item["rk"]].get("current_revision"):
            for other_id in self.state["regions"][item["rk"]]["item_order"]:
                other = self.state["items"][other_id]
                if other.get("action") == item.get("action") and other.get("revision") != item.get("revision"):
                    previous.append(
                        {
                            "revision": other.get("revision"),
                            "phase": other.get("phase"),
                            "scheduled_utc": other.get("scheduled_utc"),
                            "actual": other.get("actual"),
                            "disposition": other.get("disposition"),
                            "active_dispatch_id": other.get("active_dispatch_id"),
                        }
                    )

        audience_report = None
        if item.get("phase") in {"ready", "catch_up_ready", "held", "expired_unconfirmed"}:
            try:
                audience_report = self._audience_for_item_locked(
                    self.state["regions"][item["rk"]], item, now
                )
            except Exception:  # noqa: BLE001 - 查询不应因演示人群失败
                audience_report = None

        return {
            "action": item.get("action"),
            "revision": item.get("revision"),
            "kind": item.get("kind"),
            "state": item.get("phase"),
            "status": item.get("phase"),
            "completed": bool(item.get("actual")),
            "lifecycle": item.get("lifecycle"),
            "should_execute_at_utc": item.get("scheduled_utc"),
            "scheduled_utc": item.get("scheduled_utc"),
            "executed_at_utc": (item.get("actual") or {}).get("first_executed_utc"),
            "scheduled_local": item.get("local"),
            "fold": item.get("fold"),
            "window_seconds": item.get("window_seconds"),
            "deadline_utc": item.get("deadline_utc"),
            "miss_policy": item.get("miss"),
            "missed": bool(item.get("missed")),
            "needs": item.get("needs", []),
            "requires_region_open": item.get("requires_region_open"),
            "missing_preconditions": item.get("missing_preconditions", []),
            "satisfied_preconditions": item.get("satisfied_preconditions", []),
            "redelivery_gate": item.get("redelivery_gate"),
            "actual_execution": item.get("actual"),
            "disposition": item.get("disposition"),
            "active_dispatch": self._dispatch_payload_locked(dispatch) if dispatch else None,
            "audience_recomputation": audience_report,
            "previous_attempts": previous,
            "timeliness": item.get("timeliness"),
            "pull_count": item.get("pull_count"),
        }

    def _correction_view_locked(self, correction: dict) -> dict:
        return {
            "id": correction.get("id"),
            "regarding_action": correction.get("regarding_action"),
            "old_revision": correction.get("old_revision"),
            "status": correction.get("status"),
            "reason": correction.get("reason"),
            "send_timing": correction.get("send_timing"),
            "reschedule_recorded_utc": correction.get("reschedule_recorded_utc"),
            "recipients_who_need_correction": correction.get("recipients", []),
            "audience_needing_correction": correction.get("recipients", []),
            "recipients": correction.get("recipients", []),
            "recipient_decisions": correction.get("recipient_decisions", []),
            "original_sent_utc": correction.get("first_original_sent_utc"),
            "correction_sent_utc": correction.get("sent_utc"),
            "message": correction.get("message"),
            "active_dispatch_id": correction.get("active_dispatch_id"),
        }

    def _unresolved_locked(self, rk: str) -> list[dict]:
        result: list[dict] = []
        region_state = self.state["regions"][rk]
        for dispatch in self.state["dispatches"].values():
            if dispatch.get("rk") != rk:
                continue
            if dispatch.get("state") == "withdraw_requested":
                result.append(
                    {
                        "code": "withdrawal_unconfirmed",
                        "dispatch_id": dispatch["id"],
                        "action": dispatch.get("action"),
                        "revision": dispatch.get("revision"),
                        "provider": dispatch.get("provider"),
                        "requested_utc": dispatch.get("withdraw_requested_utc"),
                        "respond_by_utc": iso_utc(dispatch.get("respond_by_epoch")),
                        "message": "合作方已持有旧项并被请求撤回；在确认 withdrawn 或 sent 前，不能按未执行重排",
                    }
                )
            elif dispatch.get("state") == "leased":
                item = self.state["items"].get(dispatch.get("item_id"), {})
                if item.get("phase") == "expired_unconfirmed":
                    result.append(
                        {
                            "code": "expired_unconfirmed",
                            "dispatch_id": dispatch["id"],
                            "action": dispatch.get("action"),
                            "revision": dispatch.get("revision"),
                            "provider": dispatch.get("provider"),
                            "message": "窗口已过且无 sent/withdrawn 确认；不能盖章为执行完成",
                        }
                    )
                elif item.get("phase") == "held_catch_up":
                    result.append(
                        {
                            "code": "catch_up_receipt_pending",
                            "severity": "info",
                            "dispatch_id": dispatch["id"],
                            "action": dispatch.get("action"),
                            "revision": dispatch.get("revision"),
                            "provider": dispatch.get("provider"),
                            "message": "catch_up 项已被领取且已过窗口，但尚无 sent 回执；仍不能视为执行完成",
                        }
                    )

        for item_id in region_state["item_order"]:
            item = self.state["items"][item_id]
            if item.get("phase") in {"blocked_old_disposition", "missed_waiting_precondition", "waiting_precondition"}:
                result.append(
                    {
                        "code": item["phase"],
                        "action": item.get("action"),
                        "revision": item.get("revision"),
                        "missing_preconditions": item.get("missing_preconditions", []),
                        "message": "下一步仍被前置事实或旧通知处置约束",
                    }
                )
        return result


# 常见接口命名的语义化别名/兼容封装，方便不同调用方和测试发现。
Middleware.schedule = Middleware.schedule_plan  # type: ignore[attr-defined]
Middleware.callback = Middleware.provider_callback  # type: ignore[attr-defined]


def _pull_compat(self, provider_or_record, show=None, region=None, **kwargs):
    """兼容 pull(provider, show, region) 与 pull(record) 两种调用。"""
    if isinstance(provider_or_record, dict):
        record = provider_or_record
        return self.provider_pull(
            record["provider"],
            record["show"],
            record["region"],
            logical_at=record.get("logical_at"),
            **kwargs,
        )
    return self.provider_pull(provider_or_record, show, region, **kwargs)


def _inspect_compat(self, show_or_record, region=None, **kwargs):
    """兼容 inspect(show, region) 与 inspect(record) 两种调用。"""
    if isinstance(show_or_record, dict):
        return Middleware.inspect(self, show_or_record["show"], show_or_record["region"], **kwargs)
    return Middleware.inspect(self, show_or_record, region, **kwargs)


Middleware.pull = _pull_compat  # type: ignore[attr-defined]
Middleware.get_region = _inspect_compat  # type: ignore[attr-defined]
Middleware.query_region = _inspect_compat  # type: ignore[attr-defined]
Middleware.inspect_record = lambda self, record, **kw: Middleware.inspect(self, record["show"], record["region"], **kw)  # type: ignore[attr-defined]
