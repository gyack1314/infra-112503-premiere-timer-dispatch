"""端到端和关键不变量测试。"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from delayed_middleware.engine import (
    Middleware,
    PlanValidationError,
)
from delayed_middleware.scenario import run_path
from delayed_middleware.timeutil import parse_iso_utc, resolve_local_time


ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples"
DRILLS = ROOT / "drills"
T0 = parse_iso_utc("2026-12-01T09:00:00Z")
T_NEW = parse_iso_utc("2026-12-01T10:00:00Z")


def plan_record(revision=1, scheduled="2026-12-01T09:00:00", **action_overrides):
    action = {
        "action": "notice",
        "local": scheduled,
        "window": 180,
        "miss": "catch_up",
    }
    action.update(action_overrides)
    return {
        "show": "S",
        "region": "R",
        "revision": revision,
        "tz": "UTC",
        "actions": [action],
    }


class TimeContractTests(unittest.TestCase):
    def test_fold_zero_and_one_map_to_first_and_second_instants(self):
        first = resolve_local_time("2026-11-01T01:30:00", "America/New_York", 0)
        second = resolve_local_time("2026-11-01T01:30:00", "America/New_York", 1)
        self.assertEqual(first.utc, "2026-11-01T05:30:00Z")
        self.assertEqual(second.utc, "2026-11-01T06:30:00Z")
        self.assertEqual(second.epoch - first.epoch, 3600)

    def test_ambiguous_time_requires_explicit_fold(self):
        with self.assertRaisesRegex(Exception, "fold"):
            resolve_local_time("2026-11-01T01:30:00", "America/New_York")

    def test_nonexistent_gap_time_is_rejected_for_replan(self):
        engine = Middleware()
        record = {
            "show": "S",
            "region": "R",
            "revision": 1,
            "tz": "America/New_York",
            "actions": [
                {"action": "notice", "local": "2026-03-08T02:30:00", "window": 60, "miss": "skip"}
            ],
        }
        with self.assertRaises(PlanValidationError) as ctx:
            engine.schedule_plan(record)
        self.assertTrue(any(e["code"] == "bad_local_time" for e in ctx.exception.errors))
        report = engine.inspect("S", "R") if "S\u001fR" in engine.state["regions"] else None
        # 整个无法解析的预约不安装为当前版本，但留下退回编排修改的票据。
        self.assertIsNone(report)
        self.assertEqual(engine.state["replan_requests"][0]["status"], "needs_editor_change")


class ExampleScenarioTests(unittest.TestCase):
    def test_fallback_restart_skips_strict_and_pulls_catch_up(self):
        result = run_path(EXAMPLES / "fall-back-and-missed-window.jsonl")
        self.assertEqual(result["errors"], [])
        pull = next(
            r["outcome"]["result"]
            for r in result["responses"]
            if r["kind"] == "pull"
        )
        self.assertEqual([d["action"] for d in pull["deliveries"]], ["room"])
        self.assertTrue(pull["deliveries"][0]["late"])

        report = result["inspections"][-1]["report"]
        states = {a["action"]: a["state"] for a in report["actions"]}
        self.assertEqual(states["remind"], "skipped_missed")
        self.assertEqual(states["room"], "held_catch_up")
        self.assertIsNone(report["actions"][1]["actual_execution"])

    def test_moved_before_callback_withdraws_then_detects_correction_audience(self):
        result = run_path(EXAMPLES / "moved-before-callback.jsonl")
        self.assertEqual(result["errors"], [])

        pulls = [r["outcome"]["result"] for r in result["responses"] if r["kind"] == "pull"]
        self.assertEqual([d["action"] for d in pulls[0]["deliveries"]], ["announce"])
        self.assertEqual(pulls[0]["deliveries"][0]["revision"], 1)

        self.assertEqual(pulls[1]["deliveries"], [])
        self.assertEqual([w["action"] for w in pulls[1]["withdrawals"]], ["announce"])
        self.assertEqual(pulls[1]["withdrawals"][0]["revision"], 1)

        report = result["inspections"][-1]["report"]
        by_action = {a["action"]: a for a in report["actions"]}
        self.assertEqual(by_action["announce"]["state"], "skipped_missed")
        self.assertIsNotNone(by_action["announce"]["previous_attempts"][0]["actual"])

        correction = report["corrections"][0]
        self.assertEqual(correction["recipients_who_need_correction"], ["a", "c"])
        self.assertTrue(all(d["needs_correction"] for d in correction["recipient_decisions"]))

        join_missing_codes = {m["code"] for m in by_action["join"]["missing_preconditions"]}
        self.assertIn("region_open", join_missing_codes)
        satisfied = {p["name"]: p for p in by_action["join"]["satisfied_preconditions"]}
        self.assertEqual(satisfied["announce"]["revision"], 1)
        self.assertFalse(satisfied["announce"]["revision_matches"])


    def test_ordered_room_remind_interaction_example(self):
        result = run_path(DRILLS / "open-depend-and-interact.jsonl")
        self.assertEqual(result["errors"], [])
        pulls = [r["outcome"]["result"] for r in result["responses"] if r["kind"] == "pull"]
        delivery_order = [[d["action"] for d in pull["deliveries"]] for pull in pulls]
        self.assertEqual(delivery_order, [["room"], ["remind"], ["join"]])
        report = result["inspections"][-1]["report"]
        self.assertTrue(report["region_open"])
        self.assertEqual([a["state"] for a in report["actions"]], ["sent", "sent", "sent"])


class RescheduleDispatchTests(unittest.TestCase):
    def test_unclaimed_old_item_is_recomputed_without_withdrawal(self):
        eleven = parse_iso_utc("2026-12-01T11:00:00Z")
        engine = Middleware()
        engine.schedule_plan(plan_record(1), now=T0 - 100)
        engine.schedule_plan(plan_record(2, "2026-12-01T11:00:00"), now=T0 - 50)
        pull = engine.provider_pull("p", "S", "R", now=eleven + 10)
        self.assertEqual([d["revision"] for d in pull["deliveries"]], [2])
        self.assertEqual(pull["withdrawals"], [])

    def test_unknown_old_dispatch_blocks_new_until_withdrawn(self):
        engine = Middleware()
        engine.schedule_plan(plan_record(1, window=600), now=T0 - 10)
        first = engine.provider_pull("partner", "S", "R", now=T0 + 5)
        old_id = first["deliveries"][0]["id"]

        revised = plan_record(2, "2026-12-01T09:10:00", window=600)
        engine.schedule_plan(revised, now=T0 + 20)
        engine.provider_callback(
            {
                "provider": "partner",
                "show": "S",
                "region": "R",
                "action": "notice",
                "revision": 1,
                "state": "unknown",
            },
            now=T0 + 21,
        )

        blocked = engine.provider_pull("partner", "S", "R", now=T0 + 600)
        self.assertEqual(blocked["deliveries"], [])
        self.assertEqual(blocked["withdrawals"][0]["id"], old_id)

        engine.provider_callback(
            {
                "provider": "partner",
                "show": "S",
                "region": "R",
                "action": "notice",
                "revision": 1,
                "state": "withdrawn",
            },
            now=T0 + 610,
        )
        released = engine.provider_pull("partner", "S", "R", now=T0 + 611)
        self.assertEqual([d["revision"] for d in released["deliveries"]], [2])

    def test_pull_request_is_not_execution_fact_and_expired_item_not_completed(self):
        engine = Middleware()
        engine.schedule_plan(plan_record(1, window=60, miss="skip"), now=T0 - 1)
        pulled = engine.provider_pull("p", "S", "R", now=T0 + 1)
        dispatch_id = pulled["deliveries"][0]["id"]
        report = engine.inspect("S", "R", now=T0 + 120)
        action = report["actions"][0]
        self.assertIsNone(action["actual_execution"])
        self.assertNotEqual(action["state"], "sent")
        self.assertTrue(any(u["dispatch_id"] == dispatch_id for u in report["unresolved"]))

    def test_sent_callback_without_executed_at_uses_last_clock(self):
        engine = Middleware()
        engine.schedule_plan(plan_record(1, window=600), now=T0 - 1)
        engine.set_clock("2026-12-01T09:00:30Z")
        engine.provider_pull("p", "S", "R")
        result = engine.provider_callback(
            {
                "provider": "p",
                "show": "S",
                "region": "R",
                "action": "notice",
                "revision": 1,
                "state": "sent",
                "recipients": ["a"],
            }
        )
        self.assertEqual(result["actual_executed_utc"], "2026-12-01T09:00:30Z")


    def test_confirmed_old_send_creates_claimable_and_reportable_correction(self):
        engine = Middleware()
        engine.schedule_plan(plan_record(1, window=600), now=T0 - 1)
        engine.provider_pull("p", "S", "R", now=T0 + 1)
        engine.schedule_plan(plan_record(2, "2026-12-01T11:00:00", window=600), now=T0 + 2)
        engine.provider_callback(
            {
                "provider": "p", "show": "S", "region": "R", "action": "notice", "revision": 1,
                "state": "sent", "recipients": ["a", "c"], "executed_at": "2026-12-01T09:00:05Z",
            }
        )
        pulled = engine.provider_pull("p", "S", "R", now=T0 + 3)
        self.assertEqual(len(pulled["correction_deliveries"]), 1)
        correction_dispatch = pulled["correction_deliveries"][0]
        self.assertEqual(correction_dispatch["recipients"], ["a", "c"])

        result = engine.provider_callback(
            {
                "correction_id": correction_dispatch["correction_id"],
                "dispatch_id": correction_dispatch["id"],
                "state": "sent",
                "recipients": ["a", "c"],
                "executed_at": "2026-12-01T09:05:00Z",
            }
        )
        self.assertEqual(result["state"], "sent")
        report = engine.inspect("S", "R", now=T0 + 4)
        self.assertEqual(report["corrections"][0]["status"], "sent")


class DependencyRegionAndIsolationTests(unittest.TestCase):
    def test_dependency_and_publication_gate_interaction_delivery(self):
        engine = Middleware()
        record = {
            "show": "Premiere",
            "region": "east",
            "revision": 1,
            "tz": "UTC",
            "actions": [
                {"action": "room_open", "local": "2026-12-01T10:00:00", "window": 600, "miss": "catch_up"},
                {
                    "action": "join",
                    "local": "2026-12-01T10:01:00",
                    "window": 600,
                    "miss": "catch_up",
                    "needs": ["room_open"],
                    "requires_region_open": True,
                },
            ],
        }
        engine.schedule_plan(record, now=T0)
        first_pull = engine.provider_pull("p", "Premiere", "east", now=parse_iso_utc("2026-12-01T10:01:05Z"))
        self.assertEqual([d["action"] for d in first_pull["deliveries"]], ["room_open"])

        engine.receive_publication(
            {"show": "Premiere", "region": "east", "status": "open", "published_at": "2026-12-01T10:01:10Z"}
        )
        before_receipt = engine.provider_pull("p", "Premiere", "east", now=parse_iso_utc("2026-12-01T10:01:20Z"))
        self.assertEqual(before_receipt["deliveries"], [])

        engine.provider_callback(
            {
                "provider": "p",
                "show": "Premiere",
                "region": "east",
                "action": "room_open",
                "revision": 1,
                "state": "sent",
                "executed_at": "2026-12-01T10:00:40Z",
            }
        )
        after_receipt = engine.provider_pull("p", "Premiere", "east", now=parse_iso_utc("2026-12-01T10:01:30Z"))
        self.assertEqual([d["action"] for d in after_receipt["deliveries"]], ["join"])

    def test_external_publication_fact_can_satisfy_need(self):
        engine = Middleware()
        record = {
            "show": "S",
            "region": "R",
            "revision": 1,
            "tz": "UTC",
            "actions": [
                {
                    "action": "follow_up",
                    "local": "2026-12-01T10:00:00",
                    "window": 600,
                    "miss": "catch_up",
                    "needs": ["trailer_published"],
                    "requires_region_open": False,
                }
            ],
        }
        due = parse_iso_utc("2026-12-01T10:00:05Z")
        engine.schedule_plan(record, now=due - 5)
        self.assertEqual(engine.provider_pull("p", "S", "R", now=due)["deliveries"], [])
        engine.record_fact(
            {
                "show": "S",
                "region": "R",
                "fact": "trailer_published",
                "published_at": "2026-12-01T09:59:00Z",
            }
        )
        pulled = engine.provider_pull("p", "S", "R", now=due + 1)
        self.assertEqual([d["action"] for d in pulled["deliveries"]], ["follow_up"])

    def test_regions_are_independent(self):
        engine = Middleware()
        base = {
            "revision": 1,
            "tz": "UTC",
            "actions": [{"action": "notice", "local": "2026-12-01T09:00:00", "window": 60, "miss": "skip"}],
        }
        engine.schedule_plan({**base, "show": "Show", "region": "ok"}, now=T0)
        with self.assertRaises(PlanValidationError):
            engine.schedule_plan(
                {
                    **base,
                    "show": "Show",
                    "region": "bad",
                    "tz": "Not/AZone",
                }
            )
        pull = engine.provider_pull("p", "Show", "ok", now=T0 + 5)
        self.assertEqual(len(pull["deliveries"]), 1)
        self.assertEqual(engine.inspect("Show", "ok")["region"], "ok")

        # 一个地区的旧合作单阻塞时，同栏目其他地区仍可独立领取。
        engine.schedule_plan({**base, "show": "Show", "region": "blocked"}, now=T0 - 1)
        held = engine.provider_pull("p", "Show", "blocked", now=T0 + 1)
        engine.schedule_plan(
            {**base, "show": "Show", "region": "blocked", "revision": 2,
             "actions": [{"action": "notice", "local": "2026-12-01T10:00:00", "window": 60, "miss": "skip"}]},
            now=T0 + 2,
        )
        engine.provider_callback(
            {"provider": "p", "show": "Show", "region": "blocked", "action": "notice",
             "revision": 1, "state": "unknown"},
            now=T0 + 3,
        )
        other = engine.provider_pull("p", "Show", "ok", now=T0 + 6)
        self.assertEqual(other["deliveries"], [])  # ok region's only item already held
        engine.schedule_plan({**base, "show": "Show", "region": "free"}, now=T0 - 1)
        free_pull = engine.provider_pull("p", "Show", "free", now=T0 + 6)
        self.assertEqual([d["region"] for d in free_pull["deliveries"]], ["free"])
        blocked_pull = engine.provider_pull("p", "Show", "blocked", now=parse_iso_utc("2026-12-01T10:00:05Z"))
        self.assertEqual(blocked_pull["deliveries"], [])
        self.assertTrue(blocked_pull["withdrawals"])


class PersistenceTests(unittest.TestCase):
    def test_restart_reloads_durable_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            engine = Middleware(path)
            engine.schedule_plan(plan_record(), now=T0 - 1)
            engine.provider_pull("p", "S", "R", now=T0 + 1)

            reloaded = Middleware(path)
            report = reloaded.inspect("S", "R", now=T0 + 2)
            self.assertEqual(report["actions"][0]["state"], "held")
            restart = reloaded.restart()
            self.assertTrue(restart["durable_state"])
            self.assertEqual(reloaded.inspect("S", "R", now=T0 + 3)["actions"][0]["state"], "held")


if __name__ == "__main__":
    unittest.main()
