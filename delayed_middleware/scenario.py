"""按仓库 JSONL 演练契约驱动中间件。"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from scenario_reader import iter_messages

from .engine import (
    CallbackConflict,
    Middleware,
    NotFoundError,
    PlanValidationError,
    RevisionConflict,
)
from .reporting import print_inspect


DEFAULT_KINDS = {
    "plan",
    "clock",
    "pull",
    "callback",
    "restart",
    "inspect",
    "publish",
    "publication",
    "region_fact",
    "fact",
    "audience",
}


def supported_kinds_for(path: Path) -> set[str]:
    registry = path.parent / "message_types.json"
    kinds = set(DEFAULT_KINDS)
    if registry.exists():
        try:
            kinds.update(json.loads(registry.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            pass
    return kinds


def handle_message(engine: Middleware, row: dict) -> dict[str, Any]:
    kind = row["kind"]
    record = row["record"]
    at = row["at"]

    if kind == "plan":
        return {"kind": "plan", "result": engine.schedule_plan(record, logical_at=at)}
    if kind == "clock":
        return {"kind": "clock", "result": engine.set_clock(record["utc"], logical_at=at)}
    if kind == "pull":
        return {
            "kind": "pull",
            "result": engine.provider_pull(
                record["provider"], record["show"], record["region"], logical_at=at
            ),
        }
    if kind == "callback":
        return {"kind": "callback", "result": engine.provider_callback(record, logical_at=at)}
    if kind == "restart":
        return {"kind": "restart", "result": engine.restart(logical_at=at)}
    if kind == "inspect":
        report = engine.inspect(record["show"], record["region"])
        return {"kind": "inspect", "report": report}
    if kind in {"publish", "publication", "region_fact"}:
        return {"kind": kind, "result": engine.receive_publication(record, logical_at=at)}
    if kind == "fact":
        return {"kind": kind, "result": engine.record_fact(record, logical_at=at)}
    if kind == "audience":
        return {
            "kind": kind,
            "result": engine.set_audience(record["show"], record["region"], record["audience"]),
        }
    raise ValueError(f"未处理的消息类型 {kind}")


def run_path(path: str | Path, *, state_path: str | Path | None = None) -> dict:
    path = Path(path)
    kinds = supported_kinds_for(path)
    inspections: list[dict] = []
    responses: list[dict] = []
    errors: list[dict] = []

    temporary_directory = None
    if state_path is None:
        temporary_directory = tempfile.TemporaryDirectory(prefix="delayed-middleware-")
        state_path = Path(temporary_directory.name) / "state.json"

    engine = Middleware(state_path=state_path, wall_clock_fallback=False)
    try:
        for row in iter_messages(path, kinds):
            try:
                outcome = handle_message(engine, row)
                responses.append({"at": row["at"], "kind": row["kind"], "outcome": outcome})
                if row["kind"] == "inspect":
                    inspections.append({"at": row["at"], "report": outcome["report"]})
            except (PlanValidationError, RevisionConflict, CallbackConflict, NotFoundError, KeyError, ValueError) as exc:
                error = {
                    "at": row["at"],
                    "kind": row["kind"],
                    "error": str(exc),
                    "errors": getattr(exc, "errors", None),
                }
                errors.append(error)
                responses.append({"at": row["at"], "kind": row["kind"], "error": error})
        return {
            "path": str(path),
            "inspections": inspections,
            "responses": responses,
            "errors": errors,
            "state": engine.state,
        }
    finally:
        if temporary_directory is not None:
            temporary_directory.cleanup()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行节目延时消息 JSONL 演练")
    parser.add_argument("scenarios", nargs="+", type=Path, help="一个或多个 .jsonl 演练文件")
    parser.add_argument("--json", action="store_true", help="输出完整 inspect JSON")
    parser.add_argument("--show-pulls", action="store_true", help="同时输出每次 pull 的结果 JSON")
    args = parser.parse_args(argv)

    exit_code = 0
    json_reports: list[dict] = []
    for scenario in args.scenarios:
        result = run_path(scenario)
        if args.json:
            json_reports.extend(item["report"] for item in result["inspections"])
            if result["errors"]:
                exit_code = 1
            continue

        print(f"\n=== {scenario} ===")
        if result["errors"]:
            exit_code = 1
            for error in result["errors"]:
                print(f"[错误 at={error['at']} {error['kind']}] {error['error']}")
        if args.show_pulls:
            for response in result["responses"]:
                if response["kind"] == "pull":
                    print(json.dumps(response["outcome"]["result"], ensure_ascii=False, indent=2))
        for inspection in result["inspections"]:
            print_inspect(inspection["report"], as_json=False)

    if args.json:
        json.dump(json_reports, sys.stdout, ensure_ascii=False, indent=2)
        print()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
