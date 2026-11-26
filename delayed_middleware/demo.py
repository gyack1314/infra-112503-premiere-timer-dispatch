"""本地演练入口：运行仓库自带的两个关键场景。"""

from __future__ import annotations

from pathlib import Path

from .scenario import main as scenario_main


def main(argv: list[str] | None = None) -> int:
    root = Path(__file__).resolve().parent.parent
    files = [
        root / "examples" / "fall-back-and-missed-window.jsonl",
        root / "examples" / "moved-before-callback.jsonl",
        root / "drills" / "open-depend-and-interact.jsonl",
    ]
    return scenario_main([str(path) for path in files])


if __name__ == "__main__":
    raise SystemExit(main())
