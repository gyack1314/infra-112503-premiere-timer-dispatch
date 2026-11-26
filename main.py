"""仓库根目录统一入口。

示例：
    python3 main.py drill
    python3 main.py scenario examples/moved-before-callback.jsonl --show-pulls
    python3 main.py query --show D --region s --state delayed-middleware-state.json
    python3 main.py serve --port 8080
"""

from __future__ import annotations

import sys


def _query(argv: list[str]) -> int:
    import argparse
    import json

    from delayed_middleware.engine import Middleware, NotFoundError
    from delayed_middleware.reporting import format_inspect

    parser = argparse.ArgumentParser(description="查询节目地区实时状态")
    parser.add_argument("--show", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--state", default="delayed-middleware-state.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    middleware = Middleware(args.state, wall_clock_fallback=True)
    try:
        report = middleware.inspect(args.show, args.region)
    except NotFoundError as exc:
        print(f"未找到地区：{exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_inspect(report))
    return 0


def main() -> int:
    if len(sys.argv) == 1 or sys.argv[1] in {"drill", "demo"}:
        from delayed_middleware.demo import main as demo_main

        return demo_main(sys.argv[2:])
    if sys.argv[1] == "scenario":
        from delayed_middleware.scenario import main as scenario_main

        return scenario_main(sys.argv[2:])
    if sys.argv[1] in {"query", "inspect"}:
        return _query(sys.argv[2:])
    if sys.argv[1] in {"serve", "server"}:
        from delayed_middleware.server import main as server_main

        return server_main(sys.argv[2:])
    print(__doc__)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
