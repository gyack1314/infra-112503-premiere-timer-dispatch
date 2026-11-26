"""``python -m delayed_middleware`` 入口。"""

from __future__ import annotations

import sys


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "scenario":
        from .scenario import main as scenario_main

        return scenario_main(sys.argv[2:])
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        from .server import main as server_main

        return server_main(sys.argv[2:])
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        from .demo import main as demo_main

        return demo_main(sys.argv[2:])

    print("用法：python -m delayed_middleware [scenario|server|demo] ...")
    print("  scenario examples/xxx.jsonl [--show-pulls] [--json]")
    print("  server --host 127.0.0.1 --port 8080 --state state.json")
    print("  demo")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
