"""HTTP 服务便捷入口：``python3 app.py``。

环境变量：
- HOST，默认 127.0.0.1
- PORT，默认 8080
- STATE_FILE，默认 ./delayed-middleware-state.json
"""

from __future__ import annotations

import os

from delayed_middleware.server import build_server


if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8080"))
    state_file = os.environ.get("STATE_FILE", "delayed-middleware-state.json")
    server = build_server(host, port, state_file)
    print(f"delayed program middleware on http://{host}:{port} (state={state_file})")
    server.serve_forever()
