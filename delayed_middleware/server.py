"""仅用标准库实现的 JSON/HTTP 接口与值班只读页面。"""

from __future__ import annotations

import json
import threading
from html import escape as html_escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .engine import (
    CallbackConflict,
    Middleware,
    NotFoundError,
    PlanValidationError,
    RevisionConflict,
)
from .reporting import format_inspect


def create_handler(engine: Middleware):
    class Handler(BaseHTTPRequestHandler):
        server_version = "DelayedProgramMiddleware/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 保持测试输出干净
            return

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, status: int, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"请求体不是合法 JSON: {exc}") from exc
            if not isinstance(data, dict):
                raise ValueError("请求体必须是 JSON 对象")
            return data

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            try:
                if path == "/health":
                    self._send_json(200, {"ok": True})
                    return
                if path in {"/v1/regions", "/regions", "/region"}:
                    show, region = query.get("show"), query.get("region")
                    if show and region:
                        self._send_json(200, engine.inspect(show, region))
                    else:
                        self._send_json(200, engine.list_regions())
                    return
                if path.startswith("/v1/regions/") or path.startswith("/regions/"):
                    prefix = "/v1/regions/" if path.startswith("/v1/regions/") else "/regions/"
                    rest = unquote(path[len(prefix) :])
                    if "/" not in rest:
                        raise NotFoundError("路径应为 /regions/{show}/{region}")
                    show_raw, region_raw = rest.split("/", 1)
                    self._send_json(200, engine.inspect(unquote(show_raw), unquote(region_raw)))
                    return
                if path in {"/v1/inspect", "/inspect"}:
                    self._send_json(200, engine.inspect(query["show"], query["region"]))
                    return
                if path.startswith("/v1/shows/") and "/regions/" in path:
                    rest = path[len("/v1/shows/") :]
                    show_raw, region_raw = rest.split("/regions/", 1)
                    self._send_json(200, engine.inspect(unquote(show_raw), unquote(region_raw)))
                    return
                if path == "/dashboard" or path == "/":
                    self._dashboard(query)
                    return
                if path == "/v1":
                    self._send_json(200, self._api_index())
                    return
                self._send_json(404, {"error": "not_found", "path": path})
            except NotFoundError as exc:
                self._send_json(404, {"error": "not_found", "message": str(exc)})
            except KeyError as exc:
                self._send_json(400, {"error": "missing_field", "message": f"缺少字段 {exc}"})
            except Exception as exc:  # noqa: BLE001 - HTTP 边界
                self._send_json(500, {"error": "internal_error", "message": str(exc)})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")
            try:
                payload = self._read_json()
                if path in {"/v1/plans", "/v1/reservations", "/plans", "/schedule", "/reserve"}:
                    result = engine.schedule_plan(payload)
                    self._send_json(202 if not result.get("idempotent") else 200, result)
                elif path in {"/v1/reschedules", "/reschedule", "/reschedules"}:
                    result = engine.reschedule(payload)
                    self._send_json(202, result)
                elif path in {"/v1/pull", "/pull", "/claim", "/provider/pull"}:
                    result = engine.provider_pull(
                        payload["provider"], payload["show"], payload["region"]
                    )
                    self._send_json(200, result)
                elif path in {"/v1/callbacks", "/callback", "/callbacks", "/provider/callback"}:
                    result = engine.provider_callback(payload)
                    self._send_json(200, result)
                elif path in {"/v1/publications", "/v1/region-facts", "/publications", "/publish"}:
                    result = engine.receive_publication(payload)
                    self._send_json(202, result)
                elif path in {"/v1/facts", "/facts"}:
                    result = engine.record_fact(payload)
                    self._send_json(202, result)
                elif path in {"/v1/audience", "/audience"}:
                    result = engine.set_audience(
                        payload["show"], payload["region"], payload["audience"]
                    )
                    self._send_json(200, result)
                elif path in {"/v1/clock", "/clock"}:
                    result = engine.set_clock(payload["utc"])
                    self._send_json(200, result)
                elif path in {"/v1/restart", "/restart"}:
                    result = engine.restart()
                    self._send_json(200, result)
                else:
                    self._send_json(404, {"error": "not_found", "path": path})
            except PlanValidationError as exc:
                self._send_json(422, {"error": "needs_editor_change", "needs_replan": True, "errors": exc.errors})
            except RevisionConflict as exc:
                self._send_json(409, {"error": "revision_conflict", "message": str(exc)})
            except CallbackConflict as exc:
                self._send_json(409, {"error": "callback_conflict", "message": str(exc)})
            except NotFoundError as exc:
                self._send_json(404, {"error": "not_found", "message": str(exc)})
            except (ValueError, KeyError) as exc:
                self._send_json(400, {"error": "bad_request", "message": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send_json(500, {"error": "internal_error", "message": str(exc)})

        def _api_index(self) -> dict:
            return {
                "interfaces": {
                    "reserve_or_reschedule": "POST /v1/plans",
                    "provider_pull": "POST /v1/pull",
                    "provider_callback": "POST /v1/callbacks",
                    "region_publication_fact": "POST /v1/publications",
                    "generic_upstream_fact": "POST /v1/facts",
                    "audience": "POST /v1/audience",
                    "region_query": "GET /v1/regions?show=...&region=...",
                    "path_query": "GET /v1/shows/{show}/regions/{region}",
                    "dashboard": "GET /?show=...&region=...",
                    "simulated_clock": "POST /v1/clock",
                    "restart": "POST /v1/restart",
                }
            }

        def _dashboard(self, query: dict) -> None:
            show = query.get("show", "D")
            region = query.get("region", "s")
            try:
                report = engine.inspect(show, region)
                text = format_inspect(report)
                status = 200
            except Exception as exc:  # noqa: BLE001
                text = f"暂无区域报告：{exc}\n可先 POST /v1/plans 创建预约。"
                status = 200
            html = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>节目延时消息值班台</title>
<style>
body {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; margin: 2rem; background:#f7f7f5; color:#222; }}
pre {{ white-space: pre-wrap; background:white; border:1px solid #ddd; border-radius:8px; padding:1rem; line-height:1.45; }}
input,button {{ font:inherit; padding:.35rem .6rem; }}
h1 {{ font-size:1.2rem; }}
</style></head><body>
<h1>节目预约 / 改期 / 更正值班台</h1>
<form method="get" action="/">
  Show <input name="show" value="{html_escape(show, quote=True)}">
  Region <input name="region" value="{html_escape(region, quote=True)}">
  <button type="submit">实时查询</button>
  <a href="/v1">JSON API</a>
</form>
<pre>{html_escape(text)}</pre>
</body></html>"""
            self._send_html(status, html)

    return Handler


def build_server(host: str = "127.0.0.1", port: int = 8080, state_path: str | Path | None = None) -> ThreadingHTTPServer:
    engine = Middleware(state_path=state_path, wall_clock_fallback=True)
    server = ThreadingHTTPServer((host, port), create_handler(engine))
    server.engine = engine  # type: ignore[attr-defined]
    return server


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="节目延时消息中间件 HTTP 服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--state", type=Path, default=Path("delayed-middleware-state.json"))
    args = parser.parse_args(argv)

    server = build_server(args.host, args.port, args.state)
    print(f"延时消息中间件监听 http://{args.host}:{args.port} ；状态文件 {args.state}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
