"""标准库 HTTP 接口测试。"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from delayed_middleware.server import build_server


class HttpApiTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        state = Path(self.temporary_directory.name) / "state.json"
        self.server = build_server("127.0.0.1", 0, state)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temporary_directory.cleanup()

    def request(self, method: str, path: str, payload: dict | None = None):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_http_contract(self):
        status, health = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

        plan = {
            "show": "HTTP",
            "region": "r1",
            "revision": 1,
            "tz": "UTC",
            "actions": [
                {"action": "notice", "local": "2026-12-01T09:00:00", "window": 600, "miss": "catch_up"}
            ],
        }
        status, created = self.request("POST", "/v1/plans", plan)
        self.assertEqual(status, 202)
        self.assertTrue(created["accepted"])

        self.request("POST", "/v1/clock", {"utc": "2026-12-01T09:00:05Z"})
        status, pulled = self.request("POST", "/v1/pull", {"provider": "p", "show": "HTTP", "region": "r1"})
        self.assertEqual(status, 200)
        self.assertEqual(pulled["deliveries"][0]["action"], "notice")

        status, callback = self.request(
            "POST",
            "/v1/callbacks",
            {
                "provider": "p",
                "show": "HTTP",
                "region": "r1",
                "action": "notice",
                "revision": 1,
                "state": "sent",
                "recipients": ["a"],
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(callback["completed"])

        status, report = self.request("GET", "/v1/regions/HTTP/r1")
        self.assertEqual(status, 200)
        self.assertEqual(report["actions"][0]["state"], "sent")

        status, invalid = self.request(
            "POST",
            "/v1/plans",
            {
                "show": "HTTP",
                "region": "bad-time",
                "revision": 1,
                "tz": "America/New_York",
                "actions": [
                    {"action": "x", "local": "2026-03-08T02:30:00", "window": 10, "miss": "skip"}
                ],
            },
        )
        self.assertEqual(status, 422)
        self.assertTrue(invalid["needs_replan"])


if __name__ == "__main__":
    unittest.main()
