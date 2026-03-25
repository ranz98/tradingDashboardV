"""
GET /api/stats
──────────────
Reads the latest stats snapshot from Vercel KV (written by stats_pusher.py)
and returns it as JSON. No Binance keys needed here — data is pre-computed.
"""
import os
import json
from http.server import BaseHTTPRequestHandler
import requests


def kv_get(key: str):
    url   = os.environ.get("KV_REST_API_URL",   "")
    token = os.environ.get("KV_REST_API_TOKEN",  "")
    if not url or not token:
        return None
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
        json=["GET", key],
        timeout=8,
    )
    r.raise_for_status()
    result = r.json().get("result")
    return json.loads(result) if result else None


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            data = kv_get("cruzebot_stats")
            if data is None:
                self._respond(503, json.dumps({
                    "error": "No data yet — make sure stats_pusher.py is running locally."
                }))
                return
            self._respond(200, json.dumps(data))
        except Exception as exc:
            self._respond(500, json.dumps({"error": str(exc)}))

    def do_OPTIONS(self):
        self._respond(200, "")

    def _respond(self, code: int, body: str):
        self.send_response(code)
        self.send_header("Content-Type",                  "application/json")
        self.send_header("Access-Control-Allow-Origin",   "*")
        self.send_header("Access-Control-Allow-Methods",  "GET, OPTIONS")
        self.send_header("Cache-Control",                 "no-store")
        self.end_headers()
        if body:
            self.wfile.write(body.encode())

    def log_message(self, *_):
        pass
