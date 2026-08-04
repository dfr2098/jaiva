#!/usr/bin/env python3
"""Mock HTTP receptor para ai_trigger_webhook (fase 10C).

Uso:
  python3 scripts/mock-ml-webhook.py
  # Escucha http://127.0.0.1:8099/hook

El flujo examples/ai-prep-plant.yaml apunta aquí con optional: true.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


HOST = "127.0.0.1"
PORT = 8099


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            body = {"_raw": raw.decode("utf-8", errors="replace")}
        print(f"[mock-ml] {self.command} {self.path}")
        print(json.dumps(body, indent=2, ensure_ascii=False))
        payload = json.dumps({"ok": True, "received": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Mock ML webhook en http://{HOST}:{PORT}/hook  (Ctrl+C para salir)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nparado")
