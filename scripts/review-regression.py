"""Offline regressions for memory admission, persistence and the administrative API.

Build first: cargo build -p jaiba-cli --bin jaiba
Run: python scripts/review-regression.py [path/to/jaiba]
Uses temporary data and starts/stops only its own local server.
"""

import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "target" / "debug" / ("jaiba.exe" if os.name == "nt" else "jaiba")


def environment(directory):
    env = os.environ.copy()
    for name in list(env):
        if name.startswith(("JAIBA_", "JAIVA_")):
            del env[name]
    env["JAIBA_DATA_DIR"] = str(directory)
    return env


def cli_regressions(directory):
    flow = directory / "flow.yaml"
    flow.write_text("""id: memory-regression
engine:
  repository: {enabled: false}
  logging: {enabled: false}
processors:
  - id: source
    type: generate_records
    config: {records: [{id: 1}]}
  - id: transform
    type: rename_fields
    config: {fields: {id: renamed}}
  - id: sink
    type: log_records
connections:
  - {from: source, to: transform, relationship: success}
  - {from: transform, to: sink, relationship: success}
""", encoding="utf-8")
    for budget, success in [(262144, True), (65536, False)]:
        env = environment(directory)
        env["JAIBA_MEMORY_MAX_BYTES"] = str(budget)
        result = subprocess.run([str(BINARY), str(flow)], cwd=directory, env=env, capture_output=True, text=True, timeout=10)
        assert (result.returncode == 0) == success, result.stderr
        if not success:
            assert "memory capacity exhausted" in result.stderr, result.stderr
    print("PASS: low memory returns an error without hanging; sufficient memory completes")

    (directory / "policy.yaml").write_text("memory:\n  max_hot_bytes: 1\n  classes:\n    item:\n      policy: immediate\n", encoding="utf-8")
    flow.write_text("""id: persist-regression
engine:
  logging: {enabled: false}
  domain_memory: {enabled: true, policy_file: policy.yaml}
processors:
  - id: source
    type: generate_records
    config: {records: [{id: 1}]}
  - id: save
    type: memory_upsert
    retry: {maximum_attempts: 1}
    config: {class: item, id_attribute: id}
connections:
  - {from: source, to: save, relationship: success}
""", encoding="utf-8")
    result = subprocess.run([str(BINARY), str(flow)], cwd=directory, env=environment(directory), capture_output=True, text=True, timeout=10)
    assert "hot byte capacity exceeded" in result.stdout + result.stderr
    sink = directory / "jme" / "persist-regression" / "persist.jsonl"
    assert not sink.exists() or not sink.read_text(encoding="utf-8").strip()
    print("PASS: rejected Hot writes do not persist, including retries")


def api_regressions(directory):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    admin, viewer = secrets.token_hex(16), secrets.token_hex(16)
    users = directory / "users.json"
    users.write_text(json.dumps({"users": [
        {"id": "admin", "role": "admin", "token": admin, "projects": ["*"]},
        {"id": "viewer", "role": "viewer", "token": viewer, "projects": ["allowed"]},
    ]}), encoding="utf-8")
    env = environment(directory)
    env.update(JAIBA_SERVER_ADDR=f"127.0.0.1:{port}", JAIBA_ADMIN_AUTH="bearer", JAIBA_ADMIN_USERS_FILE=str(users))
    server = subprocess.Popen([str(BINARY), "serve"], cwd=directory, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def request(path, method="GET", body=None, token=None, headers=None):
        headers = dict(headers or {})
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(f"http://127.0.0.1:{port}" + path, method=method, data=body.encode() if body else None, headers=headers)
        try:
            response = urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, dict(response.headers), response.read().decode()

    def flow(name, broken=False):
        domain = "  domain_memory: {enabled: true, policy_file: absent.yaml}\n" if broken else ""
        return f"id: {name}\nengine:\n  repository: {{enabled: false}}\n{domain}processors:\n  - id: source\n    type: generate_records\n    config: {{records: []}}\nconnections: []\n"

    try:
        for _ in range(100):
            try:
                if request("/health")[0] == 200:
                    break
            except OSError:
                if server.poll() is not None:
                    raise AssertionError("test server exited during startup")
                time.sleep(0.1)
        else:
            raise AssertionError("test server did not start")

        for name in ["allowed", "private"]:
            status, _, body = request(f"/api/v1/flows/{name}?start=true", "PUT", flow(name), admin)
            assert status == 200, body
        status, _, body = request("/api/v1/flows/allowed?start=true", "PUT", flow("allowed", broken=True), admin)
        assert status >= 400, body
        _, _, body = request("/api/v1/flows/allowed", token=admin)
        record = json.loads(body)
        assert record["active_version"] == 1
        assert record["versions"][0]["state"] == "DEPLOYED"
        assert record["versions"][1]["state"] == "VALIDATED"
        print("PASS: failed initialization preserves the previous deployed version")

        assert request("/metrics")[0] == 401
        status, _, body = request("/metrics", token=viewer)
        assert status == 200 and 'flow="allowed"' in body and 'flow="private"' not in body
        status, _, body = request("/metrics", token=admin)
        assert status == 200 and 'flow="private"' in body
        _, _, body = request("/ready")
        assert set(json.loads(body)) == {"ready"}
        print("PASS: metrics require authentication and filter projects; readiness has no flow details")

        for origin, allowed in [("http://tauri.localhost", True), ("https://untrusted.example", False)]:
            status, headers, _ = request("/api/v1/flows", "OPTIONS", headers={"Origin": origin, "Access-Control-Request-Method": "PUT", "Access-Control-Request-Headers": "authorization,content-type"})
            normalized = {key.lower(): value for key, value in headers.items()}
            assert status < 400
            assert (normalized.get("access-control-allow-origin") == origin) == allowed
        print("PASS: desktop CORS preflight works; untrusted origins are not allowed")
    finally:
        server.terminate()
        server.wait(timeout=10)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="jaiba-regression-") as temporary:
        directory = Path(temporary)
        cli_regressions(directory)
        api_regressions(directory)
