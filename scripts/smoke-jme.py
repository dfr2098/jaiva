"""Offline smoke for the Jaiba Memory Engine (JME) runtime integration.

Build first: cargo build -p jaiba-cli --bin jaiba
Run: python scripts/smoke-jme.py [path/to/jaiba]

Each run uses a temporary JAIBA_DATA_DIR and a working directory outside the
repository, so the flows must not depend on files under examples/.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = ROOT / "target" / "debug" / ("jaiba.exe" if os.name == "nt" else "jaiba")
BINARY = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_BINARY
FLOW_ID = "jme-smoke"
CARRIERS = ["A1", "A2", "A3"]

POLICY = """\
  domain_memory:
    enabled: true
    policy:
      version: {version}
      max_entries: {max_entries}
      cold:
        backend: segmented
        segment_max_bytes: 4096
      classes:
        carrier:
          policy: cache
          temperature: cold
          ttl: 1h
        recovered:
          policy: immediate
"""


def flow(stage, version=1):
    max_entries = 1 if stage == "write" else 16
    lines = [
        f"id: {FLOW_ID}",
        "engine:",
        "  max_concurrency: 8",
        "  repository: {enabled: false}",
        "  logging: {enabled: false}",
        POLICY.format(version=version, max_entries=max_entries).rstrip(),
        "processors:",
    ]
    connections = []
    for carrier in CARRIERS:
        source = f"source_{carrier}"
        lines += [
            f"  - id: {source}",
            "    type: generate_records",
            f"    config: {{records: [{{carrier_id: {carrier}, lane: {carrier[1:]}}}]}}",
        ]
        if stage == "write":
            connections.append(f"  - {{from: {source}, to: remember, relationship: success}}")
        else:
            connections.append(f"  - {{from: {source}, to: recall, relationship: success}}")
    if stage == "write":
        lines += [
            "  - id: remember",
            "    type: memory_upsert",
            "    config: {class: carrier, id_attribute: carrier_id}",
        ]
    else:
        lines += [
            "  - id: recall",
            "    type: memory_get",
            "    config: {class: carrier, id_attribute: carrier_id, attribute: memory.value}",
            "  - id: save",
            "    type: memory_upsert",
            "    config: {class: recovered, id_attribute: carrier_id, value_attribute: memory.value}",
        ]
        connections.append("  - {from: recall, to: save, relationship: success}")
    return "\n".join(lines + ["connections:"] + connections) + "\n"


def run(workdir, data_dir, text):
    path = workdir / "flow.yaml"
    path.write_text(text, encoding="utf-8")
    env = {name: value for name, value in os.environ.items() if not name.startswith(("JAIBA_", "JAIVA_"))}
    env["JAIBA_DATA_DIR"] = str(data_dir)
    return subprocess.run([str(BINARY), str(path)], cwd=workdir, env=env, capture_output=True, text=True, timeout=30)


def main():
    if not BINARY.exists():
        raise SystemExit(f"binary not found: {BINARY} (cargo build -p jaiba-cli --bin jaiba)")
    with tempfile.TemporaryDirectory(prefix="jaiba-jme-smoke-") as temporary:
        workdir = Path(temporary) / "work"
        data_dir = Path(temporary) / "data"
        workdir.mkdir()

        result = run(workdir, data_dir, flow("write"))
        assert result.returncode == 0, result.stderr
        cold_dir = data_dir / "jme" / "cold" / FLOW_ID
        segments = list(cold_dir.rglob("segment-*.jmc"))
        assert segments, f"no Cold segments under {cold_dir}"
        assert not (workdir / "data").exists(), "JME wrote relative to the working directory"
        print("PASS: Hot evictions demote to Cold under JAIBA_DATA_DIR with an inline policy")

        result = run(workdir, data_dir, flow("recover"))
        assert result.returncode == 0, result.stderr
        persisted = data_dir / "jme" / FLOW_ID / "persist.jsonl"
        records = [json.loads(line) for line in persisted.read_text(encoding="utf-8").splitlines() if line.strip()]
        keys = sorted(record["key"] for record in records)
        assert len(keys) == len(CARRIERS) - 1, keys
        assert set(keys) <= {f"recovered:{carrier}" for carrier in CARRIERS}, keys
        for record in records:
            carrier = record["key"].split(":", 1)[1]
            assert record["value"] == {"carrier_id": carrier, "lane": int(carrier[1:])}, record
        print("PASS: Cold values survive a restart and feed immediate persistence")

        result = run(workdir, data_dir, flow("write", version=2))
        assert result.returncode != 0, "memory.version 2 must be rejected"
        assert "memory.version 2" in result.stdout + result.stderr, result.stderr
        print("PASS: unsupported memory.version is rejected")


if __name__ == "__main__":
    main()
