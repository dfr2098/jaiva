#!/usr/bin/env bash
# Smoke Prioridad 1 — release-core (sin drivers opcionales).
#
# Uso (desde la raíz del repo):
#   ./scripts/smoke-release-core.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== smoke release-core ==="

echo "--- 1) unit: master key + admin loopback ---"
cargo test -p jaiba-server master_key -- --nocapture
cargo test -p jaiba-server unauthenticated_admin -- --nocapture

echo "--- 2) unit: unknown processor feature hint ---"
cargo test -p jaiba-runtime unknown_oracle_processor_hints_feature_flag -- --nocapture

echo "--- 3) build CLI release-core ---"
cargo build -p jaiba-cli --features release-core --bin jaiba

echo "--- 4) run examples/smoke.yaml ---"
SMOKE_DIR="${TMPDIR:-/tmp}/jaiba-smoke-$$"
mkdir -p "$SMOKE_DIR"
cleanup() { rm -rf "$SMOKE_DIR"; }
trap cleanup EXIT

FLOW="$SMOKE_DIR/smoke.yaml"
python3 - "$SMOKE_DIR" "$FLOW" <<'PY'
import sys
from pathlib import Path
smoke_dir, flow_path = sys.argv[1], sys.argv[2]
src = Path("examples/smoke.yaml").read_text(encoding="utf-8")
out = src.replace(".jaiva/smoke-repository.db", f"{smoke_dir}/repository.db")
out = out.replace(".jaiva/smoke-content", f"{smoke_dir}/content")
out = out.replace(".jaiva/smoke-logs", f"{smoke_dir}/logs")
Path(flow_path).write_text(out, encoding="utf-8")
PY

cargo run -p jaiba-cli --features release-core --quiet -- "$FLOW"

echo
echo "=== smoke release-core OK ==="
