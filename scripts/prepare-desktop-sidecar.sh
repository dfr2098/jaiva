#!/usr/bin/env bash
# Wrapper compatible con CI/Linux. La lógica multiplataforma vive en Node.
# Uso:
#   scripts/prepare-desktop-sidecar.sh              # target/debug/jaiba
#   scripts/prepare-desktop-sidecar.sh release      # target/release/jaiba
#   scripts/prepare-desktop-sidecar.sh /ruta/jaiba
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec node "$ROOT/scripts/prepare-desktop-sidecar.mjs" "$@"
