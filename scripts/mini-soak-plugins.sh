#!/usr/bin/env bash
# Mini-soak local de plugins externos (capa A del endurecimiento soak/plugins).
# No levanta el stack Docker ni corre 1 h; valida N ciclos del flujo ejemplo.
#
# Uso:
#   bash scripts/mini-soak-plugins.sh [binario jaiba] [ciclos]
#   MINI_SOAK_CYCLES=50 bash scripts/mini-soak-plugins.sh ./target/debug/jaiba
#
# PASS: cada ciclo imprime name ADA/LINUS en mayúsculas y exit 0.
# FAIL: cualquier ciclo con exit != 0 o sin el registro esperado.
set -euo pipefail
cd "$(dirname "$0")/.."

JAIBA="${1:-target/debug/jaiba}"
CYCLES="${2:-${MINI_SOAK_CYCLES:-20}}"

case "$CYCLES" in
  ''|*[!0-9]*) echo "ciclos debe ser entero positivo" >&2; exit 2 ;;
esac
if (( CYCLES < 1 || CYCLES > 1000 )); then
  echo "MINI_SOAK_CYCLES / ciclos debe estar entre 1 y 1000" >&2
  exit 2
fi

if [[ ! -x "$JAIBA" ]]; then
  echo "binario no ejecutable: $JAIBA (compila con: cargo build -p jaiba-cli --bin jaiba)" >&2
  exit 2
fi

export JAIBA_PLUGIN_DIR="${JAIBA_PLUGIN_DIR:-$PWD/examples/plugins}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/jaiba-mini-soak.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

echo "Mini-soak plugins: $CYCLES ciclos, plugin_dir=$JAIBA_PLUGIN_DIR, bin=$JAIBA"
for i in $(seq 1 "$CYCLES"); do
  LOG="$WORK/cycle-$i.log"
  if ! "$JAIBA" examples/plugins/mayusculas-flow.yaml >"$LOG" 2>&1; then
    echo "FAIL ciclo $i (exit != 0)" >&2
    cat "$LOG" >&2
    exit 1
  fi
  if ! grep -q '"name":"ADA"' "$LOG" || ! grep -q '"name":"LINUS"' "$LOG"; then
    echo "FAIL ciclo $i (registro esperado ausente)" >&2
    cat "$LOG" >&2
    exit 1
  fi
  printf '\rCiclo %-4s/%s OK' "$i" "$CYCLES"
done
echo
echo "=== mini-soak plugins OK ($CYCLES ciclos) ==="
