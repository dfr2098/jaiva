#!/usr/bin/env bash
# Soak test sostenido contra el runtime levantado.
# Default: 1 hora, 250000 registros por ciclo.
#
# SOAK_DURATION_SECONDS=3600 SOAK_ROWS_PER_CYCLE=250000 \
#   ./scripts/soak-stable-stack.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/deploy/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE="$ROOT/deploy/.env.example"
fi

# shellcheck disable=SC1090
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

DURATION="${SOAK_DURATION_SECONDS:-3600}"
ROWS="${SOAK_ROWS_PER_CYCLE:-250000}"
for VALUE in "$DURATION" "$ROWS"; do
  case "$VALUE" in
    ''|*[!0-9]*) echo "duración y registros deben ser enteros positivos" >&2; exit 2 ;;
  esac
done
if (( DURATION < 60 || DURATION > 86400 )); then
  echo "SOAK_DURATION_SECONDS debe estar entre 60 y 86400" >&2
  exit 2
fi
if (( ROWS < 1 || ROWS > 10000000 )); then
  echo "SOAK_ROWS_PER_CYCLE debe estar entre 1 y 10000000" >&2
  exit 2
fi

API="http://127.0.0.1:${JAIBA_API_PORT:-19090}"
TOKEN="${JAIBA_ADMIN_TOKEN:-jaiba-stable-admin-token}"
FLOW_ID="stable-runtime-stress"
FLOW_FILE="$(mktemp "${TMPDIR:-/tmp}/jaiba-soak.XXXXXX.yaml")"
RUNNING=0

cleanup() {
  if (( RUNNING == 1 )); then
    curl -fsS -H "Authorization: Bearer $TOKEN" -X POST \
      "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
  fi
  rm -f "$FLOW_FILE"
}
trap cleanup EXIT INT TERM

sed "s/__ROWS__/$ROWS/g" "$ROOT/examples/stable-runtime-stress.yaml" > "$FLOW_FILE"

echo "Soak Jaiba: ${DURATION}s, ${ROWS} registros/ciclo"
curl -fsS \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/yaml" \
  --data-binary "@$FLOW_FILE" \
  -X PUT "$API/api/v1/flows/$FLOW_ID?start=true" >/dev/null
RUNNING=1

START="$(date +%s)"
DEADLINE="$(( START + DURATION ))"
CYCLES=0
LAST_PROCESSED=0
LAST_RECORDS=0

while (( $(date +%s) < DEADLINE )); do
  BODY="$(curl -fsS -H "Authorization: Bearer $TOKEN" "$API/api/v1/flows/$FLOW_ID")"
  STATE="$(jq -r '.runtime.control.state // "UNKNOWN"' <<<"$BODY")"
  FAILED="$(jq -r '.runtime.metrics.failed // 0' <<<"$BODY")"
  LAST_PROCESSED="$(jq -r '.runtime.metrics.processed // 0' <<<"$BODY")"
  LAST_RECORDS="$(jq -r '.runtime.metrics.processors.generate.records // 0' <<<"$BODY")"
  NOW="$(date +%s)"
  ELAPSED="$(( NOW - START ))"
  REMAINING="$(( DEADLINE - NOW ))"

  printf '\rEstado=%-9s ciclos=%-5s registros=%-12s fallos=%-5s transcurrido=%-5ss restante=%-5ss' \
    "$STATE" "$CYCLES" "$LAST_RECORDS" "$FAILED" "$ELAPSED" "$REMAINING"

  if [[ "$STATE" == "FAILED" ]] || (( FAILED > 0 )); then
    echo
    echo "Soak abortado por fallo del runtime" >&2
    exit 1
  fi
  if [[ "$STATE" == "STOPPED" ]]; then
    CYCLES="$(( CYCLES + 1 ))"
    curl -fsS -H "Authorization: Bearer $TOKEN" -X POST \
      "$API/api/v1/flows/$FLOW_ID/trigger" >/dev/null
  fi
  sleep 1
done

echo
curl -fsS -H "Authorization: Bearer $TOKEN" -X POST \
  "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
RUNNING=0

TOTAL_RECORDS="$(( CYCLES * ROWS ))"
echo "Soak completado: ciclos=$CYCLES registros_aprox=$TOTAL_RECORDS paquetes_ultimo_ciclo=$LAST_PROCESSED"
echo "Grafana: http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"
