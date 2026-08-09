#!/usr/bin/env bash
# Despliega una carga PostgreSQL -> Jaiba y muestra métricas observables.
# Uso: STRESS_ROWS=250000 ./scripts/stress-stable-stack.sh
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

ROWS="${STRESS_ROWS:-250000}"
case "$ROWS" in
  ''|*[!0-9]*) echo "STRESS_ROWS debe ser entero positivo" >&2; exit 2 ;;
esac
if (( ROWS < 1 || ROWS > 10000000 )); then
  echo "STRESS_ROWS debe estar entre 1 y 10000000" >&2
  exit 2
fi

API="http://127.0.0.1:${JAIBA_API_PORT:-19090}"
PROM="http://127.0.0.1:${PROMETHEUS_PORT:-19091}"
TOKEN="${JAIBA_ADMIN_TOKEN:-jaiba-stable-admin-token}"
FLOW_ID="stable-runtime-stress"
FLOW_FILE="$(mktemp "${TMPDIR:-/tmp}/jaiba-stress.XXXXXX.yaml")"
trap 'rm -f "$FLOW_FILE"' EXIT

sed "s/__ROWS__/$ROWS/g" "$ROOT/examples/stable-runtime-stress.yaml" > "$FLOW_FILE"

echo "Carga: $ROWS registros"
echo "Desplegando $FLOW_ID..."
curl -fsS \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/yaml" \
  --data-binary "@$FLOW_FILE" \
  -X PUT "$API/api/v1/flows/$FLOW_ID?start=true" >/dev/null

START="$(date +%s)"
while :; do
  BODY="$(curl -fsS -H "Authorization: Bearer $TOKEN" "$API/api/v1/flows/$FLOW_ID")"
  STATE="$(jq -r '.runtime.control.state // .control.state // "UNKNOWN"' <<<"$BODY")"
  PROCESSED="$(jq -r '.runtime.metrics.processed // .metrics.processed // 0' <<<"$BODY")"
  FAILED="$(jq -r '.runtime.metrics.failed // .metrics.failed // 0' <<<"$BODY")"
  ELAPSED="$(( $(date +%s) - START ))"
  printf '\rEstado=%-10s procesados=%-8s fallidos=%-6s tiempo=%ss' "$STATE" "$PROCESSED" "$FAILED" "$ELAPSED"
  if [[ "$STATE" == "STOPPED" || "$STATE" == "FAILED" ]] || (( ELAPSED >= 300 )); then
    echo
    break
  fi
  sleep 1
done

if (( ELAPSED >= 300 )); then
  echo "Timeout de carga; deteniendo el flujo" >&2
  curl -fsS -H "Authorization: Bearer $TOKEN" -X POST \
    "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null || true
  exit 1
fi

echo "Esperando el siguiente scrape de Prometheus..."
sleep 6
curl -fsS --get --data-urlencode \
  'query=sum by (flow, processor) (jaiva_processor_records_total{flow="stable-runtime-stress"})' \
  "$PROM/api/v1/query" | jq '.data.result'

if [[ "$STATE" == "FAILED" || "$FAILED" != "0" ]]; then
  echo "La carga terminó con fallos" >&2
  exit 1
fi

echo "Carga terminada correctamente. Revisa Grafana: http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"
