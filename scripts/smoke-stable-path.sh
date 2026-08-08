#!/usr/bin/env bash
# Smoke del recorrido oficial: Postgres → CSV (CLI dentro del contenedor jaiba).
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

compose() {
  docker compose --env-file "$ENV_FILE" -f "$ROOT/deploy/docker-compose.release-core.yml" "$@"
}

API="http://127.0.0.1:${JAIBA_API_PORT:-9090}"
TOKEN="${JAIBA_ADMIN_TOKEN:-jaiba-stable-admin-token}"

if ! curl -fsS "$API/health" >/dev/null 2>&1; then
  bash "$ROOT/scripts/release-core-up.sh"
fi

echo "=== smoke stable-path ==="

echo "--- health ---"
curl -fsS "$API/health" | tee /tmp/jaiba-stable-health.json
python3 -c 'import json; d=json.load(open("/tmp/jaiba-stable-health.json")); assert d.get("status")=="ok", d'

echo "--- run stable-postgres-to-csv ---"
compose exec -T \
  -e DATABASE_URL="postgres://${POSTGRES_USER:-jaiba}:${POSTGRES_PASSWORD:-jaiba_stable}@postgres:5432/${POSTGRES_DB:-jaiba_stable}" \
  jaiba \
  jaiba /flows/stable-postgres-to-csv.yaml

echo "--- assert CSV ---"
CSV_CONTENT=$(compose exec -T jaiba sh -lc 'test -s /output/stable-items.csv && cat /output/stable-items.csv')
echo "$CSV_CONTENT"
echo "$CSV_CONTENT" | grep -q 'Alpha\|A1' || {
  echo "CSV sin filas esperadas" >&2
  exit 1
}

echo "--- API auth gate ---"
CODE=$(curl -sS -o /dev/null -w '%{http_code}' "$API/api/v1/connections" || true)
[[ "$CODE" == "401" || "$CODE" == "403" ]] || {
  echo "esperado 401/403 sin token, got $CODE" >&2
  exit 1
}
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" "$API/api/v1/connections")
[[ "$CODE" == "200" ]] || {
  echo "esperado 200 con token, got $CODE" >&2
  exit 1
}

echo "=== smoke stable-path OK ==="
