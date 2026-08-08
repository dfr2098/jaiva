#!/usr/bin/env bash
# Levanta el stack Estable (Postgres + Jaiba + UI) y espera health.
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

cd "$ROOT/deploy"
docker compose --env-file "$ENV_FILE" -f docker-compose.release-core.yml up -d --build

echo "Esperando health de Jaiba..."
for i in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:${JAIBA_API_PORT:-9090}/health" >/dev/null 2>&1; then
    echo "Jaiba OK"
    break
  fi
  if [[ "$i" -eq 90 ]]; then
    echo "Timeout esperando /health" >&2
    docker compose --env-file "$ENV_FILE" -f docker-compose.release-core.yml logs --tail=80 jaiba >&2 || true
    exit 1
  fi
  sleep 2
done

echo "Esperando UI (proxy /jaiba-api/health)..."
for i in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${UI_PORT:-9080}/jaiba-api/health" >/dev/null 2>&1; then
    echo "UI proxy OK"
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    echo "Timeout esperando UI proxy /jaiba-api/health" >&2
    docker compose --env-file "$ENV_FILE" -f docker-compose.release-core.yml logs --tail=40 ui >&2 || true
    exit 1
  fi
  sleep 2
done

echo "UI:  http://127.0.0.1:${UI_PORT:-9080}"
echo "API: http://127.0.0.1:${JAIBA_API_PORT:-9090}"
echo "Siguiente: ./scripts/smoke-stable-path.sh"
