#!/usr/bin/env bash
# Suite de regresión corta: Compose + smoke Estable + Playwright (≈14 tests).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

bash "$ROOT/scripts/release-core-up.sh"
bash "$ROOT/scripts/smoke-stable-path.sh"

cd "$ROOT/apps/jaiba-ui"
if [[ ! -d node_modules ]]; then
  npm ci
fi
if ! npx playwright --version >/dev/null 2>&1; then
  npx playwright install chromium
fi

export JAIBA_E2E_BASE_URL="${JAIBA_E2E_BASE_URL:-http://127.0.0.1:19080}"
export JAIBA_E2E_API_URL="${JAIBA_E2E_API_URL:-http://127.0.0.1:19090}"
export JAIBA_ADMIN_TOKEN="${JAIBA_ADMIN_TOKEN:-jaiba-stable-admin-token}"

npm run e2e

echo "=== smoke-regression OK ==="
