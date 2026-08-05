#!/usr/bin/env bash
# Prueba Jaiba contra las bases de desarrollo ya existentes. Este script no
# crea, inicia, detiene ni recrea contenedores.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
oracle_container="${JAIBA_TEST_ORACLE_CONTAINER:-oracle19}"
postgres_container="${JAIBA_TEST_POSTGRES_CONTAINER:-dma_postgres}"

require_running() {
  local container="$1"
  if [[ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" != "true" ]]; then
    echo "ERROR: el contenedor existente '$container' no estÃ¡ activo." >&2
    exit 1
  fi
}

port_open() {
  local host="$1" port="$2"
  timeout 3 bash -c "echo >/dev/tcp/${host}/${port}" >/dev/null 2>&1
}

if [[ -z "${JAIBA_TEST_MONGODB_PASSWORD:-}" ]]; then
  echo "ERROR: define JAIBA_TEST_MONGODB_PASSWORD para el MongoDB de pruebas." >&2
  exit 1
fi

require_running "$oracle_container"
require_running "$postgres_container"
for port in 1521 5432 27017; do
  if ! port_open 127.0.0.1 "$port"; then
    echo "ERROR: 127.0.0.1:${port} no estÃ¡ publicado; no se modificarÃ¡ Docker automÃ¡ticamente." >&2
    exit 1
  fi
done

cd "$repo_dir"
echo "[1/2] Oracle -> PostgreSQL"
./scripts/test-oracle-to-postgres.sh

echo "[2/2] MongoDB: conexiÃ³n, diagnÃ³stico y metadatos"
export JAIBA_TEST_MONGODB_HOST="${JAIBA_TEST_MONGODB_HOST:-127.0.0.1}"
export JAIBA_TEST_MONGODB_PORT="${JAIBA_TEST_MONGODB_PORT:-27017}"
export JAIBA_TEST_MONGODB_DATABASE="${JAIBA_TEST_MONGODB_DATABASE:-pruebas}"
export JAIBA_TEST_MONGODB_USER="${JAIBA_TEST_MONGODB_USER:-admin}"
cargo test -p jaiba-server --features mongodb-driver \
  mongodb_real_connection_diagnostics_and_collection_metadata -- --nocapture

echo
echo "Multi-DB OK: Oracle -> PostgreSQL y MongoDB validados sin crear servicios."
