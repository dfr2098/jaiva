#!/usr/bin/env bash
# Fase 8: integración / fallos / smoke de rendimiento contra un entorno de pruebas.
# No levanta contenedores. Requiere Postgres, Kafka, MongoDB y SQL Server en el host.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

postgres_host="${JAIBA_TEST_POSTGRES_HOST:-127.0.0.1}"
postgres_port="${JAIBA_TEST_POSTGRES_PORT:-55432}"
postgres_db="${JAIBA_TEST_POSTGRES_DATABASE:-dma}"
postgres_user="${JAIBA_TEST_POSTGRES_USER:-dma}"
kafka_brokers="${JAIBA_TEST_KAFKA_BROKERS:-127.0.0.1:29092}"
kafka_host="${kafka_brokers%%:*}"
kafka_port="${kafka_brokers##*:}"
mongodb_host="${JAIBA_TEST_MONGODB_HOST:-127.0.0.1}"
mongodb_port="${JAIBA_TEST_MONGODB_PORT:-27018}"
mongodb_db="${JAIBA_TEST_MONGODB_DATABASE:-dma_test}"
mongodb_user="${JAIBA_TEST_MONGODB_USER:-dma_test}"
mongodb_password="${JAIBA_TEST_MONGODB_PASSWORD:-}"
sqlserver_host="${JAIBA_TEST_SQLSERVER_HOST:-127.0.0.1}"
sqlserver_port="${JAIBA_TEST_SQLSERVER_PORT:-11433}"
sqlserver_db="${JAIBA_TEST_SQLSERVER_DATABASE:-master}"
sqlserver_user="${JAIBA_TEST_SQLSERVER_USER:-sa}"
sqlserver_password="${JAIBA_TEST_SQLSERVER_PASSWORD:-}"

usage() {
  cat <<'EOF'
Uso: ./scripts/phase8-integration.sh [--password <postgres_password>]

Variables útiles:
  JAIBA_TEST_POSTGRES_PASSWORD   (obligatoria, o --password, o .env del entorno)
  JAIBA_TEST_POSTGRES_URL        (se deriva si falta)
  JAIBA_TEST_KAFKA_BROKERS       (default 127.0.0.1:29092)
  JAIBA_TEST_MONGODB_PASSWORD    (obligatoria)
  JAIBA_TEST_MONGODB_HOST/PORT/DATABASE/USER/URL
  JAIBA_TEST_SQLSERVER_PASSWORD  (obligatoria; SA del contenedor de pruebas)
  JAIBA_TEST_SQLSERVER_HOST/PORT/DATABASE/USER
  JAIBA_TEST_ENV                 ruta opcional a un .env con POSTGRES_APP_PASSWORD
EOF
}

postgres_password="${JAIBA_TEST_POSTGRES_PASSWORD:-}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --password)
      postgres_password="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Argumento desconocido: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

load_password_from_test_env() {
  local env_file="${JAIBA_TEST_ENV:-${DMA_CORE_ENV:-}}"
  if [[ -z "$env_file" ]]; then
    for candidate in \
      "${HOME}/Escritorio/DMA_CORE/DMA_CORE/.env" \
      "${repo_dir}/../DMA_CORE/DMA_CORE/.env"
    do
      if [[ -f "$candidate" ]]; then
        env_file="$candidate"
        break
      fi
    done
  fi
  if [[ -n "$env_file" && -f "$env_file" ]]; then
    postgres_password="$(
      sed -n 's/^POSTGRES_APP_PASSWORD=//p' "$env_file" | tail -n 1 | tr -d '\r' | sed "s/^['\"]//; s/['\"]$//"
    )"
    if [[ -n "$postgres_password" ]]; then
      echo "Usando POSTGRES_APP_PASSWORD desde el .env del entorno de pruebas"
    fi
  fi
}

if [[ -z "$postgres_password" ]]; then
  load_password_from_test_env
fi
if [[ -z "$postgres_password" ]]; then
  echo "ERROR: define JAIBA_TEST_POSTGRES_PASSWORD o pasa --password." >&2
  exit 1
fi

port_open() {
  local host="$1" port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z -w 2 "$host" "$port" >/dev/null 2>&1
    return $?
  fi
  timeout 2 bash -c "echo >/dev/tcp/${host}/${port}" >/dev/null 2>&1
}

echo "Comprobando Postgres ${postgres_host}:${postgres_port}..."
if ! port_open "$postgres_host" "$postgres_port"; then
  echo "ERROR: Postgres no responde en ${postgres_host}:${postgres_port}" >&2
  exit 1
fi
echo "Comprobando Kafka ${kafka_host}:${kafka_port}..."
if ! port_open "$kafka_host" "$kafka_port"; then
  echo "ERROR: Kafka no responde en ${kafka_host}:${kafka_port}" >&2
  exit 1
fi
echo "Comprobando MongoDB ${mongodb_host}:${mongodb_port}..."
if ! port_open "$mongodb_host" "$mongodb_port"; then
  echo "ERROR: MongoDB no responde en ${mongodb_host}:${mongodb_port}" >&2
  exit 1
fi
echo "Comprobando SQL Server ${sqlserver_host}:${sqlserver_port}..."
if ! port_open "$sqlserver_host" "$sqlserver_port"; then
  echo "ERROR: SQL Server no responde en ${sqlserver_host}:${sqlserver_port}" >&2
  exit 1
fi

if [[ -z "$mongodb_password" ]]; then
  echo "ERROR: define JAIBA_TEST_MONGODB_PASSWORD (usuario raíz del contenedor de pruebas)." >&2
  exit 1
fi
if [[ -z "$sqlserver_password" ]]; then
  echo "ERROR: define JAIBA_TEST_SQLSERVER_PASSWORD (SA del contenedor de pruebas)." >&2
  exit 1
fi

export JAIBA_TEST_KAFKA_BROKERS="$kafka_brokers"
export JAIBA_TEST_KAFKA_FAIL_BROKER="${JAIBA_TEST_KAFKA_FAIL_BROKER:-127.0.0.1:1}"
export JAIBA_TEST_POSTGRES_HOST="$postgres_host"
export JAIBA_TEST_POSTGRES_PORT="$postgres_port"
export JAIBA_TEST_POSTGRES_DATABASE="$postgres_db"
export JAIBA_TEST_POSTGRES_USER="$postgres_user"
export JAIBA_TEST_POSTGRES_PASSWORD="$postgres_password"
export JAIBA_TEST_MONGODB_HOST="$mongodb_host"
export JAIBA_TEST_MONGODB_PORT="$mongodb_port"
export JAIBA_TEST_MONGODB_DATABASE="$mongodb_db"
export JAIBA_TEST_MONGODB_USER="$mongodb_user"
export JAIBA_TEST_MONGODB_PASSWORD="$mongodb_password"
export JAIBA_TEST_SQLSERVER_HOST="$sqlserver_host"
export JAIBA_TEST_SQLSERVER_PORT="$sqlserver_port"
export JAIBA_TEST_SQLSERVER_DATABASE="$sqlserver_db"
export JAIBA_TEST_SQLSERVER_USER="$sqlserver_user"
export JAIBA_TEST_SQLSERVER_PASSWORD="$sqlserver_password"
if [[ -z "${JAIBA_TEST_POSTGRES_URL:-}" ]]; then
  # URL-encode mínimo de caracteres frecuentes en passwords de prueba.
  encoded_password="$(
    python3 - <<PY
import urllib.parse
print(urllib.parse.quote("""${postgres_password}""", safe=""))
PY
  )"
  export JAIBA_TEST_POSTGRES_URL="postgres://${postgres_user}:${encoded_password}@${postgres_host}:${postgres_port}/${postgres_db}"
fi
if [[ -z "${JAIBA_TEST_MONGODB_URL:-}" ]]; then
  encoded_mongo_password="$(
    python3 - <<PY
import urllib.parse
print(urllib.parse.quote("""${mongodb_password}""", safe=""))
PY
  )"
  export JAIBA_TEST_MONGODB_URL="mongodb://${mongodb_user}:${encoded_mongo_password}@${mongodb_host}:${mongodb_port}/${mongodb_db}?authSource=admin"
fi

echo "Ejecutando suite Fase 8..."
log_file="$(mktemp)"
trap 'rm -f "$log_file"' EXIT

set +e
# cargo test acepta un solo filtro (subcadena); se parten en invocaciones.
cargo test -p jaiba-runtime --features kafka-driver,mongodb-driver kafka_ \
  -- --nocapture 2>&1 | tee "$log_file"
kafka_status=${PIPESTATUS[0]}

cargo test -p jaiba-runtime --features kafka-driver,mongodb-driver flow_retry_then_dead_letter \
  -- --nocapture 2>&1 | tee -a "$log_file"
dlq_status=${PIPESTATUS[0]}

cargo test -p jaiba-runtime --features mongodb-driver \
  mongodb_real_query_to_upsert_flow_is_idempotent \
  -- --nocapture 2>&1 | tee -a "$log_file"
mongo_flow_status=${PIPESTATUS[0]}

cargo test -p jaiba-server \
  postgres_real_connection_query_builder_and_flow_execution \
  -- --nocapture 2>&1 | tee -a "$log_file"
postgres_status=${PIPESTATUS[0]}

cargo test -p jaiba-server --features mongodb-driver \
  mongodb_real_connection_diagnostics_and_collection_metadata \
  -- --nocapture 2>&1 | tee -a "$log_file"
mongo_meta_status=${PIPESTATUS[0]}

cargo test -p jaiba-server --features sqlserver-driver \
  sqlserver_real_connection_diagnostics_and_metadata \
  -- --nocapture 2>&1 | tee -a "$log_file"
sqlserver_status=${PIPESTATUS[0]}
set -e

if grep -E 'skipping real (Kafka|PostgreSQL|MongoDB|SQL Server)|skipping MongoDB flow|skipping real MySQL' "$log_file" >/dev/null; then
  echo "ERROR: un test requerido se omitió (faltan variables o entorno)." >&2
  grep -E 'skipping ' "$log_file" >&2 || true
  exit 1
fi

# Exigir que hayan corrido los casos Kafka/Postgres/Mongo/SQL Server.
if ! grep -E 'kafka_real_consume_kafka_processor \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'kafka_throughput_smoke \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'kafka_fail_broker_is_controlled \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'kafka_real_publish_is_acknowledged_and_consumable \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'flow_retry_then_dead_letter \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'postgres_real_connection_query_builder_and_flow_execution \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'mongodb_real_connection_diagnostics_and_collection_metadata \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'mongodb_real_query_to_upsert_flow_is_idempotent \.\.\. ok' "$log_file" >/dev/null \
  || ! grep -E 'sqlserver_real_connection_diagnostics_and_metadata \.\.\. ok' "$log_file" >/dev/null
then
  echo "ERROR: faltan resultados OK de pruebas requeridas en la salida." >&2
  exit 1
fi

if [[ "$kafka_status" -ne 0 || "$dlq_status" -ne 0 || "$mongo_flow_status" -ne 0 || "$postgres_status" -ne 0 || "$mongo_meta_status" -ne 0 || "$sqlserver_status" -ne 0 ]]; then
  echo "ERROR: la suite Fase 8 falló (kafka=$kafka_status dlq=$dlq_status mongo_flow=$mongo_flow_status postgres=$postgres_status mongo_meta=$mongo_meta_status sqlserver=$sqlserver_status)." >&2
  exit 1
fi

echo
echo "Fase 8 OK contra el entorno de pruebas (${postgres_host}:${postgres_port}, ${kafka_brokers}, mongo ${mongodb_host}:${mongodb_port}, sqlserver ${sqlserver_host}:${sqlserver_port})."
