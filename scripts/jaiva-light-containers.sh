#!/usr/bin/env bash
# Modo ligero para probar Jaiva en un host ~16 GiB sin trabar la PC.
# Para servicios pesados del producto (Angular, backends, Kafka) y deja solo
# lo necesario según el perfil.
set -euo pipefail

usage() {
  cat <<'EOF'
Uso: ./scripts/jaiva-light-containers.sh <perfil>

Perfiles:
  fanout     Postgres + Mongo (multi-db-fanout / pruebas Mongo)
  phase8     Postgres + Mongo + Kafka + SQL Server (suite Fase 8)
  oracle     Postgres + Mongo + Oracle (fan-out Oracle)
  stop-extra Solo detiene UI/backends/Kafka; no arranca DBs de prueba
  status     Muestra uso de memoria de contenedores

Ejemplos:
  ./scripts/jaiva-light-containers.sh fanout
  ./scripts/jaiva-light-containers.sh stop-extra
EOF
}

# Contenedores del producto que más pesan en desktop (opcionales para Jaiva).
HEAVY=(
  dma_angular
  dma_python_heavy
  dma_python
  dma_python_journal
  dma_nginx
  dma_kafka
  dma_kafka_init
  dma_kpi_consumer
  dma_journal_outbox
  dma_journal_worker
)

stop_heavy() {
  echo "Deteniendo contenedores pesados del producto (si existen)..."
  for name in "${HEAVY[@]}"; do
    if docker ps -q -f "name=^${name}$" >/dev/null 2>&1; then
      docker stop "$name" >/dev/null 2>&1 && echo "  stop $name" || true
    fi
  done
}

start_one() {
  local name="$1"
  if docker ps -a -q -f "name=^${name}$" >/dev/null 2>&1 && [[ -n "$(docker ps -aq -f "name=^${name}$")" ]]; then
    docker start "$name" >/dev/null
    echo "  start $name"
  else
    echo "  AVISO: no existe el contenedor $name (créalo con compose del entorno)" >&2
  fi
}

status() {
  docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' 2>/dev/null || true
  echo
  free -h | head -n 2
}

compose_tests() {
  local dma_dir="${JAIBA_TEST_COMPOSE_DIR:-${HOME}/Escritorio/DMA_CORE/DMA_CORE}"
  local file="${dma_dir}/compose.test-databases.yml"
  if [[ ! -f "$file" ]]; then
    echo "No se encontró $file" >&2
    exit 1
  fi
  echo "Usando $file"
  (cd "$dma_dir" && docker compose -f compose.test-databases.yml "$@")
}

profile="${1:-}"
case "$profile" in
  -h|--help|"")
    usage
    exit 0
    ;;
  status)
    status
    ;;
  stop-extra)
    stop_heavy
    status
    ;;
  fanout)
    stop_heavy
    echo "Arrancando Postgres (producto) + Mongo (pruebas)..."
    start_one dma_postgres
    compose_tests up -d mongodb-test
    status
    echo
    echo "Listo para: cargo run --features mongodb-driver,oracle-driver -- examples/multi-db-fanout.yaml"
    echo "(Oracle aparte: ./scripts/jaiva-light-containers.sh oracle)"
    ;;
  phase8)
    stop_heavy
    echo "Arrancando Postgres + Mongo + SQL Server + Kafka (Fase 8)..."
    start_one dma_postgres
    # Kafka vive en el compose del producto (profile kafka).
    if docker ps -a -q -f name=^dma_kafka$ >/dev/null 2>&1; then
      start_one dma_kafka
    else
      echo "  AVISO: dma_kafka no existe; levántalo con el compose del producto --profile kafka" >&2
    fi
    compose_tests up -d mongodb-test sqlserver-test
    status
    ;;
  oracle)
    stop_heavy
    echo "Arrancando Postgres + Mongo + Oracle..."
    start_one dma_postgres
    compose_tests up -d mongodb-test oracle-test
    status
    echo
    echo "Oracle en 127.0.0.1:11521 (FREEPDB1). Espera healthcheck antes de correr el flujo."
    ;;
  *)
    echo "Perfil desconocido: $profile" >&2
    usage >&2
    exit 2
    ;;
esac
