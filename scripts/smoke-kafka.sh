#!/usr/bin/env bash
# Smoke Kafka (Beta): publica N mensajes con el binario `jaiba`, los consume con
# otro flow y comprueba que llegaron todos y que el offset quedó confirmado.
# Lo usa .github/workflows/kafka.yml; ver docs/kafka.md.
#
# Requisitos:
#   - Broker Kafka compatible (Redpanda o Apache Kafka) en
#     JAIBA_TEST_KAFKA_BROKERS (por defecto 127.0.0.1:29092).
#   - Opcional: JAIBA_KAFKA_CONTAINER para crear el tópico con rpk o
#     kafka-topics.sh dentro del contenedor.
#
# Uso: ./scripts/smoke-kafka.sh [ruta-al-binario-jaiba]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

brokers="${JAIBA_TEST_KAFKA_BROKERS:-127.0.0.1:29092}"
messages="${JAIBA_SMOKE_KAFKA_MESSAGES:-50}"
suffix="$(date +%s)-$$"
topic="jaiba.smoke.${suffix}"
group="jaiba-smoke-${suffix}"

# Forzar filtro: el resumen del CLI a info; silenciar log_records/retry para no
# inundar GitHub Actions. No respetar un RUST_LOG=error externo.
export RUST_LOG="jaiba_cli=info,jaiba_runtime::processors::log_records=off,jaiba_runtime::engine::executor::retry=warn,jaiba_runtime=warn,rdkafka=warn"
export JAIBA_TEST_KAFKA_BROKERS="$brokers"

if [[ $# -ge 1 ]]; then
  jaiba="$1"
else
  cargo build -p jaiba-cli --features release-core,kafka-driver --bin jaiba
  jaiba="${CARGO_TARGET_DIR:-target}/debug/jaiba"
fi

if [[ ! -x "$jaiba" ]]; then
  echo "ERROR: no hay binario ejecutable en $jaiba" >&2
  exit 1
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

create_topic() {
  local container="${JAIBA_KAFKA_CONTAINER:-}"
  if [[ -z "$container" ]]; then
    return 0
  fi
  if docker exec "$container" rpk topic create "$topic" -p 1 -r 1 >/dev/null 2>&1; then
    echo "tópico $topic creado (rpk)"
    return 0
  fi
  if docker exec "$container" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
    echo "tópico $topic creado (kafka-topics)"
    return 0
  fi
  echo "AVISO: no se pudo crear el tópico en el contenedor; se confía en auto-create" >&2
}

summary_field() {
  local log="$1" field="$2"
  sed -n "s/.*flow completed .*${field}=\([0-9][0-9]*\).*/\1/p" "$log" | tail -1
}

run_flow() {
  local label="$1" flow="$2" log="$3"
  echo "--- ${label} ---"
  if ! "$jaiba" "$flow" >"$log" 2>&1; then
    echo "ERROR: jaiba salió con error en ${label}" >&2
    cat "$log" >&2
    exit 1
  fi
  grep 'flow completed' "$log" || {
    echo "ERROR: no apareció el resumen 'flow completed'" >&2
    cat "$log" >&2
    exit 1
  }
}

echo "=== smoke Kafka (${messages} mensajes, brokers=${brokers}) ==="
create_topic

records=""
for n in $(seq 1 "$messages"); do
  records+="        - event_id: e-${n}"$'\n'
  records+="          n: ${n}"$'\n'
done

publish_flow="$work_dir/publish.yaml"
cat >"$publish_flow" <<YAML
id: kafka-smoke-publish
kafka_connections:
  bus:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiba-smoke-publish
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
engine:
  repository:
    enabled: false
processors:
  - id: records
    type: generate_records
    config:
      records:
${records}
  - id: publish
    type: publish_kafka
    config:
      connection: bus
      topic: ${topic}
      key_field: event_id
      queue_timeout_ms: 15000
connections:
  - from: records
    relationship: success
    to: publish
YAML

run_flow "1) publicar ${messages} mensajes" "$publish_flow" "$work_dir/publish.log"
pub_failed="$(summary_field "$work_dir/publish.log" failed)"
pub_kafka="$(summary_field "$work_dir/publish.log" kafka_published)"
echo "failed=${pub_failed:-?} kafka_published=${pub_kafka:-?}"
if [[ "${pub_failed:-}" != "0" || "${pub_kafka:-}" != "$messages" ]]; then
  echo "ERROR: se esperaban kafka_published=${messages} failed=0" >&2
  cat "$work_dir/publish.log" >&2
  exit 1
fi

consume_flow="$work_dir/consume.yaml"
cat >"$consume_flow" <<YAML
id: kafka-smoke-consume
kafka_connections:
  bus:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiba-smoke-consume
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
engine:
  repository:
    enabled: false
processors:
  - id: consume
    type: consume_kafka
    config:
      connection: bus
      topic: ${topic}
      group_id: ${group}
      auto_offset_reset: earliest
      max_poll_messages: $((messages + 10))
      max_poll_ms: 1000
      max_idle_ms: 60000
      decode: json
  - id: sink
    type: log_records
connections:
  - from: consume
    relationship: success
    to: sink
YAML

run_flow "2) consumir" "$consume_flow" "$work_dir/consume.log"
consumed="$(summary_field "$work_dir/consume.log" kafka_consumed)"
failed="$(summary_field "$work_dir/consume.log" failed)"
echo "kafka_consumed=${consumed:-?} failed=${failed:-?}"
if [[ "${failed:-}" != "0" || "${consumed:-}" != "$messages" ]]; then
  echo "ERROR: se esperaban kafka_consumed=${messages} failed=0" >&2
  cat "$work_dir/consume.log" >&2
  exit 1
fi

# Idle corto: el grupo ya está al día; no hace falta esperar 60s.
consume2_flow="$work_dir/consume2.yaml"
sed 's/max_idle_ms: 60000/max_idle_ms: 8000/' "$consume_flow" >"$consume2_flow"
run_flow "3) segundo consume (offsets confirmados → 0 mensajes)" "$consume2_flow" "$work_dir/consume2.log"
consumed2="$(summary_field "$work_dir/consume2.log" kafka_consumed)"
echo "kafka_consumed=${consumed2:-?}"
if [[ "${consumed2:-}" != "0" ]]; then
  echo "ERROR: el segundo consume no debió leer mensajes (kafka_consumed=${consumed2})" >&2
  cat "$work_dir/consume2.log" >&2
  exit 1
fi

echo "=== smoke Kafka OK ==="
