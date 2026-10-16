#!/usr/bin/env bash
# Smoke Kafka (Beta): publica N mensajes con el binario `jaiba`, los consume con
# otro flow y comprueba que llegaron todos y que el offset quedó confirmado.
# Lo usa .github/workflows/kafka.yml; ver docs/kafka.md.
#
# Requisitos:
#   - Broker Kafka compatible (Redpanda o Apache Kafka) en
#     JAIBA_TEST_KAFKA_BROKERS (por defecto 127.0.0.1:29092).
#
# Uso: ./scripts/smoke-kafka.sh [ruta-al-binario-jaiba]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

brokers="${JAIBA_TEST_KAFKA_BROKERS:-127.0.0.1:29092}"
messages="${JAIBA_SMOKE_KAFKA_MESSAGES:-200}"
suffix="$(date +%s)-$$"
topic="jaiba.smoke.${suffix}"
group="jaiba-smoke-${suffix}"

if [[ $# -ge 1 ]]; then
  jaiba="$1"
else
  cargo build -p jaiba-cli --features release-core,kafka-driver --bin jaiba
  jaiba="${CARGO_TARGET_DIR:-target}/debug/jaiba"
fi

export JAIBA_TEST_KAFKA_BROKERS="$brokers"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

if [[ -n "${JAIBA_KAFKA_CONTAINER:-}" ]]; then
  docker exec "$JAIBA_KAFKA_CONTAINER" rpk topic create "$topic" -p 1 -r 1 >/dev/null 2>&1 || true
fi

echo "=== smoke Kafka (${messages} mensajes, brokers=${brokers}) ==="

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
      queue_timeout_ms: 10000
connections:
  - from: records
    relationship: success
    to: publish
YAML

echo "--- 1) publicar ${messages} mensajes ---"
"$jaiba" "$publish_flow" >"$work_dir/publish.log" 2>&1 || {
  cat "$work_dir/publish.log" >&2
  exit 1
}
cat "$work_dir/publish.log"
if ! grep -q 'failed=0' "$work_dir/publish.log"; then
  echo "ERROR: la publicación reportó fallos" >&2
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
      max_idle_ms: 15000
      decode: json
  - id: inspect
    type: log_records
connections:
  - from: consume
    relationship: success
    to: inspect
YAML

echo "--- 2) consumir ---"
"$jaiba" "$consume_flow" >"$work_dir/consume.log" 2>&1 || {
  cat "$work_dir/consume.log" >&2
  exit 1
}
cat "$work_dir/consume.log"
emitted="$(sed -n 's/.*emitted=\([0-9][0-9]*\).*/\1/p' "$work_dir/consume.log" | tail -1)"
failed="$(sed -n 's/.*failed=\([0-9][0-9]*\).*/\1/p' "$work_dir/consume.log" | tail -1)"
echo "emitted=${emitted:-?} failed=${failed:-?}"
if [[ "${failed:-1}" != "0" || "${emitted:-0}" != "$messages" ]]; then
  echo "ERROR: se esperaban emitted=${messages} failed=0" >&2
  exit 1
fi

echo "--- 3) segundo consume (offsets confirmados → 0 mensajes) ---"
"$jaiba" "$consume_flow" >"$work_dir/consume2.log" 2>&1 || {
  cat "$work_dir/consume2.log" >&2
  exit 1
}
cat "$work_dir/consume2.log"
emitted2="$(sed -n 's/.*emitted=\([0-9][0-9]*\).*/\1/p' "$work_dir/consume2.log" | tail -1)"
if [[ "${emitted2:-1}" != "0" ]]; then
  echo "ERROR: el segundo consume no debió emitir paquetes (emitted=${emitted2})" >&2
  exit 1
fi

echo "=== smoke Kafka OK ==="
