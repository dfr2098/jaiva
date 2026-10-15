#!/usr/bin/env bash
# Smoke MongoDB (Beta): flow real query_mongodb -> put_mongodb (upsert) con el
# binario `jaiba`, ejecutado dos veces para comprobar que el upsert es
# idempotente y que la copia conserva cada tipo BSON. Lo usa
# .github/workflows/mongodb.yml; ver docs/mongodb.md.
#
# Requisitos:
#   - Contenedor mongo con mongosh (JAIBA_MONGODB_CONTAINER, por defecto
#     jaiba-mongo8-test).
#   - JAIBA_TEST_MONGODB_PASSWORD (+ HOST/PORT/DATABASE/USER opcionales).
#     El usuario se autentica contra la base admin.
#
# Uso: ./scripts/smoke-mongodb.sh [ruta-al-binario-jaiba]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

container="${JAIBA_MONGODB_CONTAINER:-jaiba-mongo8-test}"
password="${JAIBA_TEST_MONGODB_PASSWORD:?define JAIBA_TEST_MONGODB_PASSWORD}"
host="${JAIBA_TEST_MONGODB_HOST:-127.0.0.1}"
port="${JAIBA_TEST_MONGODB_PORT:-27028}"
database="${JAIBA_TEST_MONGODB_DATABASE:-jaiba}"
user="${JAIBA_TEST_MONGODB_USER:-jaiba}"
documents="${JAIBA_SMOKE_MONGODB_DOCUMENTS:-5000}"

if [[ $# -ge 1 ]]; then
  jaiba="$1"
else
  cargo build -p jaiba-cli --features release-core,mongodb-driver --bin jaiba
  jaiba="${CARGO_TARGET_DIR:-target}/debug/jaiba"
fi

mongo() {
  # mongosh dentro del contenedor: no hace falta instalarlo en el host. Lee el
  # script de stdin y lo pasa con --eval (por stdin mongosh actúa como REPL).
  local script
  script="$(cat)"
  docker exec "$container" mongosh --quiet -u "$user" -p "$password" \
    --authenticationDatabase admin "$database" --eval "$script"
}

echo "=== smoke MongoDB (${documents} documentos) ==="

echo "--- 1) colecciones de prueba ---"
mongo <<JS
db.jaiba_smoke_source.drop();
db.jaiba_smoke_target.drop();
const documents = [];
for (let n = 1; n <= ${documents}; n++) {
  documents.push({
    _id: n,
    name: n % 10 === 0 ? null : "cliente-" + n,
    amount: NumberDecimal((n * 1.37).toFixed(2)),
    counter: NumberLong(String(n)),
    ratio: n / 7,
    created: new Date(Date.UTC(2026, 0, 1) + n * 60000),
    ref: ObjectId(),
    tags: ["a", "b", n],
    nested: { n: NumberInt(n), ok: n % 2 === 0 },
  });
}
db.jaiba_smoke_source.insertMany(documents);
print("origen=" + db.jaiba_smoke_source.countDocuments());
JS

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
flow="$work_dir/mongodb-smoke.yaml"
cat >"$flow" <<YAML
id: mongodb-smoke

database_connections:
  mongo:
    type: mongodb
    url_env: JAIBA_SMOKE_MONGODB_URL
    max_connections: 2

engine:
  repository:
    enabled: false

processors:
  - id: read
    type: query_mongodb
    config:
      connection: mongo
      collection: jaiba_smoke_source
      sort: { _id: 1 }
      batch_size: 700
      json_format: canonical

  - id: write
    type: put_mongodb
    config:
      connection: mongo
      collection: jaiba_smoke_target
      mode: upsert
      batch_size: 1000

  - id: failures
    type: log_records

connections:
  - from: read
    relationship: success
    to: write
  - from: write
    relationship: failure
    to: failures
YAML

export JAIBA_SMOKE_MONGODB_URL
JAIBA_SMOKE_MONGODB_URL="$(python3 -c 'import sys, urllib.parse as u; print("mongodb://%s:%s@%s:%s/%s?authSource=admin" % (u.quote(sys.argv[1], safe=""), u.quote(sys.argv[2], safe=""), *sys.argv[3:]))' \
  "$user" "$password" "$host" "$port" "$database")"

for run in 1 2; do
  echo "--- 2.${run}) flow query_mongodb -> put_mongodb (upsert) ---"
  "$jaiba" "$flow" >"$work_dir/run-${run}.log" 2>&1 || {
    cat "$work_dir/run-${run}.log" >&2
    exit 1
  }
  # El proceso termina con 0 aunque haya paquetes en failure; se revisa el log.
  if grep -q 'processor failed' "$work_dir/run-${run}.log"; then
    cat "$work_dir/run-${run}.log" >&2
    echo "el flow envió documentos a failure" >&2
    exit 1
  fi
done

echo "--- 3) origen y destino idénticos (tipos incluidos) ---"
result="$(mongo <<'JS'
let differences = 0;
db.jaiba_smoke_source.find().forEach((source) => {
  const target = db.jaiba_smoke_target.findOne({ _id: source._id });
  if (EJSON.stringify(source, { relaxed: false }) !== EJSON.stringify(target, { relaxed: false })) {
    differences++;
  }
});
print(db.jaiba_smoke_target.countDocuments() + " " + differences);
JS
)"
read -r target_documents differences <<<"$result"
echo "destino=${target_documents} diferencias=${differences}"
if [[ "$target_documents" != "$documents" || "$differences" != "0" ]]; then
  echo "el destino no coincide con el origen" >&2
  exit 1
fi

echo
echo "=== smoke MongoDB OK ==="
