#!/usr/bin/env bash
# Smoke MySQL/MariaDB (Beta): flow real query_mysql -> put_database (upsert)
# con el binario `jaiba`, ejecutado dos veces para comprobar que el upsert es
# idempotente. Lo usa .github/workflows/mysql.yml; ver docs/mysql.md.
#
# Requisitos:
#   - Contenedor mysql o mariadb con su cliente
#     (JAIBA_MYSQL_CONTAINER, por defecto jaiba-mysql-test).
#   - JAIBA_TEST_MYSQL_PASSWORD (+ HOST/PORT/DATABASE/USER opcionales).
#   - JAIBA_SMOKE_MYSQL_KIND: mysql (por defecto) o mariadb.
#
# Uso: ./scripts/smoke-mysql.sh [ruta-al-binario-jaiba]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

container="${JAIBA_MYSQL_CONTAINER:-jaiba-mysql-test}"
password="${JAIBA_TEST_MYSQL_PASSWORD:?define JAIBA_TEST_MYSQL_PASSWORD}"
host="${JAIBA_TEST_MYSQL_HOST:-127.0.0.1}"
port="${JAIBA_TEST_MYSQL_PORT:-13306}"
database="${JAIBA_TEST_MYSQL_DATABASE:-jaiba}"
user="${JAIBA_TEST_MYSQL_USER:-jaiba}"
kind="${JAIBA_SMOKE_MYSQL_KIND:-mysql}"
rows="${JAIBA_SMOKE_MYSQL_ROWS:-5000}"

if [[ $# -ge 1 ]]; then
  jaiba="$1"
else
  cargo build -p jaiba-cli --features release-core --bin jaiba
  jaiba="${CARGO_TARGET_DIR:-target}/debug/jaiba"
fi

client="$(docker exec "$container" sh -c 'command -v mariadb || command -v mysql')"
sql() {
  # Cliente dentro del contenedor: no hace falta instalarlo en el host.
  docker exec -i -e MYSQL_PWD="$password" "$container" "$client" -u"$user" -N -B "$database"
}

echo "=== smoke ${kind} (${rows} filas) ==="

echo "--- 1) tablas de prueba ---"
sql <<SQL
DROP TABLE IF EXISTS jaiba_smoke_source, jaiba_smoke_target;
CREATE TABLE jaiba_smoke_source (
  id INT PRIMARY KEY,
  name VARCHAR(40) NULL,
  amount DECIMAL(12,2) NOT NULL,
  created DATETIME(3) NOT NULL,
  seen TIMESTAMP(3) NULL,
  payload JSON NULL
);
INSERT INTO jaiba_smoke_source
SELECT n,
       CASE WHEN MOD(n, 10) = 0 THEN NULL ELSE CONCAT('cliente-', n) END,
       ROUND(n * 1.37, 2),
       TIMESTAMP('2026-01-01 00:00:00.250') + INTERVAL n MINUTE,
       TIMESTAMP('2026-01-01 06:00:00.500') + INTERVAL n SECOND,
       JSON_OBJECT('n', n, 'tags', JSON_ARRAY('a', 'b'))
FROM (
  SELECT a.d + 10 * b.d + 100 * c.d + 1000 * e.d + 10000 * f.d AS n
  FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
       (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
       (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c,
       (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e,
       (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) f
) numbers
WHERE n BETWEEN 1 AND ${rows};
CREATE TABLE jaiba_smoke_target LIKE jaiba_smoke_source;
SQL

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
flow="$work_dir/mysql-smoke.yaml"
cat >"$flow" <<YAML
id: mysql-smoke

database_connections:
  source:
    type: ${kind}
    url_env: JAIBA_SMOKE_MYSQL_URL
    max_connections: 2

engine:
  repository:
    enabled: false

processors:
  - id: read
    type: query_mysql
    config:
      connection: source
      query: SELECT id, name, amount, created, seen, payload FROM jaiba_smoke_source ORDER BY id
      batch_size: 700

  - id: write
    type: put_database
    config:
      connection: source
      table: jaiba_smoke_target
      mode: upsert
      batch_size: 1000
      columns:
        id: id
        name: name
        amount: amount
        created: created
        seen: seen
        payload: payload
      conflict_columns: [id]

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

export JAIBA_SMOKE_MYSQL_URL
JAIBA_SMOKE_MYSQL_URL="$(python3 -c 'import sys, urllib.parse as u; print("mysql://%s:%s@%s:%s/%s" % (u.quote(sys.argv[1], safe=""), u.quote(sys.argv[2], safe=""), *sys.argv[3:]))' \
  "$user" "$password" "$host" "$port" "$database")"

for run in 1 2; do
  echo "--- 2.${run}) flow query_mysql -> put_database (upsert) ---"
  "$jaiba" "$flow" >"$work_dir/run-${run}.log" 2>&1 || {
    cat "$work_dir/run-${run}.log" >&2
    exit 1
  }
  # El proceso termina con 0 aunque haya paquetes en failure; se revisa el log.
  if grep -q 'processor failed' "$work_dir/run-${run}.log"; then
    cat "$work_dir/run-${run}.log" >&2
    echo "el flow envió registros a failure" >&2
    exit 1
  fi
done

echo "--- 3) origen y destino idénticos ---"
result="$(sql <<'SQL'
SELECT (SELECT COUNT(*) FROM jaiba_smoke_target),
       (SELECT COUNT(*) FROM jaiba_smoke_source s
        LEFT JOIN jaiba_smoke_target t
          ON t.id = s.id AND t.name <=> s.name AND t.amount = s.amount
         AND t.created = s.created AND t.seen <=> s.seen
         AND JSON_EXTRACT(t.payload, '$') <=> JSON_EXTRACT(s.payload, '$')
        WHERE t.id IS NULL);
SQL
)"
read -r target_rows missing_rows <<<"$result"
echo "destino=${target_rows} diferencias=${missing_rows}"
if [[ "$target_rows" != "$rows" || "$missing_rows" != "0" ]]; then
  echo "el destino no coincide con el origen" >&2
  exit 1
fi

echo
echo "=== smoke ${kind} OK ==="
