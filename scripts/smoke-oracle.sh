#!/usr/bin/env bash
# Smoke Oracle (Beta): flow real query_oracle -> put_database (MERGE) con el
# binario `jaiba`, ejecutado dos veces para comprobar que el upsert es
# idempotente. Lo usa .github/workflows/oracle.yml; ver docs/oracle.md.
#
# Requisitos:
#   - Contenedor gvenzl/oracle-xe u oracle-free con sqlplus
#     (JAIBA_ORACLE_CONTAINER, por defecto jaiba-oracle-test).
#   - Instant Client en LD_LIBRARY_PATH.
#   - JAIBA_TEST_ORACLE_PASSWORD (+ HOST/PORT/SERVICE/USER opcionales).
#
# Uso: ./scripts/smoke-oracle.sh [ruta-al-binario-jaiba]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

container="${JAIBA_ORACLE_CONTAINER:-jaiba-oracle-test}"
password="${JAIBA_TEST_ORACLE_PASSWORD:?define JAIBA_TEST_ORACLE_PASSWORD}"
host="${JAIBA_TEST_ORACLE_HOST:-127.0.0.1}"
port="${JAIBA_TEST_ORACLE_PORT:-11521}"
service="${JAIBA_TEST_ORACLE_SERVICE:-FREEPDB1}"
user="${JAIBA_TEST_ORACLE_USER:-dma_test}"
rows="${JAIBA_SMOKE_ORACLE_ROWS:-5000}"

if [[ $# -ge 1 ]]; then
  jaiba="$1"
else
  cargo build -p jaiba-cli --features release-core,oracle-driver --bin jaiba
  jaiba="${CARGO_TARGET_DIR:-target}/debug/jaiba"
fi

sql() {
  # sqlplus dentro del contenedor: no requiere cliente completo en el host.
  docker exec -i "$container" sqlplus -s "${user}/${password}@localhost:1521/${service}"
}

echo "=== smoke oracle (${rows} filas) ==="

echo "--- 1) tablas de prueba ---"
sql <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
BEGIN
  FOR t IN (SELECT table_name FROM user_tables
            WHERE table_name IN ('JAIBA_SMOKE_SOURCE', 'JAIBA_SMOKE_TARGET')) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' PURGE';
  END LOOP;
END;
/
CREATE TABLE jaiba_smoke_source (
  id NUMBER(10) PRIMARY KEY,
  name VARCHAR2(40),
  amount NUMBER(12,2),
  created DATE,
  seen TIMESTAMP(3) WITH TIME ZONE
);
INSERT INTO jaiba_smoke_source
SELECT LEVEL,
       CASE WHEN MOD(LEVEL, 10) = 0 THEN NULL ELSE 'cliente-' || LEVEL END,
       ROUND(LEVEL * 1.37, 2),
       DATE '2026-01-01' + LEVEL / 24,
       TIMESTAMP '2026-01-01 00:00:00.250 -06:00' + NUMTODSINTERVAL(LEVEL, 'MINUTE')
FROM dual CONNECT BY LEVEL <= ${rows};
CREATE TABLE jaiba_smoke_target AS SELECT * FROM jaiba_smoke_source WHERE 1 = 0;
ALTER TABLE jaiba_smoke_target ADD PRIMARY KEY (id);
COMMIT;
EXIT
SQL

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
flow="$work_dir/oracle-smoke.yaml"
cat >"$flow" <<YAML
id: oracle-smoke

database_connections:
  oracle:
    type: oracle
    url_env: JAIBA_SMOKE_ORACLE_URL
    max_connections: 2

engine:
  repository:
    enabled: false

processors:
  - id: read
    type: query_oracle
    config:
      connection: oracle
      query: SELECT id, name, amount, created, seen FROM jaiba_smoke_source ORDER BY id
      batch_size: 700

  - id: write
    type: put_database
    config:
      connection: oracle
      table: JAIBA_SMOKE_TARGET
      mode: upsert
      batch_size: 1000
      columns:
        id: ID
        name: NAME
        amount: AMOUNT
        created: CREATED
        seen: SEEN
      conflict_columns: [ID]

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

export JAIBA_SMOKE_ORACLE_URL
JAIBA_SMOKE_ORACLE_URL="$(python3 -c 'import sys, urllib.parse as u; print("oracle://%s:%s@%s:%s/%s" % (u.quote(sys.argv[1], safe=""), u.quote(sys.argv[2], safe=""), *sys.argv[3:]))' \
  "$user" "$password" "$host" "$port" "$service")"

for run in 1 2; do
  echo "--- 2.${run}) flow query_oracle -> put_database (MERGE) ---"
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
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT (SELECT COUNT(*) FROM jaiba_smoke_target) || ' ' ||
       (SELECT COUNT(*) FROM (
          SELECT id, name, amount, created, seen FROM jaiba_smoke_source
          MINUS
          SELECT id, name, amount, created, seen FROM jaiba_smoke_target))
FROM dual;
EXIT
SQL
)"
read -r target_rows missing_rows <<<"$(echo "$result" | tr -s '[:space:]' ' ')"
echo "destino=${target_rows} diferencias=${missing_rows}"
if [[ "$target_rows" != "$rows" || "$missing_rows" != "0" ]]; then
  echo "el destino no coincide con el origen" >&2
  exit 1
fi

echo
echo "=== smoke oracle OK ==="
