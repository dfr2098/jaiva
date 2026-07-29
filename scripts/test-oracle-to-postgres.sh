#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_password="${JAIBA_TRANSFER_TEST_PASSWORD:-$(openssl rand -hex 24)}"
cargo_bin="${CARGO_BIN:-$(command -v cargo || true)}"
if [[ -z "${cargo_bin}" && -x "${HOME}/.cargo/bin/cargo" ]]; then
  cargo_bin="${HOME}/.cargo/bin/cargo"
fi
if [[ -z "${cargo_bin}" ]]; then
  echo "No se encontró cargo en WSL." >&2
  exit 1
fi

oracle_sql="
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER=FREEPDB1;
DECLARE
  role_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO role_count
  FROM DBA_USERS
  WHERE USERNAME = 'JAIVA_FLOW_TEST';
  IF role_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER JAIVA_FLOW_TEST IDENTIFIED BY \"${test_password}\"';
  ELSE
    EXECUTE IMMEDIATE 'ALTER USER JAIVA_FLOW_TEST IDENTIFIED BY \"${test_password}\"';
  END IF;
END;
/
GRANT CREATE SESSION TO JAIVA_FLOW_TEST;
EXIT
"

printf '%s\n' "${oracle_sql}" |
  docker exec -i oracle19 sqlplus -s / as sysdba >/dev/null

docker exec dma_postgres psql \
  -v ON_ERROR_STOP=1 \
  -U dma \
  -d dma \
  -c "DO \$do\$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jaiva_flow_test') THEN
          CREATE ROLE jaiva_flow_test LOGIN;
        END IF;
      END \$do\$;
      ALTER ROLE jaiva_flow_test PASSWORD '${test_password}';
      CREATE TABLE IF NOT EXISTS public.jaiva_oracle_load_test (
        id BIGINT PRIMARY KEY,
        name TEXT NOT NULL,
        loaded_at TEXT
      );
      GRANT CONNECT ON DATABASE dma TO jaiva_flow_test;
      GRANT USAGE ON SCHEMA public TO jaiva_flow_test;
      GRANT SELECT, INSERT, UPDATE, DELETE
        ON public.jaiva_oracle_load_test TO jaiva_flow_test;" >/dev/null

export ORACLE_DATABASE_URL="oracle://JAIVA_FLOW_TEST:${test_password}@127.0.0.1:1521/FREEPDB1"
export DATABASE_URL="postgres://jaiva_flow_test:${test_password}@127.0.0.1:5432/dma?sslmode=disable"

cd "${repo_dir}"
"${cargo_bin}" run --features oracle-driver -- examples/oracle-to-postgres.yaml

echo
echo "Filas verificadas en PostgreSQL:"
docker exec dma_postgres psql \
  -U dma \
  -d dma \
  -c "SELECT id, name, loaded_at
      FROM public.jaiva_oracle_load_test
      WHERE id IN (1001, 1002)
      ORDER BY id;"
