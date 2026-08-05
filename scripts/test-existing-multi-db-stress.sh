#!/usr/bin/env bash
# EstrÃ©s de 10k registros contra Oracle, PostgreSQL y MongoDB existentes.
# No administra el ciclo de vida de ningÃºn contenedor.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
oracle_container="${JAIBA_TEST_ORACLE_CONTAINER:-oracle19}"
postgres_container="${JAIBA_TEST_POSTGRES_CONTAINER:-dma_postgres}"
mongo_container="${JAIBA_TEST_MONGODB_CONTAINER:-mongodb-pruebas}"
mongo_database="${JAIBA_TEST_MONGODB_DATABASE:-pruebas}"
mongo_user="${JAIBA_TEST_MONGODB_USER:-admin}"
mongo_password="${JAIBA_TEST_MONGODB_PASSWORD:-}"
mongo_port="${JAIBA_TEST_MONGODB_PORT:-27017}"
expected_rows=10000
docker_windows="${JAIBA_WINDOWS_DOCKER:-/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe}"

if [[ -z "$mongo_password" ]]; then
  echo "ERROR: define JAIBA_TEST_MONGODB_PASSWORD." >&2
  exit 1
fi
if [[ ! "$mongo_database" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "ERROR: nombre MongoDB no vÃ¡lido: $mongo_database" >&2
  exit 1
fi
if [[ ! -x "$docker_windows" ]]; then
  echo "ERROR: no se encontrÃ³ docker.exe en '$docker_windows'." >&2
  exit 1
fi

for port in 1521 5432 "$mongo_port"; do
  if ! timeout 3 bash -c "echo >/dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1; then
    echo "ERROR: 127.0.0.1:${port} no responde; el script no iniciarÃ¡ servicios." >&2
    exit 1
  fi
done

test_password="${JAIBA_STRESS_TEST_PASSWORD:-$(openssl rand -hex 24)}"

oracle_sql="
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER=FREEPDB1;
DECLARE
  user_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO user_count FROM DBA_USERS WHERE USERNAME = 'JAIVA_FLOW_TEST';
  IF user_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER JAIVA_FLOW_TEST IDENTIFIED BY \"${test_password}\"';
  ELSE
    EXECUTE IMMEDIATE 'ALTER USER JAIVA_FLOW_TEST IDENTIFIED BY \"${test_password}\"';
  END IF;
END;
/
GRANT CREATE SESSION TO JAIVA_FLOW_TEST;
EXIT
"
printf '%s\n' "$oracle_sql" | docker exec -i "$oracle_container" sqlplus -s / as sysdba >/dev/null

docker exec "$postgres_container" psql -v ON_ERROR_STOP=1 -U dma -d dma -c \
  "DO \$do\$ BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jaiva_flow_test') THEN
       CREATE ROLE jaiva_flow_test LOGIN;
     END IF;
   END \$do\$;
   ALTER ROLE jaiva_flow_test PASSWORD '${test_password}';
   CREATE TABLE IF NOT EXISTS public.jaiva_oracle_stress (
     id BIGINT PRIMARY KEY,
     name TEXT NOT NULL,
     loaded_at TEXT NOT NULL
   );
   TRUNCATE TABLE public.jaiva_oracle_stress;
   GRANT CONNECT ON DATABASE dma TO jaiva_flow_test;
   GRANT USAGE ON SCHEMA public TO jaiva_flow_test;
   GRANT SELECT, INSERT, UPDATE, DELETE ON public.jaiva_oracle_stress TO jaiva_flow_test;" \
  >/dev/null

"$docker_windows" exec "$mongo_container" mongosh --quiet \
  --username "$mongo_user" --password "$mongo_password" --authenticationDatabase admin \
  --eval "db.getSiblingDB('${mongo_database}').jaiva_oracle_stress.deleteMany({})" \
  >/dev/null

encoded_test_password="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$test_password")"
encoded_mongo_user="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$mongo_user")"
encoded_mongo_password="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$mongo_password")"
export ORACLE_DATABASE_URL="oracle://JAIVA_FLOW_TEST:${encoded_test_password}@127.0.0.1:1521/FREEPDB1"
export DATABASE_URL="postgres://jaiva_flow_test:${encoded_test_password}@127.0.0.1:5432/dma?sslmode=disable"
export MONGODB_URL="mongodb://${encoded_mongo_user}:${encoded_mongo_password}@127.0.0.1:${mongo_port}/${mongo_database}?authSource=admin"

cd "$repo_dir"
echo "Compilando Jaiba fuera de la mediciÃ³n..."
cargo build --features oracle-driver,mongodb-driver --bin jaiva-flow
started_at="$(date +%s)"
RUST_LOG="${RUST_LOG:-jaiba_cli=info,jaiba_runtime::engine::executor=info,jaiba_runtime::processors::log_records=warn}" \
  target/debug/jaiva-flow examples/oracle-fanout-stress.yaml
elapsed_seconds="$(( $(date +%s) - started_at ))"

postgres_count="$(docker exec "$postgres_container" psql -U dma -d dma -tAc \
  'SELECT COUNT(*) FROM public.jaiva_oracle_stress' | tr -d '[:space:]')"
mongo_count="$("$docker_windows" exec "$mongo_container" mongosh --quiet \
  --username "$mongo_user" --password "$mongo_password" --authenticationDatabase admin \
  --eval "db.getSiblingDB('${mongo_database}').jaiva_oracle_stress.countDocuments({})" | tr -d '\r[:space:]')"

if [[ "$postgres_count" != "$expected_rows" || "$mongo_count" != "$expected_rows" ]]; then
  echo "ERROR: conteo inesperado (PostgreSQL=$postgres_count, MongoDB=$mongo_count)." >&2
  exit 1
fi

echo
echo "EstrÃ©s multi-DB OK: ${expected_rows} registros en PostgreSQL y MongoDB."
echo "Tiempo del flujo Jaiba: ${elapsed_seconds}s."
