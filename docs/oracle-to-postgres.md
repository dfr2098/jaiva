# Prueba Oracle → PostgreSQL

Esta prueba valida una carga real entre los contenedores locales de Oracle Free
y PostgreSQL usando Jaiba. Es repetible: cada ejecución actualiza las mismas dos
filas mediante `upsert` y no elimina volúmenes ni tablas existentes.

```mermaid
flowchart LR
    O[(Oracle FREEPDB1)] -->|query_oracle| J[Jaiba]
    J -->|objetos JSON por lotes| A[auto_destination]
    A -->|upsert por id| P[(PostgreSQL dma)]
```

## Requisitos

- WSL con Rust y Cargo.
- Contenedor `oracle19` accesible en `127.0.0.1:1521`.
- Contenedor `dma_postgres` accesible en `127.0.0.1:5432`.
- Oracle Instant Client de 64 bits registrado en el cargador de WSL.
- `sqlplus` y `psql` disponibles dentro de sus respectivos contenedores.

Comprueba los motores:

```bash
docker ps --filter name=oracle19 --filter name=dma_postgres
```

## Ejecución

Desde WSL:

```bash
cd /mnt/d/dma/jaiva
bash scripts/test-oracle-to-postgres.sh
```

El script:

1. Genera una contraseña aleatoria que solo vive durante la ejecución.
2. Crea o actualiza `JAIVA_FLOW_TEST` dentro de `FREEPDB1` y le concede
   únicamente `CREATE SESSION`.
3. Crea o actualiza `jaiva_flow_test` en PostgreSQL.
4. Crea, si no existe, `public.jaiva_oracle_load_test`.
5. Concede al usuario técnico acceso solamente a esa tabla.
6. Exporta `ORACLE_DATABASE_URL` y `DATABASE_URL` sin guardar secretos en YAML.
7. Ejecuta [el flujo de ejemplo](../examples/oracle-to-postgres.yaml).
8. Consulta PostgreSQL para verificar los IDs `1001` y `1002`.

Un resultado correcto termina así:

```text
 id   | name                  | loaded_at
------+-----------------------+--------------------------------
 1001 | Fila 1 desde Oracle   | 2026-07-29T22:29:56.360+00:00
 1002 | Fila 2 desde Oracle   | 2026-07-29T22:29:56.360+00:00
```

El aviso `relation "jaiva_oracle_load_test" already exists, skipping` es normal:
confirma que PostgreSQL conservó la tabla de una ejecución anterior.

## Funcionamiento

`query_oracle` acepta consultas cuyo primer término sea `SELECT` o `WITH`.
Convierte cada fila en un objeto JSON, normaliza los nombres de columnas sin
comillas a minúsculas y emite paquetes según `batch_size`.

La consulta de prueba usa `DUAL`, por lo que no crea objetos en Oracle. Produce
registros con esta forma:

```json
{
  "id": 1001,
  "name": "Fila 1 desde Oracle",
  "loaded_at": "2026-07-29T22:29:56.360+00:00"
}
```

`auto_destination` detecta PostgreSQL como destino. Debido a
`conflict_columns: [id]`, selecciona `upsert`: una segunda ejecución actualiza
las filas en lugar de duplicarlas.

## Fan-out multi-DB (prueba): Oracle → PostgreSQL + MongoDB

Ejemplo canónico de prueba del motor **1 → N** (stack conservador):

[`examples/multi-db-fanout.yaml`](../examples/multi-db-fanout.yaml)

Alias histórico (mismo flujo):
[`examples/oracle-to-postgres-mongodb.yaml`](../examples/oracle-to-postgres-mongodb.yaml)

```mermaid
flowchart LR
  Oracle[query_oracle] -->|success| PG[auto_destination Postgres]
  Oracle -->|success| Mongo[put_mongodb]
  Oracle -->|failure| Errors[log_records]
  PG -->|failure| Errors
  Mongo -->|failure| Errors
```

### Validación comprobada (2 de agosto de 2026)

En el entorno de pruebas local se validó de punta a punta:

| Flujo | Filas | Resultado |
|---|---|---|
| `multi-db-fanout.yaml` (bajo) | 2 (`DUAL`) | `failed=0` → Postgres + Mongo |
| `oracle-fanout-stress.yaml` | ~10 000 (`CONNECT BY`) | `failed=0` → Postgres + Mongo (+ tap log) |

Comprobado en UI: MongoDB Compass (`dma_test.jaiva_oracle_stress`) y DBeaver
(`public.jaiva_oracle_stress`, 10 000 filas).

### Instant Client en el host

Jaiba corre en el host (`cargo run`), no dentro del contenedor Oracle. Hace
falta **Oracle Instant Client** visible para el cargador dinámico.

Si el client vive solo en el contenedor `dma_test_oracle_client`:

```bash
mkdir -p "$HOME/oracle"
docker cp dma_test_oracle_client:/opt/oracle/instantclient_23_26 "$HOME/oracle/"
export LD_LIBRARY_PATH="$HOME/oracle/instantclient_23_26${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

Comprueba: `ls "$LD_LIBRARY_PATH"/libclntsh.so*`. Sin esto, `read_oracle` falla
(suele ser `DPI-1047`) y el paquete va a `errors`.

### Contenedores y puertos del entorno de pruebas

| Servicio | Contenedor | Host |
|---|---|---|
| Oracle | `dma_test_oracle` | `127.0.0.1:11521` → 1521, servicio `FREEPDB1` |
| PostgreSQL | `dma_postgres` | `127.0.0.1:55432` |
| MongoDB | `dma_test_mongodb` | `127.0.0.1:27018` |

Oracle debe estar `healthy` antes de correr. En hosts ~16 GiB conviene el modo
ligero: [`../scripts/jaiva-light-containers.sh`](../scripts/jaiva-light-containers.sh)
(`oracle` / `fanout` / `status`). Ver también [operations.md](operations.md).

### Ejecución (flujo bajo)

Tablas/colecciones:

```sql
-- Postgres (una vez)
CREATE TABLE IF NOT EXISTS public.jaiva_oracle_load_test (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  loaded_at text NOT NULL
);
```

Mongo crea `jaiva_oracle_load_test` al escribir.

```bash
export LD_LIBRARY_PATH="$HOME/oracle/instantclient_23_26${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Postgres: usuario/clave del .env del entorno (no uses placeholders TU_CLAVE)
source <(sed -n 's/^POSTGRES_APP_PASSWORD=/export JAIBA_PG_PASS=/p; s/^POSTGRES_APP_USER=/export JAIBA_PG_USER=/p; s/^POSTGRES_DB=/export JAIBA_PG_DB=/p; s/^POSTGRES_PORT=/export JAIBA_PG_PORT=/p' \
  ~/Escritorio/DMA_CORE/DMA_CORE/.env)
ENC="$(python3 -c 'import os,urllib.parse; print(urllib.parse.quote(os.environ["JAIBA_PG_PASS"], safe=""))')"
export DATABASE_URL="postgres://${JAIBA_PG_USER}:${ENC}@127.0.0.1:${JAIBA_PG_PORT:-55432}/${JAIBA_PG_DB:-dma}"

# Oracle / Mongo del compose de pruebas
export ORACLE_DATABASE_URL='oracle://dma_test:TestOracleUser_2026@127.0.0.1:11521/FREEPDB1'
export MONGODB_URL='mongodb://dma_test:TestMongoUser_2026!@127.0.0.1:27018/dma_test?authSource=admin'

cd ~/Escritorio/jaiva
cargo run --features oracle-driver,mongodb-driver -- examples/multi-db-fanout.yaml
```

Salida esperada: `flow completed … failed=0` y aristas `success` en
`load_postgres` y `load_mongo`. Una segunda ejecución es idempotente (upsert por
`id`).

### Estrés (~10 000 filas)

[`examples/oracle-fanout-stress.yaml`](../examples/oracle-fanout-stress.yaml):
`CONNECT BY LEVEL <= 10000`, fan-out ×3 (Postgres + Mongo + `log_records`),
lotes de 500, `max_concurrency: 16`, repo `.jaiva/repository-stress.db`.

```sql
CREATE TABLE IF NOT EXISTS public.jaiva_oracle_stress (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  loaded_at text NOT NULL
);
```

```bash
# mismas variables + LD_LIBRARY_PATH que arriba
cargo run --features oracle-driver,mongodb-driver -- examples/oracle-fanout-stress.yaml
```

Sin la tabla Postgres verás `failed>0` y muchos `retried` aunque Mongo cargue
bien. Con la tabla: `failed=0` y ~10 000 filas/documentos en ambos destinos.

Para aflojar: baja el `LEVEL` (p. ej. 1000). No es un flujo de producción.

### Cómo verificar

```bash
# Postgres
docker exec -i dma_postgres psql -U "$JAIBA_PG_USER" -d "$JAIBA_PG_DB" \
  -c 'SELECT COUNT(*) FROM public.jaiva_oracle_stress;'

# Mongo
docker exec dma_test_mongodb mongosh --quiet \
  -u dma_test -p 'TestMongoUser_2026!' --authenticationDatabase admin dma_test \
  --eval 'db.jaiva_oracle_stress.countDocuments({})'
```

También: MongoDB Compass → `dma_test.jaiva_oracle_stress`; DBeaver →
`public.jaiva_oracle_stress`.

## Adaptación a una tabla real

Cambia la consulta y el mapeo en el YAML:

```yaml
- id: read_oracle
  type: query_oracle
  config:
    connection: source
    query: >-
      SELECT CUSTOMER_ID, CUSTOMER_NAME, UPDATED_AT
      FROM APP.CUSTOMERS
      WHERE UPDATED_AT >= TIMESTAMP '2026-01-01 00:00:00'
    batch_size: 1000

- id: load_postgres
  type: auto_destination
  config:
    connection: destination
    table: public.customers
    mode: auto
    columns:
      customer_id: customer_id
      customer_name: customer_name
      updated_at: updated_at
    conflict_columns:
      - customer_id
```

Antes de una carga real, crea la tabla de destino, concede permisos mínimos y
prueba primero con un filtro pequeño.

## Diagnóstico

### `DPI-1047`

Jaiba no encuentra `libclntsh.so`. Verifica:

```bash
/sbin/ldconfig -p | grep libclntsh
ldd /opt/oracle/instantclient_23_26/libclntsh.so.23.1 | grep "not found"
```

Jaiba debe iniciarse dentro del mismo WSL donde está registrado Instant Client.

### `ORA-01005`, `ORA-01017` o `ORA-28009`

- `ORA-01005`: contraseña vacía.
- `ORA-01017`: usuario o contraseña incorrectos.
- `ORA-28009`: se intentó usar `SYS` sin modo `SYSDBA`.

La prueba evita estos casos usando `JAIVA_FLOW_TEST`; el flujo no utiliza `SYS`.

### PostgreSQL rechaza la contraseña

Modificar `POSTGRES_PASSWORD` después de crear el volumen no cambia la
contraseña almacenada en PostgreSQL. No borres el volumen: restaura la
contraseña anterior o rota el rol explícitamente.

## Seguridad

- No guardes URLs con contraseñas dentro del YAML o Git.
- Usa perfiles del Connection Manager o variables de entorno.
- Emplea usuarios separados para extracción y carga.
- Concede solo `SELECT` en Oracle y los permisos necesarios sobre las tablas de
  destino en PostgreSQL.
- En producción usa un Secret Manager, Vault o mecanismo equivalente.
