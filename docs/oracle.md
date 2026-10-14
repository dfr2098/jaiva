# Oracle

Estado: **Beta** desde v0.8.0 (ver [criterios de madurez](product-roadmap.md#criterios-de-madurez)).

| Criterio Beta | Cómo se cumple |
| --- | --- |
| Pruebas reales automáticas | Workflow [`oracle.yml`](../.github/workflows/oracle.yml): cada noche y en cada PR que toca Oracle, contra Oracle Database Free 23ai |
| Memoria acotada | Lectura por streaming y escritura por lotes (array DML) |
| Fallos definidos | Pool con espera máxima, timeout por llamada, cancelación, errores reintentables vs. rechazos |
| Tipos documentados | [§ Tipos](#tipos), cubiertos por pruebas reales |
| Guía de uso | Esta página |

Para pasar a **Estable** falta: un soak largo verde con Oracle, el descriptor
con claves e índices (ver [Limitaciones](#limitaciones)) y dos releases sin
incidencias críticas.

## Requisitos

1. Compilar con la feature `oracle-driver` (`all-drivers` la incluye; los
   builds de Windows del release ya la traen):

   ```bash
   cargo build --release -p jaiba-cli --features release-core,oracle-driver
   ```

2. Instalar **Oracle Instant Client** Basic o Basic Light (no se empaqueta con
   Jaiba) y exponerlo al cargador:

   | Sistema | Variable |
   | --- | --- |
   | Linux | `LD_LIBRARY_PATH=/ruta/instantclient_23_26` (y `libaio`) |
   | macOS | `DYLD_LIBRARY_PATH` |
   | Windows | `PATH` |

   Descarga sin login (Linux x64):
   `https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip`.
   Si falta, el error es `DPI-1047` y Jaiba lo reporta como error de
   configuración con esta misma indicación.

## Conexión

```yaml
database_connections:
  oracle:
    type: oracle
    url_env: ORACLE_DATABASE_URL   # oracle://usuario:contraseña@host:1521/SERVICIO
    max_connections: 4
```

- La contraseña nunca va en YAML. Si tiene caracteres reservados (`@`, `:`,
  `/`, `%`…) escríbelos con *percent-encoding* (`%40`, `%3A`…). Las conexiones
  del Connection Manager ya lo hacen solas.
- El pool se crea **en el primer uso**: si Oracle no responde al arrancar, el
  flow arranca igual y cada llamada reintenta.
- `max_connections` es el tope de sesiones del pool. Una llamada espera como
  mucho 10 s (`acquire_timeout` del Connection Manager si se define) por una
  sesión libre; si no la obtiene, falla con un error reintentable.
- Cada sesión nueva fija formatos ISO 8601 y punto decimal
  (`NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`, `NLS_TIMESTAMP_TZ_FORMAT`,
  `NLS_NUMERIC_CHARACTERS`), así que el resultado no depende de la
  configuración regional del servidor.

## Lectura: `query_oracle`

```yaml
- id: read
  type: query_oracle
  config:
    connection: oracle
    query: SELECT id, name, amount, created FROM ventas.pedidos
    batch_size: 1000        # filas por paquete
    call_timeout_ms: 300000 # por viaje a Oracle; 0 = sin límite
```

- **Streaming:** emite un paquete cada `batch_size` filas y deja como máximo
  dos lotes por delante del flow. Un resultado de millones de filas no se
  carga entero en memoria.
- **Cancelación:** si el nodo excede su `timeout_ms` o el flow se detiene, la
  llamada en curso se interrumpe (OCIBreak) y la sesión vuelve al pool.
- **Reintentos:** un fallo a mitad de lectura repite la consulta desde el
  principio. La entrega es *al menos una vez*: usa `mode: upsert` en el destino.
- Solo acepta `SELECT` o `WITH`. Las columnas salen en minúsculas.

## Escritura: `put_database`

```yaml
- id: write
  type: put_database
  config:
    connection: oracle
    table: VENTAS.PEDIDOS_COPIA
    mode: upsert            # insert | upsert (MERGE)
    batch_size: 1000
    columns: { id: ID, name: NAME, amount: AMOUNT, created: CREATED }
    conflict_columns: [ID]
```

- **Array DML:** cada lote de hasta `batch_size` filas es un solo viaje a
  Oracle, tanto para `INSERT` como para `MERGE`.
- **Transacción por paquete:** todos los lotes de un paquete se confirman
  juntos; si uno falla, se hace rollback de todo el paquete.
- Cada columna usa un único tipo de bind en el lote: enteros como `NUMBER`,
  todo lo demás como texto (`VARCHAR2`, o `CLOB` si pasa de 32 767 bytes).
  Los decimales viajan como texto exacto, sin pasar por `f64`.

## Tipos

Oracle → JSON (lectura):

| Oracle | JSON |
| --- | --- |
| `NUMBER`, `INTEGER`, `FLOAT` | entero si cabe en 64 bits; decimal si el valor se representa exacto; si no, **texto** con todos los dígitos (`"12345678901234567890.123"`) |
| `BINARY_DOUBLE`, `BINARY_FLOAT` | número (`NaN`/`Infinity` como texto) |
| `DATE` | `"2026-10-13T10:20:30"` |
| `TIMESTAMP(p)` | `"2026-10-13T10:20:30.123456"` (p decimales) |
| `TIMESTAMP(p) WITH TIME ZONE` | `"2026-10-13T10:20:30.500-06:00"` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | igual que `TIMESTAMP`, en la zona de la sesión |
| `VARCHAR2`, `CHAR`, `NVARCHAR2`, `CLOB`, `NCLOB`, `LONG` | texto |
| `RAW`, `LONG RAW`, `BLOB` | texto **base64** |
| `BOOLEAN` (23ai) | booleano |
| `JSON`, `VECTOR` (23ai) | no se leen directo: la consulta falla con un rechazo que lo indica. Selecciónalos como texto: `JSON_SERIALIZE(col RETURNING CLOB)`, `FROM_VECTOR(col)` |
| `NULL` | `null` |
| otros (`INTERVAL`, `ROWID`…) | texto de Oracle |

JSON → Oracle (escritura):

| JSON | Bind |
| --- | --- |
| entero, booleano (`1`/`0`) | `NUMBER` |
| decimal | texto exacto → `NUMBER` |
| texto ISO 8601 a `DATE`/`TIMESTAMP` | `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS[.ffffff]`; para `WITH TIME ZONE` añade `Z` o `±HH:MM` (sin zona usa la de la sesión) |
| objeto o arreglo | JSON serializado como texto |
| `null` | `NULL` |

## Errores y reintentos

| Tipo | Ejemplos | Qué hace el motor |
| --- | --- | --- |
| Sesión o red (reintentable) | `ORA-03113`, `ORA-03135`, `ORA-12541`, `ORA-12170`, `ORA-01013` (timeout/cancelación), pool lleno, `DPI-*` | reintenta según `retry` del nodo y luego `failure` |
| Rechazo de Oracle (permanente) | `ORA-00942`, `ORA-00001`, `ORA-01400`, `ORA-01722`, `ORA-12899`, credenciales inválidas | va a `failure` **sin reintentar** y no abre el circuit breaker |

La lista completa de códigos reintentables está en
`crates/jaiba-runtime/src/connectors/oracle.rs` (`TRANSIENT_ORA_CODES`).

## Pruebas

Contenedor local (no toca otros Oracle de la máquina):

```bash
docker run -d --name jaiba-oracle-test -p 127.0.0.1:15211:1521 \
  -e ORACLE_PASSWORD=<clave> -e APP_USER=jaiba -e APP_USER_PASSWORD=<clave> \
  gvenzl/oracle-free:23-slim-faststart
docker logs -f jaiba-oracle-test   # esperar "DATABASE IS READY TO USE"

export LD_LIBRARY_PATH=/ruta/instantclient_23_26
export JAIBA_TEST_ORACLE_PASSWORD=<clave> JAIBA_TEST_ORACLE_PORT=15211 \
       JAIBA_TEST_ORACLE_SERVICE=FREEPDB1 JAIBA_TEST_ORACLE_USER=jaiba

# Conector: tipos, streaming, cancelación, timeout, array DML, MERGE, rollback
cargo test -p jaiba-runtime --features oracle-driver --lib -- oracle
# Diagnóstico y metadatos del Connection Manager
cargo test -p jaiba-server --features oracle-driver --lib oracle_real
# Flow real con el binario, dos veces (upsert idempotente)
JAIBA_ORACLE_CONTAINER=jaiba-oracle-test ./scripts/smoke-oracle.sh
```

Sin `JAIBA_TEST_ORACLE_PASSWORD` las pruebas reales se omiten; con
`JAIBA_TEST_ORACLE_REQUIRED=1` (como en CI) fallan en lugar de omitirse.
También se validó con Oracle XE 21 (`gvenzl/oracle-xe:21-slim`, servicio
`XEPDB1`).

## Limitaciones

- El descriptor de esquema devuelve columnas pero no claves ni índices; el
  constructor visual de SQL sigue desactivado para Oracle.
- Escribir en `RAW`/`BLOB` desde el base64 que produce la lectura no está
  soportado (se enviaría como texto).
- Solo TCP `host:puerto/servicio`: sin TCPS, wallets ni alias de `tnsnames.ora`.
- Columnas `JSON`/`VECTOR` nativas de 23ai solo vía conversión a texto en la
  consulta (límite de `rust-oracle` 0.6).
- Lectura *al menos una vez* tras un reintento (ver arriba).

Runbook de migración Oracle → PostgreSQL / MongoDB:
[oracle-to-postgres.md](oracle-to-postgres.md).
