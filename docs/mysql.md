# MySQL y MariaDB

Estado: **Beta** desde v0.9.0 (ver [criterios de madurez](product-roadmap.md#criterios-de-madurez)).

| Criterio Beta | Cómo se cumple |
| --- | --- |
| Pruebas reales automáticas | Workflow [`mysql.yml`](../.github/workflows/mysql.yml): cada noche y en cada PR que toca MySQL, contra MySQL 8.4 y MariaDB 11.4 |
| Memoria acotada | Lectura por streaming y escritura por lotes multi-fila |
| Fallos definidos | Timeout por llamada, cancelación con `KILL QUERY`, errores reintentables vs. rechazos |
| Tipos documentados | [§ Tipos](#tipos), cubiertos por pruebas reales en ambos motores |
| Guía de uso | Esta página |

Para pasar a **Estable** falta: un soak largo verde con MySQL y dos releases sin
incidencias críticas.

## Cambios respecto a v0.8 (revisa tus flows)

v0.9.0 cambia el formato de algunos valores que entrega `query_mysql` y el
comportamiento ante errores y esperas largas. Si un flow compara, parsea o
guarda estos valores como texto en otro sistema, revísalo antes de actualizar.

| Qué | Hasta v0.8 | Desde v0.9 | Para conservar lo anterior |
| --- | --- | --- | --- |
| `DATETIME`, `TIMESTAMP` | `"2026-10-13 10:20:30.123"` (espacio) | `"2026-10-13T10:20:30.123"` (`T`, ISO 8601) | `CAST(col AS CHAR)` en la SELECT |
| `DECIMAL` con decimales que no caben exactos en un número | número `f64` redondeado (`12345678901234.568`) | **texto** con todos los dígitos (`"12345678901234.5678"`) | `CAST(col AS DOUBLE)` en la SELECT (pierde precisión, como antes) |
| `DECIMAL` entero mayor que `BIGINT` | número `f64` redondeado | entero sin signo o texto exacto | `CAST(col AS DOUBLE)` |
| `FLOAT` | ensanchado a doble precisión (`0.10000000149011612`) | valor corto (`0.1`) | `CAST(col AS DOUBLE)` |
| `TIME` | fracción con ceros de relleno (`"07:05:09.500"` en `TIME(3)`); negativo o ≥ 24 h hacía fallar la consulta | fracción sin ceros (`"07:05:09.5"`); acepta `"-838:59:58.5"` | `CAST(col AS CHAR)` |
| `GEOMETRY` | texto si los bytes eran UTF-8 válido, si no base64 | siempre base64 | — |
| Espera por la siguiente fila en `query_mysql` | sin límite | 300 s (`call_timeout_ms`), luego error reintentable | `call_timeout_ms: 0` |
| Cada sentencia de `put_database` | sin límite | 300 s, luego rollback y error reintentable | bajar `batch_size` |
| Rechazos (clave duplicada, NULL, dato muy largo, tabla inexistente, sintaxis) | se reintentaban y abrían el circuit breaker | van a `failure` al primer intento; el circuit breaker no se abre | — |

Los valores sin cambio (enteros, `BOOLEAN`, `DATE`, `JSON`, texto, binarios,
`DECIMAL` exactos como `12.5`) salen igual que antes. Escribir en MySQL valores
leídos con v0.8 sigue funcionando: MySQL acepta fechas con espacio o con `T`.
Las conversiones de la última columna están comprobadas en MySQL 8.4 y
MariaDB 11.4.

## Conexión

Va incluido en todos los binarios (no necesita feature ni bibliotecas extra).

```yaml
database_connections:
  main:
    type: mysql          # o mariadb
    url_env: MYSQL_URL   # mysql://usuario:contraseña@host:3306/base
    max_connections: 4
```

- La contraseña nunca va en YAML. Si tiene caracteres reservados (`@`, `:`,
  `/`, `%`…) escríbelos con *percent-encoding* (`%40`, `%3A`…).
- El pool se conecta **al arrancar el flow**. Si el servidor no responde,
  reintenta con espera creciente (hasta ~2 min) antes de fallar.
- `max_connections` es el tope de sesiones. Una llamada espera como mucho 10 s
  (`acquire_timeout` del Connection Manager si se define) por una sesión libre.
- Cada sesión usa UTC (`time_zone = '+00:00'`) y `utf8mb4`.

## Lectura: `query_mysql`

```yaml
- id: read
  type: query_mysql
  config:
    connection: main
    query: SELECT id, name, amount, created FROM ventas.pedidos WHERE activo = ?
    parameters: [true]
    batch_size: 1000        # filas por paquete
    call_timeout_ms: 300000 # espera máxima por la siguiente fila; 0 = sin límite
```

- **Streaming:** las filas se leen del socket a medida que el flow las consume
  y se emite un paquete cada `batch_size` filas. Un resultado de millones de
  filas no se carga entero en memoria.
- **Timeout y cancelación:** si MySQL tarda más de `call_timeout_ms` en entregar
  la siguiente fila, o el nodo excede su `timeout_ms`, o el flow se detiene, la
  consulta se detiene en el servidor con `KILL QUERY` desde otra conexión y la
  sesión se cierra en lugar de volver al pool.
- **Reintentos:** un fallo a mitad de lectura repite la consulta desde el
  principio. La entrega es *al menos una vez*: usa `mode: upsert` en el destino.
- Los parámetros (`?`) siempre se ligan, nunca se interpolan en el texto.

## Escritura: `put_database`

```yaml
- id: write
  type: put_database
  config:
    connection: main
    table: ventas.pedidos_copia
    mode: upsert            # insert | upsert (ON DUPLICATE KEY UPDATE)
    batch_size: 1000
    columns: { id: id, name: name, amount: amount, created: created }
    conflict_columns: [id]
```

- **Multi-fila:** cada lote de hasta `batch_size` filas es un solo `INSERT`
  (limitado a 65 535 parámetros por sentencia).
- **Transacción por paquete:** todos los lotes de un paquete se confirman
  juntos; si uno falla, se hace rollback de todo el paquete.
- Cada sentencia tiene un límite de 300 s; al excederlo o cancelarse el nodo,
  se detiene con `KILL QUERY` y la transacción se revierte.
- `upsert` necesita una clave primaria o única sobre `conflict_columns`.

## Tipos

MySQL → JSON (lectura):

| MySQL / MariaDB | JSON |
| --- | --- |
| `TINYINT`…`BIGINT`, `YEAR`, `BIGINT UNSIGNED` | entero |
| `BOOLEAN` (`TINYINT(1)`) | booleano |
| `DECIMAL` | entero o decimal si se representa exacto (`12.50` → `12.5`); si no, **texto** con todos los dígitos |
| `FLOAT`, `DOUBLE` | número (`FLOAT 0.1` → `0.1`) |
| `DATE` | `"2026-10-13"` |
| `DATETIME(p)` | `"2026-10-13T10:20:30.123456"` |
| `TIMESTAMP(p)` | igual que `DATETIME`, **en UTC** |
| `TIME` | `"07:05:09"`, `"-838:59:58.5"` (es una duración) |
| `CHAR`, `VARCHAR`, `TEXT`, `ENUM`, `SET` | texto |
| `JSON` (MySQL) | valor JSON |
| `JSON` (MariaDB) | texto (MariaDB lo guarda como `LONGTEXT`) |
| `BINARY`, `VARBINARY`, `BLOB` | texto si los bytes son UTF-8 válido; si no, **base64** |
| `BIT(n)` | entero |
| `GEOMETRY` | base64 del formato interno (SRID + WKB) |
| `NULL` | `null` |

MySQL marca como binarias también las columnas de texto con collation `_bin`;
por eso un `BLOB` que contiene texto UTF-8 se lee como texto.

JSON → MySQL (escritura):

| JSON | Bind |
| --- | --- |
| entero | `BIGINT` / `BIGINT UNSIGNED` |
| decimal | `DOUBLE` (usa texto para valores exactos: `"12345678901234.56"`) |
| booleano | `1` / `0` |
| texto ISO 8601 a `DATE`/`DATETIME`/`TIMESTAMP` | `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS[.ffffff]`, **sin** `Z` ni `±HH:MM` (MySQL y MariaDB no aceptan ambos); `TIMESTAMP` se interpreta en UTC |
| objeto o arreglo | JSON serializado |
| `null` | `NULL` |

La salida de `query_mysql` se puede escribir tal cual en otra tabla MySQL o
MariaDB.

## Errores y reintentos

| Tipo | Ejemplos | Qué hace el motor |
| --- | --- | --- |
| Sesión o red (reintentable) | conexión caída, pool lleno, `1205` lock wait timeout, `1213` deadlock, `1317` consulta interrumpida, `1040` demasiadas conexiones, timeout por llamada | reintenta según `retry` del nodo y luego `failure` |
| Rechazo (permanente) | `1062` clave duplicada, `1048` NULL en NOT NULL, `1406` dato muy largo, `1146` tabla inexistente, `1064` sintaxis, credenciales inválidas | va a `failure` **sin reintentar** y no abre el circuit breaker |

La lista completa está en `crates/jaiba-runtime/src/connectors/mysql.rs`
(`TRANSIENT_MYSQL_ERRORS`).

## Pruebas

Contenedores locales (puertos libres; no tocan otros MySQL de la máquina):

```bash
docker run -d --name jaiba-mysql-test -p 127.0.0.1:13306:3306 \
  -e MYSQL_ROOT_PASSWORD=<clave> -e MYSQL_DATABASE=jaiba \
  -e MYSQL_USER=jaiba -e MYSQL_PASSWORD=<clave> mysql:8.4
docker run -d --name jaiba-mariadb-test -p 127.0.0.1:13307:3306 \
  -e MARIADB_ROOT_PASSWORD=<clave> -e MARIADB_DATABASE=jaiba \
  -e MARIADB_USER=jaiba -e MARIADB_PASSWORD=<clave> mariadb:11.4

# Conector y query_mysql: tipos, streaming, cancelación, timeout, errores,
# insert/upsert y rollback
JAIBA_TEST_MYSQL_URL='mysql://jaiba:<clave>@127.0.0.1:13306/jaiba' \
  cargo test -p jaiba-runtime --lib -- mysql
# Metadatos y compilación de consultas del Connection Manager
JAIBA_TEST_MYSQL_PASSWORD=<clave> JAIBA_TEST_MYSQL_PORT=13306 \
JAIBA_TEST_MYSQL_DATABASE=jaiba JAIBA_TEST_MYSQL_USER=jaiba \
  cargo test -p jaiba-server --lib mysql_real
# Flow real con el binario, dos veces (upsert idempotente)
JAIBA_TEST_MYSQL_PASSWORD=<clave> ./scripts/smoke-mysql.sh
# MariaDB: mismo comando con puerto 13307, JAIBA_MYSQL_CONTAINER=jaiba-mariadb-test
# y JAIBA_SMOKE_MYSQL_KIND=mariadb
```

Sin `JAIBA_TEST_MYSQL_URL` / `JAIBA_TEST_MYSQL_PASSWORD` las pruebas reales se
omiten; con `JAIBA_TEST_MYSQL_REQUIRED=1` (como en CI) fallan en lugar de
omitirse.

## Limitaciones

- `upsert` usa `VALUES(col)`, obsoleto en MySQL 8.0.20+ (sigue funcionando; es
  la forma que también acepta MariaDB).
- Fechas cero (`0000-00-00`) no se pueden leer: la consulta va a `failure` como
  rechazo. Conviértelas en la SELECT (`NULLIF(col, '0000-00-00')`).
- Un `BLOB` con bytes que resultan UTF-8 válido se lee como texto (ver Tipos).
- Lectura *al menos una vez* tras un reintento (ver arriba).
