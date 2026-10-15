# MongoDB

Estado: **Beta** desde v0.9.0 (ver [criterios de madurez](product-roadmap.md#criterios-de-madurez)).

| Criterio Beta | Cómo se cumple |
| --- | --- |
| Pruebas reales automáticas | Workflow [`mongodb.yml`](../.github/workflows/mongodb.yml): cada noche y en cada PR que toca MongoDB, contra MongoDB 7.0 y 8.0 |
| Memoria acotada | Lectura por cursor en paquetes de `batch_size`; escritura por lotes de tamaño acotado |
| Fallos definidos | Timeout por llamada, `maxTimeMS`, cancelación en el servidor, errores reintentables vs. rechazos |
| Tipos documentados | [§ Tipos](#tipos), cubiertos por pruebas reales en ambas versiones |
| Guía de uso | Esta página |

Para pasar a **Estable** falta: un soak largo verde con MongoDB, pruebas contra
un replica set y dos releases sin incidencias críticas.

## Cambios respecto a v0.8 (revisa tus flows)

El formato de los documentos que entrega `query_mongodb` **no cambia**. Cambia
el comportamiento ante errores, esperas largas y el tamaño del pool:

| Qué | Hasta v0.8 | Desde v0.9 | Para conservar lo anterior |
| --- | --- | --- | --- |
| Rechazos (clave duplicada, validación del esquema, registro sin la clave del upsert, Extended JSON inválido, credenciales) | se reintentaban y abrían el circuit breaker | van a `failure` al primer intento; el circuit breaker no se abre | — |
| Espera por cada respuesta de `query_mongodb` (`find`/`getMore`) | sin límite | 300 s (`call_timeout_ms`), luego error reintentable | `call_timeout_ms: 0` |
| Cada lote de `put_mongodb` | sin límite | 300 s (`call_timeout_ms`), luego error reintentable | `call_timeout_ms: 0` |
| `upsert` | un `replaceOne` por documento; `ordered` se ignoraba | un comando `update` por lote; con `ordered: false` sigue con el resto del lote tras un error | — |
| Pool de una conexión con `url_env` | 10 (valor del driver) | `max_connections` del YAML (5 por defecto) si la URI no trae `maxPoolSize` | `maxPoolSize=10` en la URI o `max_connections: 10` |

## Conexión

Requiere un binario con `--features mongodb-driver` (incluido en los binarios
de Windows del release y en `all-drivers`).

```yaml
database_connections:
  mongo:
    type: mongodb
    url_env: MONGODB_URL   # mongodb://usuario:contraseña@host:27017/base?authSource=admin
    max_connections: 4
```

- La URI **debe incluir la base** (`/base`): es la base de todos los nodos.
- La contraseña nunca va en YAML. Si tiene caracteres reservados (`@`, `:`,
  `/`, `%`…) escríbelos con *percent-encoding* (`%40`, `%3A`…).
- `max_connections` fija el pool si la URI no trae `maxPoolSize`. Las demás
  opciones (`replicaSet`, `tls`, `serverSelectionTimeoutMS`, `mongodb+srv://`)
  van en la URI.
- El Connection Manager arma la URI con `authSource`, TLS, pool y
  `connectTimeoutMS`/`serverSelectionTimeoutMS` desde el perfil
  ([connection-manager.md](connection-manager.md#mongodb)).

## Lectura: `query_mongodb`

```yaml
- id: read
  type: query_mongodb
  config:
    connection: mongo
    collection: pedidos
    filter: { estado: "pagado", total: { $gte: 100 } }
    projection: { cliente: 1, total: 1, creado: 1 }
    sort: { _id: 1 }
    skip: 0
    limit: 100000
    batch_size: 1000          # documentos por paquete (y por getMore)
    call_timeout_ms: 300000   # espera máxima por cada respuesta; 0 = sin límite
    max_time_ms: 0            # maxTimeMS del servidor para el cursor; 0 = sin límite
    json_format: relaxed      # relaxed (por defecto), canonical o plain
```

- **Streaming:** el cursor trae `batch_size` documentos por viaje y se emite un
  paquete por cada `batch_size`. Una colección de millones de documentos no se
  carga entera en memoria.
- **Timeout y cancelación:** si una respuesta tarda más de `call_timeout_ms`, o
  el nodo excede su `timeout_ms`, o el flow se detiene, el driver cierra la
  conexión y el servidor interrumpe la operación (comprobado con `$currentOp`).
  `max_time_ms` lo aplica el propio servidor sobre el tiempo de ejecución del
  cursor y devuelve `MaxTimeMSExpired`.
- Cada operación lleva el comentario `jaiba:<id del nodo>`, visible en
  `db.currentOp()` y en el profiler.
- **Reintentos:** un fallo a mitad de lectura repite la consulta desde el
  principio. La entrega es *al menos una vez*: usa `mode: upsert` en el destino.

## Escritura: `put_mongodb`

```yaml
- id: write
  type: put_mongodb
  config:
    connection: mongo
    collection: pedidos_copia
    mode: upsert              # insert | upsert
    key_fields: [_id]         # admite rutas: [tenant, cliente.id]
    batch_size: 1000
    ordered: true
    call_timeout_ms: 300000   # espera máxima por lote; 0 = sin límite
```

- **`insert`:** `insertMany` por lote.
- **`upsert`:** un comando `update` por lote, con un reemplazo
  `{q: claves, u: documento, upsert: true}` por documento. Reemplaza el
  documento completo y es idempotente con claves estables.
- Un lote nunca supera ~15 MiB: si los documentos son grandes, se parte en más
  comandos (límite del servidor: 16 MiB por comando).
- **Sin transacción:** en MongoDB las escrituras de un lote no son atómicas. Con
  `ordered: true` el lote se detiene en el primer error y los documentos
  anteriores ya quedaron escritos; con `ordered: false` el servidor intenta todos
  y reporta los errores. Un reintento con `upsert` deja el mismo resultado; con
  `insert` puede chocar con los ya insertados (clave duplicada).
- `write.rows` cuenta los documentos insertados o encontrados por clave.

## Tipos

`json_format` decide cómo llegan los documentos al resto del flow:

- **`relaxed`** (por defecto): Extended JSON relajado. Números legibles; los
  tipos sin equivalente JSON van envueltos (`{"$oid": …}`, `{"$date": …}`).
- **`canonical`**: Extended JSON canónico, todo tipo envuelto. Copia exacta
  MongoDB → MongoDB.
- **`plain`**: JSON sin envolturas, para destinos SQL, CSV o archivos.

`put_mongodb` acepta JSON normal o Extended JSON, así que la salida `relaxed` o
`canonical` se puede escribir tal cual.

BSON → JSON (lectura):

| BSON | `relaxed` | `canonical` | `plain` |
| --- | --- | --- | --- |
| `int` (Int32) | `42` | `{"$numberInt": "42"}` | `42` |
| `long` (Int64) | `7` | `{"$numberLong": "7"}` | `7` |
| `double` | `0.1`; `NaN`/`±Infinity` como `{"$numberDouble": "NaN"}` | `{"$numberDouble": "0.1"}` | `0.1`; `"NaN"`, `"inf"`, `"-inf"` |
| `decimal` (Decimal128) | `{"$numberDecimal": "12.50"}` | igual | número si es exacto (`12.5`); si no, texto con todos los dígitos |
| `date` | `{"$date": "2025-10-13T10:20:30.123Z"}` (años 1970–9999; fuera de ese rango `{"$date": {"$numberLong": "…"}}`) | `{"$date": {"$numberLong": "…"}}` | `"2025-10-13T10:20:30.123Z"` (UTC, siempre 3 decimales) |
| `objectId` | `{"$oid": "65f1…"}` | igual | `"65f1…"` (hex) |
| `binData` | `{"$binary": {"base64": "…", "subType": "00"}}` | igual | base64; UUID (subtipo 4) como `"3b241101-e2bb-…"` |
| `timestamp`, `regex`, `minKey`, `maxKey`, `javascript` | `{"$timestamp": …}`, `{"$regularExpression": …}`, … | igual | igual que `relaxed` |
| `string`, `bool`, `null`, objetos y arreglos | JSON normal | números envueltos como arriba | JSON normal |

JSON → BSON (escritura):

| JSON | BSON |
| --- | --- |
| entero que cabe en 32 bits | `int` |
| entero mayor | `long` |
| entero mayor que `i64` o número con decimales | `double` (puede perder precisión; para exactos envía `{"$numberDecimal": "…"}`) |
| texto | `string`: una fecha en texto (`"2026-10-13T10:20:30Z"`) queda como **texto**; para `date` envía `{"$date": "…"}` |
| `{"$oid"}`, `{"$date"}`, `{"$numberDecimal"}`, `{"$numberLong"}`… | el tipo BSON correspondiente |

**Copia exacta MongoDB → MongoDB:** con `relaxed`, un `long` pequeño (`7`) vuelve
como `int` al escribirse. Si el tipo importa (validadores de esquema, índices
por tipo), usa `json_format: canonical`. Así la copia conserva cada tipo,
comprobado documento por documento en el smoke.

**MongoDB → SQL/CSV:** usa `json_format: plain`. Los `ObjectId`, fechas y
decimales llegan como texto o número y se escriben con `put_database` o
`encode_csv` sin objetos `$...`. `plain` no sirve para volver a MongoDB: las
fechas y los `ObjectId` quedarían como texto.

## Errores y reintentos

| Tipo | Ejemplos | Qué hace el motor |
| --- | --- | --- |
| Sesión, red o servidor (reintentable) | servidor inalcanzable, selección de servidor, conexión cerrada, cambio de primario (`10107`, `189`, `11602`), `112` WriteConflict, `50` MaxTimeMSExpired, `43` cursor perdido, error de write concern, errores con etiqueta `RetryableWriteError`, timeout por llamada | reintenta según `retry` del nodo y luego `failure` |
| Rechazo (permanente) | `11000` clave duplicada, `121` validación del esquema, consulta inválida (`2` BadValue), sin permisos, credenciales inválidas, registro sin la clave del upsert, Extended JSON inválido | va a `failure` **sin reintentar** y no abre el circuit breaker |

La lista completa de códigos reintentables está en
`crates/jaiba-runtime/src/processors/mongodb_support.rs`
(`TRANSIENT_MONGODB_CODES`).

## Pruebas

Contenedores locales (puertos libres; no tocan otros MongoDB de la máquina):

```bash
docker run -d --name jaiba-mongo7-test -p 127.0.0.1:27027:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=jaiba -e MONGO_INITDB_ROOT_PASSWORD=<clave> mongo:7.0
docker run -d --name jaiba-mongo8-test -p 127.0.0.1:27028:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=jaiba -e MONGO_INITDB_ROOT_PASSWORD=<clave> mongo:8.0

# query_mongodb y put_mongodb: tipos, streaming, cancelación, timeouts,
# errores, upsert por lotes, copia exacta
JAIBA_TEST_MONGODB_URL='mongodb://jaiba:<clave>@127.0.0.1:27028/jaiba?authSource=admin' \
  cargo test -p jaiba-runtime --features mongodb-driver --lib -- mongo
# Diagnóstico y metadatos del Connection Manager
JAIBA_TEST_MONGODB_PASSWORD=<clave> JAIBA_TEST_MONGODB_PORT=27028 \
JAIBA_TEST_MONGODB_DATABASE=jaiba JAIBA_TEST_MONGODB_USER=jaiba \
  cargo test -p jaiba-server --features mongodb-driver --lib mongodb_real
# Flow real con el binario, dos veces (upsert idempotente, tipos exactos)
JAIBA_TEST_MONGODB_PASSWORD=<clave> ./scripts/smoke-mongodb.sh
# MongoDB 7.0: mismo comando con puerto 27027 y JAIBA_MONGODB_CONTAINER=jaiba-mongo7-test
```

Sin `JAIBA_TEST_MONGODB_URL` / `JAIBA_TEST_MONGODB_PASSWORD` las pruebas reales
se omiten; con `JAIBA_TEST_MONGODB_REQUIRED=1` (como en CI) fallan en lugar de
omitirse. Las pruebas de cancelación usan `$where` con `sleep()`: requieren
JavaScript del servidor habilitado (lo está por defecto).

## Limitaciones

- Sin transacciones multi-documento (requieren replica set); ver `put_mongodb`.
- CI con servidor independiente, no replica set ni sharding.
- Una escritura interrumpida por timeout puede completarse igualmente en el
  servidor; el reintento con `upsert` deja el mismo resultado.
- Sin agregaciones (`aggregate`) como fuente; solo `find`.
- Lectura *al menos una vez* tras un reintento (ver arriba).
