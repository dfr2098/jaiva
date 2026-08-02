# Administrador de conexiones

Jaiba UI incluye un módulo independiente para crear y comprobar perfiles de
conexión reutilizables. Se abre desde **Conexiones** en la barra principal.

## Separación de responsabilidades

- La UI captura la configuración y consume la API administrativa.
- `jaiba-server` valida los datos y nunca devuelve la contraseña.
- `jaiba-connection-manager` conserva perfiles, estado y referencias a secretos.
- Los plugins de conexión realizan la prueba específica del motor.
- Los flujos deben referirse al **alias** (`connection: postgres_dma`), no a una
  URL embebida en el procesador.

El proveedor predeterminado actual es `InMemorySecretStore`, pensado para
desarrollo local. Los perfiles y secretos desaparecen al reiniciar el motor. En
producción debe reemplazarse por Vault, Kubernetes Secrets o un almacén cifrado
con una clave externa; no se debe persistir el mapa en memoria como JSON.

## Adaptadores extensibles

El núcleo no contiene una lista cerrada de motores. `ConnectionType` se
serializa como un identificador de texto y conserva valores desconocidos como
`sqlite`. Cada implementación de `ConnectionPlugin` publica nombre, versión,
categoría, puerto predeterminado y capacidades.

`ConnectionManager::register_plugin` es el único punto de registro. El catálogo
REST se genera desde los adaptadores instalados y la UI renderiza sus
capacidades. Para agregar un motor solo se implementa y registra un adaptador;
no se modifica el Connection Manager, sus handlers ni la interfaz.

```mermaid
flowchart LR
    UI["UI<br/>sin drivers"] --> API["REST neutral"]
    API --> CM["Connection Manager"]
    CM --> REG["Registro de adaptadores"]
    REG --> PG["PostgreSQL"]
    REG --> MY["MySQL"]
    REG --> MONGO["MongoDB"]
    REG --> ORA["Oracle"]
    REG --> MSSQL["SQL Server"]
    REG --> NEW["SQLite / nuevo motor"]
    CM --> SEC["SecretStore"]
```

## API

- `GET /api/v1/connection-types`
- `GET /api/v1/connections`
- `POST /api/v1/connections`
- `GET|PUT|DELETE /api/v1/connections/{id}`
- `POST /api/v1/connections/{id}/duplicate`
- `POST /api/v1/connections/{id}/test`
- `GET /api/v1/connections/{id}/diagnostics`
- `GET /api/v1/connections/{id}/metadata`
- `GET /api/v1/connections/{id}/metadata/{schema}/{name}`
- `POST /api/v1/connections/{id}/query/compile`

Estas rutas usan la misma autenticación administrativa que los flujos. Una
prueba correcta informa disponibilidad, latencia, versión, uso del pool y fecha.
Una prueba fallida conserva la fecha y el diagnóstico sin incluir el secreto.

### Cuerpo de creación / actualización

Campos comunes (`ConnectionInput`):

| Campo | Tipo | Notas |
|---|---|---|
| `name` | string | Obligatorio |
| `connection_type` | string | p. ej. `postgres`, `mongodb`, `sql_server` |
| `host` | string | Obligatorio salvo URL MongoDB |
| `port` | number | Obligatorio salvo URL MongoDB |
| `database` | string \| null | Base o servicio |
| `username` | string | Obligatorio salvo URL MongoDB con usuario |
| `password` | string | Obligatoria al crear (o en la URL Mongo) |
| `url` | string | **Solo MongoDB.** URI completa opcional |
| `ssl` | bool | TLS |
| `pool_min` / `pool_max` | number | Pool |
| `timeout_ms` | number | Timeout de conexión |

La respuesta (`ConnectionView`) nunca incluye `password`, `url` ni `secret_ref`.

## Explorador y constructor SQL

PostgreSQL y MySQL permiten explorar tablas, vistas, rutinas, columnas, llaves
e índices. El constructor envía una especificación neutral `QuerySpec`; el
servidor genera SQL seguro para el dialecto y devuelve la sentencia junto con
sus parámetros separados.

Cuando existe un procesador ejecutable, el adaptador devuelve
`processor_type` y `execution_statement`. La UI los trata como datos opacos y
no decide según el motor. Si el adaptador solo compila, la interfaz permite
copiar el SQL sin ofrecer un nodo.

Oracle y SQL Server exploran columnas; todavía no exponen llaves/índices ni
constructor visual SQL.

## MongoDB

MongoDB se habilita con `--features mongodb-driver`:

```bash
cargo run --features mongodb-driver -- serve examples/visualisa-flow.yaml
```

Capacidades del adaptador:

- guarda el perfil sin devolver la contraseña;
- prueba autenticación y conectividad mediante `ping`;
- muestra versión, latencia y acceso a metadatos en el diagnóstico;
- lista las colecciones de la base seleccionada;
- infiere los campos y tipos BSON a partir de un documento de muestra;
- nodos ejecutables `query_mongodb` y `put_mongodb` en el diseñador.

El adaptador no publica un constructor visual de consultas; el filtro, la
proyección y el orden se editan como documentos JSON en el nodo.

### Dos formas de configurar el perfil

#### 1. Campos sueltos (host / puerto / usuario)

| Campo | Valor típico (entorno de pruebas) |
|---|---|
| Host | `127.0.0.1` |
| Puerto | `27018` (mapeo host → 27017 del contenedor) |
| Base | `dma_test` |
| Usuario | `dma_test` |
| Contraseña | la de `MONGO_INITDB_ROOT_PASSWORD` del compose de pruebas |
| SSL/TLS | desactivado |

Sin URL, el servidor construye:

```text
mongodb://usuario:clave@host:puerto/base?authSource=…&tls=…&minPoolSize=…&maxPoolSize=…
```

`authSource` por defecto es `admin` (usuario raíz de la imagen oficial).

#### 2. URL de conexión (`mongodb://` / `mongodb+srv://`)

En la UI, el formulario MongoDB muestra **URL de conexión**. Al pegar una URI:

1. Se rellenan host, puerto, base, usuario, contraseña y TLS cuando la URI los trae.
2. La URI completa se guarda en el secreto del perfil (`connection_url`).
3. Al probar, diagnosticar o resolver el alias en un flujo, se usa **esa URI**
   (Atlas, SRV, `replicaSet`, `retryWrites`, etc. se conservan).
4. Solo se actualizan usuario/contraseña del secreto sobre la URI guardada.

Ejemplo local:

```text
mongodb://dma_test:PASSWORD@127.0.0.1:27018/dma_test?authSource=admin
```

Ejemplo Atlas:

```text
mongodb+srv://app:PASSWORD@cluster0.example.net/prod?retryWrites=true&w=majority
```

Creación solo con URL (API):

```json
{
  "name": "mongo_prod",
  "connection_type": "mongodb",
  "url": "mongodb://dma_test:PASSWORD@127.0.0.1:27018/dma_test?authSource=admin",
  "pool_min": 1,
  "pool_max": 10,
  "timeout_ms": 10000
}
```

`host` / `port` / `username` / `password` pueden omitirse si ya vienen en la URL.
Si se envían ambos, los campos del formulario tienen prioridad para credenciales;
la URI sigue siendo la base de conexión (esquema, query string, SRV).

Al editar un perfil que ya tenía URI, si no se reenvía `url`, se conserva la URI
anterior y se actualizan las credenciales del secreto.

### URL en flujos YAML (`url_env`)

Los flujos también pueden declarar conexiones Mongo por variable de entorno,
sin Connection Manager:

```yaml
database_connections:
  source:
    type: mongodb
    url_env: MONGODB_URL
    max_connections: 4
```

```bash
export MONGODB_URL='mongodb://dma_test:PASSWORD@127.0.0.1:27018/dma_test?authSource=admin'
cargo run --features mongodb-driver -- examples/mongodb-copy.yaml
```

Eso es independiente del campo `url` del Connection Manager: uno es perfil
reutilizable en la UI; el otro es configuración estática del YAML.

### Pruebas

```bash
export JAIBA_TEST_MONGODB_PASSWORD='...'
cargo test -p jaiba-server --features mongodb-driver \
  mongodb_real_connection_diagnostics_and_collection_metadata -- --nocapture
cargo test -p jaiba-server --features mongodb-driver \
  mongodb_real_connection_from_url -- --nocapture
```

La suite Fase 8 incluye Mongo (metadatos + flujo query→upsert). Ver
[priority-8-integration-tests.md](priority-8-integration-tests.md).

Ejemplo de copia: [`examples/mongodb-copy.yaml`](../examples/mongodb-copy.yaml).

## SQL Server

SQL Server se habilita con `--features sqlserver-driver`:

```bash
cargo run --features sqlserver-driver -- serve examples/visualisa-flow.yaml
```

Capacidades del adaptador en Connection Manager:

- prueba de conexión TDS y lectura de versión (16.x = SQL Server 2022);
- diagnóstico de conectividad, edición y acceso a `sys.objects`;
- listado de objetos del esquema y descripción de columnas;
- escritura mediante `put_database` en flujos (ver
  [configuration.md](configuration.md) y `examples/sqlserver-write.yaml`).

Valores típicos del entorno de pruebas:

| Campo | Valor |
|---|---|
| Host | `127.0.0.1` |
| Puerto | `11433` (mapeo host → 1433) |
| Base | `master` (o la base de aplicación) |
| Usuario | `sa` |
| Contraseña | `MSSQL_SA_PASSWORD` del compose de pruebas |
| SSL/TLS | desactivado en local (TrustServerCertificate en la URL de runtime) |

Prueba opt-in:

```bash
export JAIBA_TEST_SQLSERVER_PASSWORD='...'
cargo test -p jaiba-server --features sqlserver-driver \
  sqlserver_real_connection_diagnostics_and_metadata -- --nocapture
```

Limitación: todavía no se devuelven llaves/índices ni hay constructor visual SQL.

## Flujo UI → diseñador (SQL)

```mermaid
sequenceDiagram
    actor U as Usuario
    participant UI as jaiba-ui
    participant API as jaiba-server
    participant CM as Connection Manager
    participant DB as PostgreSQL / MySQL
    participant SQL as SQL Builder
    participant FB as Flow Builder

    U->>UI: Abre una conexión
    UI->>API: GET metadata
    API->>CM: Resuelve perfil y secreto
    CM->>DB: Consulta information_schema
    DB-->>CM: Tablas, columnas, llaves e índices
    CM-->>UI: Metadatos sin credenciales
    U->>UI: Selecciona columnas y filtros
    UI->>API: POST QuerySpec
    API->>SQL: Compilar según dialecto
    SQL-->>UI: SQL + parámetros separados

    alt adaptador publica procesador
        U->>UI: Enviar al diseñador
        UI->>FB: Guarda traspaso temporal
        FB->>FB: Crea el tipo indicado por la API
    else adaptador solo compila
        UI-->>U: Permite copiar SQL; nodo aún no disponible
    end
```

## Seguridad

- Las respuestas nunca contienen `password`, `url` completa ni `secret_ref`.
- La contraseña queda vacía al editar; dejarla vacía conserva la anterior.
- El frontend no usa `localStorage` ni `sessionStorage` para credenciales.
- Los logs no imprimen `ConnectionSecret`; su implementación de `Debug` redacta
  usuario y contraseña.
- Sin autenticación, el plano administrativo solo puede exponerse en loopback.

## Referencias

- Bitácora: [implementation-notes.md](implementation-notes.md)
- Suite Fase 8: [priority-8-integration-tests.md](priority-8-integration-tests.md)
- Operación / features: [operations.md](operations.md)
