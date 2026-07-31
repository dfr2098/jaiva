# Administrador de conexiones

Jaiba UI incluye un módulo independiente para crear y comprobar perfiles de
conexión reutilizables. Se abre desde **Conexiones** en la barra principal.

## Separación de responsabilidades

- La UI captura la configuración y consume la API administrativa.
- `jaiba-server` valida los datos y nunca devuelve la contraseña.
- `jaiba-connection-manager` conserva perfiles, estado y referencias a secretos.
- Los plugins de conexión realizan la prueba específica del motor.
- Los flujos deben referirse al alias (`connection: postgres_dma`), no a una URL.

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

## Explorador y constructor SQL

PostgreSQL y MySQL permiten explorar tablas, vistas, rutinas, columnas, llaves
e índices. El constructor envía una especificación neutral `QuerySpec`; el
servidor genera SQL seguro para el dialecto y devuelve la sentencia junto con
sus parámetros separados.

Cuando existe un procesador ejecutable, el adaptador devuelve
`processor_type` y `execution_statement`. La UI los trata como datos opacos y
no decide según el motor. Si el adaptador solo compila, la interfaz permite
copiar el SQL sin ofrecer un nodo.

## MongoDB: primera fase

MongoDB se habilita con `--features mongodb-driver`. En esta fase el adaptador:

- guarda el perfil sin devolver la contraseña;
- prueba autenticación y conectividad mediante `ping`;
- muestra versión, latencia y acceso a metadatos en el diagnóstico;
- lista las colecciones de la base seleccionada;
- infiere los campos y tipos BSON a partir de un documento de muestra.

La fase 2 añade los nodos ejecutables `query_mongodb` y `put_mongodb` al
diseñador. El adaptador todavía no publica un constructor visual de consultas;
el filtro, la proyección y el orden se editan como documentos JSON en el nodo.

Para iniciar el servidor con el adaptador:

```bash
cargo run --features mongodb-driver -- serve examples/visualisa-flow.yaml
```

Para el contenedor local de pruebas use este perfil en **Conexiones**:

| Campo | Valor |
|---|---|
| Host | `127.0.0.1` |
| Puerto | `27017` |
| Base | `pruebas` |
| Usuario | `admin` |
| Contraseña | la definida en `MONGO_INITDB_ROOT_PASSWORD` |
| SSL/TLS | desactivado |

El adaptador usa `authSource=admin` de manera predeterminada, que corresponde al
usuario raíz creado por la imagen oficial de MongoDB.

Para ejecutar una copia MongoDB → MongoDB:

```bash
export MONGODB_URL='mongodb://admin:admin123@127.0.0.1:27017/pruebas?authSource=admin'
cargo run --features mongodb-driver -- examples/mongodb-copy.yaml
```

El flujo completo está en
[`examples/mongodb-copy.yaml`](../examples/mongodb-copy.yaml).

El recorrido técnico completo está descrito en
[la bitácora de implementación](implementation-notes.md).

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

- Las respuestas nunca contienen `password` ni `secret_ref`.
- La contraseña queda vacía al editar; dejarla vacía conserva la anterior.
- El frontend no usa `localStorage` ni `sessionStorage` para credenciales.
- Los logs no imprimen `ConnectionSecret`; su implementación de `Debug` redacta
  usuario y contraseña.
- Sin autenticación, el plano administrativo solo puede exponerse en loopback.
