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

## Drivers visibles

| Motor | Perfil | Prueba real |
| --- | --- | --- |
| PostgreSQL | Sí | Sí, SQLx |
| MySQL | Sí | Sí, SQLx |
| MariaDB | Sí | Sí, SQLx |
| Oracle | Se muestra si se compila el feature | Pendiente en API administrativa |
| SQL Server | Se muestra si se compila el feature | Pendiente en API administrativa |
| Kafka | Se muestra si se compila el feature | Se administrará como bus |
| OPC-UA / REST | Catálogo futuro | No |

La interfaz deshabilita motores que todavía no pueden probarse para evitar
crear configuraciones engañosas.

## API

- `GET /api/v1/connection-types`
- `GET /api/v1/connections`
- `POST /api/v1/connections`
- `GET|PUT|DELETE /api/v1/connections/{id}`
- `POST /api/v1/connections/{id}/duplicate`
- `POST /api/v1/connections/{id}/test`
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

PostgreSQL permite enviar la consulta compilada al diseñador y crear
automáticamente un nodo `query_postgres`. MySQL permite explorar y compilar,
pero la creación de un nodo ejecutable queda deshabilitada hasta incorporar
`query_mysql` al runtime.

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

    alt conexión PostgreSQL
        U->>UI: Enviar al diseñador
        UI->>FB: Guarda traspaso temporal
        FB->>FB: Crea nodo query_postgres
    else conexión MySQL
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
