# Prioridad 4: escritura masiva y transaccional

## Estado

Fase 4.1 implementada:

- Contrato `DatabaseWriter`.
- Validación multi-dialecto de identificadores.
- Procesador `put_database`.
- PostgreSQL `insert` y `upsert`.
- Transacción atómica por paquete.
- Cálculo de lotes por límite de parámetros.
- Métricas, pruebas unitarias y pruebas reales de rollback.

Fase 4.2.1 implementada:

- Pool MySQL/MariaDB en `ConnectionManager`.
- Inserción multi-row.
- `ON DUPLICATE KEY UPDATE`.
- Transacción atómica por paquete.
- Cálculo automático del lote.
- Pruebas contractuales y de integración MySQL 8.4.

Fase 4.2.3 implementada:

- Adaptador Oracle opcional mediante `oracle-driver`.
- URL `oracle://usuario:contraseña@host:puerto/servicio`.
- `INSERT` parametrizado y `MERGE` para upsert.
- Transacción atómica por paquete con rollback ante cualquier fila fallida.
- Ejecución bloqueante aislada del runtime asíncrono.
- Pruebas unitarias de SQL y pruebas reales contra Oracle Free.

Fase 4.2.2 implementada:

- Cliente TDS asíncrono aislado mediante `sqlserver-driver`.
- `INSERT` parametrizado.
- Upsert sin `MERGE`, con bloqueos `UPDLOCK` y aislamiento `SERIALIZABLE`.
- Transacción atómica y rollback probado contra SQL Server 2022 Developer.

## Objetivo

Jaiva debe escribir paquetes de registros en diferentes motores sin acoplar el
runtime a un driver específico:

- PostgreSQL
- MySQL
- MariaDB
- Oracle
- SQL Server

El flujo esperado es:

```text
DataPacket
    ↓
Validar contenido, esquema e identificadores
    ↓
Resolver capacidades del conector
    ↓
Calcular tamaño efectivo del lote
    ↓
BEGIN
    ↓
Insert o upsert
    ↓
COMMIT
    ↓
Emitir success
```

Si la escritura falla:

```text
Error
  ↓
ROLLBACK
  ↓
Reintento del procesador
  ↓
failure / dead-letter
```

## Principios

1. El motor no genera SQL directamente.
2. Cada dialecto valida y escapa sus identificadores.
3. Los valores siempre utilizan parámetros.
4. El paquete se confirma después del `COMMIT`.
5. Un lote se escribe completo o se revierte completo.
6. Los destinos deben permitir idempotencia porque Jaiva ofrece entrega
   `at-least-once`.
7. Las capacidades no soportadas deben detectarse antes de ejecutar el flujo.

## Configuración propuesta

```yaml
database_connections:
  destination:
    type: postgres
    url_env: DESTINATION_DATABASE_URL
    max_connections: 10

processors:
  - id: write
    type: put_database
    config:
      connection: destination
      table: public.customers
      mode: upsert
      batch_size: 1000

      columns:
        id: customer_id
        name: customer_name
        updated_at: updated_at

      conflict_columns:
        - customer_id

      transaction:
        strategy: per_batch
        isolation: read_committed

    retry:
      maximum_attempts: 5
      initial_delay_ms: 500
      maximum_delay_ms: 30000
```

`columns` utiliza la forma:

```text
campo_del_paquete: columna_del_destino
```

## Contratos públicos propuestos

Las APIs públicas deberán incluir documentación Rust `///` con garantías,
errores y ejemplos.

```rust
/// Escribe lotes de registros mediante un motor de base de datos.
///
/// Una implementación debe ejecutar cada lote dentro de una transacción y
/// devolver éxito únicamente después de confirmar el commit.
#[async_trait]
pub trait DatabaseWriter: Send + Sync {
    /// Informa las operaciones y límites admitidos por el driver.
    fn capabilities(&self) -> WriteCapabilities;

    /// Valida la solicitud sin modificar la base de datos.
    async fn validate(&self, request: &WriteRequest)
        -> Result<(), ConnectorError>;

    /// Escribe un lote completo y lo revierte si algún registro falla.
    async fn write_batch(
        &self,
        request: &WriteRequest,
        records: &[Record],
    ) -> Result<WriteSummary, ConnectorError>;
}
```

```rust
pub struct WriteCapabilities {
    pub transactions: bool,
    pub bulk_insert: bool,
    pub native_upsert: bool,
    pub maximum_parameters: Option<usize>,
    pub returning: bool,
}
```

```rust
pub enum WriteMode {
    Insert,
    Upsert,
}
```

## Registro de conectores

`ConnectionManager` deberá devolver una abstracción común:

```rust
pub trait DatabaseConnector: Send + Sync {
    fn kind(&self) -> DatabaseKind;
    fn writer(&self) -> &dyn DatabaseWriter;
}
```

Tipos:

```rust
pub enum DatabaseKind {
    PostgreSql,
    MySql,
    MariaDb,
    Oracle,
    SqlServer,
}
```

La primera implementación real será PostgreSQL. Los demás conectores utilizarán
el mismo contrato y no requerirán cambios en `put_database`.

## Dialectos

### PostgreSQL

```sql
INSERT INTO "public"."customers" (...)
VALUES (...)
ON CONFLICT ("customer_id")
DO UPDATE SET ...;
```

Optimización posterior: `COPY` hacia una tabla temporal seguido de un upsert.

### MySQL y MariaDB

```sql
INSERT INTO `customers` (...)
VALUES (...)
ON DUPLICATE KEY UPDATE ...;
```

La implementación debe declarar diferencias de capacidades entre MySQL y
MariaDB.

### Oracle

```sql
MERGE INTO "CUSTOMERS" destination
USING (...) source
ON (...)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

La primera implementación ejecuta una sentencia parametrizada por fila dentro
de una única transacción. La optimización posterior es array binding y un pool
de sesiones. El driver se mantiene detrás de una feature para aislar sus
dependencias nativas.

### SQL Server

La primera opción será una tabla temporal dentro de una transacción:

```text
Crear/cargar tabla temporal
        ↓
UPDATE de coincidencias
        ↓
INSERT de faltantes
        ↓
COMMIT
```

No se utilizará `MERGE` automáticamente sin documentar y probar sus implicaciones
de concurrencia.

## Seguridad de identificadores

Los valores se parametrizan, pero tablas y columnas no. Cada dialecto debe:

1. Separar catálogo, esquema y objeto.
2. Rechazar componentes vacíos.
3. Rechazar caracteres no permitidos.
4. Escapar identificadores con la sintaxis correspondiente.
5. Limitar la cantidad de componentes.

Ejemplos:

```text
PostgreSQL → "public"."customers"
MySQL      → `database`.`customers`
Oracle     → "SCHEMA"."CUSTOMERS"
SQL Server → [dbo].[customers]
```

No se permitirá recibir una expresión SQL como nombre de tabla o columna.

## Lotes y límites de parámetros

El tamaño efectivo se calculará así:

```text
máximo_por_parámetros =
    límite_del_driver / cantidad_de_columnas

lote_efectivo =
    mínimo(batch_size_configurado, máximo_por_parámetros)
```

El procesador dividirá un `DataPacket` grande sin duplicar su contenido completo
en memoria.

## Transacciones

La primera estrategia admitida será `per_batch`:

```text
BEGIN
  escribir lote
COMMIT
```

Ante cualquier error:

```text
ROLLBACK
```

El paquete persistente de Jaiva permanecerá `RUNNING` durante la transacción y
solo cambiará a `COMPLETED` después del commit.

## Idempotencia

La entrega es `at-least-once`. Para `upsert` se exigirán
`conflict_columns`.

Ejemplo para el journal de DMA:

```yaml
mode: upsert
conflict_columns:
  - site_code
  - external_key
```

Esto corresponde a:

```sql
UNIQUE (site_code, external_key)
```

`insert` puro puede producir una violación de clave después de recuperar un
paquete. Esa condición debe poder tratarse como error o como duplicado aceptado
mediante una política explícita.

## Tipos

El conector utilizará el esquema lógico de Jaiva:

| Jaiva | PostgreSQL | Oracle | MySQL | SQL Server |
|---|---|---|---|---|
| `Int64` | `BIGINT` | `NUMBER(19)` | `BIGINT` | `BIGINT` |
| `Decimal` | `NUMERIC` | `NUMBER` | `DECIMAL` | `DECIMAL` |
| `String` | `TEXT/VARCHAR` | `VARCHAR2/CLOB` | `VARCHAR/TEXT` | `NVARCHAR` |
| `Timestamp` | `TIMESTAMP` | `TIMESTAMP` | `DATETIME` | `DATETIME2` |
| `TimestampWithTimezone` | `TIMESTAMPTZ` | `TIMESTAMP WITH TIME ZONE` | conversión | `DATETIMEOFFSET` |
| `Uuid` | `UUID` | `RAW(16)` | `BINARY(16)` | `UNIQUEIDENTIFIER` |
| `Binary` | `BYTEA` | `BLOB` | `BLOB` | `VARBINARY` |
| `Json` | `JSONB` | `JSON/CLOB` | `JSON` | `NVARCHAR/JSON` |

Las conversiones ambiguas deben fallar; no se convertirán silenciosamente a
texto.

## Relaciones

La primera versión emitirá:

```text
success
failure
```

No habrá éxito parcial: un lote se confirma por completo o se revierte.

El paquete exitoso incluirá:

```text
write.rows
write.batches
write.connection
write.database_type
write.duration_ms
```

## Métricas

```text
jaiva_database_rows_written_total
jaiva_database_batches_written_total
jaiva_database_write_errors_total
jaiva_database_write_duration_seconds
jaiva_database_transaction_rollbacks_total
```

Etiquetas permitidas:

```text
flow
processor
database_type
connection
```

No se utilizarán identificadores de paquete como etiquetas.

## Estrategia de comentarios en el código

Se usarán:

- `///` para contratos, estructuras y métodos públicos.
- `//!` para explicar el propósito de cada módulo.
- Comentarios internos para transacciones, seguridad, idempotencia o decisiones
  específicas de un dialecto.

No se comentarán operaciones obvias. Los comentarios deben explicar el motivo o
la garantía, no repetir literalmente el código.

Ejemplo:

```rust
// El paquete se confirma después del commit. Confirmarlo antes podría perder
// registros si la base rechaza la transacción.
transaction.commit().await?;
```

## Pruebas requeridas antes de considerar terminada la prioridad

### Unitarias

- Validación de identificadores por dialecto.
- Cálculo del tamaño de lote.
- Generación de `insert`.
- Generación de `upsert`.
- Mapeo de columnas.
- Rechazo de registros sin columnas requeridas.
- Conversión y rechazo de tipos.

### Integración PostgreSQL

- Insertar un lote.
- Repetir un upsert sin duplicar.
- Provocar rollback.
- Reiniciar Jaiva antes de confirmar.
- Recuperar el paquete persistente.
- Confirmar que el checkpoint no se adelanta.

### Adaptadores posteriores

Cada conector deberá ejecutar el mismo conjunto contractual contra:

- MySQL/MariaDB.
- Oracle.
- SQL Server.

## Orden de implementación

1. Crear contratos y validadores comunes.
2. Añadir adaptador PostgreSQL.
3. Implementar `put_database`.
4. Probar transacciones, rollback e idempotencia.
5. Publicar métricas.
6. Añadir MySQL/MariaDB.
7. Añadir SQL Server. **Completado en 4.2.2.**
8. Añadir Oracle. **Completado en 4.2.3.**
9. Incorporar rutas nativas de máxima velocidad.
