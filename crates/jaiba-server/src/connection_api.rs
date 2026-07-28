//! API del administrador de conexiones.
//!
//! Las respuestas exponen metadatos y el nombre de usuario, pero jamás la
//! contraseña ni la referencia interna del secreto. El almacén en memoria es
//! deliberadamente temporal; puede sustituirse por Vault/KMS sin cambiar la UI.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use jaiba_connection_manager::{
    AuditSink, ConnectionManager, ConnectionManagerError, ConnectionProfile, ConnectionStatus,
    ProfileRepository, SecretStore,
};
use jaiba_plugin_sdk::{
    Availability, ColumnMetadata, CompiledQuery, ConnectionEndpoint, ConnectionPlugin,
    ConnectionSecret, ConnectionTestResult, ConnectionType, DatabaseObject, DatabaseObjectKind,
    DiagnosticCheck, IndexMetadata, KeyMetadata, ObjectDescription, PluginDescriptor, PluginError,
    PoolStatus, QuerySpec,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode},
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
#[cfg(feature = "sqlserver-driver")]
use tiberius::{AuthMethod, Client, Config};
#[cfg(feature = "sqlserver-driver")]
use tokio::net::TcpStream;
#[cfg(feature = "sqlserver-driver")]
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

use crate::observability::{AppState, authorize};

#[derive(Debug, Serialize)]
pub(crate) struct ConnectionTypeView {
    id: &'static str,
    name: &'static str,
    category: &'static str,
    default_port: u16,
    enabled: bool,
    test_supported: bool,
    note: &'static str,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectionInput {
    name: String,
    connection_type: ConnectionType,
    host: String,
    port: u16,
    database: Option<String>,
    username: String,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    ssl: bool,
    #[serde(default = "pool_min")]
    pool_min: u32,
    #[serde(default = "pool_max")]
    pool_max: u32,
    #[serde(default = "timeout_ms")]
    timeout_ms: u64,
}

fn pool_min() -> u32 {
    1
}
fn pool_max() -> u32 {
    10
}
fn timeout_ms() -> u64 {
    10_000
}

#[derive(Debug, Serialize)]
pub(crate) struct ConnectionView {
    id: String,
    name: String,
    connection_type: ConnectionType,
    host: String,
    port: u16,
    database: Option<String>,
    username: String,
    ssl: bool,
    pool_min: u32,
    pool_max: u32,
    timeout_ms: u64,
    status: ConnectionStatus,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DuplicateInput {
    name: String,
}

#[derive(Serialize)]
struct ErrorMessage {
    message: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MetadataQuery {
    schema: Option<String>,
}

pub(crate) async fn list_metadata(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<MetadataQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state
        .connection_manager
        .list_objects(&id, query.schema.as_deref())
        .await
    {
        Ok(objects) => Json(objects).into_response(),
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn describe_metadata(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, schema, name)): Path<(String, String, String)>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let object = DatabaseObject {
        schema: Some(schema),
        name,
        kind: DatabaseObjectKind::Table,
    };
    match state.connection_manager.describe_object(&id, &object).await {
        Ok(description) => Json(description).into_response(),
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn connection_manager(
    secrets: Arc<dyn SecretStore>,
    persistence: Option<Arc<dyn ProfileRepository>>,
    audit: Option<Arc<dyn AuditSink>>,
) -> Result<Arc<ConnectionManager>, ConnectionManagerError> {
    let mut builder = ConnectionManager::new(secrets);
    if let Some(persistence) = persistence {
        builder = builder.with_persistence(persistence);
    }
    if let Some(audit) = audit {
        builder = builder.with_audit(audit);
    }
    let manager = Arc::new(builder);
    let restored = manager.load_persisted().await?;
    if restored > 0 {
        tracing::info!(target: "jaiba.connections", restored, "perfiles de conexión restaurados");
    }
    manager
        .register_plugin(Arc::new(PostgresConnectionPlugin))
        .await;
    manager
        .register_plugin(Arc::new(MySqlConnectionPlugin {
            connection_type: ConnectionType::MySql,
        }))
        .await;
    #[cfg(feature = "oracle-driver")]
    manager
        .register_plugin(Arc::new(OracleConnectionPlugin))
        .await;
    #[cfg(feature = "sqlserver-driver")]
    manager
        .register_plugin(Arc::new(SqlServerConnectionPlugin))
        .await;
    manager
        .register_plugin(Arc::new(MySqlConnectionPlugin {
            connection_type: ConnectionType::MariaDb,
        }))
        .await;
    Ok(manager)
}

pub(crate) async fn list_connection_types(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    Json(vec![
        type_view(
            "postgres",
            "PostgreSQL",
            "SQL",
            5432,
            true,
            true,
            "Driver SQLx activo",
        ),
        type_view(
            "mysql",
            "MySQL",
            "SQL",
            3306,
            true,
            true,
            "Driver SQLx activo",
        ),
        type_view(
            "maria_db",
            "MariaDB",
            "SQL",
            3306,
            true,
            true,
            "Driver SQLx activo",
        ),
        type_view(
            "oracle",
            "Oracle",
            "SQL",
            1521,
            cfg!(feature = "oracle-driver"),
            cfg!(feature = "oracle-driver"),
            "Activa la feature oracle-driver (requiere Oracle Client)",
        ),
        type_view(
            "sql_server",
            "SQL Server",
            "SQL",
            1433,
            cfg!(feature = "sqlserver-driver"),
            cfg!(feature = "sqlserver-driver"),
            "Activa la feature sqlserver-driver",
        ),
        type_view(
            "kafka",
            "Apache Kafka",
            "Mensajería",
            9092,
            cfg!(feature = "kafka-driver"),
            false,
            "Se administra como bus, no como base SQL",
        ),
        type_view(
            "opc_ua",
            "OPC-UA",
            "Industrial",
            4840,
            false,
            false,
            "Plugin futuro",
        ),
        type_view(
            "rest",
            "REST API",
            "API",
            443,
            false,
            false,
            "Plugin futuro",
        ),
    ])
    .into_response()
}

#[allow(clippy::too_many_arguments)]
fn type_view(
    id: &'static str,
    name: &'static str,
    category: &'static str,
    default_port: u16,
    enabled: bool,
    test_supported: bool,
    note: &'static str,
) -> ConnectionTypeView {
    ConnectionTypeView {
        id,
        name,
        category,
        default_port,
        enabled,
        test_supported,
        note,
    }
}

pub(crate) async fn list_connections(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let profiles = state.connection_manager.list().await;
    let mut views = Vec::with_capacity(profiles.len());
    for profile in profiles {
        match view(&state, profile).await {
            Ok(item) => views.push(item),
            Err(response) => return response,
        }
    }
    Json(views).into_response()
}

pub(crate) async fn get_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.connection_manager.get(&id).await {
        Ok(profile) => match view(&state, profile).await {
            Ok(item) => Json(item).into_response(),
            Err(response) => response,
        },
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn create_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<ConnectionInput>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if let Err(response) = validate_input(&input, true) {
        return response;
    }
    let secret_ref = format!("secret://connection/{}", Uuid::new_v4());
    if let Err(error) = state
        .connection_secrets
        .store(
            &secret_ref,
            ConnectionSecret {
                username: input.username.clone(),
                password: input.password.clone().unwrap_or_default(),
                options: Default::default(),
            },
        )
        .await
    {
        return manager_error(error);
    }
    let endpoint = endpoint(&input);
    match state
        .connection_manager
        .create(input.name, input.connection_type, endpoint, secret_ref.clone())
        .await
    {
        Ok(profile) => match view(&state, profile).await {
            Ok(item) => (StatusCode::CREATED, Json(item)).into_response(),
            Err(response) => response,
        },
        Err(error) => {
            let _ = state.connection_secrets.remove(&secret_ref).await;
            manager_error(error)
        }
    }
}

pub(crate) async fn update_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<ConnectionInput>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    if let Err(response) = validate_input(&input, false) {
        return response;
    }
    let mut profile = match state.connection_manager.get(&id).await {
        Ok(profile) => profile,
        Err(error) => return manager_error(error),
    };
    let previous = match state.connection_secrets.resolve(&profile.secret_ref).await {
        Ok(secret) => secret,
        Err(error) => return manager_error(error),
    };
    if let Err(error) = state
        .connection_secrets
        .store(
            &profile.secret_ref,
            ConnectionSecret {
                username: input.username.clone(),
                password: input.password.clone().unwrap_or(previous.password),
                options: previous.options,
            },
        )
        .await
    {
        return manager_error(error);
    }
    let endpoint = endpoint(&input);
    profile.name = input.name;
    profile.connection_type = input.connection_type;
    profile.endpoint = endpoint;
    if let Err(error) = state.connection_manager.update(profile.clone()).await {
        return manager_error(error);
    }
    match view(&state, profile).await {
        Ok(item) => Json(item).into_response(),
        Err(response) => response,
    }
}

pub(crate) async fn delete_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.connection_manager.delete(&id).await {
        Ok(profile) => {
            let _ = state.connection_secrets.remove(&profile.secret_ref).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn duplicate_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<DuplicateInput>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.connection_manager.duplicate(&id, input.name).await {
        Ok(profile) => match view(&state, profile).await {
            Ok(item) => (StatusCode::CREATED, Json(item)).into_response(),
            Err(response) => response,
        },
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn test_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.connection_manager.test(&id).await {
        Ok(_) => match state.connection_manager.get(&id).await {
            Ok(profile) => match view(&state, profile).await {
                Ok(item) => Json(item).into_response(),
                Err(response) => response,
            },
            Err(error) => manager_error(error),
        },
        Err(error) => manager_error(error),
    }
}

async fn view(state: &AppState, profile: ConnectionProfile) -> Result<ConnectionView, Response> {
    let secret = state
        .connection_secrets
        .resolve(&profile.secret_ref)
        .await
        .map_err(manager_error)?;
    let status = state
        .connection_manager
        .status(&profile.id)
        .await
        .map_err(manager_error)?;
    Ok(ConnectionView {
        id: profile.id,
        name: profile.name,
        connection_type: profile.connection_type,
        host: profile.endpoint.host,
        port: profile.endpoint.port,
        database: profile.endpoint.database,
        username: secret.username,
        ssl: profile.endpoint.ssl,
        pool_min: profile.endpoint.pool_min,
        pool_max: profile.endpoint.pool_max,
        timeout_ms: profile.endpoint.timeout_ms,
        status,
    })
}

fn endpoint(input: &ConnectionInput) -> ConnectionEndpoint {
    ConnectionEndpoint {
        host: input.host.trim().to_owned(),
        port: input.port,
        database: input
            .database
            .as_ref()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty()),
        ssl: input.ssl,
        pool_min: input.pool_min,
        pool_max: input.pool_max,
        timeout_ms: input.timeout_ms,
        options: Default::default(),
    }
}

#[allow(clippy::result_large_err)]
fn validate_input(input: &ConnectionInput, password_required: bool) -> Result<(), Response> {
    if input.name.trim().is_empty()
        || input.host.trim().is_empty()
        || input.username.trim().is_empty()
        || input.port == 0
    {
        return Err(bad_request(
            "nombre, host, puerto y usuario son obligatorios",
        ));
    }
    if password_required && input.password.as_deref().unwrap_or_default().is_empty() {
        return Err(bad_request(
            "la contraseña es obligatoria al crear la conexión",
        ));
    }
    if input.pool_min > input.pool_max || input.pool_max == 0 {
        return Err(bad_request("el pool mínimo no puede superar al máximo"));
    }
    if !matches!(
        input.connection_type,
        ConnectionType::Postgres
            | ConnectionType::MySql
            | ConnectionType::MariaDb
            | ConnectionType::Oracle
            | ConnectionType::SqlServer
    ) {
        return Err(bad_request(
            "el driver seleccionado todavía no admite perfiles comprobables",
        ));
    }
    Ok(())
}

fn manager_error(error: ConnectionManagerError) -> Response {
    let status = match error {
        ConnectionManagerError::NotFound(_) => StatusCode::NOT_FOUND,
        ConnectionManagerError::DuplicateName(_) => StatusCode::CONFLICT,
        ConnectionManagerError::MissingPlugin(_) | ConnectionManagerError::Plugin(_) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ConnectionManagerError::SecretUnavailable(_)
        | ConnectionManagerError::Persistence(_) => StatusCode::INTERNAL_SERVER_ERROR,
        ConnectionManagerError::MetadataTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
    };
    (
        status,
        Json(ErrorMessage {
            message: error.to_string(),
        }),
    )
        .into_response()
}

fn bad_request(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorMessage {
            message: message.to_owned(),
        }),
    )
        .into_response()
}

struct PostgresConnectionPlugin;

#[async_trait]
impl ConnectionPlugin for PostgresConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.postgres", "PostgreSQL")
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::Postgres
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let options = PgConnectOptions::new()
            .host(&endpoint.host)
            .port(endpoint.port)
            .username(&secret.username)
            .password(&secret.password)
            .database(endpoint.database.as_deref().unwrap_or("postgres"))
            .ssl_mode(if endpoint.ssl {
                PgSslMode::Require
            } else {
                PgSslMode::Disable
            });
        let pool = PgPoolOptions::new()
            .min_connections(endpoint.pool_min)
            .max_connections(endpoint.pool_max)
            .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
            .connect_with(options)
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let version = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(&pool)
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let result = success(
            started,
            version,
            pool.size(),
            pool.num_idle() as u32,
            endpoint.pool_max,
        );
        pool.close().await;
        Ok(result)
    }

    async fn diagnose(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        Err(PluginError::Unsupported("diagnóstico avanzado".to_owned()))
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let pool = postgres_pool(endpoint, secret).await?;
        let tables = sqlx::query(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables \
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
             AND ($1::text IS NULL OR table_schema = $1) ORDER BY table_schema, table_name",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let sequences = sqlx::query(
            "SELECT sequence_schema, sequence_name FROM information_schema.sequences \
             WHERE ($1::text IS NULL OR sequence_schema = $1) \
             ORDER BY sequence_schema, sequence_name",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let routines = sqlx::query(
            "SELECT routine_schema, routine_name, routine_type FROM information_schema.routines \
             WHERE routine_schema NOT IN ('pg_catalog', 'information_schema') \
             AND ($1::text IS NULL OR routine_schema = $1) ORDER BY routine_schema, routine_name",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        pool.close().await;
        let mut objects: Vec<DatabaseObject> = tables
            .into_iter()
            .map(|row| DatabaseObject {
                schema: Some(row.get("table_schema")),
                name: row.get("table_name"),
                kind: if row.get::<String, _>("table_type") == "VIEW" {
                    DatabaseObjectKind::View
                } else {
                    DatabaseObjectKind::Table
                },
            })
            .collect();
        objects.extend(sequences.into_iter().map(|row| DatabaseObject {
            schema: Some(row.get("sequence_schema")),
            name: row.get("sequence_name"),
            kind: DatabaseObjectKind::Sequence,
        }));
        objects.extend(routines.into_iter().map(|row| DatabaseObject {
            schema: Some(row.get("routine_schema")),
            name: row.get("routine_name"),
            kind: if row.get::<String, _>("routine_type") == "PROCEDURE" {
                DatabaseObjectKind::Procedure
            } else {
                DatabaseObjectKind::Function
            },
        }));
        Ok(with_schemas(objects, schema.is_none()))
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let schema_name = object.schema.as_deref().unwrap_or("public");
        let pool = postgres_pool(endpoint, secret).await?;
        let column_rows = sqlx::query(
            "SELECT column_name, data_type, is_nullable, ordinal_position, column_default \
             FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let key_rows = sqlx::query(
            "SELECT tc.constraint_name, tc.constraint_type, \
                    string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS columns \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON tc.constraint_name = kcu.constraint_name \
              AND tc.table_schema = kcu.table_schema \
             WHERE tc.table_schema = $1 AND tc.table_name = $2 \
               AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE') \
             GROUP BY tc.constraint_name, tc.constraint_type ORDER BY tc.constraint_type",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let index_rows = sqlx::query(
            "SELECT i.relname AS index_name, ix.indisunique AS is_unique, \
                    array_to_string(array_agg(a.attname ORDER BY x.ord), ',') AS columns \
             FROM pg_index ix \
             JOIN pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_class t ON t.oid = ix.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             JOIN unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord) ON true \
             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum \
             WHERE n.nspname = $1 AND t.relname = $2 \
             GROUP BY i.relname, ix.indisunique ORDER BY i.relname",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        pool.close().await;
        Ok(ObjectDescription {
            object: object.clone(),
            columns: column_rows
                .into_iter()
                .map(|row| ColumnMetadata {
                    name: row.get("column_name"),
                    data_type: row.get("data_type"),
                    nullable: row.get::<String, _>("is_nullable") == "YES",
                    ordinal: row.get::<i32, _>("ordinal_position") as u32,
                    default_value: row.try_get("column_default").ok(),
                })
                .collect(),
            keys: key_rows
                .into_iter()
                .map(|row| KeyMetadata {
                    name: row.get("constraint_name"),
                    kind: row.get("constraint_type"),
                    columns: split_columns(row.try_get::<String, _>("columns").ok()),
                })
                .collect(),
            indexes: index_rows
                .into_iter()
                .map(|row| IndexMetadata {
                    name: row.get("index_name"),
                    columns: split_columns(row.try_get::<String, _>("columns").ok()),
                    unique: row.get::<bool, _>("is_unique"),
                })
                .collect(),
        })
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("constructor SQL".to_owned()))
    }
}

struct MySqlConnectionPlugin {
    connection_type: ConnectionType,
}

#[async_trait]
impl ConnectionPlugin for MySqlConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.mysql", "MySQL / MariaDB")
    }

    fn connection_type(&self) -> ConnectionType {
        self.connection_type.clone()
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let mut options = MySqlConnectOptions::new()
            .host(&endpoint.host)
            .port(endpoint.port)
            .username(&secret.username)
            .password(&secret.password)
            .ssl_mode(if endpoint.ssl {
                MySqlSslMode::Required
            } else {
                MySqlSslMode::Disabled
            });
        if let Some(database) = endpoint.database.as_deref() {
            options = options.database(database);
        }
        let pool = MySqlPoolOptions::new()
            .min_connections(endpoint.pool_min)
            .max_connections(endpoint.pool_max)
            .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
            .connect_with(options)
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let version = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(&pool)
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let result = success(
            started,
            version,
            pool.size(),
            pool.num_idle() as u32,
            endpoint.pool_max,
        );
        pool.close().await;
        Ok(result)
    }

    async fn diagnose(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        Err(PluginError::Unsupported("diagnóstico avanzado".to_owned()))
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let pool = mysql_pool(endpoint, secret).await?;
        let selected = schema.or(endpoint.database.as_deref());
        let tables = sqlx::query(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables \
             WHERE (? IS NULL OR table_schema = ?) AND table_schema NOT IN \
             ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY table_schema, table_name",
        )
        .bind(selected)
        .bind(selected)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let routines = sqlx::query(
            "SELECT routine_schema, routine_name, routine_type FROM information_schema.routines \
             WHERE (? IS NULL OR routine_schema = ?) AND routine_schema NOT IN \
             ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY routine_schema, routine_name",
        )
        .bind(selected)
        .bind(selected)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        pool.close().await;
        let mut objects: Vec<DatabaseObject> = tables
            .into_iter()
            .map(|row| DatabaseObject {
                schema: Some(row.get("table_schema")),
                name: row.get("table_name"),
                kind: if row.get::<String, _>("table_type") == "VIEW" {
                    DatabaseObjectKind::View
                } else {
                    DatabaseObjectKind::Table
                },
            })
            .collect();
        objects.extend(routines.into_iter().map(|row| DatabaseObject {
            schema: Some(row.get("routine_schema")),
            name: row.get("routine_name"),
            kind: if row.get::<String, _>("routine_type") == "PROCEDURE" {
                DatabaseObjectKind::Procedure
            } else {
                DatabaseObjectKind::Function
            },
        }));
        Ok(with_schemas(objects, schema.is_none()))
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let schema_name = object.schema.as_deref().or(endpoint.database.as_deref());
        let pool = mysql_pool(endpoint, secret).await?;
        let column_rows = sqlx::query(
            "SELECT column_name, column_type, is_nullable, ordinal_position, column_default \
             FROM information_schema.columns WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let key_rows = sqlx::query(
            "SELECT tc.constraint_name, tc.constraint_type, \
                    GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) AS columns \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON tc.constraint_name = kcu.constraint_name \
              AND tc.table_schema = kcu.table_schema \
              AND tc.table_name = kcu.table_name \
             WHERE tc.table_schema = ? AND tc.table_name = ? \
             GROUP BY tc.constraint_name, tc.constraint_type ORDER BY tc.constraint_type",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let index_rows = sqlx::query(
            "SELECT index_name, non_unique, \
                    GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns \
             FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? \
             GROUP BY index_name, non_unique ORDER BY index_name",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        pool.close().await;
        Ok(ObjectDescription {
            object: object.clone(),
            columns: column_rows
                .into_iter()
                .map(|row| ColumnMetadata {
                    name: row.get("column_name"),
                    data_type: row.get("column_type"),
                    nullable: row.get::<String, _>("is_nullable") == "YES",
                    ordinal: row.get::<u32, _>("ordinal_position"),
                    default_value: row.try_get("column_default").ok(),
                })
                .collect(),
            keys: key_rows
                .into_iter()
                .map(|row| KeyMetadata {
                    name: row.get("constraint_name"),
                    kind: row.get("constraint_type"),
                    columns: split_columns(row.try_get::<String, _>("columns").ok()),
                })
                .collect(),
            indexes: index_rows
                .into_iter()
                .map(|row| IndexMetadata {
                    name: row.get("index_name"),
                    columns: split_columns(row.try_get::<String, _>("columns").ok()),
                    unique: row.get::<i64, _>("non_unique") == 0,
                })
                .collect(),
        })
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("constructor SQL".to_owned()))
    }
}

#[cfg(feature = "oracle-driver")]
struct OracleConnectionPlugin;

#[cfg(feature = "oracle-driver")]
#[async_trait]
impl ConnectionPlugin for OracleConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.oracle", "Oracle")
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::Oracle
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let connection = oracle_connect(&endpoint, &secret)?;
            let version = connection
                .query_row_as::<String>("SELECT banner FROM v$version WHERE ROWNUM = 1", &[])
                .map_err(|error| PluginError::Connection(error.to_string()))?;
            Ok(success(started, version, 1, 0, endpoint.pool_max))
        })
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))?
    }

    async fn diagnose(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        Err(PluginError::Unsupported("diagnóstico avanzado".to_owned()))
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        let schema = schema.map(str::to_owned);
        let include_schemas = schema.is_none();
        tokio::task::spawn_blocking(move || {
            let connection = oracle_connect(&endpoint, &secret)?;
            let owner = schema.unwrap_or_else(|| secret.username.to_uppercase());
            let rows = connection
                .query(
                    "SELECT owner, object_name, object_type FROM all_objects \
                     WHERE owner = :1 AND object_type IN ('TABLE', 'VIEW') \
                     ORDER BY object_name",
                    &[&owner],
                )
                .map_err(|error| PluginError::Exploration(error.to_string()))?;
            let objects = rows
                .map(|row| {
                    let row = row.map_err(|error| PluginError::Exploration(error.to_string()))?;
                    let object_type: String = row.get(2).map_err(oracle_exploration)?;
                    Ok(DatabaseObject {
                        schema: Some(row.get(0).map_err(oracle_exploration)?),
                        name: row.get(1).map_err(oracle_exploration)?,
                        kind: if object_type == "VIEW" {
                            DatabaseObjectKind::View
                        } else {
                            DatabaseObjectKind::Table
                        },
                    })
                })
                .collect::<Result<Vec<_>, PluginError>>()?;
            Ok(with_schemas(objects, include_schemas))
        })
        .await
        .map_err(|error| PluginError::Exploration(error.to_string()))?
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        let object = object.clone();
        tokio::task::spawn_blocking(move || {
            let connection = oracle_connect(&endpoint, &secret)?;
            let owner = object
                .schema
                .clone()
                .unwrap_or_else(|| secret.username.to_uppercase());
            let rows = connection
                .query(
                    "SELECT column_name, data_type, nullable, column_id, data_default \
                 FROM all_tab_columns WHERE owner = :1 AND table_name = :2 ORDER BY column_id",
                    &[&owner, &object.name],
                )
                .map_err(oracle_exploration)?;
            let columns = rows
                .map(|row| {
                    let row = row.map_err(oracle_exploration)?;
                    Ok(ColumnMetadata {
                        name: row.get(0).map_err(oracle_exploration)?,
                        data_type: row.get(1).map_err(oracle_exploration)?,
                        nullable: row.get::<_, String>(2).map_err(oracle_exploration)? == "Y",
                        ordinal: row.get::<_, u32>(3).map_err(oracle_exploration)?,
                        default_value: row.get(4).ok(),
                    })
                })
                .collect::<Result<Vec<_>, PluginError>>()?;
            Ok(description(&object, columns))
        })
        .await
        .map_err(|error| PluginError::Exploration(error.to_string()))?
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("constructor SQL".to_owned()))
    }
}

#[cfg(feature = "oracle-driver")]
fn oracle_connect(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<oracle::Connection, PluginError> {
    let service = endpoint.database.as_deref().unwrap_or("");
    oracle::Connection::connect(
        &secret.username,
        &secret.password,
        format!("{}:{}/{}", endpoint.host, endpoint.port, service),
    )
    .map_err(|error| PluginError::Connection(error.to_string()))
}

#[cfg(feature = "oracle-driver")]
fn oracle_exploration(error: oracle::Error) -> PluginError {
    PluginError::Exploration(error.to_string())
}

#[cfg(feature = "sqlserver-driver")]
struct SqlServerConnectionPlugin;

#[cfg(feature = "sqlserver-driver")]
type SqlServerClient = Client<Compat<TcpStream>>;

#[cfg(feature = "sqlserver-driver")]
#[async_trait]
impl ConnectionPlugin for SqlServerConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.sqlserver", "SQL Server")
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::SqlServer
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let row = client
            .simple_query("SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))")
            .await
            .map_err(sqlserver_connection)?
            .into_row()
            .await
            .map_err(sqlserver_connection)?
            .ok_or_else(|| PluginError::Connection("SQL Server no devolvió versión".to_owned()))?;
        Ok(success(
            started,
            row.get::<&str, _>(0).unwrap_or("SQL Server").to_owned(),
            1,
            0,
            endpoint.pool_max,
        ))
    }

    async fn diagnose(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        Err(PluginError::Unsupported("diagnóstico avanzado".to_owned()))
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let selected = schema.unwrap_or("");
        let rows = client
            .query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES \
                 WHERE (@P1 = '' OR TABLE_SCHEMA = @P1) ORDER BY TABLE_SCHEMA, TABLE_NAME",
                &[&selected],
            )
            .await
            .map_err(sqlserver_exploration)?
            .into_first_result()
            .await
            .map_err(sqlserver_exploration)?;
        Ok(with_schemas(
            rows.into_iter()
                .map(|row| {
                    let object_type = row.get::<&str, _>(2).unwrap_or("BASE TABLE");
                    DatabaseObject {
                        schema: row.get::<&str, _>(0).map(str::to_owned),
                        name: row.get::<&str, _>(1).unwrap_or_default().to_owned(),
                        kind: if object_type == "VIEW" {
                            DatabaseObjectKind::View
                        } else {
                            DatabaseObjectKind::Table
                        },
                    }
                })
                .collect(),
            schema.is_none(),
        ))
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let schema = object.schema.as_deref().unwrap_or("dbo");
        let rows = client
            .query(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION, COLUMN_DEFAULT \
                 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 \
                 ORDER BY ORDINAL_POSITION",
                &[&schema, &object.name.as_str()],
            )
            .await
            .map_err(sqlserver_exploration)?
            .into_first_result()
            .await
            .map_err(sqlserver_exploration)?;
        let columns = rows
            .into_iter()
            .map(|row| ColumnMetadata {
                name: row.get::<&str, _>(0).unwrap_or_default().to_owned(),
                data_type: row.get::<&str, _>(1).unwrap_or_default().to_owned(),
                nullable: row.get::<&str, _>(2) == Some("YES"),
                ordinal: row.get::<i32, _>(3).unwrap_or_default() as u32,
                default_value: row.get::<&str, _>(4).map(str::to_owned),
            })
            .collect();
        Ok(description(object, columns))
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("constructor SQL".to_owned()))
    }
}

#[cfg(feature = "sqlserver-driver")]
async fn sqlserver_connect(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<SqlServerClient, PluginError> {
    let mut config = Config::new();
    config.host(&endpoint.host);
    config.port(endpoint.port);
    if let Some(database) = endpoint.database.as_deref() {
        config.database(database);
    }
    config.authentication(AuthMethod::sql_server(&secret.username, &secret.password));
    if !endpoint.ssl {
        config.trust_cert();
    }
    let tcp = tokio::time::timeout(
        Duration::from_millis(endpoint.timeout_ms),
        TcpStream::connect(config.get_addr()),
    )
    .await
    .map_err(|_| PluginError::Connection("timeout conectando a SQL Server".to_owned()))?
    .map_err(sqlserver_connection)?;
    tcp.set_nodelay(true).map_err(sqlserver_connection)?;
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(sqlserver_connection)
}

#[cfg(feature = "sqlserver-driver")]
fn sqlserver_connection(error: impl std::fmt::Display) -> PluginError {
    PluginError::Connection(error.to_string())
}

#[cfg(feature = "sqlserver-driver")]
fn sqlserver_exploration(error: impl std::fmt::Display) -> PluginError {
    PluginError::Exploration(error.to_string())
}

async fn postgres_pool(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<sqlx::PgPool, PluginError> {
    let options = PgConnectOptions::new()
        .host(&endpoint.host)
        .port(endpoint.port)
        .username(&secret.username)
        .password(&secret.password)
        .database(endpoint.database.as_deref().unwrap_or("postgres"))
        .ssl_mode(if endpoint.ssl {
            PgSslMode::Require
        } else {
            PgSslMode::Disable
        });
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
        .connect_with(options)
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))
}

async fn mysql_pool(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<sqlx::MySqlPool, PluginError> {
    let mut options = MySqlConnectOptions::new()
        .host(&endpoint.host)
        .port(endpoint.port)
        .username(&secret.username)
        .password(&secret.password)
        .ssl_mode(if endpoint.ssl {
            MySqlSslMode::Required
        } else {
            MySqlSslMode::Disabled
        });
    if let Some(database) = endpoint.database.as_deref() {
        options = options.database(database);
    }
    MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
        .connect_with(options)
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))
}

fn exploration_error(error: sqlx::Error) -> PluginError {
    PluginError::Exploration(error.to_string())
}

#[cfg(any(feature = "oracle-driver", feature = "sqlserver-driver"))]
fn description(object: &DatabaseObject, columns: Vec<ColumnMetadata>) -> ObjectDescription {
    ObjectDescription {
        object: object.clone(),
        columns,
        keys: Vec::new(),
        indexes: Vec::new(),
    }
}

/// Divide una lista de columnas separadas por comas (proveniente de `string_agg` /
/// `GROUP_CONCAT`) en nombres individuales, descartando entradas vacías.
fn split_columns(value: Option<String>) -> Vec<String> {
    value
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn with_schemas(mut objects: Vec<DatabaseObject>, include: bool) -> Vec<DatabaseObject> {
    if !include {
        return objects;
    }
    let mut schemas = objects
        .iter()
        .filter_map(|object| object.schema.clone())
        .collect::<Vec<_>>();
    schemas.sort();
    schemas.dedup();
    let mut result = schemas
        .into_iter()
        .map(|name| DatabaseObject {
            schema: None,
            name,
            kind: DatabaseObjectKind::Schema,
        })
        .collect::<Vec<_>>();
    result.append(&mut objects);
    result
}

fn descriptor(id: &str, name: &str) -> PluginDescriptor {
    PluginDescriptor {
        id: id.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        display_name: name.to_owned(),
        capabilities: vec!["test_connection".to_owned()],
    }
}

fn success(
    started: Instant,
    version: String,
    active: u32,
    idle: u32,
    maximum: u32,
) -> ConnectionTestResult {
    ConnectionTestResult {
        availability: Availability::Available,
        latency_ms: started.elapsed().as_millis() as u64,
        version: Some(version),
        pool: Some(PoolStatus {
            active,
            idle,
            maximum,
        }),
        tested_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
        message: Some("Conexión validada".to_owned()),
    }
}
