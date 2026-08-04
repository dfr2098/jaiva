//! API del administrador de conexiones.
//!
//! Las respuestas exponen metadatos y el nombre de usuario, pero jamás la
//! contraseña ni la referencia interna del secreto. El almacén en memoria es
//! deliberadamente temporal; puede sustituirse por Vault/KMS sin cambiar la UI.

use std::{
    collections::BTreeMap,
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
#[cfg(feature = "mongodb-driver")]
use mongodb::{
    Client as MongoClient,
    bson::{Bson, Document, doc},
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
#[cfg(feature = "mongodb-driver")]
use url::Url;
use uuid::Uuid;

use crate::auth::Permission;
use crate::observability::{AppState, admin_actor, authorize_perm};

#[derive(Debug, Serialize)]
pub(crate) struct ConnectionTypeView {
    id: ConnectionType,
    plugin_id: String,
    version: String,
    name: String,
    category: String,
    default_port: u16,
    capabilities: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectionInput {
    name: String,
    connection_type: ConnectionType,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u16,
    database: Option<String>,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: Option<String>,
    /// URL completa (`mongodb://` / `mongodb+srv://`). Solo MongoDB.
    /// Si se envía, tiene prioridad sobre host/puerto/usuario/contraseña sueltos.
    #[serde(default)]
    url: Option<String>,
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
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
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
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
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

pub(crate) async fn compile_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(specification): Json<QuerySpec>,
) -> Response {
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
        return response;
    }
    match state
        .connection_manager
        .compile_query(&id, &specification)
        .await
    {
        Ok(compiled) => Json(compiled).into_response(),
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
    #[cfg(feature = "mongodb-driver")]
    manager
        .register_plugin(Arc::new(MongoDbConnectionPlugin))
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
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
        return response;
    }
    Json(
        state
            .connection_manager
            .adapters()
            .await
            .into_iter()
            .map(|(id, descriptor)| ConnectionTypeView {
                id,
                plugin_id: descriptor.id,
                version: descriptor.version,
                name: descriptor.display_name,
                category: descriptor.category,
                default_port: descriptor.default_port,
                capabilities: descriptor.capabilities,
            })
            .collect::<Vec<_>>(),
    )
    .into_response()
}

pub(crate) async fn list_connections(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
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
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
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
    let ctx = match authorize_perm(&state, &headers, Permission::Admin) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    if let Err(response) = validate_input(&input, true) {
        return response;
    }
    if !state
        .connection_manager
        .supports(&input.connection_type)
        .await
    {
        return manager_error(ConnectionManagerError::MissingPlugin(
            input.connection_type.clone(),
        ));
    }
    let (endpoint, secret) = match materialize_connection(&input, None) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let secret_ref = format!("secret://connection/{}", Uuid::new_v4());
    if let Err(error) = state.connection_secrets.store(&secret_ref, secret).await {
        return manager_error(error);
    }
    match state
        .connection_manager
        .create(
            input.name.trim().to_owned(),
            input.connection_type,
            endpoint,
            secret_ref.clone(),
        )
        .await
    {
        Ok(profile) => {
            tracing::warn!(
                audit_action = "connection_create",
                actor = admin_actor(&ctx),
                profile_id = %profile.id,
                profile_name = %profile.name,
                "administrative action"
            );
            match view(&state, profile).await {
                Ok(item) => (StatusCode::CREATED, Json(item)).into_response(),
                Err(response) => response,
            }
        }
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
    let ctx = match authorize_perm(&state, &headers, Permission::Admin) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    if let Err(response) = validate_input(&input, false) {
        return response;
    }
    if !state
        .connection_manager
        .supports(&input.connection_type)
        .await
    {
        return manager_error(ConnectionManagerError::MissingPlugin(
            input.connection_type.clone(),
        ));
    }
    let mut profile = match state.connection_manager.get(&id).await {
        Ok(profile) => profile,
        Err(error) => return manager_error(error),
    };
    let previous = match state.connection_secrets.resolve(&profile.secret_ref).await {
        Ok(secret) => secret,
        Err(error) => return manager_error(error),
    };
    let (endpoint, secret) = match materialize_connection(&input, Some(&previous)) {
        Ok(value) => value,
        Err(response) => return response,
    };
    if let Err(error) = state
        .connection_secrets
        .store(&profile.secret_ref, secret)
        .await
    {
        return manager_error(error);
    }
    profile.name = input.name.trim().to_owned();
    profile.connection_type = input.connection_type;
    profile.endpoint = endpoint;
    if let Err(error) = state.connection_manager.update(profile.clone()).await {
        return manager_error(error);
    }
    tracing::warn!(
        audit_action = "connection_update",
        actor = admin_actor(&ctx),
        profile_id = %profile.id,
        profile_name = %profile.name,
        "administrative action"
    );
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
    let ctx = match authorize_perm(&state, &headers, Permission::Admin) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    match state.connection_manager.delete(&id).await {
        Ok(profile) => {
            tracing::warn!(
                audit_action = "connection_delete",
                actor = admin_actor(&ctx),
                profile_id = %profile.id,
                profile_name = %profile.name,
                "administrative action"
            );
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
    let ctx = match authorize_perm(&state, &headers, Permission::Admin) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    match state.connection_manager.duplicate(&id, input.name).await {
        Ok(profile) => {
            tracing::warn!(
                audit_action = "connection_duplicate",
                actor = admin_actor(&ctx),
                profile_id = %profile.id,
                profile_name = %profile.name,
                source_id = %id,
                "administrative action"
            );
            match view(&state, profile).await {
                Ok(item) => (StatusCode::CREATED, Json(item)).into_response(),
                Err(response) => response,
            }
        }
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn test_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let ctx = match authorize_perm(&state, &headers, Permission::Admin) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    match state.connection_manager.test(&id).await {
        Ok(_) => {
            tracing::warn!(
                audit_action = "connection_test",
                actor = admin_actor(&ctx),
                profile_id = %id,
                "administrative action"
            );
            match state.connection_manager.get(&id).await {
                Ok(profile) => match view(&state, profile).await {
                    Ok(item) => Json(item).into_response(),
                    Err(response) => response,
                },
                Err(error) => manager_error(error),
            }
        }
        Err(error) => manager_error(error),
    }
}

pub(crate) async fn diagnose_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize_perm(&state, &headers, Permission::Read) {
        return response;
    }
    match state.connection_manager.diagnose(&id).await {
        Ok(checks) => Json(checks).into_response(),
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

fn endpoint_from_parts(
    host: String,
    port: u16,
    database: Option<String>,
    ssl: bool,
    input: &ConnectionInput,
) -> ConnectionEndpoint {
    ConnectionEndpoint {
        host,
        port,
        database,
        ssl,
        pool_min: input.pool_min,
        pool_max: input.pool_max,
        timeout_ms: input.timeout_ms,
        options: BTreeMap::new(),
    }
}

/// Construye endpoint + secreto a partir de campos sueltos o de una URL MongoDB.
#[allow(clippy::result_large_err)]
fn materialize_connection(
    input: &ConnectionInput,
    previous: Option<&ConnectionSecret>,
) -> Result<(ConnectionEndpoint, ConnectionSecret), Response> {
    let url = input
        .url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(raw) = url {
        if input.connection_type != ConnectionType::MongoDb {
            return Err(bad_request(
                "el campo url solo está soportado para conexiones MongoDB",
            ));
        }
        #[cfg(feature = "mongodb-driver")]
        {
            let parsed = parse_mongodb_connection_url(raw).map_err(bad_request)?;
            let username = if !input.username.trim().is_empty() {
                input.username.trim().to_owned()
            } else {
                parsed.username.clone()
            };
            let password = input
                .password
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .or_else(|| parsed.password.clone())
                .or_else(|| previous.map(|secret| secret.password.clone()))
                .unwrap_or_default();
            let mut options = BTreeMap::new();
            if let Some(auth_source) = &parsed.auth_source {
                options.insert("auth_source".to_owned(), auth_source.clone());
            }
            let connection_url =
                apply_credentials_to_mongo_url(raw, &username, &password).map_err(bad_request)?;
            options.insert("connection_url".to_owned(), connection_url);
            let database = input
                .database
                .as_ref()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
                .or(parsed.database);
            let endpoint = endpoint_from_parts(
                parsed.host,
                parsed.port,
                database,
                input.ssl || parsed.ssl,
                input,
            );
            return Ok((
                endpoint,
                ConnectionSecret {
                    username,
                    password,
                    options,
                },
            ));
        }
        #[cfg(not(feature = "mongodb-driver"))]
        {
            let _ = raw;
            return Err(bad_request(
                "MongoDB requiere compilar con --features mongodb-driver",
            ));
        }
    }

    #[cfg_attr(not(feature = "mongodb-driver"), allow(unused_mut))]
    let mut options = previous
        .map(|secret| secret.options.clone())
        .unwrap_or_default();
    let username = input.username.trim().to_owned();
    let password = input
        .password
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| previous.map(|secret| secret.password.clone()))
        .unwrap_or_default();
    // Si el perfil ya tenía URI (Atlas/SRV), actualizar credenciales en ella.
    #[cfg(feature = "mongodb-driver")]
    if input.connection_type == ConnectionType::MongoDb {
        if let Some(stored) = options.get("connection_url").cloned() {
            let updated = apply_credentials_to_mongo_url(&stored, &username, &password)
                .map_err(bad_request)?;
            options.insert("connection_url".to_owned(), updated);
        }
    }
    let database = input
        .database
        .as_ref()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let endpoint = endpoint_from_parts(
        input.host.trim().to_owned(),
        input.port,
        database,
        input.ssl,
        input,
    );
    Ok((
        endpoint,
        ConnectionSecret {
            username,
            password,
            options,
        },
    ))
}

#[allow(clippy::result_large_err)]
fn validate_input(input: &ConnectionInput, password_required: bool) -> Result<(), Response> {
    if input.name.trim().is_empty() {
        return Err(bad_request("el nombre del perfil es obligatorio"));
    }
    if input.pool_min > input.pool_max || input.pool_max == 0 {
        return Err(bad_request("el pool mínimo no puede superar al máximo"));
    }

    let url = input
        .url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(raw) = url {
        if input.connection_type != ConnectionType::MongoDb {
            return Err(bad_request(
                "el campo url solo está soportado para conexiones MongoDB",
            ));
        }
        #[cfg(feature = "mongodb-driver")]
        {
            let parsed = parse_mongodb_connection_url(raw).map_err(bad_request)?;
            let username = if !input.username.trim().is_empty() {
                input.username.trim()
            } else {
                parsed.username.as_str()
            };
            if username.is_empty() {
                return Err(bad_request(
                    "la URL MongoDB debe incluir usuario, o indíquelo en el formulario",
                ));
            }
            let password = input
                .password
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .or(parsed.password.as_deref());
            if password_required && password.unwrap_or_default().is_empty() {
                return Err(bad_request(
                    "la contraseña es obligatoria (en la URL o en el formulario)",
                ));
            }
            return Ok(());
        }
        #[cfg(not(feature = "mongodb-driver"))]
        {
            let _ = raw;
            return Err(bad_request(
                "MongoDB requiere compilar con --features mongodb-driver",
            ));
        }
    }

    if input.host.trim().is_empty() || input.username.trim().is_empty() || input.port == 0 {
        return Err(bad_request(
            "nombre, host, puerto y usuario son obligatorios (o una URL MongoDB válida)",
        ));
    }
    if password_required && input.password.as_deref().unwrap_or_default().is_empty() {
        return Err(bad_request(
            "la contraseña es obligatoria al crear la conexión",
        ));
    }
    Ok(())
}

fn manager_error(error: ConnectionManagerError) -> Response {
    // Detalle completo solo en logs; el cliente recibe mensaje redactado.
    tracing::warn!(
        target: "jaiba.connections",
        error = %error,
        "connection manager error"
    );
    let status = match error {
        ConnectionManagerError::NotFound(_) => StatusCode::NOT_FOUND,
        ConnectionManagerError::DuplicateName(_) => StatusCode::CONFLICT,
        ConnectionManagerError::MissingPlugin(_) | ConnectionManagerError::Plugin(_) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ConnectionManagerError::SecretUnavailable(_) | ConnectionManagerError::Persistence(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        ConnectionManagerError::MetadataTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
    };
    (
        status,
        Json(ErrorMessage {
            message: error.client_message(),
        }),
    )
        .into_response()
}

fn bad_request(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorMessage {
            message: message.into(),
        }),
    )
        .into_response()
}

struct PostgresConnectionPlugin;

#[async_trait]
impl ConnectionPlugin for PostgresConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.postgres", "PostgreSQL", 5432, true, true)
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
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let pool = postgres_pool(endpoint, secret).await?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let version = sqlx::query_scalar::<_, String>("SELECT VERSION()")
            .fetch_one(&pool)
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let version_latency = version_started.elapsed().as_millis() as u64;

        let metadata_started = Instant::now();
        let visible_objects = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = current_schema()",
        )
        .fetch_one(&pool)
        .await
        .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let metadata_latency = metadata_started.elapsed().as_millis() as u64;
        pool.close().await;

        Ok(vec![
            DiagnosticCheck {
                code: "connectivity".to_owned(),
                label: "Conectividad".to_owned(),
                status: Availability::Available,
                latency_ms: Some(connection_latency),
                details: serde_json::json!({
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "ssl": endpoint.ssl,
                }),
            },
            DiagnosticCheck {
                code: "server_version".to_owned(),
                label: "Versión del servidor".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({ "version": version }),
            },
            DiagnosticCheck {
                code: "metadata_access".to_owned(),
                label: "Acceso a metadatos".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": endpoint.database,
                    "visible_objects": visible_objects,
                }),
            },
        ])
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

    fn compile_query(&self, specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        let mut compiled =
            crate::sql_builder::compile(specification, crate::sql_builder::Dialect::Postgres)?;
        compiled.processor_type = Some("query_postgres".to_owned());
        compiled.execution_statement = Some(format!(
            "SELECT to_jsonb(t) AS record FROM (\n{}\n) AS t",
            compiled.statement
        ));
        Ok(compiled)
    }
}

struct MySqlConnectionPlugin {
    connection_type: ConnectionType,
}

#[async_trait]
impl ConnectionPlugin for MySqlConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.mysql", "MySQL / MariaDB", 3306, true, false)
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
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let pool = mysql_pool(endpoint, secret).await?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let version = sqlx::query_scalar::<_, String>("SELECT VERSION()")
            .fetch_one(&pool)
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let version_latency = version_started.elapsed().as_millis() as u64;

        let metadata_started = Instant::now();
        let visible_objects = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = DATABASE()",
        )
        .fetch_one(&pool)
        .await
        .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let metadata_latency = metadata_started.elapsed().as_millis() as u64;
        pool.close().await;

        Ok(vec![
            DiagnosticCheck {
                code: "connectivity".to_owned(),
                label: "Conectividad".to_owned(),
                status: Availability::Available,
                latency_ms: Some(connection_latency),
                details: serde_json::json!({
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "ssl": endpoint.ssl,
                }),
            },
            DiagnosticCheck {
                code: "server_version".to_owned(),
                label: "Versión del servidor".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({ "version": version }),
            },
            DiagnosticCheck {
                code: "metadata_access".to_owned(),
                label: "Acceso a metadatos".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": endpoint.database,
                    "visible_objects": visible_objects,
                }),
            },
        ])
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
            "SELECT CAST(table_schema AS CHAR) AS table_schema, \
                    CAST(table_name AS CHAR) AS table_name, \
                    CAST(table_type AS CHAR) AS table_type FROM information_schema.tables \
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
            "SELECT CAST(routine_schema AS CHAR) AS routine_schema, \
                    CAST(routine_name AS CHAR) AS routine_name, \
                    CAST(routine_type AS CHAR) AS routine_type FROM information_schema.routines \
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
            "SELECT CAST(column_name AS CHAR) AS column_name, \
                    CAST(column_type AS CHAR) AS column_type, \
                    CAST(is_nullable AS CHAR) AS is_nullable, \
                    ordinal_position AS ordinal_position, \
                    CAST(column_default AS CHAR) AS column_default \
             FROM information_schema.columns WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let key_rows = sqlx::query(
            "SELECT CAST(tc.constraint_name AS CHAR) AS constraint_name, \
                    CAST(tc.constraint_type AS CHAR) AS constraint_type, \
                    CAST(GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) AS CHAR) AS columns \
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
            "SELECT CAST(index_name AS CHAR) AS index_name, non_unique AS non_unique, \
                    CAST(GROUP_CONCAT(column_name ORDER BY seq_in_index) AS CHAR) AS columns \
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
                    ordinal: row
                        .try_get::<u64, _>("ordinal_position")
                        .map(|value| value as u32)
                        .or_else(|_| row.try_get::<u32, _>("ordinal_position"))
                        .unwrap_or(0),
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
                    unique: row
                        .try_get::<i64, _>("non_unique")
                        .or_else(|_| row.try_get::<i32, _>("non_unique").map(i64::from))
                        .unwrap_or(1)
                        == 0,
                })
                .collect(),
        })
    }

    fn compile_query(&self, specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        crate::sql_builder::compile(specification, crate::sql_builder::Dialect::MySql)
    }
}

#[cfg(feature = "mongodb-driver")]
struct MongoDbConnectionPlugin;

#[cfg(feature = "mongodb-driver")]
#[async_trait]
impl ConnectionPlugin for MongoDbConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            id: "jaiba.mongodb".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            display_name: "MongoDB".to_owned(),
            category: "Documental".to_owned(),
            default_port: 27_017,
            capabilities: vec![
                "test".to_owned(),
                "diagnostics".to_owned(),
                "schema_explorer".to_owned(),
            ],
        }
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::MongoDb
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let client = mongodb_client(endpoint, secret).await?;
        let database = mongodb_database(endpoint)?;
        client
            .database(database)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let build_info = client
            .database("admin")
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let version = build_info
            .get_str("version")
            .unwrap_or("MongoDB")
            .to_owned();
        Ok(success(started, version, 1, 0, endpoint.pool_max))
    }

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let client = mongodb_client(endpoint, secret).await?;
        let database_name = mongodb_database(endpoint)?;
        client
            .database(database_name)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let build_info = client
            .database("admin")
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let version_latency = version_started.elapsed().as_millis() as u64;
        let version = build_info
            .get_str("version")
            .unwrap_or("MongoDB")
            .to_owned();

        let metadata_started = Instant::now();
        let visible_objects = client
            .database(database_name)
            .list_collection_names()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .len();
        let metadata_latency = metadata_started.elapsed().as_millis() as u64;

        Ok(vec![
            DiagnosticCheck {
                code: "connectivity".to_owned(),
                label: "Conectividad".to_owned(),
                status: Availability::Available,
                latency_ms: Some(connection_latency),
                details: serde_json::json!({
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "ssl": endpoint.ssl,
                }),
            },
            DiagnosticCheck {
                code: "server_version".to_owned(),
                label: "Versión del servidor".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({ "version": version }),
            },
            DiagnosticCheck {
                code: "metadata_access".to_owned(),
                label: "Acceso a colecciones".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": database_name,
                    "visible_objects": visible_objects,
                }),
            },
        ])
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let client = mongodb_client(endpoint, secret).await?;
        let database_name = schema.unwrap_or(mongodb_database(endpoint)?);
        let mut collections = client
            .database(database_name)
            .list_collection_names()
            .await
            .map_err(|error| PluginError::Exploration(error.to_string()))?
            .into_iter()
            .map(|name| DatabaseObject {
                schema: Some(database_name.to_owned()),
                name,
                kind: DatabaseObjectKind::Collection,
            })
            .collect::<Vec<_>>();
        collections.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(collections)
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let client = mongodb_client(endpoint, secret).await?;
        let database_name = object
            .schema
            .as_deref()
            .unwrap_or(mongodb_database(endpoint)?);
        let sample = client
            .database(database_name)
            .collection::<Document>(&object.name)
            .find_one(doc! {})
            .await
            .map_err(|error| PluginError::Exploration(error.to_string()))?;
        let columns = sample
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(ordinal, (name, value))| ColumnMetadata {
                data_type: mongodb_bson_type(&value).to_owned(),
                nullable: name != "_id",
                ordinal: ordinal as u32 + 1,
                name,
                default_value: None,
            })
            .collect();
        let mut collection = object.clone();
        collection.kind = DatabaseObjectKind::Collection;
        Ok(description(&collection, columns))
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported(
            "constructor de consultas MongoDB".to_owned(),
        ))
    }
}

#[cfg(feature = "oracle-driver")]
struct OracleConnectionPlugin;

#[cfg(feature = "oracle-driver")]
#[async_trait]
impl ConnectionPlugin for OracleConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.oracle", "Oracle", 1521, false, false)
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
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        tokio::task::spawn_blocking(move || {
            let connected_at = Instant::now();
            let connection = oracle_connect(&endpoint, &secret)?;
            let connection_latency = connected_at.elapsed().as_millis() as u64;

            let version_started = Instant::now();
            let version = connection
                .query_row_as::<String>("SELECT banner FROM v$version WHERE ROWNUM = 1", &[])
                .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
            let version_latency = version_started.elapsed().as_millis() as u64;

            let metadata_started = Instant::now();
            let owner = secret.username.to_uppercase();
            let visible_objects = connection
                .query_row_as::<i64>(
                    "SELECT COUNT(*) FROM all_objects WHERE owner = :1",
                    &[&owner],
                )
                .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
            let metadata_latency = metadata_started.elapsed().as_millis() as u64;

            Ok(vec![
                DiagnosticCheck {
                    code: "connectivity".to_owned(),
                    label: "Conectividad".to_owned(),
                    status: Availability::Available,
                    latency_ms: Some(connection_latency),
                    details: serde_json::json!({
                        "host": endpoint.host,
                        "port": endpoint.port,
                        "service": endpoint.database,
                    }),
                },
                DiagnosticCheck {
                    code: "server_version".to_owned(),
                    label: "Versión del servidor".to_owned(),
                    status: Availability::Available,
                    latency_ms: Some(version_latency),
                    details: serde_json::json!({ "version": version }),
                },
                DiagnosticCheck {
                    code: "metadata_access".to_owned(),
                    label: "Acceso a metadatos".to_owned(),
                    status: Availability::Available,
                    latency_ms: Some(metadata_latency),
                    details: serde_json::json!({
                        "owner": owner,
                        "visible_objects": visible_objects,
                    }),
                },
            ])
        })
        .await
        .map_err(|error| PluginError::Diagnostic(error.to_string()))?
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
    .map_err(oracle_connection)
}

#[cfg(feature = "oracle-driver")]
fn oracle_connection(error: oracle::Error) -> PluginError {
    let message = error.to_string();
    if message.contains("DPI-1047") {
        return PluginError::Connection(format!(
            "{message}. Instala Oracle Instant Client de 64 bits y agrega su directorio a \
             LD_LIBRARY_PATH antes de iniciar Jaiba"
        ));
    }
    PluginError::Connection(message)
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
        descriptor("jaiba.sqlserver", "SQL Server", 1433, false, false)
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
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let version_row = client
            .simple_query(
                "SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), \
                        CAST(SERVERPROPERTY('Edition') AS nvarchar(128))",
            )
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .into_row()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .ok_or_else(|| PluginError::Diagnostic("versión no disponible".to_owned()))?;
        let version = version_row.get::<&str, _>(0).unwrap_or_default().to_owned();
        let edition = version_row.get::<&str, _>(1).unwrap_or_default().to_owned();
        let version_latency = version_started.elapsed().as_millis() as u64;

        let metadata_started = Instant::now();
        let metadata_row = client
            .simple_query(
                "SELECT CAST(DB_NAME() AS nvarchar(128)), COUNT_BIG(*) \
                 FROM sys.objects WHERE is_ms_shipped = 0",
            )
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .into_row()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .ok_or_else(|| PluginError::Diagnostic("metadatos no disponibles".to_owned()))?;
        let database = metadata_row
            .get::<&str, _>(0)
            .unwrap_or_default()
            .to_owned();
        let visible_objects = metadata_row.get::<i64, _>(1).unwrap_or_default();
        let metadata_latency = metadata_started.elapsed().as_millis() as u64;

        Ok(vec![
            DiagnosticCheck {
                code: "connectivity".to_owned(),
                label: "Conectividad".to_owned(),
                status: Availability::Available,
                latency_ms: Some(connection_latency),
                details: serde_json::json!({
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "encrypted": endpoint.ssl,
                }),
            },
            DiagnosticCheck {
                code: "server_version".to_owned(),
                label: "Versión del servidor".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({
                    "version": version,
                    "edition": edition,
                }),
            },
            DiagnosticCheck {
                code: "metadata_access".to_owned(),
                label: "Acceso a metadatos".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": database,
                    "visible_objects": visible_objects,
                }),
            },
        ])
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

#[cfg(feature = "mongodb-driver")]
#[derive(Debug, Clone)]
struct ParsedMongoUrl {
    host: String,
    port: u16,
    database: Option<String>,
    username: String,
    password: Option<String>,
    auth_source: Option<String>,
    ssl: bool,
}

#[cfg(feature = "mongodb-driver")]
fn parse_mongodb_connection_url(raw: &str) -> Result<ParsedMongoUrl, String> {
    let url = Url::parse(raw.trim()).map_err(|error| format!("URL MongoDB inválida: {error}"))?;
    match url.scheme() {
        "mongodb" | "mongodb+srv" => {}
        other => {
            return Err(format!(
                "esquema '{other}' no soportado; use mongodb:// o mongodb+srv://"
            ));
        }
    }
    let host = url
        .host_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "la URL MongoDB debe incluir host".to_owned())?
        .to_owned();
    let port = url.port().unwrap_or(27_017);
    let database = {
        let path = url.path().trim_matches('/');
        if path.is_empty() {
            None
        } else {
            Some(path.to_owned())
        }
    };
    let username = url.username().to_owned();
    let password = url.password().map(str::to_owned);
    let mut auth_source = None;
    let mut ssl = url.scheme() == "mongodb+srv";
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "authSource" => auth_source = Some(value.into_owned()),
            "tls" | "ssl" => {
                ssl = matches!(value.as_ref(), "true" | "1" | "yes");
            }
            _ => {}
        }
    }
    Ok(ParsedMongoUrl {
        host,
        port,
        database,
        username,
        password,
        auth_source,
        ssl,
    })
}

#[cfg(feature = "mongodb-driver")]
fn apply_credentials_to_mongo_url(
    raw: &str,
    username: &str,
    password: &str,
) -> Result<String, String> {
    let mut url =
        Url::parse(raw.trim()).map_err(|error| format!("URL MongoDB inválida: {error}"))?;
    url.set_username(username)
        .map_err(|_| "usuario MongoDB inválido en la URL".to_owned())?;
    url.set_password(Some(password))
        .map_err(|_| "contraseña MongoDB inválida en la URL".to_owned())?;
    Ok(url.into())
}

#[cfg(feature = "mongodb-driver")]
async fn mongodb_client(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<MongoClient, PluginError> {
    MongoClient::with_uri_str(mongodb_url(endpoint, secret)?)
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))
}

#[cfg(feature = "mongodb-driver")]
fn mongodb_url(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<String, PluginError> {
    if let Some(stored) = secret
        .options
        .get("connection_url")
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return apply_credentials_to_mongo_url(stored, &secret.username, &secret.password)
            .map_err(PluginError::Configuration);
    }

    let mut url = Url::parse(&format!("mongodb://{}:{}", endpoint.host, endpoint.port))
        .map_err(|error| PluginError::Configuration(error.to_string()))?;
    url.set_username(&secret.username)
        .map_err(|_| PluginError::Configuration("usuario MongoDB inválido".to_owned()))?;
    url.set_password(Some(&secret.password))
        .map_err(|_| PluginError::Configuration("contraseña MongoDB inválida".to_owned()))?;
    if let Some(database) = endpoint.database.as_deref() {
        url.set_path(database);
    }
    let auth_source = secret
        .options
        .get("auth_source")
        .or_else(|| endpoint.options.get("auth_source"))
        .map(String::as_str)
        .unwrap_or("admin");
    url.query_pairs_mut()
        .append_pair("authSource", auth_source)
        .append_pair("tls", if endpoint.ssl { "true" } else { "false" })
        .append_pair("minPoolSize", &endpoint.pool_min.to_string())
        .append_pair("maxPoolSize", &endpoint.pool_max.to_string())
        .append_pair("connectTimeoutMS", &endpoint.timeout_ms.to_string())
        .append_pair("serverSelectionTimeoutMS", &endpoint.timeout_ms.to_string());
    Ok(url.into())
}

#[cfg(feature = "mongodb-driver")]
fn mongodb_database(endpoint: &ConnectionEndpoint) -> Result<&str, PluginError> {
    endpoint
        .database
        .as_deref()
        .filter(|database| !database.trim().is_empty())
        .ok_or_else(|| PluginError::Configuration("la base MongoDB es obligatoria".to_owned()))
}

#[cfg(feature = "mongodb-driver")]
fn mongodb_bson_type(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "document",
        Bson::Boolean(_) => "boolean",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
        Bson::Int32(_) => "int32",
        Bson::Int64(_) => "int64",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binary",
        Bson::ObjectId(_) => "object_id",
        Bson::DateTime(_) => "datetime",
        Bson::Symbol(_) => "symbol",
        Bson::Decimal128(_) => "decimal128",
        Bson::Undefined => "undefined",
        Bson::MaxKey => "max_key",
        Bson::MinKey => "min_key",
        Bson::DbPointer(_) => "db_pointer",
    }
}

fn exploration_error(error: sqlx::Error) -> PluginError {
    PluginError::Exploration(error.to_string())
}

#[cfg(any(
    feature = "mongodb-driver",
    feature = "oracle-driver",
    feature = "sqlserver-driver"
))]
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

fn descriptor(
    id: &str,
    name: &str,
    default_port: u16,
    query_builder: bool,
    query_node: bool,
) -> PluginDescriptor {
    let mut capabilities = vec![
        "test".to_owned(),
        "diagnostics".to_owned(),
        "schema_explorer".to_owned(),
    ];
    if query_builder {
        capabilities.push("query_builder".to_owned());
    }
    if query_node {
        capabilities.push("query_node".to_owned());
    }
    PluginDescriptor {
        id: id.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        display_name: name.to_owned(),
        category: "SQL".to_owned(),
        default_port,
        capabilities,
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

#[cfg(test)]
mod integration_tests {
    use std::{collections::BTreeMap, env, fs};

    use jaiba_connection_manager::InMemorySecretStore;
    use jaiba_plugin_sdk::{FilterOperator, QueryFilter, QueryOrder, QuerySource, SortDirection};
    use serde_json::Value;

    use super::*;

    fn mysql_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
        let password = env::var("JAIBA_TEST_MYSQL_PASSWORD").ok()?;
        let host = env::var("JAIBA_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
        let port = env::var("JAIBA_TEST_MYSQL_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(13_306);
        let database =
            env::var("JAIBA_TEST_MYSQL_DATABASE").unwrap_or_else(|_| "dma_test".to_owned());
        let username = env::var("JAIBA_TEST_MYSQL_USER").unwrap_or_else(|_| "dma_test".to_owned());
        Some((
            ConnectionEndpoint {
                host,
                port,
                database: Some(database),
                ssl: false,
                pool_min: 1,
                pool_max: 2,
                timeout_ms: 5_000,
                options: BTreeMap::new(),
            },
            ConnectionSecret {
                username,
                password,
                options: BTreeMap::new(),
            },
        ))
    }

    fn postgres_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
        let password = env::var("JAIBA_TEST_POSTGRES_PASSWORD").ok()?;
        let host = env::var("JAIBA_TEST_POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
        let port = env::var("JAIBA_TEST_POSTGRES_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(55_432);
        let database =
            env::var("JAIBA_TEST_POSTGRES_DATABASE").unwrap_or_else(|_| "dma".to_owned());
        let username = env::var("JAIBA_TEST_POSTGRES_USER").unwrap_or_else(|_| "dma".to_owned());
        Some((
            ConnectionEndpoint {
                host,
                port,
                database: Some(database),
                ssl: false,
                pool_min: 1,
                pool_max: 2,
                timeout_ms: 5_000,
                options: BTreeMap::new(),
            },
            ConnectionSecret {
                username,
                password,
                options: BTreeMap::new(),
            },
        ))
    }

    #[cfg(feature = "mongodb-driver")]
    fn mongodb_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
        let password = env::var("JAIBA_TEST_MONGODB_PASSWORD").ok()?;
        let host = env::var("JAIBA_TEST_MONGODB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
        // Puerto host típico del contenedor de pruebas (mapeo 27018→27017).
        let port = env::var("JAIBA_TEST_MONGODB_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(27_018);
        let database =
            env::var("JAIBA_TEST_MONGODB_DATABASE").unwrap_or_else(|_| "dma_test".to_owned());
        let username =
            env::var("JAIBA_TEST_MONGODB_USER").unwrap_or_else(|_| "dma_test".to_owned());
        Some((
            ConnectionEndpoint {
                host,
                port,
                database: Some(database),
                ssl: false,
                pool_min: 1,
                pool_max: 2,
                timeout_ms: 5_000,
                options: BTreeMap::new(),
            },
            ConnectionSecret {
                username,
                password,
                options: BTreeMap::new(),
            },
        ))
    }

    #[cfg(feature = "oracle-driver")]
    fn oracle_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
        let password = env::var("JAIBA_TEST_ORACLE_PASSWORD").ok()?;
        let host = env::var("JAIBA_TEST_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
        let port = env::var("JAIBA_TEST_ORACLE_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(11_521);
        let service =
            env::var("JAIBA_TEST_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
        let username = env::var("JAIBA_TEST_ORACLE_USER").unwrap_or_else(|_| "dma_test".to_owned());
        Some((
            ConnectionEndpoint {
                host,
                port,
                database: Some(service),
                ssl: false,
                pool_min: 1,
                pool_max: 1,
                timeout_ms: 10_000,
                options: BTreeMap::new(),
            },
            ConnectionSecret {
                username,
                password,
                options: BTreeMap::new(),
            },
        ))
    }

    #[cfg(feature = "sqlserver-driver")]
    fn sqlserver_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
        let password = env::var("JAIBA_TEST_SQLSERVER_PASSWORD").ok()?;
        let host = env::var("JAIBA_TEST_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
        let port = env::var("JAIBA_TEST_SQLSERVER_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(11_433);
        let database =
            env::var("JAIBA_TEST_SQLSERVER_DATABASE").unwrap_or_else(|_| "master".to_owned());
        let username = env::var("JAIBA_TEST_SQLSERVER_USER").unwrap_or_else(|_| "sa".to_owned());
        Some((
            ConnectionEndpoint {
                host,
                port,
                database: Some(database),
                ssl: false,
                pool_min: 1,
                pool_max: 1,
                timeout_ms: 10_000,
                options: BTreeMap::new(),
            },
            ConnectionSecret {
                username,
                password,
                options: BTreeMap::new(),
            },
        ))
    }

    #[cfg(feature = "mongodb-driver")]
    #[tokio::test]
    async fn mongodb_real_connection_diagnostics_and_collection_metadata() {
        let Some((endpoint, secret)) = mongodb_test_configuration() else {
            eprintln!("skipping real MongoDB test: JAIBA_TEST_MONGODB_PASSWORD is not set");
            return;
        };

        let client = mongodb_client(&endpoint, &secret)
            .await
            .expect("connect to the MongoDB integration database");
        let database = client.database(mongodb_database(&endpoint).expect("database name"));
        let collection = database.collection::<Document>("jaiba_phase_1_probe");
        let _ = collection.drop().await;
        collection
            .insert_one(doc! {
                "_id": "probe-1",
                "amount": 125.50,
                "active": true,
                "note": "Jaiba integration test",
            })
            .await
            .expect("seed MongoDB integration document");

        let secrets = Arc::new(InMemorySecretStore::default());
        secrets.insert("test://mongodb", secret).await;
        let manager = connection_manager(secrets, None, None)
            .await
            .expect("build connection manager");
        let profile = manager
            .create(
                "mongodb_phase_1",
                ConnectionType::MongoDb,
                endpoint.clone(),
                "test://mongodb",
            )
            .await
            .expect("create MongoDB profile");

        let test_result = manager.test(&profile.id).await.expect("test connection");
        assert_eq!(test_result.availability, Availability::Available);
        // MongoDB 7.x / 8.x en entornos de prueba.
        assert!(
            test_result.version.as_deref().is_some_and(|value| {
                value
                    .split(|c: char| !c.is_ascii_digit())
                    .next()
                    .and_then(|major| major.parse::<u32>().ok())
                    .is_some_and(|major| (7..=9).contains(&major))
            }),
            "unexpected MongoDB version: {:?}",
            test_result.version
        );

        let diagnostics = manager
            .diagnose(&profile.id)
            .await
            .expect("run MongoDB diagnostics");
        assert_eq!(diagnostics.len(), 3);
        assert!(
            diagnostics
                .iter()
                .all(|check| check.status == Availability::Available)
        );

        let objects = manager
            .list_objects(&profile.id, endpoint.database.as_deref())
            .await
            .expect("list MongoDB collections");
        let probe = objects
            .iter()
            .find(|object| {
                object.name == "jaiba_phase_1_probe"
                    && object.kind == DatabaseObjectKind::Collection
            })
            .expect("probe collection appears in metadata");
        let description = manager
            .describe_object(&profile.id, probe)
            .await
            .expect("describe MongoDB collection");
        assert_eq!(description.object.kind, DatabaseObjectKind::Collection);
        assert!(
            description
                .columns
                .iter()
                .any(|column| column.name == "_id" && column.data_type == "string")
        );
        assert!(
            description
                .columns
                .iter()
                .any(|column| column.name == "active" && column.data_type == "boolean")
        );

        collection
            .drop()
            .await
            .expect("clean MongoDB integration collection");
    }

    /// Prueba opt-in contra MySQL real. Se omite cuando no se define
    /// `JAIBA_TEST_MYSQL_PASSWORD`, por lo que la suite local y CI no necesitan
    /// una base externa.
    #[tokio::test]
    async fn mysql_real_connection_metadata_and_query_compilation() {
        let Some((endpoint, secret)) = mysql_test_configuration() else {
            eprintln!("skipping real MySQL test: JAIBA_TEST_MYSQL_PASSWORD is not set");
            return;
        };

        let pool = mysql_pool(&endpoint, &secret)
            .await
            .expect("connect to the MySQL integration database");
        sqlx::query("DROP TABLE IF EXISTS jaiba_phase_9_3_probe")
            .execute(&pool)
            .await
            .expect("remove stale integration table");
        sqlx::query(
            "CREATE TABLE jaiba_phase_9_3_probe (\
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                external_id VARCHAR(64) NOT NULL UNIQUE,\
                amount DECIMAL(12,2) NOT NULL,\
                active BOOLEAN NOT NULL DEFAULT TRUE,\
                note VARCHAR(255) NULL,\
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
                INDEX idx_jaiba_probe_active (active)\
            )",
        )
        .execute(&pool)
        .await
        .expect("create integration table");
        sqlx::query(
            "INSERT INTO jaiba_phase_9_3_probe (external_id, amount, active, note) \
             VALUES ('probe-1', 125.50, TRUE, 'Jaiba integration test')",
        )
        .execute(&pool)
        .await
        .expect("seed integration row");
        pool.close().await;

        let secrets = Arc::new(InMemorySecretStore::default());
        secrets.insert("test://mysql", secret).await;
        let manager = connection_manager(secrets, None, None)
            .await
            .expect("build connection manager");
        let profile = manager
            .create(
                "mysql_phase_9_3",
                ConnectionType::MySql,
                endpoint.clone(),
                "test://mysql",
            )
            .await
            .expect("create MySQL profile");

        let test_result = manager.test(&profile.id).await.expect("test connection");
        assert_eq!(test_result.availability, Availability::Available);
        assert!(
            test_result
                .version
                .as_deref()
                .is_some_and(|version| version.starts_with("8."))
        );

        let diagnostics = manager
            .diagnose(&profile.id)
            .await
            .expect("run diagnostics");
        assert!(!diagnostics.is_empty());
        assert!(
            diagnostics
                .iter()
                .all(|check| check.status == Availability::Available)
        );

        let objects = manager
            .list_objects(&profile.id, endpoint.database.as_deref())
            .await
            .expect("list database objects");
        let table = objects
            .iter()
            .find(|object| {
                object.name == "jaiba_phase_9_3_probe" && object.kind == DatabaseObjectKind::Table
            })
            .expect("integration table appears in metadata")
            .clone();

        let description = manager
            .describe_object(&profile.id, &table)
            .await
            .expect("describe integration table");
        assert_eq!(
            description
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "id",
                "external_id",
                "amount",
                "active",
                "note",
                "created_at"
            ]
        );
        assert!(
            description
                .keys
                .iter()
                .any(|key| key.kind == "PRIMARY KEY" && key.columns == ["id"])
        );
        assert!(
            description
                .indexes
                .iter()
                .any(|index| index.name == "idx_jaiba_probe_active")
        );

        let compiled = manager
            .compile_query(
                &profile.id,
                &QuerySpec {
                    source: QuerySource {
                        schema: endpoint.database.clone(),
                        table: table.name.clone(),
                    },
                    columns: vec!["id".to_owned(), "external_id".to_owned()],
                    joins: vec![],
                    filters: vec![QueryFilter {
                        field: "active".to_owned(),
                        operator: FilterOperator::Eq,
                        value: Value::Bool(true),
                    }],
                    group_by: vec![],
                    order_by: vec![QueryOrder {
                        field: "id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    limit: Some(10),
                },
            )
            .await
            .expect("compile MySQL query");
        assert_eq!(
            compiled.statement,
            "SELECT `id`, `external_id` FROM `dma_test`.`jaiba_phase_9_3_probe` \
             WHERE `active` = ? ORDER BY `id` ASC LIMIT 10"
        );
        assert_eq!(compiled.parameters, vec![Value::Bool(true)]);

        let pool = mysql_pool(
            &endpoint,
            &ConnectionSecret {
                username: env::var("JAIBA_TEST_MYSQL_USER")
                    .unwrap_or_else(|_| "dma_test".to_owned()),
                password: env::var("JAIBA_TEST_MYSQL_PASSWORD")
                    .expect("password remains available during the test"),
                options: BTreeMap::new(),
            },
        )
        .await
        .expect("reconnect for cleanup");
        sqlx::query("DROP TABLE jaiba_phase_9_3_probe")
            .execute(&pool)
            .await
            .expect("clean integration table");
        pool.close().await;
    }

    /// Prueba opt-in de extremo a extremo contra PostgreSQL real: Connection
    /// Manager, compilación SQL y ejecución de `query_postgres`.
    #[tokio::test]
    async fn postgres_real_connection_query_builder_and_flow_execution() {
        let Some((endpoint, secret)) = postgres_test_configuration() else {
            eprintln!("skipping real PostgreSQL test: JAIBA_TEST_POSTGRES_PASSWORD is not set");
            return;
        };
        if env::var("JAIBA_TEST_POSTGRES_URL").is_err() {
            eprintln!("skipping real PostgreSQL test: JAIBA_TEST_POSTGRES_URL is not set");
            return;
        }

        let pool = postgres_pool(&endpoint, &secret)
            .await
            .expect("connect to the PostgreSQL integration database");
        sqlx::query("DROP TABLE IF EXISTS public.jaiba_phase_9_3_probe")
            .execute(&pool)
            .await
            .expect("remove stale integration table");
        sqlx::query(
            "CREATE TABLE public.jaiba_phase_9_3_probe (\
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\
                external_id VARCHAR(64) NOT NULL UNIQUE,\
                amount NUMERIC(12,2) NOT NULL,\
                active BOOLEAN NOT NULL DEFAULT TRUE,\
                note VARCHAR(255),\
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\
            )",
        )
        .execute(&pool)
        .await
        .expect("create PostgreSQL integration table");
        sqlx::query(
            "CREATE INDEX idx_jaiba_probe_active \
             ON public.jaiba_phase_9_3_probe (active)",
        )
        .execute(&pool)
        .await
        .expect("create PostgreSQL integration index");
        sqlx::query(
            "INSERT INTO public.jaiba_phase_9_3_probe \
             (external_id, amount, active, note) VALUES \
             ('probe-1', 125.50, TRUE, 'visible'), \
             ('probe-2', 80.00, FALSE, 'filtered')",
        )
        .execute(&pool)
        .await
        .expect("seed PostgreSQL integration rows");
        pool.close().await;

        let secrets = Arc::new(InMemorySecretStore::default());
        secrets.insert("test://postgres", secret.clone()).await;
        let manager = connection_manager(secrets, None, None)
            .await
            .expect("build connection manager");
        let profile = manager
            .create(
                "postgres_phase_9_3",
                ConnectionType::Postgres,
                endpoint.clone(),
                "test://postgres",
            )
            .await
            .expect("create PostgreSQL profile");

        let test_result = manager.test(&profile.id).await.expect("test connection");
        assert_eq!(test_result.availability, Availability::Available);
        assert!(
            test_result
                .version
                .as_deref()
                .is_some_and(|version| version.contains("PostgreSQL 16"))
        );
        let diagnostics = manager
            .diagnose(&profile.id)
            .await
            .expect("run PostgreSQL diagnostics");
        assert_eq!(diagnostics.len(), 3);
        assert!(
            diagnostics
                .iter()
                .all(|check| check.status == Availability::Available)
        );

        let objects = manager
            .list_objects(&profile.id, Some("public"))
            .await
            .expect("list PostgreSQL objects");
        let table = objects
            .iter()
            .find(|object| {
                object.name == "jaiba_phase_9_3_probe" && object.kind == DatabaseObjectKind::Table
            })
            .expect("integration table appears in PostgreSQL metadata")
            .clone();
        let description = manager
            .describe_object(&profile.id, &table)
            .await
            .expect("describe PostgreSQL integration table");
        assert!(
            description
                .keys
                .iter()
                .any(|key| key.kind == "PRIMARY KEY" && key.columns == ["id"])
        );
        assert!(
            description
                .indexes
                .iter()
                .any(|index| index.name == "idx_jaiba_probe_active")
        );

        let compiled = manager
            .compile_query(
                &profile.id,
                &QuerySpec {
                    source: QuerySource {
                        schema: Some("public".to_owned()),
                        table: table.name,
                    },
                    columns: vec![
                        "id".to_owned(),
                        "external_id".to_owned(),
                        "active".to_owned(),
                    ],
                    joins: vec![],
                    filters: vec![QueryFilter {
                        field: "active".to_owned(),
                        operator: FilterOperator::Eq,
                        value: Value::Bool(true),
                    }],
                    group_by: vec![],
                    order_by: vec![QueryOrder {
                        field: "id".to_owned(),
                        direction: SortDirection::Asc,
                    }],
                    limit: Some(10),
                },
            )
            .await
            .expect("compile PostgreSQL query");
        assert_eq!(compiled.parameters, vec![Value::Bool(true)]);

        let output = format!("/tmp/jaiba-postgres-phase-9-3-{}.json", Uuid::new_v4());
        let wrapped_query = format!(
            "SELECT to_jsonb(t) AS record FROM ({}) AS t",
            compiled.statement
        );
        let flow_yaml = format!(
            r#"
id: postgres-phase-9-3
database_connections:
  integration:
    type: postgres
    url_env: JAIBA_TEST_POSTGRES_URL
    max_connections: 2
engine:
  repository:
    enabled: false
processors:
  - id: read
    type: query_postgres
    config:
      connection: integration
      query: {query}
      parameters: [true]
      batch_size: 100
  - id: encode
    type: encode_json
    config:
      pretty: false
  - id: write
    type: write_file
    config:
      path: {output}
connections:
  - from: read
    relationship: success
    to: encode
  - from: encode
    relationship: success
    to: write
"#,
            query = serde_json::to_string(&wrapped_query).expect("quote query for YAML"),
        );
        let config: jaiba_core::config::FlowConfig =
            serde_yaml::from_str(&flow_yaml).expect("parse integration flow");
        let summary = jaiba_runtime::engine::FlowEngine::new(config)
            .expect("build integration flow")
            .run()
            .await
            .expect("run query_postgres integration flow");
        assert_eq!(summary.failed, 0);
        let records: Value =
            serde_json::from_slice(&fs::read(&output).expect("read query_postgres output"))
                .expect("output is valid JSON");
        assert_eq!(records.as_array().map(Vec::len), Some(1));
        assert_eq!(records[0]["external_id"], "probe-1");
        assert_eq!(records[0]["active"], true);

        fs::remove_file(&output).expect("remove integration output");
        let pool = postgres_pool(&endpoint, &secret)
            .await
            .expect("reconnect to PostgreSQL for cleanup");
        sqlx::query("DROP TABLE public.jaiba_phase_9_3_probe")
            .execute(&pool)
            .await
            .expect("clean PostgreSQL integration table");
        pool.close().await;
    }

    /// Prueba opt-in contra Oracle Free real. Requiere `oracle-driver`, las
    /// bibliotecas de Oracle Client y `JAIBA_TEST_ORACLE_PASSWORD`.
    #[cfg(feature = "oracle-driver")]
    #[tokio::test]
    async fn oracle_real_connection_diagnostics_and_metadata() {
        let Some((endpoint, secret)) = oracle_test_configuration() else {
            eprintln!("skipping real Oracle test: JAIBA_TEST_ORACLE_PASSWORD is not set");
            return;
        };

        let setup_endpoint = endpoint.clone();
        let setup_secret = secret.clone();
        tokio::task::spawn_blocking(move || {
            let connection = oracle_connect(&setup_endpoint, &setup_secret)
                .expect("connect to the Oracle integration database");
            connection
                .execute(
                    "BEGIN \
                        EXECUTE IMMEDIATE 'DROP TABLE JAIBA_PHASE_9_3_PROBE PURGE'; \
                     EXCEPTION WHEN OTHERS THEN \
                        IF SQLCODE != -942 THEN RAISE; END IF; \
                     END;",
                    &[],
                )
                .expect("remove stale integration table");
            connection
                .execute(
                    "CREATE TABLE JAIBA_PHASE_9_3_PROBE (\
                        ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\
                        EXTERNAL_ID VARCHAR2(64) NOT NULL UNIQUE,\
                        AMOUNT NUMBER(12,2) NOT NULL,\
                        ACTIVE NUMBER(1) DEFAULT 1 NOT NULL,\
                        NOTE VARCHAR2(255),\
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL\
                    )",
                    &[],
                )
                .expect("create integration table");
            connection
                .execute(
                    "CREATE INDEX IDX_JAIBA_PROBE_ACTIVE \
                     ON JAIBA_PHASE_9_3_PROBE (ACTIVE)",
                    &[],
                )
                .expect("create integration index");
            connection
                .execute(
                    "INSERT INTO JAIBA_PHASE_9_3_PROBE \
                     (EXTERNAL_ID, AMOUNT, ACTIVE, NOTE) \
                     VALUES ('probe-1', 125.50, 1, 'Jaiba integration test')",
                    &[],
                )
                .expect("seed integration row");
            connection.commit().expect("commit integration fixture");
        })
        .await
        .expect("finish Oracle setup task");

        let secrets = Arc::new(InMemorySecretStore::default());
        secrets.insert("test://oracle", secret.clone()).await;
        let manager = connection_manager(secrets, None, None)
            .await
            .expect("build connection manager");
        let profile = manager
            .create(
                "oracle_phase_9_3",
                ConnectionType::Oracle,
                endpoint.clone(),
                "test://oracle",
            )
            .await
            .expect("create Oracle profile");

        let test_result = manager.test(&profile.id).await.expect("test connection");
        assert_eq!(test_result.availability, Availability::Available);
        assert!(
            test_result
                .version
                .as_deref()
                .is_some_and(|version| version.contains("Oracle"))
        );

        let diagnostics = manager
            .diagnose(&profile.id)
            .await
            .expect("run Oracle diagnostics");
        assert_eq!(diagnostics.len(), 3);
        assert!(
            diagnostics
                .iter()
                .all(|check| check.status == Availability::Available)
        );

        let owner = secret.username.to_uppercase();
        let objects = manager
            .list_objects(&profile.id, Some(&owner))
            .await
            .expect("list Oracle objects");
        let table = objects
            .iter()
            .find(|object| {
                object.name == "JAIBA_PHASE_9_3_PROBE" && object.kind == DatabaseObjectKind::Table
            })
            .expect("integration table appears in Oracle metadata")
            .clone();

        let description = manager
            .describe_object(&profile.id, &table)
            .await
            .expect("describe Oracle integration table");
        assert_eq!(
            description
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "ID",
                "EXTERNAL_ID",
                "AMOUNT",
                "ACTIVE",
                "NOTE",
                "CREATED_AT"
            ]
        );

        let cleanup_endpoint = endpoint;
        tokio::task::spawn_blocking(move || {
            let connection = oracle_connect(&cleanup_endpoint, &secret)
                .expect("reconnect to Oracle for cleanup");
            connection
                .execute("DROP TABLE JAIBA_PHASE_9_3_PROBE PURGE", &[])
                .expect("clean Oracle integration table");
        })
        .await
        .expect("finish Oracle cleanup task");
    }

    /// Prueba opt-in contra SQL Server real. Requiere `sqlserver-driver` y
    /// `JAIBA_TEST_SQLSERVER_PASSWORD`.
    #[cfg(feature = "sqlserver-driver")]
    #[tokio::test]
    async fn sqlserver_real_connection_diagnostics_and_metadata() {
        let Some((endpoint, secret)) = sqlserver_test_configuration() else {
            eprintln!("skipping real SQL Server test: JAIBA_TEST_SQLSERVER_PASSWORD is not set");
            return;
        };

        let mut client = sqlserver_connect(&endpoint, &secret)
            .await
            .expect("connect to the SQL Server integration database");
        client
            .simple_query(
                "IF OBJECT_ID('dbo.JAIBA_PHASE_9_3_PROBE', 'U') IS NOT NULL \
                    DROP TABLE dbo.JAIBA_PHASE_9_3_PROBE; \
                 CREATE TABLE dbo.JAIBA_PHASE_9_3_PROBE (\
                    ID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,\
                    EXTERNAL_ID nvarchar(64) NOT NULL UNIQUE,\
                    AMOUNT decimal(12,2) NOT NULL,\
                    ACTIVE bit NOT NULL CONSTRAINT DF_JAIBA_PROBE_ACTIVE DEFAULT 1,\
                    NOTE nvarchar(255) NULL,\
                    CREATED_AT datetime2 NOT NULL \
                        CONSTRAINT DF_JAIBA_PROBE_CREATED DEFAULT SYSUTCDATETIME()\
                 ); \
                 CREATE INDEX IDX_JAIBA_PROBE_ACTIVE \
                    ON dbo.JAIBA_PHASE_9_3_PROBE (ACTIVE); \
                 INSERT INTO dbo.JAIBA_PHASE_9_3_PROBE \
                    (EXTERNAL_ID, AMOUNT, ACTIVE, NOTE) \
                    VALUES ('probe-1', 125.50, 1, 'Jaiba integration test');",
            )
            .await
            .expect("prepare SQL Server fixture")
            .into_results()
            .await
            .expect("execute SQL Server fixture");
        drop(client);

        let secrets = Arc::new(InMemorySecretStore::default());
        secrets.insert("test://sqlserver", secret.clone()).await;
        let manager = connection_manager(secrets, None, None)
            .await
            .expect("build connection manager");
        let profile = manager
            .create(
                "sqlserver_phase_9_3",
                ConnectionType::SqlServer,
                endpoint.clone(),
                "test://sqlserver",
            )
            .await
            .expect("create SQL Server profile");

        let test_result = manager.test(&profile.id).await.expect("test connection");
        assert_eq!(test_result.availability, Availability::Available);
        assert!(
            test_result
                .version
                .as_deref()
                .is_some_and(|version| version.starts_with("16."))
        );

        let diagnostics = manager
            .diagnose(&profile.id)
            .await
            .expect("run SQL Server diagnostics");
        assert_eq!(diagnostics.len(), 3);
        assert!(
            diagnostics
                .iter()
                .all(|check| check.status == Availability::Available)
        );

        let objects = manager
            .list_objects(&profile.id, Some("dbo"))
            .await
            .expect("list SQL Server objects");
        let table = objects
            .iter()
            .find(|object| {
                object.name == "JAIBA_PHASE_9_3_PROBE" && object.kind == DatabaseObjectKind::Table
            })
            .expect("integration table appears in SQL Server metadata")
            .clone();

        let description = manager
            .describe_object(&profile.id, &table)
            .await
            .expect("describe SQL Server integration table");
        assert_eq!(
            description
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "ID",
                "EXTERNAL_ID",
                "AMOUNT",
                "ACTIVE",
                "NOTE",
                "CREATED_AT"
            ]
        );

        let mut client = sqlserver_connect(&endpoint, &secret)
            .await
            .expect("reconnect to SQL Server for cleanup");
        client
            .simple_query("DROP TABLE dbo.JAIBA_PHASE_9_3_PROBE")
            .await
            .expect("prepare SQL Server cleanup")
            .into_results()
            .await
            .expect("clean SQL Server integration table");
    }

    /// Valida que un perfil creado solo con URL MongoDB pueda probarse.
    #[cfg(feature = "mongodb-driver")]
    #[tokio::test]
    async fn mongodb_real_connection_from_url() {
        let Some(password) = env::var("JAIBA_TEST_MONGODB_PASSWORD").ok() else {
            eprintln!("skipping real MongoDB URL test: JAIBA_TEST_MONGODB_PASSWORD is not set");
            return;
        };
        let host = env::var("JAIBA_TEST_MONGODB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
        let port = env::var("JAIBA_TEST_MONGODB_PORT").unwrap_or_else(|_| "27018".to_owned());
        let database =
            env::var("JAIBA_TEST_MONGODB_DATABASE").unwrap_or_else(|_| "dma_test".to_owned());
        let username =
            env::var("JAIBA_TEST_MONGODB_USER").unwrap_or_else(|_| "dma_test".to_owned());
        let raw =
            format!("mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin");

        let input = ConnectionInput {
            name: "mongo_from_url".to_owned(),
            connection_type: ConnectionType::MongoDb,
            host: String::new(),
            port: 0,
            database: None,
            username: String::new(),
            password: None,
            url: Some(raw),
            ssl: false,
            pool_min: 1,
            pool_max: 2,
            timeout_ms: 5_000,
        };
        validate_input(&input, true).expect("URL MongoDB válida");
        let (endpoint, secret) =
            materialize_connection(&input, None).expect("materializar desde URL");
        assert_eq!(endpoint.host, host);
        assert!(secret.options.contains_key("connection_url"));

        let client = mongodb_client(&endpoint, &secret)
            .await
            .expect("conectar con URL materializada");
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .expect("ping MongoDB vía URL");
    }
}

#[cfg(all(test, feature = "mongodb-driver"))]
mod mongo_url_unit_tests {
    use super::*;

    #[test]
    fn parses_mongodb_url_with_auth_source() {
        let parsed = parse_mongodb_connection_url(
            "mongodb://dma_test:s3cret@127.0.0.1:27018/dma_test?authSource=admin&tls=false",
        )
        .expect("parse");
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 27_018);
        assert_eq!(parsed.database.as_deref(), Some("dma_test"));
        assert_eq!(parsed.username, "dma_test");
        assert_eq!(parsed.password.as_deref(), Some("s3cret"));
        assert_eq!(parsed.auth_source.as_deref(), Some("admin"));
        assert!(!parsed.ssl);
    }

    #[test]
    fn parses_mongodb_srv_as_tls() {
        let parsed = parse_mongodb_connection_url(
            "mongodb+srv://app:pass@cluster0.example.net/prod?retryWrites=true",
        )
        .expect("parse srv");
        assert_eq!(parsed.host, "cluster0.example.net");
        assert_eq!(parsed.port, 27_017);
        assert!(parsed.ssl);
        assert_eq!(parsed.database.as_deref(), Some("prod"));
    }

    #[test]
    fn rejects_non_mongo_scheme() {
        let error = parse_mongodb_connection_url("postgres://u:p@h/db").expect_err("reject");
        assert!(error.contains("mongodb://"));
    }
}
