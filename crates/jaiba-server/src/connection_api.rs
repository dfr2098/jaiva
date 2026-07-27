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
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use jaiba_connection_manager::{
    ConnectionManager, ConnectionManagerError, ConnectionProfile, ConnectionStatus,
    InMemorySecretStore, SecretStore,
};
use jaiba_plugin_sdk::{
    Availability, CompiledQuery, ConnectionEndpoint, ConnectionPlugin, ConnectionSecret,
    ConnectionTestResult, ConnectionType, DatabaseObject, DiagnosticCheck, ObjectDescription,
    PluginDescriptor, PluginError, PoolStatus, QuerySpec,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode},
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
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

pub(crate) async fn connection_manager(
    secrets: Arc<InMemorySecretStore>,
) -> Arc<ConnectionManager> {
    let manager = Arc::new(ConnectionManager::new(secrets));
    manager
        .register_plugin(Arc::new(PostgresConnectionPlugin))
        .await;
    manager
        .register_plugin(Arc::new(MySqlConnectionPlugin {
            connection_type: ConnectionType::MySql,
        }))
        .await;
    manager
        .register_plugin(Arc::new(MySqlConnectionPlugin {
            connection_type: ConnectionType::MariaDb,
        }))
        .await;
    manager
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
            false,
            "Conector de ejecución opcional; prueba administrativa pendiente",
        ),
        type_view(
            "sql_server",
            "SQL Server",
            "SQL",
            1433,
            cfg!(feature = "sqlserver-driver"),
            false,
            "Conector de ejecución opcional; prueba administrativa pendiente",
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
    let secret_ref = format!("memory://connection/{}", Uuid::new_v4());
    state
        .connection_secrets
        .insert(
            &secret_ref,
            ConnectionSecret {
                username: input.username.clone(),
                password: input.password.clone().unwrap_or_default(),
                options: Default::default(),
            },
        )
        .await;
    let endpoint = endpoint(&input);
    match state
        .connection_manager
        .create(input.name, input.connection_type, endpoint, secret_ref)
        .await
    {
        Ok(profile) => match view(&state, profile).await {
            Ok(item) => (StatusCode::CREATED, Json(item)).into_response(),
            Err(response) => response,
        },
        Err(error) => manager_error(error),
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
    state
        .connection_secrets
        .insert(
            &profile.secret_ref,
            ConnectionSecret {
                username: input.username.clone(),
                password: input.password.clone().unwrap_or(previous.password),
                options: previous.options,
            },
        )
        .await;
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
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
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
        ConnectionType::Postgres | ConnectionType::MySql | ConnectionType::MariaDb
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
        ConnectionManagerError::SecretUnavailable(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
        _schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        Err(PluginError::Unsupported("explorador SQL".to_owned()))
    }

    async fn describe_object(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
        _object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        Err(PluginError::Unsupported("explorador SQL".to_owned()))
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
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
        _schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        Err(PluginError::Unsupported("explorador SQL".to_owned()))
    }

    async fn describe_object(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
        _object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        Err(PluginError::Unsupported("explorador SQL".to_owned()))
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("constructor SQL".to_owned()))
    }
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
