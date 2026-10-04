//! API del administrador de conexiones.
//!
//! Las respuestas exponen metadatos y el nombre de usuario, pero jamás la
//! contraseña ni la referencia interna del secreto. El almacén en memoria es
//! deliberadamente temporal; puede sustituirse por Vault/KMS sin cambiar la UI.

use std::{collections::BTreeMap, sync::Arc};

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
    ConnectionEndpoint, ConnectionSecret, ConnectionType, DatabaseObject, DatabaseObjectKind,
    QuerySpec,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::Permission;
use crate::observability::{AppState, admin_actor, authorize_perm};

mod plugins;
#[cfg(test)]
mod tests;

#[cfg(feature = "clickhouse-driver")]
use plugins::clickhouse::ClickHouseConnectionPlugin;
#[cfg(feature = "mongodb-driver")]
use plugins::mongodb::{
    MongoDbConnectionPlugin, apply_credentials_to_mongo_url, parse_mongodb_connection_url,
};
#[cfg(feature = "oracle-driver")]
use plugins::oracle::OracleConnectionPlugin;
#[cfg(feature = "sqlserver-driver")]
use plugins::sqlserver::SqlServerConnectionPlugin;
use plugins::{mysql::MySqlConnectionPlugin, postgres::PostgresConnectionPlugin};

/// Un tipo de conexión disponible, tal como lo lista la UI.
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

/// Cuerpo de alta o edición de un perfil. La contraseña solo viaja de entrada.
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

/// Perfil tal como sale por la API: usuario sí, contraseña y `secret_ref` nunca.
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

/// GET `/api/v1/connections/{id}/metadata[?schema=]`: esquemas, tablas y vistas (permiso Read).
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

/// GET `/api/v1/connections/{id}/metadata/{schema}/{name}`: columnas, llaves e índices (permiso Read).
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

/// POST `/api/v1/connections/{id}/query/compile`: `QuerySpec` → SQL parametrizado del motor (permiso Read).
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

/// Crea el `ConnectionManager` del servidor: restaura perfiles persistidos y
/// registra un plugin por motor compilado (los de drivers opcionales solo con
/// su feature).
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
    #[cfg(feature = "clickhouse-driver")]
    manager
        .register_plugin(Arc::new(ClickHouseConnectionPlugin))
        .await;
    manager
        .register_plugin(Arc::new(MySqlConnectionPlugin {
            connection_type: ConnectionType::MariaDb,
        }))
        .await;
    Ok(manager)
}

/// GET `/api/v1/connection-types`: plugins registrados en este binario (permiso Read).
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

/// GET `/api/v1/connections` (permiso Read).
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

/// GET `/api/v1/connections/{id}` (permiso Read).
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

/// POST `/api/v1/connections`: valida, guarda el secreto aparte y audita (permiso Admin).
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

/// PUT `/api/v1/connections/{id}`: sin contraseña conserva la anterior (permiso Admin).
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
    profile.name = input.name.trim().to_owned();
    profile.connection_type = input.connection_type;
    profile.endpoint = endpoint;
    if let Err(error) = state
        .connection_manager
        .update_with_secret(profile.clone(), secret)
        .await
    {
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

/// DELETE `/api/v1/connections/{id}` (permiso Admin).
pub(crate) async fn delete_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let ctx = match authorize_perm(&state, &headers, Permission::Admin) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    match state.connection_manager.delete_with_secret(&id).await {
        Ok(profile) => {
            tracing::warn!(
                audit_action = "connection_delete",
                actor = admin_actor(&ctx),
                profile_id = %profile.id,
                profile_name = %profile.name,
                "administrative action"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => manager_error(error),
    }
}

/// POST `/api/v1/connections/{id}/duplicate`: copia perfil y secreto con otro nombre (permiso Admin).
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

/// POST `/api/v1/connections/{id}/test`: abre una conexión real y mide latencia (permiso Admin).
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

/// GET `/api/v1/connections/{id}/diagnostics`: chequeos del plugin sin modificar nada (permiso Read).
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

/// Arma la vista pública de un perfil con su usuario y estado actual.
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

/// Valida un `ConnectionInput` antes de tocar el manager. Con `url` (solo
/// MongoDB) usuario y contraseña pueden venir dentro de la URL;
/// `password_required` es `true` al crear y `false` al editar.
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

/// Traduce un error del manager a HTTP. El detalle va al log; el cliente
/// recibe `client_message()`, sin datos sensibles.
fn manager_error(error: ConnectionManagerError) -> Response {
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
