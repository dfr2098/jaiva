//! Health, readiness, Prometheus, WebSocket and authenticated administration.

use std::{
    env,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, time::interval};

use jaiba_connection_manager::{
    AuditSink, ConnectionManager, EncryptedFileSecretStore, FileAuditSink, FileProfileRepository,
    InMemorySecretStore, ProfileRepository, SecretStore, master_key_id,
};
use jaiba_core::config::{AdminAuthentication, FlowConfig};
use jaiba_runtime::{
    engine::{
        ConnectionResolver, FlowEngine, FlowMetrics, FlowSupervisor, LocalPacketRepository,
        PacketRepository, ProfileConnectionResolver, SupervisedFlowSnapshot,
    },
    error::FlowError,
};

use crate::connection_api::{
    compile_query, create_connection, delete_connection, describe_metadata, duplicate_connection,
    get_connection, list_connection_types, list_connections, list_metadata, test_connection,
    update_connection,
};
use crate::flow_registry::{FlowRecord, FlowRegistry, RegistryError};

#[derive(Clone)]
pub(crate) struct AppState {
    registry: Arc<FlowRegistry>,
    admin: Arc<RwLock<AdminAccess>>,
    address: SocketAddr,
    pub(crate) connection_manager: Arc<ConnectionManager>,
    pub(crate) connection_secrets: Arc<dyn SecretStore>,
}

#[derive(Clone)]
struct AdminAccess {
    enabled: bool,
    authentication: AdminAuthentication,
    token: Option<String>,
}

#[derive(Clone)]
pub struct ObservabilityServer {
    metrics: FlowMetrics,
    supervisor: Option<FlowSupervisor>,
    source: Option<String>,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    service: &'static str,
}

#[derive(Serialize)]
struct ApiMessage {
    message: String,
}

#[derive(Default, Deserialize)]
struct DeployQuery {
    #[serde(default)]
    start: bool,
}

#[derive(Default, Deserialize)]
struct FlowQuery {
    flow: Option<String>,
    limit: Option<u32>,
    packet_id: Option<String>,
}

#[derive(Serialize)]
struct ValidationResult {
    valid: bool,
    flow_id: String,
    processors: usize,
    connections: usize,
}

#[derive(Serialize)]
struct DraftCreated {
    flow_id: String,
    version: u32,
}

#[derive(Serialize)]
struct FlowView {
    #[serde(flatten)]
    record: FlowRecord,
    runtime: Option<jaiba_runtime::engine::SupervisedFlowSnapshot>,
}

type ConnectionStores = (
    Arc<dyn SecretStore>,
    Option<Arc<dyn ProfileRepository>>,
    Option<Arc<dyn AuditSink>>,
);

/// Construye el almacén de secretos y la persistencia de conexiones según el
/// entorno.
///
/// - `JAIBA_MASTER_KEY`: clave maestra para cifrar secretos (AES-256-GCM). Si
///   está presente, los perfiles y secretos se persisten y auditan en disco.
/// - `JAIBA_DATA_DIR`: carpeta de datos (por defecto `data`).
///
/// Sin clave maestra se usa un almacén en memoria (solo desarrollo).
fn build_connection_stores() -> Result<ConnectionStores, FlowError> {
    match env::var("JAIBA_MASTER_KEY") {
        Ok(master_key) if !master_key.trim().is_empty() => {
            let base = std::path::PathBuf::from(
                env::var("JAIBA_DATA_DIR").unwrap_or_else(|_| "data".to_owned()),
            );
            let store = EncryptedFileSecretStore::open(base.join("secrets.enc"), &master_key)
                .map_err(|error| {
                    FlowError::Configuration(format!(
                        "no se pudo abrir el almacén de secretos cifrado: {error}"
                    ))
                })?;
            tracing::info!(
                target: "jaiba.connections",
                key_id = %master_key_id(&master_key),
                data_dir = %base.display(),
                "persistencia segura de conexiones activada"
            );
            let secrets: Arc<dyn SecretStore> = Arc::new(store);
            let persistence: Arc<dyn ProfileRepository> =
                Arc::new(FileProfileRepository::new(base.join("connections.json")));
            let audit: Arc<dyn AuditSink> = Arc::new(FileAuditSink::new(base.join("audit.log")));
            Ok((secrets, Some(persistence), Some(audit)))
        }
        _ => {
            tracing::warn!(
                target: "jaiba.connections",
                "JAIBA_MASTER_KEY no configurada: usando almacén en memoria; los perfiles y secretos se perderán al reiniciar"
            );
            Ok((Arc::new(InMemorySecretStore::default()), None, None))
        }
    }
}

/// Rota la clave maestra usada para cifrar los secretos de conexión.
///
/// Lee la clave actual de `JAIBA_MASTER_KEY` (necesaria para descifrar) y
/// vuelve a cifrar todos los secretos con `new_master_key`. Devuelve la huella
/// (`key_id`) de la nueva clave. Tras rotar, actualiza `JAIBA_MASTER_KEY` con la
/// nueva clave antes de reiniciar el servidor.
pub async fn rotate_connection_master_key(new_master_key: &str) -> Result<String, FlowError> {
    let current = env::var("JAIBA_MASTER_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            FlowError::Configuration(
                "JAIBA_MASTER_KEY no está configurada; no hay clave que rotar".to_owned(),
            )
        })?;
    if new_master_key.trim().is_empty() {
        return Err(FlowError::Configuration(
            "la nueva clave maestra no puede estar vacía".to_owned(),
        ));
    }
    let base =
        std::path::PathBuf::from(env::var("JAIBA_DATA_DIR").unwrap_or_else(|_| "data".to_owned()));
    let store = EncryptedFileSecretStore::open(base.join("secrets.enc"), &current)
        .map_err(|error| FlowError::Configuration(error.to_string()))?;
    store
        .rotate_key(new_master_key)
        .await
        .map_err(|error| FlowError::Configuration(error.to_string()))
}

impl ObservabilityServer {
    pub fn new(metrics: FlowMetrics) -> Self {
        Self {
            metrics,
            supervisor: None,
            source: None,
        }
    }

    /// Registra el flujo inicial (p. ej. el servido por el CLI) junto con su
    /// YAML de origen para que quede versionado en el registro.
    pub fn with_supervisor(mut self, supervisor: FlowSupervisor, source: String) -> Self {
        self.supervisor = Some(supervisor);
        self.source = Some(source);
        self
    }

    pub async fn serve(self, address: SocketAddr) -> Result<(), FlowError> {
        let (connection_secrets, persistence, audit) = build_connection_stores()?;
        let connection_manager = crate::connection_api::connection_manager(
            connection_secrets.clone(),
            persistence,
            audit,
        )
        .await
        .map_err(|error| FlowError::Configuration(error.to_string()))?;

        // Registro de flujos: persiste bajo JAIBA_DATA_DIR/flows.json.
        let data_dir = env::var("JAIBA_DATA_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(std::path::PathBuf::from);
        let max_flows = env::var("JAIBA_MAX_FLOWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(16);
        let registry = Arc::new(FlowRegistry::new(data_dir, max_flows));
        match registry.load().await {
            Ok(count) if count > 0 => {
                tracing::info!(target: "jaiba.flows", restored = count, "registro de flujos restaurado");
            }
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "no se pudo cargar el registro de flujos"),
        }

        let (admin_enabled, admin_authentication, admin_token, body_limit) =
            if let (Some(supervisor), Some(source)) =
                (self.supervisor.as_ref(), self.source.as_ref())
            {
                let config = supervisor.config();
                validate_admin_exposure(
                    config.engine.admin.enabled,
                    config.engine.admin.authentication,
                    address,
                )?;
                let token = if config.engine.admin.enabled
                    && config.engine.admin.authentication == AdminAuthentication::Bearer
                {
                    resolve_admin_token(&config.engine.admin.token_env)
                } else {
                    None
                };
                let repository = if config.engine.repository.enabled {
                    Some(LocalPacketRepository::open(&config.engine.repository).await?)
                } else {
                    None
                };
                let admin = (
                    config.engine.admin.enabled,
                    config.engine.admin.authentication,
                    token,
                    config.engine.admin.max_request_body_bytes,
                );
                registry
                    .seed_running(source, supervisor.clone(), self.metrics.clone(), repository)
                    .await
                    .map_err(|error| FlowError::Server(error.message()))?;
                admin
            } else {
                (false, AdminAuthentication::Bearer, None, 1024 * 1024)
            };
        let state = AppState {
            registry,
            admin: Arc::new(RwLock::new(AdminAccess {
                enabled: admin_enabled,
                authentication: admin_authentication,
                token: admin_token,
            })),
            address,
            connection_manager,
            connection_secrets,
        };
        let app = Router::new()
            .route("/health", get(health))
            .route("/ready", get(readiness))
            .route("/metrics", get(prometheus))
            .route("/ws", get(websocket))
            .route("/ws/v1", get(websocket_v1))
            .route("/api/v1/flows", get(list_flows).post(create_flow))
            .route("/api/v1/flows/validate", post(validate_flow))
            .route("/api/v1/flows/{id}", get(get_flow))
            .route("/api/v1/flows/{id}", put(deploy_flow))
            .route("/api/v1/flows/{id}/versions", get(list_versions))
            .route("/api/v1/flows/{id}/versions/{version}", get(export_version))
            .route(
                "/api/v1/flows/{id}/versions/{version}/validate",
                post(validate_version),
            )
            .route(
                "/api/v1/flows/{id}/versions/{version}/deploy",
                post(deploy_version),
            )
            .route(
                "/api/v1/flows/{id}/versions/{version}/archive",
                post(archive_version),
            )
            .route("/api/v1/flows/{id}/rollback", post(rollback_flow))
            .route("/api/v1/flows/{id}/start", post(start_flow))
            .route("/api/v1/flows/{id}/pause", post(pause_flow))
            .route("/api/v1/flows/{id}/resume", post(resume_flow))
            .route("/api/v1/flows/{id}/drain", post(drain_flow))
            .route("/api/v1/flows/{id}/stop", post(stop_flow))
            .route("/api/v1/provenance", get(provenance))
            .route("/api/v1/dead-letter", get(dead_letters))
            .route("/api/v1/connection-types", get(list_connection_types))
            .route(
                "/api/v1/connections",
                get(list_connections).post(create_connection),
            )
            .route(
                "/api/v1/connections/{id}",
                get(get_connection)
                    .put(update_connection)
                    .delete(delete_connection),
            )
            .route(
                "/api/v1/connections/{id}/duplicate",
                post(duplicate_connection),
            )
            .route("/api/v1/connections/{id}/test", post(test_connection))
            .route("/api/v1/connections/{id}/metadata", get(list_metadata))
            .route(
                "/api/v1/connections/{id}/metadata/{schema}/{name}",
                get(describe_metadata),
            )
            .route(
                "/api/v1/connections/{id}/query/compile",
                post(compile_query),
            )
            .route(
                "/api/v1/dead-letter/{queue_id}/replay",
                post(replay_dead_letter),
            )
            .layer(DefaultBodyLimit::max(body_limit))
            .with_state(state.clone());
        let listener = TcpListener::bind(address).await?;
        tracing::info!(%address, "observability and administration server listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal(state.registry.clone()))
            .await
            .map_err(|error| FlowError::Server(error.to_string()))
    }
}

async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        service: "jaiva",
    })
}

async fn readiness(State(state): State<AppState>) -> Response {
    match state.registry.primary_snapshot().await {
        Some(snapshot) if snapshot.ready => (StatusCode::OK, Json(snapshot)).into_response(),
        Some(snapshot) => (StatusCode::SERVICE_UNAVAILABLE, Json(snapshot)).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiMessage {
                message: "no flow is running".to_owned(),
            }),
        )
            .into_response(),
    }
}

async fn prometheus(State(state): State<AppState>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.registry.prometheus().await,
    )
        .into_response()
}

async fn websocket(upgrade: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| stream_metrics(socket, state))
}

async fn websocket_v1(
    upgrade: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| stream_runtime(socket, state))
}

/// Lista todos los flujos registrados con su historial de versiones y estados.
async fn list_flows(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    Json(state.registry.list_records().await).into_response()
}

/// Importa/crea una nueva versión DRAFT del flujo a partir del YAML del cuerpo.
async fn create_flow(State(state): State<AppState>, headers: HeaderMap, body: String) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.registry.create_draft(&body, None).await {
        Ok((flow_id, version)) => {
            tracing::warn!(audit_action = "flow_create", flow_id = %flow_id, version, "administrative action");
            (StatusCode::CREATED, Json(DraftCreated { flow_id, version })).into_response()
        }
        Err(error) => registry_error(error),
    }
}

async fn validate_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match parse_and_validate(&body) {
        Ok(config) => Json(ValidationResult {
            valid: true,
            flow_id: config.id,
            processors: config.processors.len(),
            connections: config.connections.len(),
        })
        .into_response(),
        Err(error) => configuration_error(error),
    }
}

/// Despliegue en un paso (compatibilidad): registra el YAML como nueva versión,
/// la valida y la despliega transaccionalmente.
async fn deploy_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<DeployQuery>,
    body: String,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let config = match parse_and_validate(&body) {
        Ok(config) => config,
        Err(error) => return configuration_error(error),
    };
    if config.id != id {
        return configuration_error(FlowError::Configuration(format!(
            "flow id '{}' does not match URL id '{id}'",
            config.id
        )));
    }
    let next_admin = match admin_access_for(&config, state.address) {
        Ok(admin) => admin,
        Err(error) => return configuration_error(error),
    };
    let version = match state.registry.create_draft(&body, None).await {
        Ok((_, version)) => version,
        Err(error) => return registry_error(error),
    };
    if let Err(error) = state.registry.validate_version(&id, version).await {
        return registry_error(error);
    }
    let resolver = match build_resolver().await {
        Ok(resolver) => resolver,
        Err(error) => return configuration_error(error),
    };
    match state
        .registry
        .deploy_version(&id, version, query.start, resolver)
        .await
    {
        Ok(snapshot) => {
            *state.admin.write().expect("admin lock poisoned") = next_admin;
            tracing::warn!(audit_action = "flow_deploy", flow_id = %id, version, started = query.start, "administrative action");
            Json(snapshot).into_response()
        }
        Err(error) => registry_error(error),
    }
}

/// Valida una versión DRAFT concreta (DRAFT → VALIDATED).
async fn validate_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, u32)>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.registry.validate_version(&id, version).await {
        Ok(record) => {
            tracing::warn!(audit_action = "flow_validate", flow_id = %id, version, "administrative action");
            Json(record).into_response()
        }
        Err(error) => registry_error(error),
    }
}

/// Despliega transaccionalmente una versión VALIDATED.
async fn deploy_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, u32)>,
    Query(query): Query<DeployQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let source = match state.registry.export_version(&id, version).await {
        Some(source) => source,
        None => return not_found("flow version was not found"),
    };
    let config = match parse_and_validate(&source) {
        Ok(config) => config,
        Err(error) => return configuration_error(error),
    };
    let next_admin = match admin_access_for(&config, state.address) {
        Ok(admin) => admin,
        Err(error) => return configuration_error(error),
    };
    let resolver = match build_resolver().await {
        Ok(resolver) => resolver,
        Err(error) => return configuration_error(error),
    };
    match state
        .registry
        .deploy_version(&id, version, query.start, resolver)
        .await
    {
        Ok(snapshot) => {
            *state.admin.write().expect("admin lock poisoned") = next_admin;
            tracing::warn!(audit_action = "flow_deploy", flow_id = %id, version, started = query.start, "administrative action");
            Json(snapshot).into_response()
        }
        Err(error) => registry_error(error),
    }
}

/// Rollback a la versión previamente desplegada.
async fn rollback_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<DeployQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let source = match state.registry.rollback_source(&id).await {
        Ok(source) => source,
        Err(error) => return registry_error(error),
    };
    let config = match parse_and_validate(&source) {
        Ok(config) => config,
        Err(error) => return configuration_error(error),
    };
    let next_admin = match admin_access_for(&config, state.address) {
        Ok(admin) => admin,
        Err(error) => return configuration_error(error),
    };
    let resolver = match build_resolver().await {
        Ok(resolver) => resolver,
        Err(error) => return configuration_error(error),
    };
    match state.registry.rollback(&id, query.start, resolver).await {
        Ok(snapshot) => {
            *state.admin.write().expect("admin lock poisoned") = next_admin;
            tracing::warn!(audit_action = "flow_rollback", flow_id = %id, started = query.start, "administrative action");
            Json(snapshot).into_response()
        }
        Err(error) => registry_error(error),
    }
}

/// Archiva una versión concreta.
async fn archive_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, u32)>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.registry.archive_version(&id, version).await {
        Ok(record) => {
            tracing::warn!(audit_action = "flow_archive", flow_id = %id, version, "administrative action");
            Json(record).into_response()
        }
        Err(error) => registry_error(error),
    }
}

/// Lista las versiones de un flujo.
async fn list_versions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.registry.get_record(&id).await {
        Some(record) => Json(record.versions).into_response(),
        None => not_found("flow was not found"),
    }
}

/// Exporta el YAML inmutable de una versión.
async fn export_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, u32)>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.registry.export_version(&id, version).await {
        Some(source) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/yaml")],
            source,
        )
            .into_response(),
        None => not_found("flow version was not found"),
    }
}

fn resolve_admin_token(variable: &str) -> Option<String> {
    env::var(variable).ok().or_else(|| {
        (variable == "JAIBA_ADMIN_TOKEN")
            .then(|| env::var("JAIVA_ADMIN_TOKEN").ok())
            .flatten()
    })
}

async fn get_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    match state.registry.get_record(&id).await {
        Some(record) => {
            let runtime = state.registry.snapshot(&id).await;
            Json(FlowView { record, runtime }).into_response()
        }
        None => not_found("flow was not found"),
    }
}

async fn start_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let Some(supervisor) = state.registry.supervisor(&id).await else {
        return not_found("flow is not running");
    };
    match supervisor.start().await {
        Ok(true) => {
            tracing::warn!(audit_action = "start", flow_id = %id, "administrative action");
            Json(supervisor.snapshot()).into_response()
        }
        Ok(false) => conflict("operation is not valid in the current flow state"),
        Err(error) => internal(error),
    }
}

async fn stop_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let Some(supervisor) = state.registry.supervisor(&id).await else {
        return not_found("flow is not running");
    };
    match supervisor.stop_gracefully().await {
        Ok(()) => {
            tracing::warn!(audit_action = "stop", flow_id = %id, "administrative action");
            Json(supervisor.snapshot()).into_response()
        }
        Err(error) => internal(error),
    }
}

async fn pause_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    mutate_sync(&state, &headers, &id, "pause", FlowSupervisor::pause).await
}

async fn resume_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    mutate_sync(&state, &headers, &id, "resume", FlowSupervisor::resume).await
}

async fn drain_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    mutate_sync(&state, &headers, &id, "drain", FlowSupervisor::drain).await
}

async fn provenance(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FlowQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let Some(flow_id) = resolve_target_flow(&state, query.flow.as_deref()).await else {
        return unavailable("no flow with a persistent repository is available");
    };
    let Some(repository) = state.registry.repository(&flow_id).await else {
        return unavailable("persistent repository is disabled");
    };
    let result = if let Some(packet_id) = query.packet_id {
        repository
            .provenance_for_packet(&flow_id, &packet_id, query.limit.unwrap_or(1000))
            .await
    } else {
        repository
            .recent_provenance(&flow_id, query.limit.unwrap_or(100))
            .await
    };
    json_result(result)
}

async fn dead_letters(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FlowQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let Some(flow_id) = resolve_target_flow(&state, query.flow.as_deref()).await else {
        return unavailable("no flow with a persistent repository is available");
    };
    let Some(repository) = state.registry.repository(&flow_id).await else {
        return unavailable("persistent repository is disabled");
    };
    json_result(
        repository
            .dead_letters(&flow_id, query.limit.unwrap_or(100))
            .await,
    )
}

async fn replay_dead_letter(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(queue_id): Path<String>,
    Query(query): Query<FlowQuery>,
) -> Response {
    if let Err(response) = authorize(&state, &headers) {
        return response;
    }
    let Some(flow_id) = resolve_target_flow(&state, query.flow.as_deref()).await else {
        return unavailable("no flow with a persistent repository is available");
    };
    let Some(repository) = state.registry.repository(&flow_id).await else {
        return unavailable("persistent repository is disabled");
    };
    match repository.requeue_dead_letter(&queue_id).await {
        Ok(true) => {
            tracing::warn!(audit_action = "dead_letter_replay", %queue_id, "administrative action");
            Json(ApiMessage {
                message: "dead-letter packet requeued".to_owned(),
            })
            .into_response()
        }
        Ok(false) => not_found("dead-letter packet was not found"),
        Err(error) => internal(error),
    }
}

/// Determina el flujo objetivo: el indicado por `?flow=` o el único en ejecución.
async fn resolve_target_flow(state: &AppState, flow: Option<&str>) -> Option<String> {
    match flow {
        Some(flow) => Some(flow.to_owned()),
        None => state.registry.sole_running_id().await,
    }
}

async fn mutate_sync(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
    action: &'static str,
    operation: fn(&FlowSupervisor) -> bool,
) -> Response {
    if let Err(response) = authorize(state, headers) {
        return response;
    }
    let Some(supervisor) = state.registry.supervisor(id).await else {
        return not_found("flow is not running");
    };
    if !operation(&supervisor) {
        return conflict("operation is not valid in the current flow state");
    }
    tracing::warn!(audit_action = action, flow_id = %id, "administrative action");
    Json(supervisor.snapshot()).into_response()
}

fn registry_error(error: RegistryError) -> Response {
    let status = match &error {
        RegistryError::NotFound(_) => StatusCode::NOT_FOUND,
        RegistryError::InvalidState(_) | RegistryError::LimitExceeded(_) => StatusCode::CONFLICT,
        RegistryError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
        RegistryError::Internal(inner) => {
            tracing::error!(error = %inner, "flow registry operation failed");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (
        status,
        Json(ApiMessage {
            message: error.message(),
        }),
    )
        .into_response()
}

/// Construye el resolvedor de conexiones por alias a partir del entorno.
async fn build_resolver() -> Result<Option<Arc<dyn ConnectionResolver>>, FlowError> {
    Ok(ProfileConnectionResolver::from_env()
        .await?
        .map(|resolver| Arc::new(resolver) as Arc<dyn ConnectionResolver>))
}

/// Valida la exposición de admin y resuelve el token para un flujo concreto.
#[allow(clippy::result_large_err)]
fn admin_access_for(config: &FlowConfig, address: SocketAddr) -> Result<AdminAccess, FlowError> {
    validate_admin_exposure(
        config.engine.admin.enabled,
        config.engine.admin.authentication,
        address,
    )?;
    let token = if config.engine.admin.enabled
        && config.engine.admin.authentication == AdminAuthentication::Bearer
    {
        match resolve_admin_token(&config.engine.admin.token_env) {
            Some(token) => Some(token),
            None => {
                return Err(FlowError::Configuration(format!(
                    "administrative token environment variable '{}' is missing",
                    config.engine.admin.token_env
                )));
            }
        }
    } else {
        None
    };
    Ok(AdminAccess {
        enabled: config.engine.admin.enabled,
        authentication: config.engine.admin.authentication,
        token,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let admin = state.admin.read().expect("admin lock poisoned");
    if !admin.enabled {
        return Err(unavailable("administrative API is disabled"));
    }
    if admin.authentication == AdminAuthentication::None {
        return Ok(());
    }
    let Some(expected) = admin.token.as_deref() else {
        return Err(unavailable(
            "administrative token environment variable is missing",
        ));
    };
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided == Some(expected) {
        Ok(())
    } else {
        tracing::warn!(
            audit_action = "authorization_rejected",
            "administrative authorization failed"
        );
        Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiMessage {
                message: "a valid Bearer token is required".to_owned(),
            }),
        )
            .into_response())
    }
}

pub(crate) fn parse_and_validate(body: &str) -> Result<FlowConfig, FlowError> {
    let config: FlowConfig = serde_yaml::from_str(body)?;
    if config.id.trim().is_empty() {
        return Err(FlowError::Configuration(
            "flow id cannot be empty".to_owned(),
        ));
    }
    if !(1..=90).contains(&config.engine.memory.maximum_percent) {
        return Err(FlowError::Configuration(
            "memory maximum_percent must be between 1 and 90".to_owned(),
        ));
    }
    for (name, connection) in &config.database_connections {
        if connection.url_env.trim().is_empty() || connection.max_connections == 0 {
            return Err(FlowError::Configuration(format!(
                "database connection '{name}' requires url_env and max_connections greater than zero"
            )));
        }
        if !matches!(
            connection.connection_type.as_str(),
            "postgres" | "mysql" | "mariadb" | "oracle" | "sqlserver" | "mssql"
        ) {
            return Err(FlowError::Configuration(format!(
                "unsupported database connection type '{}'",
                connection.connection_type
            )));
        }
    }
    for (name, connection) in &config.kafka_connections {
        if connection.brokers_env.trim().is_empty() {
            return Err(FlowError::Configuration(format!(
                "Kafka connection '{name}' requires brokers_env"
            )));
        }
    }
    FlowEngine::new(config.clone())?;
    let registry = jaiba_runtime::processors::default_registry();
    for processor in &config.processors {
        registry.build(&processor.processor_type, &processor.config)?;
        let connection = processor
            .config
            .get("connection")
            .and_then(serde_json::Value::as_str);
        match (processor.processor_type.as_str(), connection) {
            ("query_postgres", Some(name)) => {
                let definition = config.database_connections.get(name).ok_or_else(|| {
                    FlowError::Configuration(format!(
                        "processor '{}' references unknown database connection '{name}'",
                        processor.id
                    ))
                })?;
                if definition.connection_type != "postgres" {
                    return Err(FlowError::Configuration(format!(
                        "processor '{}' requires a PostgreSQL connection",
                        processor.id
                    )));
                }
            }
            ("put_database", Some(name)) if !config.database_connections.contains_key(name) => {
                return Err(FlowError::Configuration(format!(
                    "processor '{}' references unknown database connection '{name}'",
                    processor.id
                )));
            }
            ("publish_kafka", Some(name)) if !config.kafka_connections.contains_key(name) => {
                return Err(FlowError::Configuration(format!(
                    "processor '{}' references unknown Kafka connection '{name}'",
                    processor.id
                )));
            }
            _ => {}
        }
    }
    Ok(config)
}

fn json_result<T: Serialize>(result: Result<T, FlowError>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => internal(error),
    }
}

fn conflict(message: &str) -> Response {
    (
        StatusCode::CONFLICT,
        Json(ApiMessage {
            message: message.to_owned(),
        }),
    )
        .into_response()
}

fn unavailable(message: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiMessage {
            message: message.to_owned(),
        }),
    )
        .into_response()
}

fn not_found(message: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ApiMessage {
            message: message.to_owned(),
        }),
    )
        .into_response()
}

fn internal(error: FlowError) -> Response {
    tracing::error!(%error, "administrative request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiMessage {
            message: "internal operation failed".to_owned(),
        }),
    )
        .into_response()
}

fn configuration_error(error: FlowError) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(ApiMessage {
            message: error.to_string(),
        }),
    )
        .into_response()
}

async fn stream_metrics(mut socket: WebSocket, state: AppState) {
    let mut ticker = interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        let summary = state.registry.primary_snapshot().await.map(|s| s.metrics);
        let Ok(message) = serde_json::to_string(&summary) else {
            break;
        };
        if socket.send(Message::Text(message.into())).await.is_err() {
            break;
        }
    }
}

#[derive(Serialize)]
struct RuntimeEvent {
    kind: &'static str,
    flow: Option<SupervisedFlowSnapshot>,
    flows: Vec<SupervisedFlowSnapshot>,
}

async fn stream_runtime(mut socket: WebSocket, state: AppState) {
    let mut ticker = interval(Duration::from_secs(1));
    loop {
        ticker.tick().await;
        let flows = state.registry.snapshots().await;
        let event = RuntimeEvent {
            kind: "runtime_snapshot",
            flow: flows.first().cloned(),
            flows,
        };
        let Ok(message) = serde_json::to_string(&event) else {
            break;
        };
        if socket.send(Message::Text(message.into())).await.is_err() {
            break;
        }
    }
}

async fn shutdown_signal(registry: Arc<FlowRegistry>) {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install shutdown signal");
        return;
    }
    registry.stop_all().await;
}

fn validate_admin_exposure(
    enabled: bool,
    authentication: AdminAuthentication,
    address: SocketAddr,
) -> Result<(), FlowError> {
    if enabled && authentication == AdminAuthentication::None && !address.ip().is_loopback() {
        return Err(FlowError::Configuration(
            "admin authentication 'none' is allowed only on a loopback address".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unauthenticated_admin_is_limited_to_loopback() {
        assert!(
            validate_admin_exposure(
                true,
                AdminAuthentication::None,
                "127.0.0.1:9090".parse().unwrap()
            )
            .is_ok()
        );
        assert!(
            validate_admin_exposure(
                true,
                AdminAuthentication::None,
                "0.0.0.0:9090".parse().unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn visual_validation_accepts_a_supported_flow() {
        let config = parse_and_validate(
            r#"
id: visual-test
engine:
  max_concurrency: 2
processors:
  - id: source
    type: generate_records
    config:
      records: []
  - id: log
    type: log_records
connections:
  - from: source
    relationship: success
    to: log
"#,
        )
        .unwrap();
        assert_eq!(config.id, "visual-test");
    }

    #[test]
    fn visual_validation_rejects_runtime_incompatible_settings() {
        let error = parse_and_validate(
            r#"
id: invalid-memory
engine:
  memory:
    maximum_percent: 100
processors:
  - id: source
    type: generate_records
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("between 1 and 90"));
    }
}
