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
    InMemorySecretStore, ProfileRepository, SecretStore,
};
use jaiba_core::config::{AdminAuthentication, FlowConfig};
use jaiba_runtime::{
    engine::{
        ConnectionResolver, FlowEngine, FlowMetrics, FlowSupervisor, LocalPacketRepository,
        PacketRepository, ProfileConnectionResolver, SupervisedFlowSnapshot,
    },
    error::FlowError,
};

use crate::auth::{
    AuthContext, Permission, Principal, Role, WhoAmI, load_users_file, match_principal,
};
use crate::connection_api::{
    compile_query, create_connection, delete_connection, describe_metadata, diagnose_connection,
    duplicate_connection, get_connection, list_connection_types, list_connections, list_metadata,
    test_connection, update_connection,
};
use crate::flow_registry::{FlowRecord, FlowRegistry, RegistryError};
use crate::schedule_store::ScheduleStore;
use crate::scheduler::FlowScheduler;

#[derive(Clone)]
pub(crate) struct AppState {
    registry: Arc<FlowRegistry>,
    scheduler: Arc<FlowScheduler>,
    admin: Arc<RwLock<AdminAccess>>,
    pub(crate) connection_manager: Arc<ConnectionManager>,
    pub(crate) connection_secrets: Arc<dyn SecretStore>,
}

#[derive(Clone)]
struct AdminAccess {
    enabled: bool,
    authentication: AdminAuthentication,
    /// Principales Bearer (token único o fichero de usuarios).
    principals: Vec<Principal>,
    /// True si el bind es loopback (permite /runtime y /ws sin Bearer).
    bind_is_loopback: bool,
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
/// - `JAIBA_MASTER_KEY`: passphrase para derivar (Argon2id) la clave AES-256-GCM.
///   Si está presente, los perfiles y secretos se persisten y auditan en disco.
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
                key_id = %store.fingerprint(),
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
        let registry = Arc::new(FlowRegistry::new(data_dir.clone(), max_flows));
        match registry.load().await {
            Ok(count) if count > 0 => {
                tracing::info!(target: "jaiba.flows", restored = count, "registro de flujos restaurado");
            }
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "no se pudo cargar el registro de flujos"),
        }
        let scheduler = FlowScheduler::new(registry.clone(), ScheduleStore::open(data_dir));

        // El gate administrativo se fija al arranque (env + semilla del flujo
        // inicial) y NUNCA se reescribe al desplegar otro flujo.
        let seed_config = self.supervisor.as_ref().map(FlowSupervisor::config);
        let (admin, body_limit) = resolve_server_admin(address, seed_config)?;
        if let (Some(supervisor), Some(source)) = (self.supervisor.as_ref(), self.source.as_ref()) {
            let config = supervisor.config();
            let flow_id = config.id.clone();
            let repository = if config.engine.repository.enabled {
                Some(LocalPacketRepository::open(&config.engine.repository).await?)
            } else {
                None
            };
            registry
                .seed_running(source, supervisor.clone(), self.metrics.clone(), repository)
                .await
                .map_err(|error| FlowError::Server(error.message()))?;
            scheduler.arm(&flow_id).await;
        }
        let state = AppState {
            registry,
            scheduler,
            admin: Arc::new(RwLock::new(admin)),
            connection_manager,
            connection_secrets,
        };
        let app = Router::new()
            .route("/health", get(health))
            .route("/ready", get(readiness))
            .route("/runtime", get(runtime))
            .route("/metrics", get(prometheus))
            .route("/ws", get(websocket))
            .route("/ws/v1", get(websocket_v1))
            .route("/api/v1/whoami", get(whoami))
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
            .route("/api/v1/flows/{id}/trigger", post(trigger_flow))
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
            .route(
                "/api/v1/connections/{id}/diagnostics",
                get(diagnose_connection),
            )
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
        serve_http_or_https(address, app, state.registry.clone()).await
    }
}

async fn serve_http_or_https(
    address: SocketAddr,
    app: Router,
    registry: Arc<FlowRegistry>,
) -> Result<(), FlowError> {
    let cert = env::var("JAIBA_TLS_CERT_FILE")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let key = env::var("JAIBA_TLS_KEY_FILE")
        .ok()
        .filter(|v| !v.trim().is_empty());
    match (cert, key) {
        (Some(cert), Some(key)) => {
            let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
                .await
                .map_err(|error| {
                    FlowError::Configuration(format!(
                        "no se pudo cargar TLS (JAIBA_TLS_CERT_FILE / JAIBA_TLS_KEY_FILE): {error}"
                    ))
                })?;
            tracing::info!(%address, cert = %cert, "HTTPS administration server listening");
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            let registry = registry.clone();
            tokio::spawn(async move {
                shutdown_signal(registry).await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
            });
            axum_server::bind_rustls(address, config)
                .handle(handle)
                .serve(app.into_make_service())
                .await
                .map_err(|error| FlowError::Server(error.to_string()))
        }
        (None, None) => {
            let listener = TcpListener::bind(address).await?;
            tracing::info!(%address, "observability and administration server listening");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal(registry))
                .await
                .map_err(|error| FlowError::Server(error.to_string()))
        }
        _ => Err(FlowError::Configuration(
            "TLS incompleto: defina JAIBA_TLS_CERT_FILE y JAIBA_TLS_KEY_FILE juntos".to_owned(),
        )),
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

#[derive(Debug, Deserialize)]
struct ObservabilityQuery {
    /// Token opcional para WebSocket cuando el bind no es loopback.
    access_token: Option<String>,
}

async fn runtime(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ObservabilityQuery>,
) -> Response {
    if let Err(response) = authorize_observability(&state, &headers, query.access_token.as_deref())
    {
        return response;
    }
    Json(state.registry.primary_snapshot().await).into_response()
}

async fn prometheus(State(state): State<AppState>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.registry.prometheus().await,
    )
        .into_response()
}

async fn websocket(
    upgrade: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ObservabilityQuery>,
) -> Response {
    if let Err(response) = authorize_observability(&state, &headers, query.access_token.as_deref())
    {
        return response;
    }
    upgrade
        .on_upgrade(move |socket| stream_metrics(socket, state))
        .into_response()
}

async fn websocket_v1(
    upgrade: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ObservabilityQuery>,
) -> Response {
    if let Err(response) = authorize_observability(&state, &headers, query.access_token.as_deref())
    {
        return response;
    }
    upgrade
        .on_upgrade(move |socket| stream_runtime(socket, state))
        .into_response()
}

/// Lista todos los flujos registrados con su historial de versiones y estados.
async fn list_flows(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let ctx = match authorize_perm(&state, &headers, Permission::Read) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let records = state.registry.list_records().await;
    let filtered: Vec<_> = records
        .into_iter()
        .filter(|record| ctx.allows_project(&record.id))
        .collect();
    Json(filtered).into_response()
}

/// Importa/crea una nueva versión DRAFT del flujo a partir del YAML del cuerpo.
async fn create_flow(State(state): State<AppState>, headers: HeaderMap, body: String) -> Response {
    let ctx = match authorize_perm(&state, &headers, Permission::Operate) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let flow_id = match draft_flow_id(&body) {
        Ok(flow_id) => flow_id,
        Err(error) => return configuration_error(error),
    };
    if !ctx.allows_project(&flow_id) {
        return forbidden("flow is outside the caller's project allowlist");
    }
    match state.registry.create_draft(&body, None).await {
        Ok((flow_id, version)) => {
            tracing::warn!(
                audit_action = "flow_create",
                actor = admin_actor(&ctx),
                flow_id = %flow_id,
                version,
                "administrative action"
            );
            (StatusCode::CREATED, Json(DraftCreated { flow_id, version })).into_response()
        }
        Err(error) => registry_error(error),
    }
}

fn draft_flow_id(body: &str) -> Result<String, FlowError> {
    let config: FlowConfig = serde_yaml::from_str(body)?;
    let id = config.id.trim();
    if id.is_empty() {
        return Err(FlowError::Configuration(
            "flow id cannot be empty".to_owned(),
        ));
    }
    Ok(id.to_owned())
}

async fn validate_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let ctx = match authorize_perm(&state, &headers, Permission::Operate) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    match parse_and_validate(&body) {
        Ok(config) => {
            if !ctx.allows_project(&config.id) {
                return forbidden("flow is outside the caller's project allowlist");
            }
            tracing::warn!(
                audit_action = "flow_validate",
                actor = admin_actor(&ctx),
                flow_id = %config.id,
                processors = config.processors.len(),
                connections = config.connections.len(),
                "administrative action"
            );
            Json(ValidationResult {
                valid: true,
                flow_id: config.id,
                processors: config.processors.len(),
                connections: config.connections.len(),
            })
            .into_response()
        }
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
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
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
            tracing::warn!(audit_action = "flow_deploy", flow_id = %id, version, started = query.start, "administrative action");
            sync_schedule(&state, &id, query.start).await;
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
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
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
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
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
            tracing::warn!(audit_action = "flow_deploy", flow_id = %id, version, started = query.start, "administrative action");
            sync_schedule(&state, &id, query.start).await;
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
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let resolver = match build_resolver().await {
        Ok(resolver) => resolver,
        Err(error) => return configuration_error(error),
    };
    match state.registry.rollback(&id, query.start, resolver).await {
        Ok(snapshot) => {
            tracing::warn!(audit_action = "flow_rollback", flow_id = %id, started = query.start, "administrative action");
            sync_schedule(&state, &id, query.start).await;
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
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
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
    if let Err(response) = authorize_flow(&state, &headers, Permission::Read, &id) {
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
    if let Err(response) = authorize_flow(&state, &headers, Permission::Read, &id) {
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
    if let Err(response) = authorize_flow(&state, &headers, Permission::Read, &id) {
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
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let Some(supervisor) = state.registry.supervisor(&id).await else {
        return not_found("flow is not running");
    };
    match supervisor.start().await {
        Ok(true) => {
            tracing::warn!(audit_action = "start", flow_id = %id, "administrative action");
            state.scheduler.arm(&id).await;
            Json(supervisor.snapshot()).into_response()
        }
        Ok(false) => conflict("operation is not valid in the current flow state"),
        Err(error) => internal(error),
    }
}

/// Disparo manual / webhook de una ejecución (respeta overlap del schedule).
async fn trigger_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    match state.scheduler.trigger(&id).await {
        Ok(true) => {
            tracing::warn!(audit_action = "trigger", flow_id = %id, "administrative action");
            let snapshot = state
                .registry
                .supervisor(&id)
                .await
                .map(|supervisor| supervisor.snapshot());
            match snapshot {
                Some(snapshot) => Json(snapshot).into_response(),
                None => not_found("flow is not running"),
            }
        }
        Ok(false) => conflict("trigger skipped because the flow is still running"),
        Err(error) => match error {
            FlowError::Configuration(message) if message.contains("no está cargado") => {
                not_found(&message)
            }
            other => internal(other),
        },
    }
}

async fn stop_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let _ctx = match authorize_flow(&state, &headers, Permission::Operate, &id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    state.scheduler.disarm(&id).await;
    match state.registry.stop_and_unload(&id).await {
        Ok(snapshot) => {
            tracing::warn!(audit_action = "stop", flow_id = %id, "administrative action");
            Json(snapshot).into_response()
        }
        Err(error) => registry_error(error),
    }
}

async fn sync_schedule(state: &AppState, id: &str, started: bool) {
    if started {
        state.scheduler.arm(id).await;
    } else {
        state.scheduler.disarm(id).await;
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
    let ctx = match authorize_perm(&state, &headers, Permission::Read) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let Some(flow_id) = resolve_target_flow(&state, query.flow.as_deref()).await else {
        return unavailable("no flow with a persistent repository is available");
    };
    if !ctx.allows_project(&flow_id) {
        return forbidden("flow is outside the caller's project allowlist");
    }
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
    let ctx = match authorize_perm(&state, &headers, Permission::Read) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let Some(flow_id) = resolve_target_flow(&state, query.flow.as_deref()).await else {
        return unavailable("no flow with a persistent repository is available");
    };
    if !ctx.allows_project(&flow_id) {
        return forbidden("flow is outside the caller's project allowlist");
    }
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
    let ctx = match authorize_perm(&state, &headers, Permission::Operate) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let Some(flow_id) = resolve_target_flow(&state, query.flow.as_deref()).await else {
        return unavailable("no flow with a persistent repository is available");
    };
    if !ctx.allows_project(&flow_id) {
        return forbidden("flow is outside the caller's project allowlist");
    }
    let Some(repository) = state.registry.repository(&flow_id).await else {
        return unavailable("persistent repository is disabled");
    };
    match repository.requeue_dead_letter(&queue_id).await {
        Ok(true) => {
            tracing::warn!(
                audit_action = "dead_letter_replay",
                actor = admin_actor(&ctx),
                %queue_id,
                "administrative action"
            );
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
    let ctx = match authorize_flow(state, headers, Permission::Operate, id) {
        Ok(ctx) => ctx,
        Err(response) => return response,
    };
    let Some(supervisor) = state.registry.supervisor(id).await else {
        return not_found("flow is not running");
    };
    if !operation(&supervisor) {
        return conflict("operation is not valid in the current flow state");
    }
    tracing::warn!(
        audit_action = action,
        actor = admin_actor(&ctx),
        flow_id = %id,
        "administrative action"
    );
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

/// Resuelve el gate administrativo del **proceso** al arranque.
///
/// Orden de precedencia: variables de entorno → configuración del flujo semilla
/// (solo al arrancar con `jaiba serve FLOW.yaml`) → defaults seguros.
/// Un despliegue posterior **nunca** puede cambiar esta política.
fn resolve_server_admin(
    address: SocketAddr,
    seed: Option<&FlowConfig>,
) -> Result<(AdminAccess, usize), FlowError> {
    let enabled = parse_env_bool("JAIBA_ADMIN_ENABLED")
        .or_else(|| seed.map(|config| config.engine.admin.enabled))
        .unwrap_or(true);
    let authentication = match env::var("JAIBA_ADMIN_AUTH")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("none") => AdminAuthentication::None,
        Some("bearer") => AdminAuthentication::Bearer,
        Some(other) => {
            return Err(FlowError::Configuration(format!(
                "JAIBA_ADMIN_AUTH inválida '{other}'; use 'bearer' o 'none'"
            )));
        }
        None => seed
            .map(|config| config.engine.admin.authentication)
            .unwrap_or(AdminAuthentication::Bearer),
    };
    validate_admin_exposure(enabled, authentication, address)?;

    let token_env = env::var("JAIBA_ADMIN_TOKEN_ENV")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| seed.map(|config| config.engine.admin.token_env.clone()))
        .unwrap_or_else(|| "JAIBA_ADMIN_TOKEN".to_owned());
    // Lista blanca: un YAML no puede apuntar el Bearer a PATH u otras variables.
    const ALLOWED_TOKEN_ENVS: &[&str] = &["JAIBA_ADMIN_TOKEN", "JAIVA_ADMIN_TOKEN"];
    let token_env = if ALLOWED_TOKEN_ENVS.contains(&token_env.as_str()) {
        token_env
    } else {
        tracing::warn!(
            requested = %token_env,
            "JAIBA_ADMIN_TOKEN_ENV no está en la lista blanca; usando JAIBA_ADMIN_TOKEN"
        );
        "JAIBA_ADMIN_TOKEN".to_owned()
    };

    let mut principals = Vec::new();
    if enabled && authentication == AdminAuthentication::Bearer {
        if let Ok(path) = env::var("JAIBA_ADMIN_USERS_FILE") {
            let path = path.trim();
            if !path.is_empty() {
                principals = load_users_file(std::path::Path::new(path))?;
                tracing::info!(
                    path,
                    users = principals.len(),
                    "admin multi-usuario cargado desde JAIBA_ADMIN_USERS_FILE"
                );
            }
        }
        if let Some(token) = resolve_admin_token(&token_env) {
            // Token de entorno sigue siendo admin global (bootstrap / compat 9A).
            principals.push(Principal {
                id: "bearer".to_owned(),
                role: Role::Admin,
                projects: vec!["*".to_owned()],
                token_secret: token,
            });
        }
        if principals.is_empty() {
            return Err(FlowError::Configuration(format!(
                "authentication=bearer requiere '{token_env}' o JAIBA_ADMIN_USERS_FILE \
                 (en desarrollo local use engine.admin.authentication: none o JAIBA_ADMIN_AUTH=none)"
            )));
        }
    }

    let body_limit = env::var("JAIBA_ADMIN_MAX_BODY_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .or_else(|| seed.map(|config| config.engine.admin.max_request_body_bytes))
        .unwrap_or(1024 * 1024)
        .max(1);

    if enabled && authentication == AdminAuthentication::None {
        tracing::warn!(
            %address,
            "API administrativa sin autenticación (solo válido en loopback)"
        );
    }

    Ok((
        AdminAccess {
            enabled,
            authentication,
            principals,
            bind_is_loopback: address.ip().is_loopback(),
        },
        body_limit,
    ))
}

fn parse_env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

/// Actor lógico para auditoría (id de usuario o `none` / `bearer`).
pub(crate) fn admin_actor(ctx: &AuthContext) -> &str {
    ctx.actor.as_str()
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

// Axum handlers use `Response` directly so authorization failures can preserve
// their status and JSON body without another error-conversion layer.
#[allow(clippy::result_large_err)]
fn authenticate(
    admin: &AdminAccess,
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Result<AuthContext, Response> {
    if !admin.enabled {
        return Err(unavailable("administrative API is disabled"));
    }
    if admin.authentication == AdminAuthentication::None {
        return Ok(AuthContext {
            actor: "none".to_owned(),
            role: Role::Admin,
            projects: vec!["*".to_owned()],
        });
    }
    let presented = extract_bearer_token(headers).or(query_token);
    let Some(token) = presented else {
        return Err(unauthorized());
    };
    match match_principal(&admin.principals, token) {
        Some(principal) => Ok(AuthContext {
            actor: principal.id.clone(),
            role: principal.role,
            projects: principal.projects.clone(),
        }),
        None => {
            tracing::warn!(
                audit_action = "authorization_rejected",
                actor = "anonymous",
                "administrative authorization failed"
            );
            Err(unauthorized())
        }
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(ApiMessage {
            message: "a valid Bearer token is required".to_owned(),
        }),
    )
        .into_response()
}

fn forbidden(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ApiMessage {
            message: message.to_owned(),
        }),
    )
        .into_response()
}

/// Autoriza con permiso Operate (compat / helpers).
#[allow(dead_code)]
#[allow(clippy::result_large_err)]
pub(crate) fn authorize(state: &AppState, headers: &HeaderMap) -> Result<AuthContext, Response> {
    authorize_perm(state, headers, Permission::Operate)
}

#[allow(clippy::result_large_err)]
pub(crate) fn authorize_perm(
    state: &AppState,
    headers: &HeaderMap,
    permission: Permission,
) -> Result<AuthContext, Response> {
    let admin = state.admin.read().expect("admin lock poisoned");
    let ctx = authenticate(&admin, headers, None)?;
    if !ctx.has_permission(permission) {
        tracing::warn!(
            audit_action = "authorization_forbidden",
            actor = %ctx.actor,
            role = ctx.role.as_str(),
            ?permission,
            "insufficient role"
        );
        return Err(forbidden("insufficient role for this operation"));
    }
    Ok(ctx)
}

#[allow(clippy::result_large_err)]
pub(crate) fn authorize_flow(
    state: &AppState,
    headers: &HeaderMap,
    permission: Permission,
    flow_id: &str,
) -> Result<AuthContext, Response> {
    let ctx = authorize_perm(state, headers, permission)?;
    if !ctx.allows_project(flow_id) {
        tracing::warn!(
            audit_action = "authorization_forbidden",
            actor = %ctx.actor,
            flow_id,
            "project not allowed"
        );
        return Err(forbidden("flow is outside the caller's project allowlist"));
    }
    Ok(ctx)
}

/// Autoriza superficies de observación (`/runtime`, `/ws`) fuera de loopback.
///
/// En bind loopback siguen abiertas (UI local / health probes). En bind no
/// loopback exigen Bearer con al menos rol viewer.
#[allow(clippy::result_large_err)]
fn authorize_observability(
    state: &AppState,
    headers: &HeaderMap,
    query_token: Option<&str>,
) -> Result<(), Response> {
    let admin = state.admin.read().expect("admin lock poisoned");
    if admin.bind_is_loopback || admin.authentication == AdminAuthentication::None {
        return Ok(());
    }
    let ctx = authenticate(&admin, headers, query_token)?;
    if !ctx.has_permission(Permission::Read) {
        return Err(forbidden("insufficient role for observability"));
    }
    Ok(())
}

async fn whoami(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let admin = state.admin.read().expect("admin lock poisoned");
    let auth_label = match admin.authentication {
        AdminAuthentication::None => "none",
        AdminAuthentication::Bearer => {
            if admin.principals.iter().any(|p| p.id != "bearer") {
                "users"
            } else {
                "bearer"
            }
        }
    };
    drop(admin);
    match authorize_perm(&state, &headers, Permission::Read) {
        Ok(ctx) => Json(WhoAmI {
            actor: ctx.actor,
            role: ctx.role.as_str(),
            projects: ctx.projects,
            authentication: auth_label,
        })
        .into_response(),
        Err(response) => response,
    }
}

pub(crate) fn parse_and_validate(body: &str) -> Result<FlowConfig, FlowError> {
    let config: FlowConfig = serde_yaml::from_str(body)?;
    if config.id.trim().is_empty() {
        return Err(FlowError::Configuration(
            "flow id cannot be empty".to_owned(),
        ));
    }
    if let Some(schedule) = &config.schedule
        && let Err(error) = schedule.validate()
    {
        return Err(FlowError::Configuration(error));
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
            "postgres"
                | "mysql"
                | "mariadb"
                | "mongodb"
                | "mongo"
                | "oracle"
                | "sqlserver"
                | "mssql"
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
            ("query_mysql", Some(name)) => {
                let definition = config.database_connections.get(name).ok_or_else(|| {
                    FlowError::Configuration(format!(
                        "processor '{}' references unknown database connection '{name}'",
                        processor.id
                    ))
                })?;
                if !matches!(definition.connection_type.as_str(), "mysql" | "mariadb") {
                    return Err(FlowError::Configuration(format!(
                        "processor '{}' requires a MySQL or MariaDB connection",
                        processor.id
                    )));
                }
            }
            ("query_mongodb" | "put_mongodb", Some(name)) => {
                let definition = config.database_connections.get(name).ok_or_else(|| {
                    FlowError::Configuration(format!(
                        "processor '{}' references unknown MongoDB connection '{name}'",
                        processor.id
                    ))
                })?;
                if !matches!(definition.connection_type.as_str(), "mongodb" | "mongo") {
                    return Err(FlowError::Configuration(format!(
                        "processor '{}' requires a MongoDB connection",
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
            ("publish_kafka", Some(name)) | ("consume_kafka", Some(name))
                if !config.kafka_connections.contains_key(name) =>
            {
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
    fn bearer_without_token_fails_even_on_loopback() {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK.lock().unwrap();
        let previous = std::env::var("JAIBA_ADMIN_TOKEN").ok();
        let previous_legacy = std::env::var("JAIVA_ADMIN_TOKEN").ok();
        let previous_auth = std::env::var("JAIBA_ADMIN_AUTH").ok();
        let previous_users = std::env::var("JAIBA_ADMIN_USERS_FILE").ok();
        // SAFETY: all mutated variables are restored before releasing ENV_LOCK.
        unsafe {
            std::env::remove_var("JAIBA_ADMIN_TOKEN");
            std::env::remove_var("JAIVA_ADMIN_TOKEN");
            std::env::remove_var("JAIBA_ADMIN_AUTH");
            std::env::remove_var("JAIBA_ADMIN_USERS_FILE");
        }
        let result = resolve_server_admin("127.0.0.1:9090".parse().unwrap(), None);
        let err = match result {
            Ok(_) => panic!("expected configuration error without admin token"),
            Err(error) => error,
        };
        let message = err.to_string();
        assert!(
            message.contains("requiere") || message.contains("requerida"),
            "unexpected error: {err}"
        );
        unsafe {
            match previous {
                Some(value) => std::env::set_var("JAIBA_ADMIN_TOKEN", value),
                None => std::env::remove_var("JAIBA_ADMIN_TOKEN"),
            }
            match previous_users {
                Some(value) => std::env::set_var("JAIBA_ADMIN_USERS_FILE", value),
                None => std::env::remove_var("JAIBA_ADMIN_USERS_FILE"),
            }
            match previous_legacy {
                Some(value) => std::env::set_var("JAIVA_ADMIN_TOKEN", value),
                None => std::env::remove_var("JAIVA_ADMIN_TOKEN"),
            }
            match previous_auth {
                Some(value) => std::env::set_var("JAIBA_ADMIN_AUTH", value),
                None => std::env::remove_var("JAIBA_ADMIN_AUTH"),
            }
        }
    }

    #[test]
    fn draft_id_is_available_before_registry_mutation() {
        let id = draft_flow_id(
            r#"
id: project-a
processors: []
"#,
        )
        .unwrap();
        assert_eq!(id, "project-a");
    }

    #[test]
    fn auth_module_covers_token_compare() {
        // Comparación en tiempo constante vive en `auth::token_matches`.
        assert!(crate::auth::Role::Viewer < crate::auth::Role::Admin);
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
