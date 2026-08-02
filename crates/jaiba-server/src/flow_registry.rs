//! Registro multi-flujo con versionado inmutable, estados de ciclo de vida,
//! despliegue transaccional y rollback.
//!
//! Un `FlowRecord` agrupa el historial de versiones de un flujo. Cada
//! `FlowVersion` guarda el YAML de origen de forma inmutable y su estado:
//!
//! ```text
//! DRAFT ──validate──▶ VALIDATED ──deploy──▶ DEPLOYED ──(nuevo deploy)──▶ ARCHIVED
//! ```
//!
//! El despliegue es transaccional: validar → resolver conexiones → preparar
//! procesadores → comprobar recursos → drenar la versión anterior → activar la
//! nueva. Si la activación falla se intenta restaurar la versión previa.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};

use jaiba_core::config::FlowConfig;
use jaiba_runtime::{
    engine::{
        ConnectionResolver, FlowMetrics, FlowSupervisor, LocalPacketRepository,
        SupervisedFlowSnapshot,
    },
    error::FlowError,
};

use crate::observability::parse_and_validate;

/// Estado de ciclo de vida de una versión de flujo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FlowVersionState {
    Draft,
    Validated,
    Deployed,
    Archived,
}

/// Una versión inmutable de un flujo. El campo `source` (YAML) nunca se
/// modifica una vez creado; los cambios generan una nueva versión.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowVersion {
    pub version: u32,
    pub state: FlowVersionState,
    pub source: String,
    pub checksum: String,
    pub created_at: u64,
    #[serde(default)]
    pub validated_at: Option<u64>,
    #[serde(default)]
    pub deployed_at: Option<u64>,
    #[serde(default)]
    pub archived_at: Option<u64>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Historial de versiones de un flujo y su versión activa (desplegada).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowRecord {
    pub id: String,
    pub versions: Vec<FlowVersion>,
    #[serde(default)]
    pub active_version: Option<u32>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl FlowRecord {
    fn next_version(&self) -> u32 {
        self.versions.iter().map(|v| v.version).max().unwrap_or(0) + 1
    }

    fn version(&self, version: u32) -> Option<&FlowVersion> {
        self.versions.iter().find(|v| v.version == version)
    }

    fn version_mut(&mut self, version: u32) -> Option<&mut FlowVersion> {
        self.versions.iter_mut().find(|v| v.version == version)
    }
}

/// Errores del registro, mapeables a códigos HTTP en los handlers.
#[derive(Debug)]
pub enum RegistryError {
    NotFound(String),
    InvalidState(String),
    LimitExceeded(String),
    Validation(FlowError),
    Internal(FlowError),
}

impl RegistryError {
    pub fn message(&self) -> String {
        match self {
            RegistryError::NotFound(message) => message.clone(),
            RegistryError::InvalidState(message) => message.clone(),
            RegistryError::LimitExceeded(message) => message.clone(),
            RegistryError::Validation(error) => error.to_string(),
            RegistryError::Internal(error) => error.to_string(),
        }
    }
}

/// Instancia de un flujo en ejecución (versión activa).
struct RunningFlow {
    version: u32,
    source: String,
    supervisor: FlowSupervisor,
    metrics: FlowMetrics,
    repository: Option<LocalPacketRepository>,
}

/// Registro central de flujos: metadatos versionados + supervisores activos.
pub(crate) struct FlowRegistry {
    dir: Option<PathBuf>,
    max_concurrent: usize,
    records: RwLock<HashMap<String, FlowRecord>>,
    running: RwLock<HashMap<String, RunningFlow>>,
    deployment_lock: Mutex<()>,
}

impl FlowRegistry {
    pub fn new(dir: Option<PathBuf>, max_concurrent: usize) -> Self {
        Self {
            dir,
            max_concurrent: max_concurrent.max(1),
            records: RwLock::new(HashMap::new()),
            running: RwLock::new(HashMap::new()),
            deployment_lock: Mutex::new(()),
        }
    }

    fn registry_path(&self) -> Option<PathBuf> {
        self.dir.as_ref().map(|dir| dir.join("flows.json"))
    }

    /// Carga el registro persistido desde disco (si hay directorio de datos).
    pub async fn load(&self) -> Result<usize, FlowError> {
        let Some(path) = self.registry_path() else {
            return Ok(0);
        };
        if !path.exists() {
            return Ok(0);
        }
        let bytes = std::fs::read(&path).map_err(|error| {
            FlowError::Configuration(format!("no se pudo leer el registro de flujos: {error}"))
        })?;
        let records: Vec<FlowRecord> = serde_json::from_slice(&bytes).map_err(|error| {
            FlowError::Configuration(format!("registro de flujos corrupto: {error}"))
        })?;
        let count = records.len();
        let mut guard = self.records.write().await;
        for record in records {
            guard.insert(record.id.clone(), record);
        }
        Ok(count)
    }

    async fn persist(&self) -> Result<(), RegistryError> {
        let Some(path) = self.registry_path() else {
            return Ok(());
        };
        let snapshot: Vec<FlowRecord> = {
            let guard = self.records.read().await;
            let mut records: Vec<FlowRecord> = guard.values().cloned().collect();
            records.sort_by(|a, b| a.id.cmp(&b.id));
            records
        };
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|error| {
            RegistryError::Internal(FlowError::Server(format!(
                "no se pudo serializar el registro de flujos: {error}"
            )))
        })?;
        write_atomic(&path, &bytes).map_err(|error| {
            RegistryError::Internal(FlowError::Server(format!(
                "no se pudo persistir el registro de flujos: {error}"
            )))
        })
    }

    // --- Consultas de metadatos -------------------------------------------

    pub async fn list_records(&self) -> Vec<FlowRecord> {
        let mut records: Vec<FlowRecord> = self.records.read().await.values().cloned().collect();
        records.sort_by(|a, b| a.id.cmp(&b.id));
        records
    }

    pub async fn get_record(&self, id: &str) -> Option<FlowRecord> {
        self.records.read().await.get(id).cloned()
    }

    pub async fn export_version(&self, id: &str, version: u32) -> Option<String> {
        self.records
            .read()
            .await
            .get(id)
            .and_then(|record| record.version(version).map(|v| v.source.clone()))
    }

    // --- Transiciones de estado -------------------------------------------

    /// Importa/crea una nueva versión DRAFT a partir del YAML. El id se toma del
    /// propio YAML. Devuelve `(flow_id, version)`.
    pub async fn create_draft(
        &self,
        source: &str,
        note: Option<String>,
    ) -> Result<(String, u32), RegistryError> {
        let config: FlowConfig = serde_yaml::from_str(source)
            .map_err(|error| RegistryError::Validation(error.into()))?;
        let id = config.id.trim().to_owned();
        if id.is_empty() {
            return Err(RegistryError::Validation(FlowError::Configuration(
                "el id del flujo no puede estar vacío".to_owned(),
            )));
        }
        let now = now();
        let version = {
            let mut guard = self.records.write().await;
            let record = guard.entry(id.clone()).or_insert_with(|| FlowRecord {
                id: id.clone(),
                versions: Vec::new(),
                active_version: None,
                created_at: now,
                updated_at: now,
            });
            let version = record.next_version();
            record.versions.push(FlowVersion {
                version,
                state: FlowVersionState::Draft,
                source: source.to_owned(),
                checksum: checksum(source),
                created_at: now,
                validated_at: None,
                deployed_at: None,
                archived_at: None,
                note,
            });
            record.updated_at = now;
            version
        };
        self.persist().await?;
        Ok((id, version))
    }

    /// Valida una versión DRAFT y la marca como VALIDATED.
    pub async fn validate_version(
        &self,
        id: &str,
        version: u32,
    ) -> Result<FlowRecord, RegistryError> {
        let source = self.version_source(id, version).await?;
        parse_and_validate(&source).map_err(RegistryError::Validation)?;
        {
            let mut guard = self.records.write().await;
            let record = guard
                .get_mut(id)
                .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no encontrado")))?;
            let entry = record.version_mut(version).ok_or_else(|| {
                RegistryError::NotFound(format!("versión {version} no encontrada"))
            })?;
            if matches!(entry.state, FlowVersionState::Archived) {
                return Err(RegistryError::InvalidState(
                    "no se puede validar una versión archivada".to_owned(),
                ));
            }
            entry.state = FlowVersionState::Validated;
            entry.validated_at = Some(now());
            record.updated_at = now();
        }
        self.persist().await?;
        self.get_record(id)
            .await
            .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no encontrado")))
    }

    /// Archiva una versión concreta (no puede ser la activa en ejecución).
    pub async fn archive_version(
        &self,
        id: &str,
        version: u32,
    ) -> Result<FlowRecord, RegistryError> {
        {
            let running = self.running.read().await;
            if running.get(id).map(|entry| entry.version) == Some(version) {
                return Err(RegistryError::InvalidState(
                    "no se puede archivar la versión en ejecución; detén el flujo primero"
                        .to_owned(),
                ));
            }
        }
        {
            let mut guard = self.records.write().await;
            let record = guard
                .get_mut(id)
                .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no encontrado")))?;
            let entry = record.version_mut(version).ok_or_else(|| {
                RegistryError::NotFound(format!("versión {version} no encontrada"))
            })?;
            entry.state = FlowVersionState::Archived;
            entry.archived_at = Some(now());
            if record.active_version == Some(version) {
                record.active_version = None;
            }
            record.updated_at = now();
        }
        self.persist().await?;
        self.get_record(id)
            .await
            .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no encontrado")))
    }

    // --- Despliegue transaccional -----------------------------------------

    /// Despliegue transaccional de una versión VALIDATED (o re-despliegue de una
    /// DEPLOYED). Pasos: validar → resolver conexiones → preparar procesadores →
    /// comprobar recursos → drenar versión anterior → activar la nueva. Si la
    /// activación falla, restaura la versión anterior.
    pub async fn deploy_version(
        &self,
        id: &str,
        version: u32,
        start: bool,
        resolver: Option<Arc<dyn ConnectionResolver>>,
    ) -> Result<SupervisedFlowSnapshot, RegistryError> {
        let _deployment_guard = self.deployment_lock.lock().await;
        self.activate(id, version, start, resolver, false).await
    }

    /// Rollback: vuelve a desplegar la versión previamente desplegada más
    /// reciente (estado ARCHIVED con `deployed_at`).
    pub async fn rollback(
        &self,
        id: &str,
        start: bool,
        resolver: Option<Arc<dyn ConnectionResolver>>,
    ) -> Result<SupervisedFlowSnapshot, RegistryError> {
        let _deployment_guard = self.deployment_lock.lock().await;
        let target = {
            let guard = self.records.read().await;
            let record = guard
                .get(id)
                .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no encontrado")))?;
            let current = record.active_version;
            record
                .versions
                .iter()
                .filter(|v| {
                    v.deployed_at.is_some()
                        && Some(v.version) != current
                        && matches!(
                            v.state,
                            FlowVersionState::Archived | FlowVersionState::Deployed
                        )
                })
                .map(|v| v.version)
                .max()
                .ok_or_else(|| {
                    RegistryError::InvalidState(
                        "no hay una versión previa desplegada para hacer rollback".to_owned(),
                    )
                })?
        };
        self.activate(id, target, start, resolver, true).await
    }

    async fn activate(
        &self,
        id: &str,
        version: u32,
        start: bool,
        resolver: Option<Arc<dyn ConnectionResolver>>,
        allow_archived: bool,
    ) -> Result<SupervisedFlowSnapshot, RegistryError> {
        // 1. Recuperar y validar la versión objetivo.
        let source = self.version_source(id, version).await?;
        {
            let guard = self.records.read().await;
            let entry = guard
                .get(id)
                .and_then(|record| record.version(version))
                .ok_or_else(|| {
                    RegistryError::NotFound(format!("versión {version} no encontrada"))
                })?;
            match entry.state {
                FlowVersionState::Draft => {
                    return Err(RegistryError::InvalidState(
                        "la versión debe validarse antes de desplegar".to_owned(),
                    ));
                }
                FlowVersionState::Archived if !allow_archived => {
                    return Err(RegistryError::InvalidState(
                        "no se puede desplegar una versión archivada; usa rollback".to_owned(),
                    ));
                }
                _ => {}
            }
        }

        // 2. Validar + preparar procesadores (build en seco).
        let config = parse_and_validate(&source).map_err(RegistryError::Validation)?;

        // 3. Comprobar recursos: el cupo cuenta flujos en `running`.
        //    Con start=true se sustituye o se añade una entrada; con start=false
        //    se elimina la entrada (si existía) y no se inserta otra.
        if start {
            let running = self.running.read().await;
            let already_running = running.contains_key(id);
            if !already_running && running.len() >= self.max_concurrent {
                return Err(RegistryError::LimitExceeded(format!(
                    "límite de flujos concurrentes alcanzado ({})",
                    self.max_concurrent
                )));
            }
        }

        // 4. Preparar repositorio de persistencia si está habilitado.
        let repository = if config.engine.repository.enabled {
            match LocalPacketRepository::open(&config.engine.repository).await {
                Ok(repository) => Some(repository),
                Err(error) => return Err(RegistryError::Internal(error)),
            }
        } else {
            None
        };

        let metrics = FlowMetrics::default();
        let supervisor = FlowSupervisor::new(config, metrics.clone())
            .with_connection_resolver(resolver.clone());

        if start {
            // 5. Drenar la versión anterior en ejecución (si la hay).
            let previous = self.running.write().await.remove(id);
            if let Some(previous) = previous.as_ref()
                && let Err(error) = previous.supervisor.stop_gracefully().await
            {
                self.running
                    .write()
                    .await
                    .insert(id.to_owned(), previous_into_running(previous));
                return Err(RegistryError::Internal(error));
            }

            // 6. Activar la nueva versión; si falla, restaurar siempre la anterior.
            if let Err(error) = supervisor.start().await {
                if let Some(previous) = previous {
                    restore_previous(self, id, previous, resolver).await;
                    tracing::error!(
                        flow_id = %id,
                        %error,
                        "activación fallida; versión anterior restaurada en el mapa runtime"
                    );
                }
                return Err(RegistryError::Internal(error));
            }

            self.running.write().await.insert(
                id.to_owned(),
                RunningFlow {
                    version,
                    source: source.clone(),
                    supervisor: supervisor.clone(),
                    metrics,
                    repository,
                },
            );
        } else {
            // Despliegue sin arranque: actualiza el registro y detiene cualquier
            // instancia en ejecución. No se insertan supervisores parados en
            // `running` (evita evadir JAIBA_MAX_FLOWS y reportar fantasmas).
            if let Some(previous) = self.running.write().await.remove(id)
                && let Err(error) = previous.supervisor.stop_gracefully().await
            {
                self.running
                    .write()
                    .await
                    .insert(id.to_owned(), previous_into_running(&previous));
                return Err(RegistryError::Internal(error));
            }
        }

        // 7. Actualizar estados: la nueva es DEPLOYED, la anterior ARCHIVED.
        {
            let now = now();
            let mut guard = self.records.write().await;
            let record = guard.get_mut(id).ok_or_else(|| {
                RegistryError::Internal(FlowError::Server(format!(
                    "registro del flujo '{id}' desapareció durante el despliegue"
                )))
            })?;
            let previous_active = record.active_version;
            if let Some(previous_active) = previous_active
                && previous_active != version
                && let Some(entry) = record.version_mut(previous_active)
            {
                entry.state = FlowVersionState::Archived;
                entry.archived_at = Some(now);
            }
            if let Some(entry) = record.version_mut(version) {
                entry.state = FlowVersionState::Deployed;
                entry.deployed_at = Some(now);
            }
            record.active_version = Some(version);
            record.updated_at = now;
        }
        if let Err(error) = self.persist().await {
            tracing::error!(
                flow_id = %id,
                error = %error.message(),
                "despliegue activo pero no se pudo persistir el registro"
            );
        }

        Ok(supervisor.snapshot())
    }

    /// Detiene el flujo y lo saca del mapa `running` para liberar cupo y permitir archivar.
    pub async fn stop_and_unload(
        &self,
        id: &str,
    ) -> Result<SupervisedFlowSnapshot, RegistryError> {
        let previous = self
            .running
            .write()
            .await
            .remove(id)
            .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no está en ejecución")))?;
        match previous.supervisor.stop_gracefully().await {
            Ok(()) => Ok(previous.supervisor.snapshot()),
            Err(error) => {
                self.running
                    .write()
                    .await
                    .insert(id.to_owned(), previous_into_running(&previous));
                Err(RegistryError::Internal(error))
            }
        }
    }

    async fn version_source(&self, id: &str, version: u32) -> Result<String, RegistryError> {
        self.records
            .read()
            .await
            .get(id)
            .ok_or_else(|| RegistryError::NotFound(format!("flujo '{id}' no encontrado")))?
            .version(version)
            .map(|v| v.source.clone())
            .ok_or_else(|| RegistryError::NotFound(format!("versión {version} no encontrada")))
    }

    // --- Runtime: control y snapshots -------------------------------------

    /// Registra un supervisor ya iniciado (p. ej. el flujo servido por el CLI)
    /// como versión desplegada del registro.
    pub async fn seed_running(
        &self,
        source: &str,
        supervisor: FlowSupervisor,
        metrics: FlowMetrics,
        repository: Option<LocalPacketRepository>,
    ) -> Result<(), RegistryError> {
        let id = supervisor.flow_id().to_owned();
        let now = now();
        let version = {
            let mut guard = self.records.write().await;
            let record = guard.entry(id.clone()).or_insert_with(|| FlowRecord {
                id: id.clone(),
                versions: Vec::new(),
                active_version: None,
                created_at: now,
                updated_at: now,
            });
            let version = record.next_version();
            record.versions.push(FlowVersion {
                version,
                state: FlowVersionState::Deployed,
                source: source.to_owned(),
                checksum: checksum(source),
                created_at: now,
                validated_at: Some(now),
                deployed_at: Some(now),
                archived_at: None,
                note: Some("servido por CLI".to_owned()),
            });
            if let Some(previous) = record.active_version
                && let Some(entry) = record.version_mut(previous)
            {
                entry.state = FlowVersionState::Archived;
                entry.archived_at = Some(now);
            }
            record.active_version = Some(version);
            record.updated_at = now;
            version
        };
        self.running.write().await.insert(
            id,
            RunningFlow {
                version,
                source: source.to_owned(),
                supervisor,
                metrics,
                repository,
            },
        );
        self.persist().await
    }

    pub async fn supervisor(&self, id: &str) -> Option<FlowSupervisor> {
        self.running
            .read()
            .await
            .get(id)
            .map(|running| running.supervisor.clone())
    }

    pub async fn repository(&self, id: &str) -> Option<LocalPacketRepository> {
        self.running
            .read()
            .await
            .get(id)
            .and_then(|running| running.repository.clone())
    }

    /// Devuelve el id del único flujo en ejecución, si hay exactamente uno.
    pub async fn sole_running_id(&self) -> Option<String> {
        let running = self.running.read().await;
        if running.len() == 1 {
            running.keys().next().cloned()
        } else {
            None
        }
    }

    pub async fn snapshot(&self, id: &str) -> Option<SupervisedFlowSnapshot> {
        self.running
            .read()
            .await
            .get(id)
            .map(|running| running.supervisor.snapshot())
    }

    pub async fn snapshots(&self) -> Vec<SupervisedFlowSnapshot> {
        let mut snapshots: Vec<SupervisedFlowSnapshot> = self
            .running
            .read()
            .await
            .values()
            .map(|running| running.supervisor.snapshot())
            .collect();
        snapshots.sort_by(|a, b| a.flow_id.cmp(&b.flow_id));
        snapshots
    }

    pub async fn primary_snapshot(&self) -> Option<SupervisedFlowSnapshot> {
        self.snapshots().await.into_iter().next()
    }

    /// Texto Prometheus agregado de todos los flujos en ejecución, deduplicando
    /// las líneas `# HELP`/`# TYPE` para producir una exposición válida.
    pub async fn prometheus(&self) -> String {
        let bodies: Vec<String> = {
            let running = self.running.read().await;
            running
                .values()
                .map(|running| running.metrics.prometheus())
                .collect()
        };
        merge_prometheus(&bodies)
    }

    /// Detiene todos los flujos en ejecución (apagado coordinado del servidor).
    pub async fn stop_all(&self) {
        let supervisors: Vec<FlowSupervisor> = {
            let running = self.running.read().await;
            running
                .values()
                .map(|running| running.supervisor.clone())
                .collect()
        };
        for supervisor in supervisors {
            if let Err(error) = supervisor.stop_gracefully().await {
                tracing::error!(%error, "apagado coordinado de flujo falló");
            }
        }
    }
}

fn previous_into_running(previous: &RunningFlow) -> RunningFlow {
    RunningFlow {
        version: previous.version,
        source: previous.source.clone(),
        supervisor: previous.supervisor.clone(),
        metrics: previous.metrics.clone(),
        repository: previous.repository.clone(),
    }
}

/// Reinserta la versión anterior en `running` tras un fallo de activación.
/// Siempre deja una entrada en el mapa, aunque el re-arranque falle.
async fn restore_previous(
    registry: &FlowRegistry,
    id: &str,
    previous: RunningFlow,
    resolver: Option<Arc<dyn ConnectionResolver>>,
) {
    let supervisor = match parse_and_validate(&previous.source) {
        Ok(config) => {
            let restored =
                FlowSupervisor::new(config, previous.metrics.clone()).with_connection_resolver(resolver);
            if let Err(error) = restored.start().await {
                tracing::error!(
                    flow_id = %id,
                    %error,
                    "no se pudo rearrancar la versión anterior; queda registrada detenida"
                );
            }
            restored
        }
        Err(error) => {
            tracing::error!(
                flow_id = %id,
                %error,
                "YAML de la versión anterior ya no valida; se reinserta el supervisor detenido"
            );
            previous.supervisor.clone()
        }
    };
    registry.running.write().await.insert(
        id.to_owned(),
        RunningFlow {
            version: previous.version,
            source: previous.source,
            supervisor,
            metrics: previous.metrics,
            repository: previous.repository,
        },
    );
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn checksum(source: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Escritura atómica con permisos restringidos (0600 en Unix).
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&temporary, path)
}

/// Concatena varias exposiciones Prometheus manteniendo sólo la primera
/// aparición de cada línea `# HELP`/`# TYPE`.
fn merge_prometheus(bodies: &[String]) -> String {
    let mut output = String::new();
    let mut seen_meta: std::collections::HashSet<String> = std::collections::HashSet::new();
    for body in bodies {
        for line in body.lines() {
            if line.starts_with("# HELP ") || line.starts_with("# TYPE ") {
                if seen_meta.insert(line.to_owned()) {
                    output.push_str(line);
                    output.push('\n');
                }
            } else {
                output.push_str(line);
                output.push('\n');
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLOW: &str = r#"
id: registry-test
engine:
  admin:
    enabled: true
    authentication: none
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
"#;

    #[tokio::test]
    async fn draft_validate_deploy_lifecycle() {
        let registry = FlowRegistry::new(None, 4);
        let (id, version) = registry.create_draft(FLOW, None).await.unwrap();
        assert_eq!(version, 1);
        let record = registry.get_record(&id).await.unwrap();
        assert_eq!(record.versions[0].state, FlowVersionState::Draft);

        registry.validate_version(&id, version).await.unwrap();
        let record = registry.get_record(&id).await.unwrap();
        assert_eq!(record.versions[0].state, FlowVersionState::Validated);

        registry
            .deploy_version(&id, version, false, None)
            .await
            .unwrap();
        let record = registry.get_record(&id).await.unwrap();
        assert_eq!(record.active_version, Some(version));
        assert_eq!(record.versions[0].state, FlowVersionState::Deployed);
    }

    #[tokio::test]
    async fn deploy_requires_validation() {
        let registry = FlowRegistry::new(None, 4);
        let (id, version) = registry.create_draft(FLOW, None).await.unwrap();
        let error = registry
            .deploy_version(&id, version, false, None)
            .await
            .unwrap_err();
        assert!(matches!(error, RegistryError::InvalidState(_)));
    }

    #[tokio::test]
    async fn second_version_archives_previous_on_deploy() {
        let registry = FlowRegistry::new(None, 4);
        let (id, v1) = registry.create_draft(FLOW, None).await.unwrap();
        registry.validate_version(&id, v1).await.unwrap();
        registry.deploy_version(&id, v1, false, None).await.unwrap();

        let (_, v2) = registry.create_draft(FLOW, None).await.unwrap();
        assert_eq!(v2, 2);
        registry.validate_version(&id, v2).await.unwrap();
        registry.deploy_version(&id, v2, false, None).await.unwrap();

        let record = registry.get_record(&id).await.unwrap();
        assert_eq!(record.active_version, Some(v2));
        assert_eq!(
            record.version(v1).unwrap().state,
            FlowVersionState::Archived
        );
        assert_eq!(
            record.version(v2).unwrap().state,
            FlowVersionState::Deployed
        );
    }

    #[tokio::test]
    async fn rollback_returns_to_previous_version() {
        let registry = FlowRegistry::new(None, 4);
        let (id, v1) = registry.create_draft(FLOW, None).await.unwrap();
        registry.validate_version(&id, v1).await.unwrap();
        registry.deploy_version(&id, v1, false, None).await.unwrap();
        let (_, v2) = registry.create_draft(FLOW, None).await.unwrap();
        registry.validate_version(&id, v2).await.unwrap();
        registry.deploy_version(&id, v2, false, None).await.unwrap();

        registry.rollback(&id, false, None).await.unwrap();
        let record = registry.get_record(&id).await.unwrap();
        assert_eq!(record.active_version, Some(v1));
    }

    #[tokio::test]
    async fn deploy_without_start_does_not_occupy_running_slot() {
        let registry = FlowRegistry::new(None, 1);
        let (id, version) = registry.create_draft(FLOW, None).await.unwrap();
        registry.validate_version(&id, version).await.unwrap();
        registry
            .deploy_version(&id, version, false, None)
            .await
            .unwrap();
        assert!(registry.supervisor(&id).await.is_none());
        assert_eq!(registry.snapshots().await.len(), 0);

        // Con start=false el cupo no se consume: otro flujo sí puede arrancar.
        let other = FLOW.replace("registry-test", "otro-flujo");
        let (other_id, other_version) = registry.create_draft(&other, None).await.unwrap();
        registry
            .validate_version(&other_id, other_version)
            .await
            .unwrap();
        registry
            .deploy_version(&other_id, other_version, true, None)
            .await
            .unwrap();
        assert!(registry.supervisor(&other_id).await.is_some());
    }
}
