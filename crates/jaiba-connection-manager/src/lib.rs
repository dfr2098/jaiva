//! Administrador reutilizable de conexiones.
//!
//! Los perfiles son serializables; las credenciales nunca lo son. Un perfil
//! contiene únicamente `secret_ref`, que se resuelve con un SecretStore del
//! servidor (Vault, Kubernetes, variables de entorno u otra implementación).

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, CompiledQuery, ConnectionEndpoint, ConnectionPlugin, ConnectionSecret,
    ConnectionTestResult, ConnectionType, DatabaseObject, DiagnosticCheck, ObjectDescription,
    PluginError, QuerySpec,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

mod redact;
mod secure;

pub use redact::redact_sensitive;
pub use secure::{
    EncryptedFileSecretStore, FileAuditSink, FileProfileRepository, SecureStoreError,
};

#[derive(Debug, Error)]
pub enum ConnectionManagerError {
    #[error("connection profile '{0}' was not found")]
    NotFound(String),
    #[error("connection profile name '{0}' already exists")]
    DuplicateName(String),
    #[error("no plugin is registered for connection type {0:?}")]
    MissingPlugin(ConnectionType),
    #[error("secret '{0}' is unavailable")]
    SecretUnavailable(String),
    #[error("metadata exploration timed out after {0} ms")]
    MetadataTimeout(u64),
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error(transparent)]
    Plugin(#[from] PluginError),
}

impl ConnectionManagerError {
    /// Mensaje seguro para respuestas HTTP / status al cliente.
    pub fn client_message(&self) -> String {
        match self {
            Self::NotFound(id) => format!("connection profile '{id}' was not found"),
            Self::DuplicateName(name) => {
                format!("connection profile name '{name}' already exists")
            }
            Self::MissingPlugin(kind) => {
                format!("no plugin is registered for connection type {kind:?}")
            }
            Self::SecretUnavailable(_) => "secret is unavailable".to_owned(),
            Self::MetadataTimeout(ms) => {
                format!("metadata exploration timed out after {ms} ms")
            }
            Self::Persistence(message) => {
                format!("persistence error: {}", redact_sensitive(message))
            }
            Self::Plugin(error) => redact_sensitive(&error.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub connection_type: ConnectionType,
    pub endpoint: ConnectionEndpoint,
    pub secret_ref: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStatus {
    pub profile_id: String,
    pub availability: Availability,
    pub latency_ms: Option<u64>,
    pub version: Option<String>,
    pub pool_active: Option<u32>,
    pub pool_maximum: Option<u32>,
    pub tested_at: Option<i64>,
    pub message: Option<String>,
}

impl ConnectionStatus {
    fn unknown(profile_id: String) -> Self {
        Self {
            profile_id,
            availability: Availability::Unknown,
            latency_ms: None,
            version: None,
            pool_active: None,
            pool_maximum: None,
            tested_at: None,
            message: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ConnectionEvent {
    ProfileChanged { profile_id: String },
    ProfileDeleted { profile_id: String },
    StatusChanged { status: ConnectionStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionExport {
    pub format: String,
    pub profiles: Vec<ConnectionProfile>,
}

/// Almacén de secretos. `resolve` obtiene credenciales por referencia; `store`
/// y `remove` permiten que el servidor persista o elimine credenciales sin
/// exponerlas nunca en los perfiles.
#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn resolve(&self, reference: &str) -> Result<ConnectionSecret, ConnectionManagerError>;
    async fn store(
        &self,
        reference: &str,
        secret: ConnectionSecret,
    ) -> Result<(), ConnectionManagerError>;
    async fn remove(&self, reference: &str) -> Result<(), ConnectionManagerError>;
}

/// Development-only secret provider. It never serializes its in-memory map.
#[derive(Default)]
pub struct InMemorySecretStore {
    secrets: RwLock<HashMap<String, ConnectionSecret>>,
}

impl InMemorySecretStore {
    pub async fn insert(&self, reference: impl Into<String>, secret: ConnectionSecret) {
        self.secrets.write().await.insert(reference.into(), secret);
    }
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn resolve(&self, reference: &str) -> Result<ConnectionSecret, ConnectionManagerError> {
        self.secrets
            .read()
            .await
            .get(reference)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::SecretUnavailable(reference.to_owned()))
    }

    async fn store(
        &self,
        reference: &str,
        secret: ConnectionSecret,
    ) -> Result<(), ConnectionManagerError> {
        self.secrets
            .write()
            .await
            .insert(reference.to_owned(), secret);
        Ok(())
    }

    async fn remove(&self, reference: &str) -> Result<(), ConnectionManagerError> {
        self.secrets.write().await.remove(reference);
        Ok(())
    }
}

/// Repositorio para persistir perfiles de conexión (sin credenciales).
#[async_trait]
pub trait ProfileRepository: Send + Sync {
    async fn load(&self) -> Result<Vec<ConnectionProfile>, ConnectionManagerError>;
    async fn save(&self, profiles: &[ConnectionProfile]) -> Result<(), ConnectionManagerError>;
}

/// Acción auditada sobre un perfil de conexión.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    Created,
    Updated,
    Deleted,
    KeyRotated,
    Tested,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: i64,
    pub action: AuditAction,
    pub profile_id: String,
    pub profile_name: Option<String>,
    pub actor: Option<String>,
}

/// Destino de auditoría para operaciones administrativas.
#[async_trait]
pub trait AuditSink: Send + Sync {
    async fn record(&self, entry: AuditEntry);
}

pub struct ConnectionManager {
    profiles: RwLock<HashMap<String, ConnectionProfile>>,
    status: RwLock<HashMap<String, ConnectionStatus>>,
    plugins: RwLock<HashMap<ConnectionType, Arc<dyn ConnectionPlugin>>>,
    secrets: Arc<dyn SecretStore>,
    events: broadcast::Sender<ConnectionEvent>,
    metadata_cache: RwLock<HashMap<String, (Instant, Vec<DatabaseObject>)>>,
    description_cache: RwLock<HashMap<String, (Instant, ObjectDescription)>>,
    metadata_ttl: Duration,
    metadata_timeout: Duration,
    persistence: Option<Arc<dyn ProfileRepository>>,
    audit: Option<Arc<dyn AuditSink>>,
}

impl ConnectionManager {
    pub fn new(secrets: Arc<dyn SecretStore>) -> Self {
        Self::with_metadata_policy(secrets, Duration::from_secs(60), Duration::from_secs(8))
    }

    pub fn with_metadata_policy(
        secrets: Arc<dyn SecretStore>,
        metadata_ttl: Duration,
        metadata_timeout: Duration,
    ) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            profiles: RwLock::new(HashMap::new()),
            status: RwLock::new(HashMap::new()),
            plugins: RwLock::new(HashMap::new()),
            secrets,
            events,
            metadata_cache: RwLock::new(HashMap::new()),
            description_cache: RwLock::new(HashMap::new()),
            metadata_ttl,
            metadata_timeout,
            persistence: None,
            audit: None,
        }
    }

    /// Activa la persistencia de perfiles. Combínalo con `load_persisted` al
    /// arrancar para restaurar los perfiles guardados.
    pub fn with_persistence(mut self, repository: Arc<dyn ProfileRepository>) -> Self {
        self.persistence = Some(repository);
        self
    }

    /// Registra un destino de auditoría para altas, ediciones y bajas.
    pub fn with_audit(mut self, audit: Arc<dyn AuditSink>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Restaura los perfiles persistidos (sin credenciales). Las credenciales
    /// se resuelven bajo demanda desde el `SecretStore`.
    pub async fn load_persisted(&self) -> Result<usize, ConnectionManagerError> {
        let Some(repository) = self.persistence.as_ref() else {
            return Ok(0);
        };
        let loaded = repository.load().await?;
        let count = loaded.len();
        let mut profiles = self.profiles.write().await;
        let mut status = self.status.write().await;
        for profile in loaded {
            let id = profile.id.clone();
            status
                .entry(id.clone())
                .or_insert_with(|| ConnectionStatus::unknown(id.clone()));
            profiles.insert(id, profile);
        }
        Ok(count)
    }

    async fn persist_profiles(
        &self,
        profiles: &HashMap<String, ConnectionProfile>,
    ) -> Result<(), ConnectionManagerError> {
        if let Some(repository) = self.persistence.as_ref() {
            let mut snapshot: Vec<_> = profiles.values().cloned().collect();
            snapshot.sort_by(|left, right| left.name.cmp(&right.name));
            repository.save(&snapshot).await?;
        }
        Ok(())
    }

    async fn audit(
        &self,
        action: AuditAction,
        profile_id: &str,
        profile_name: Option<&str>,
        actor: Option<&str>,
    ) {
        if let Some(sink) = self.audit.as_ref() {
            sink.record(AuditEntry {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
                action,
                profile_id: profile_id.to_owned(),
                profile_name: profile_name.map(str::to_owned),
                actor: actor.map(str::to_owned),
            })
            .await;
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.events.subscribe()
    }

    pub async fn register_plugin(&self, plugin: Arc<dyn ConnectionPlugin>) {
        self.plugins
            .write()
            .await
            .insert(plugin.connection_type(), plugin);
    }

    /// Returns the installed adapters and their declared capabilities. The API
    /// and UI consume this catalog instead of maintaining engine allowlists.
    pub async fn adapters(&self) -> Vec<(ConnectionType, jaiba_plugin_sdk::PluginDescriptor)> {
        let plugins = self.plugins.read().await;
        let mut adapters = plugins
            .iter()
            .map(|(connection_type, plugin)| (connection_type.clone(), plugin.descriptor()))
            .collect::<Vec<_>>();
        adapters.sort_by(|left, right| left.1.display_name.cmp(&right.1.display_name));
        adapters
    }

    pub async fn supports(&self, connection_type: &ConnectionType) -> bool {
        self.plugins.read().await.contains_key(connection_type)
    }

    pub async fn list(&self) -> Vec<ConnectionProfile> {
        let mut profiles: Vec<_> = self.profiles.read().await.values().cloned().collect();
        profiles.sort_by(|left, right| left.name.cmp(&right.name));
        profiles
    }

    pub async fn get(&self, id: &str) -> Result<ConnectionProfile, ConnectionManagerError> {
        self.profiles
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::NotFound(id.to_owned()))
    }

    pub async fn create(
        &self,
        name: impl Into<String>,
        connection_type: ConnectionType,
        endpoint: ConnectionEndpoint,
        secret_ref: impl Into<String>,
    ) -> Result<ConnectionProfile, ConnectionManagerError> {
        let profile = ConnectionProfile {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            connection_type,
            endpoint,
            secret_ref: secret_ref.into(),
            tags: Vec::new(),
        };
        self.upsert(profile.clone(), false).await?;
        Ok(profile)
    }

    pub async fn update(&self, profile: ConnectionProfile) -> Result<(), ConnectionManagerError> {
        if !self.profiles.read().await.contains_key(&profile.id) {
            return Err(ConnectionManagerError::NotFound(profile.id));
        }
        self.upsert(profile, true).await
    }

    async fn upsert(
        &self,
        profile: ConnectionProfile,
        replacing: bool,
    ) -> Result<(), ConnectionManagerError> {
        let mut profiles = self.profiles.write().await;
        if profiles.values().any(|candidate| {
            candidate.name.eq_ignore_ascii_case(&profile.name) && candidate.id != profile.id
        }) {
            return Err(ConnectionManagerError::DuplicateName(profile.name));
        }
        if !replacing && profiles.contains_key(&profile.id) {
            return Err(ConnectionManagerError::DuplicateName(profile.name));
        }
        let id = profile.id.clone();
        let name = profile.name.clone();
        let mut pending = profiles.clone();
        pending.insert(id.clone(), profile);
        self.persist_profiles(&pending).await?;
        *profiles = pending;
        drop(profiles);
        self.status
            .write()
            .await
            .entry(id.clone())
            .or_insert_with(|| ConnectionStatus::unknown(id.clone()));
        self.audit(
            if replacing {
                AuditAction::Updated
            } else {
                AuditAction::Created
            },
            &id,
            Some(&name),
            Some("api"),
        )
        .await;
        let _ = self.events.send(ConnectionEvent::ProfileChanged {
            profile_id: id.clone(),
        });
        self.invalidate_metadata(&id).await;
        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<ConnectionProfile, ConnectionManagerError> {
        let mut profiles = self.profiles.write().await;
        let profile = profiles
            .get(id)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::NotFound(id.to_owned()))?;
        let mut pending = profiles.clone();
        pending.remove(id);
        self.persist_profiles(&pending).await?;
        *profiles = pending;
        drop(profiles);
        self.status.write().await.remove(id);
        self.invalidate_metadata(id).await;
        self.audit(AuditAction::Deleted, id, Some(&profile.name), Some("api"))
            .await;
        let _ = self.events.send(ConnectionEvent::ProfileDeleted {
            profile_id: id.to_owned(),
        });
        Ok(profile)
    }

    /// Duplica el perfil y **clona** el secreto a una nueva `secret_ref`.
    /// Así borrar un perfil no invalida las credenciales del otro.
    pub async fn duplicate(
        &self,
        id: &str,
        name: impl Into<String>,
    ) -> Result<ConnectionProfile, ConnectionManagerError> {
        let original = self.get(id).await?;
        let secret = self.secrets.resolve(&original.secret_ref).await?;
        let secret_ref = format!("secret://connection/{}", Uuid::new_v4());
        self.secrets.store(&secret_ref, secret).await?;
        match self
            .create(
                name,
                original.connection_type,
                original.endpoint,
                secret_ref.clone(),
            )
            .await
        {
            Ok(profile) => Ok(profile),
            Err(error) => {
                if let Err(rollback) = self.secrets.remove(&secret_ref).await {
                    return Err(ConnectionManagerError::Persistence(format!(
                        "no se pudo crear el perfil ({error}) ni eliminar el secreto provisional ({rollback})"
                    )));
                }
                Err(error)
            }
        }
    }

    /// Actualiza perfil y credenciales como una sola operación lógica.
    pub async fn update_with_secret(
        &self,
        profile: ConnectionProfile,
        secret: ConnectionSecret,
    ) -> Result<(), ConnectionManagerError> {
        let previous = self.secrets.resolve(&profile.secret_ref).await?;
        self.secrets.store(&profile.secret_ref, secret).await?;
        if let Err(error) = self.update(profile.clone()).await {
            if let Err(rollback) = self.secrets.store(&profile.secret_ref, previous).await {
                return Err(ConnectionManagerError::Persistence(format!(
                    "no se pudo actualizar el perfil ({error}) ni restaurar su secreto ({rollback})"
                )));
            }
            return Err(error);
        }
        Ok(())
    }

    /// Elimina perfil y secreto sin publicar una baja parcial.
    pub async fn delete_with_secret(
        &self,
        id: &str,
    ) -> Result<ConnectionProfile, ConnectionManagerError> {
        let mut profiles = self.profiles.write().await;
        let profile = profiles
            .get(id)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::NotFound(id.to_owned()))?;
        let secret_is_shared = profiles
            .values()
            .any(|candidate| candidate.id != id && candidate.secret_ref == profile.secret_ref);
        if !secret_is_shared {
            self.secrets.resolve(&profile.secret_ref).await?;
        }

        let mut pending = profiles.clone();
        pending.remove(id);
        self.persist_profiles(&pending).await?;
        if !secret_is_shared && let Err(error) = self.secrets.remove(&profile.secret_ref).await {
            if let Err(rollback) = self.persist_profiles(&profiles).await {
                *profiles = pending;
                return Err(ConnectionManagerError::Persistence(format!(
                    "no se pudo eliminar el secreto ({error}) ni revertir la baja ({rollback})"
                )));
            }
            return Err(error);
        }
        *profiles = pending;
        drop(profiles);
        self.status.write().await.remove(id);
        self.invalidate_metadata(id).await;
        self.audit(AuditAction::Deleted, id, Some(&profile.name), Some("api"))
            .await;
        let _ = self.events.send(ConnectionEvent::ProfileDeleted {
            profile_id: id.to_owned(),
        });
        Ok(profile)
    }

    pub async fn export(&self) -> ConnectionExport {
        ConnectionExport {
            format: "jaiba.connections/v1".to_owned(),
            profiles: self.list().await,
        }
    }

    pub async fn import(
        &self,
        export: ConnectionExport,
    ) -> Result<Vec<ConnectionProfile>, ConnectionManagerError> {
        let mut imported = Vec::new();
        for mut profile in export.profiles {
            if profile.id.is_empty() {
                profile.id = Uuid::new_v4().to_string();
            }
            self.upsert(profile.clone(), false).await?;
            imported.push(profile);
        }
        Ok(imported)
    }

    pub async fn status(&self, id: &str) -> Result<ConnectionStatus, ConnectionManagerError> {
        self.status
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::NotFound(id.to_owned()))
    }

    pub async fn test(&self, id: &str) -> Result<ConnectionTestResult, ConnectionManagerError> {
        let profile = self.get(id).await?;
        let plugin = self.plugin(&profile.connection_type).await?;
        let secret = self.secrets.resolve(&profile.secret_ref).await?;
        self.set_testing(id).await;
        let result = plugin.test(&profile.endpoint, &secret).await;
        match result {
            Ok(result) => {
                let status = ConnectionStatus {
                    profile_id: id.to_owned(),
                    availability: result.availability,
                    latency_ms: Some(result.latency_ms),
                    version: result.version.clone(),
                    pool_active: result.pool.as_ref().map(|pool| pool.active),
                    pool_maximum: result.pool.as_ref().map(|pool| pool.maximum),
                    tested_at: Some(result.tested_at),
                    message: result.message.as_deref().map(redact_sensitive),
                };
                self.publish_status(status).await;
                self.audit(AuditAction::Tested, id, Some(&profile.name), Some("api"))
                    .await;
                Ok(result)
            }
            Err(error) => {
                let manager_error = ConnectionManagerError::from(error);
                tracing::warn!(
                    target: "jaiba.connections",
                    profile_id = %id,
                    error = %manager_error,
                    "connection test failed"
                );
                self.publish_status(ConnectionStatus {
                    profile_id: id.to_owned(),
                    availability: Availability::Unavailable,
                    latency_ms: None,
                    version: None,
                    pool_active: None,
                    pool_maximum: None,
                    tested_at: Some(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64,
                    ),
                    message: Some(manager_error.client_message()),
                })
                .await;
                self.audit(AuditAction::Tested, id, Some(&profile.name), Some("api"))
                    .await;
                Err(manager_error)
            }
        }
    }

    pub async fn diagnose(&self, id: &str) -> Result<Vec<DiagnosticCheck>, ConnectionManagerError> {
        let (profile, plugin, secret) = self.resolve(id).await?;
        Ok(plugin.diagnose(&profile.endpoint, &secret).await?)
    }

    pub async fn list_objects(
        &self,
        id: &str,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, ConnectionManagerError> {
        let cache_key = format!("{id}:{}", schema.unwrap_or("*"));
        if let Some((created, objects)) = self.metadata_cache.read().await.get(&cache_key)
            && created.elapsed() < self.metadata_ttl
        {
            return Ok(objects.clone());
        }
        let (profile, plugin, secret) = self.resolve(id).await?;
        let timeout_ms = self.metadata_timeout.as_millis() as u64;
        let objects = tokio::time::timeout(
            self.metadata_timeout,
            plugin.list_objects(&profile.endpoint, &secret, schema),
        )
        .await
        .map_err(|_| ConnectionManagerError::MetadataTimeout(timeout_ms))??;
        self.metadata_cache
            .write()
            .await
            .insert(cache_key, (Instant::now(), objects.clone()));
        Ok(objects)
    }

    pub async fn describe_object(
        &self,
        id: &str,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, ConnectionManagerError> {
        let cache_key = format!(
            "{id}:{}:{}",
            object.schema.as_deref().unwrap_or(""),
            object.name
        );
        if let Some((created, description)) = self.description_cache.read().await.get(&cache_key)
            && created.elapsed() < self.metadata_ttl
        {
            return Ok(description.clone());
        }
        let (profile, plugin, secret) = self.resolve(id).await?;
        let timeout_ms = self.metadata_timeout.as_millis() as u64;
        let description = tokio::time::timeout(
            self.metadata_timeout,
            plugin.describe_object(&profile.endpoint, &secret, object),
        )
        .await
        .map_err(|_| ConnectionManagerError::MetadataTimeout(timeout_ms))??;
        self.description_cache
            .write()
            .await
            .insert(cache_key, (Instant::now(), description.clone()));
        Ok(description)
    }

    pub async fn compile_query(
        &self,
        id: &str,
        query: &QuerySpec,
    ) -> Result<CompiledQuery, ConnectionManagerError> {
        let profile = self.get(id).await?;
        Ok(self
            .plugin(&profile.connection_type)
            .await?
            .compile_query(query)?)
    }

    async fn resolve(
        &self,
        id: &str,
    ) -> Result<
        (
            ConnectionProfile,
            Arc<dyn ConnectionPlugin>,
            ConnectionSecret,
        ),
        ConnectionManagerError,
    > {
        let profile = self.get(id).await?;
        let plugin = self.plugin(&profile.connection_type).await?;
        let secret = self.secrets.resolve(&profile.secret_ref).await?;
        Ok((profile, plugin, secret))
    }

    async fn plugin(
        &self,
        connection_type: &ConnectionType,
    ) -> Result<Arc<dyn ConnectionPlugin>, ConnectionManagerError> {
        self.plugins
            .read()
            .await
            .get(connection_type)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::MissingPlugin(connection_type.clone()))
    }

    async fn invalidate_metadata(&self, id: &str) {
        let prefix = format!("{id}:");
        self.metadata_cache
            .write()
            .await
            .retain(|key, _| !key.starts_with(&prefix));
        self.description_cache
            .write()
            .await
            .retain(|key, _| !key.starts_with(&prefix));
    }

    async fn set_testing(&self, id: &str) {
        let current = self
            .status(id)
            .await
            .unwrap_or_else(|_| ConnectionStatus::unknown(id.to_owned()));
        self.publish_status(ConnectionStatus {
            availability: Availability::Testing,
            ..current
        })
        .await;
    }

    async fn publish_status(&self, status: ConnectionStatus) {
        self.status
            .write()
            .await
            .insert(status.profile_id.clone(), status.clone());
        let _ = self.events.send(ConnectionEvent::StatusChanged { status });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jaiba_plugin_sdk::{DatabaseObjectKind, PluginDescriptor};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Default)]
    struct ToggleRepository {
        profiles: RwLock<Vec<ConnectionProfile>>,
        fail_saves: AtomicBool,
    }

    #[async_trait]
    impl ProfileRepository for ToggleRepository {
        async fn load(&self) -> Result<Vec<ConnectionProfile>, ConnectionManagerError> {
            Ok(self.profiles.read().await.clone())
        }

        async fn save(&self, profiles: &[ConnectionProfile]) -> Result<(), ConnectionManagerError> {
            if self.fail_saves.load(Ordering::SeqCst) {
                return Err(ConnectionManagerError::Persistence(
                    "simulated repository failure".to_owned(),
                ));
            }
            *self.profiles.write().await = profiles.to_vec();
            Ok(())
        }
    }

    struct SQLiteAdapter;

    #[async_trait]
    impl ConnectionPlugin for SQLiteAdapter {
        fn descriptor(&self) -> PluginDescriptor {
            PluginDescriptor {
                id: "example.sqlite".to_owned(),
                version: "1.0.0".to_owned(),
                display_name: "SQLite".to_owned(),
                category: "SQL".to_owned(),
                default_port: 0,
                capabilities: vec!["test".to_owned(), "diagnostics".to_owned()],
            }
        }

        fn connection_type(&self) -> ConnectionType {
            ConnectionType::Custom("sqlite".to_owned())
        }

        async fn test(
            &self,
            _endpoint: &ConnectionEndpoint,
            _secret: &ConnectionSecret,
        ) -> Result<ConnectionTestResult, PluginError> {
            Ok(ConnectionTestResult {
                availability: Availability::Available,
                latency_ms: 1,
                version: Some("3".to_owned()),
                pool: None,
                tested_at: 1,
                message: None,
            })
        }

        async fn diagnose(
            &self,
            _endpoint: &ConnectionEndpoint,
            _secret: &ConnectionSecret,
        ) -> Result<Vec<DiagnosticCheck>, PluginError> {
            Ok(vec![DiagnosticCheck {
                code: "open".to_owned(),
                label: "Abrir archivo".to_owned(),
                status: Availability::Available,
                latency_ms: Some(1),
                details: json!({}),
            }])
        }

        async fn list_objects(
            &self,
            _endpoint: &ConnectionEndpoint,
            _secret: &ConnectionSecret,
            _schema: Option<&str>,
        ) -> Result<Vec<DatabaseObject>, PluginError> {
            Ok(vec![])
        }

        async fn describe_object(
            &self,
            _endpoint: &ConnectionEndpoint,
            _secret: &ConnectionSecret,
            object: &DatabaseObject,
        ) -> Result<ObjectDescription, PluginError> {
            Ok(ObjectDescription {
                object: DatabaseObject {
                    schema: object.schema.clone(),
                    name: object.name.clone(),
                    kind: DatabaseObjectKind::Table,
                },
                columns: vec![],
                keys: vec![],
                indexes: vec![],
            })
        }

        fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
            Err(PluginError::Unsupported("query_builder".to_owned()))
        }
    }

    fn endpoint() -> ConnectionEndpoint {
        ConnectionEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 5432,
            database: Some("dma".to_owned()),
            ssl: false,
            pool_min: 1,
            pool_max: 10,
            timeout_ms: 5_000,
            options: Default::default(),
        }
    }

    fn secret(password: &str) -> ConnectionSecret {
        ConnectionSecret {
            username: "dma".to_owned(),
            password: password.to_owned(),
            options: Default::default(),
        }
    }

    #[tokio::test]
    async fn profiles_export_without_credentials() {
        let secrets = Arc::new(InMemorySecretStore::default());
        secrets
            .insert(
                "memory://postgres",
                ConnectionSecret {
                    username: "private-user".to_owned(),
                    password: "private-password".to_owned(),
                    options: Default::default(),
                },
            )
            .await;
        let manager = ConnectionManager::new(secrets);
        manager
            .create(
                "PostgreSQL DMA",
                ConnectionType::Postgres,
                endpoint(),
                "memory://postgres",
            )
            .await
            .unwrap();
        let json = serde_json::to_string(&manager.export().await).unwrap();
        assert!(json.contains("memory://postgres"));
        assert!(!json.contains("private-password"));
        assert!(!json.contains("private-user"));
    }

    #[tokio::test]
    async fn names_are_unique_case_insensitively() {
        let manager = ConnectionManager::new(Arc::new(InMemorySecretStore::default()));
        manager
            .create(
                "Oracle Producción",
                ConnectionType::Oracle,
                endpoint(),
                "vault://oracle",
            )
            .await
            .unwrap();
        assert!(
            manager
                .create(
                    "oracle producción",
                    ConnectionType::Oracle,
                    endpoint(),
                    "vault://other",
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn failed_profile_persistence_does_not_publish_partial_state() {
        let repository = Arc::new(ToggleRepository::default());
        repository.fail_saves.store(true, Ordering::SeqCst);
        let manager = ConnectionManager::new(Arc::new(InMemorySecretStore::default()))
            .with_persistence(repository);

        let result = manager
            .create(
                "Oracle",
                ConnectionType::Oracle,
                endpoint(),
                "memory://oracle",
            )
            .await;

        assert!(matches!(
            result,
            Err(ConnectionManagerError::Persistence(_))
        ));
        assert!(manager.list().await.is_empty());
    }

    #[tokio::test]
    async fn rejected_profile_update_restores_previous_secret() {
        let repository = Arc::new(ToggleRepository::default());
        let secrets = Arc::new(InMemorySecretStore::default());
        secrets.insert("memory://postgres", secret("old")).await;
        let manager = ConnectionManager::new(secrets.clone()).with_persistence(repository.clone());
        let mut profile = manager
            .create(
                "PostgreSQL",
                ConnectionType::Postgres,
                endpoint(),
                "memory://postgres",
            )
            .await
            .unwrap();

        repository.fail_saves.store(true, Ordering::SeqCst);
        profile.name = "PostgreSQL actualizado".to_owned();
        let result = manager
            .update_with_secret(profile.clone(), secret("new"))
            .await;

        assert!(matches!(
            result,
            Err(ConnectionManagerError::Persistence(_))
        ));
        assert_eq!(manager.get(&profile.id).await.unwrap().name, "PostgreSQL");
        assert_eq!(
            secrets.resolve("memory://postgres").await.unwrap().password,
            "old"
        );
    }

    #[tokio::test]
    async fn a_new_adapter_is_discovered_and_diagnosed_without_core_changes() {
        let secrets = Arc::new(InMemorySecretStore::default());
        secrets
            .insert(
                "memory://sqlite",
                ConnectionSecret {
                    username: "unused".to_owned(),
                    password: "unused".to_owned(),
                    options: Default::default(),
                },
            )
            .await;
        let manager = ConnectionManager::new(secrets);
        manager.register_plugin(Arc::new(SQLiteAdapter)).await;

        let adapter_type = ConnectionType::Custom("sqlite".to_owned());
        assert!(manager.supports(&adapter_type).await);
        assert_eq!(manager.adapters().await[0].1.display_name, "SQLite");

        let profile = manager
            .create("SQLite local", adapter_type, endpoint(), "memory://sqlite")
            .await
            .unwrap();
        assert_eq!(manager.diagnose(&profile.id).await.unwrap()[0].code, "open");
    }
}
