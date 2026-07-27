//! Administrador reutilizable de conexiones.
//!
//! Los perfiles son serializables; las credenciales nunca lo son. Un perfil
//! contiene únicamente `secret_ref`, que se resuelve con un SecretStore del
//! servidor (Vault, Kubernetes, variables de entorno u otra implementación).

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
    #[error(transparent)]
    Plugin(#[from] PluginError),
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

#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn resolve(&self, reference: &str) -> Result<ConnectionSecret, ConnectionManagerError>;
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
}

pub struct ConnectionManager {
    profiles: RwLock<HashMap<String, ConnectionProfile>>,
    status: RwLock<HashMap<String, ConnectionStatus>>,
    plugins: RwLock<HashMap<ConnectionType, Arc<dyn ConnectionPlugin>>>,
    secrets: Arc<dyn SecretStore>,
    events: broadcast::Sender<ConnectionEvent>,
}

impl ConnectionManager {
    pub fn new(secrets: Arc<dyn SecretStore>) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            profiles: RwLock::new(HashMap::new()),
            status: RwLock::new(HashMap::new()),
            plugins: RwLock::new(HashMap::new()),
            secrets,
            events,
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
        profiles.insert(id.clone(), profile);
        drop(profiles);
        self.status
            .write()
            .await
            .entry(id.clone())
            .or_insert_with(|| ConnectionStatus::unknown(id.clone()));
        let _ = self
            .events
            .send(ConnectionEvent::ProfileChanged { profile_id: id });
        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<ConnectionProfile, ConnectionManagerError> {
        let profile = self
            .profiles
            .write()
            .await
            .remove(id)
            .ok_or_else(|| ConnectionManagerError::NotFound(id.to_owned()))?;
        self.status.write().await.remove(id);
        let _ = self.events.send(ConnectionEvent::ProfileDeleted {
            profile_id: id.to_owned(),
        });
        Ok(profile)
    }

    pub async fn duplicate(
        &self,
        id: &str,
        name: impl Into<String>,
    ) -> Result<ConnectionProfile, ConnectionManagerError> {
        let original = self.get(id).await?;
        self.create(
            name,
            original.connection_type,
            original.endpoint,
            original.secret_ref,
        )
        .await
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
                    message: result.message.clone(),
                };
                self.publish_status(status).await;
                Ok(result)
            }
            Err(error) => {
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
                    message: Some(error.to_string()),
                })
                .await;
                Err(error.into())
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
        let (profile, plugin, secret) = self.resolve(id).await?;
        Ok(plugin
            .list_objects(&profile.endpoint, &secret, schema)
            .await?)
    }

    pub async fn describe_object(
        &self,
        id: &str,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, ConnectionManagerError> {
        let (profile, plugin, secret) = self.resolve(id).await?;
        Ok(plugin
            .describe_object(&profile.endpoint, &secret, object)
            .await?)
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
}
