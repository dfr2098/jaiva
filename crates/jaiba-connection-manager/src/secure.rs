//! Persistencia segura: secretos cifrados con AES-256-GCM, perfiles en disco y
//! auditoría en formato JSON Lines.
//!
//! La clave maestra se deriva de una variable de entorno (SHA-256 sobre el
//! valor) y nunca se guarda en disco. El fichero de secretos almacena solo un
//! `key_id` (huella de la clave) para detectar el uso de una clave equivocada.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use jaiba_plugin_sdk::ConnectionSecret;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    AuditEntry, AuditSink, ConnectionManagerError, ConnectionProfile, ProfileRepository,
    SecretStore,
};

const SECRET_FILE_VERSION: u8 = 1;
const NONCE_LEN: usize = 12;

#[derive(Debug, Error)]
pub enum SecureStoreError {
    #[error("error de entrada/salida en '{path}': {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("fallo criptográfico ({0})")]
    Crypto(&'static str),
    #[error("los secretos guardados se cifraron con otra clave maestra")]
    WrongKey,
    #[error("almacén de secretos corrupto: {0}")]
    Corrupt(String),
    #[error("fuente de aleatoriedad no disponible: {0}")]
    Random(String),
}

impl From<SecureStoreError> for ConnectionManagerError {
    fn from(error: SecureStoreError) -> Self {
        ConnectionManagerError::Persistence(error.to_string())
    }
}

/// Deriva una clave AES-256 de 32 bytes a partir del valor de la variable de
/// entorno de la clave maestra.
fn derive_key(master_key: &str) -> [u8; 32] {
    let digest = Sha256::digest(master_key.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

/// Huella pública de una clave (no revela la clave). Sirve para saber qué clave
/// está activa y para detectar rotaciones o claves equivocadas.
fn key_id(key: &[u8; 32]) -> String {
    let digest = Sha256::digest(key);
    hex_prefix(&digest, 8)
}

/// Identificador de la clave maestra configurada, útil para logs y diagnóstico.
pub fn master_key_id(master_key: &str) -> String {
    key_id(&derive_key(master_key))
}

fn hex_prefix(bytes: &[u8], take: usize) -> String {
    bytes
        .iter()
        .take(take)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Serialize, Deserialize)]
struct SecretPayload {
    username: String,
    password: String,
    #[serde(default)]
    options: BTreeMap<String, String>,
}

impl From<&ConnectionSecret> for SecretPayload {
    fn from(secret: &ConnectionSecret) -> Self {
        Self {
            username: secret.username.clone(),
            password: secret.password.clone(),
            options: secret.options.clone(),
        }
    }
}

impl From<SecretPayload> for ConnectionSecret {
    fn from(payload: SecretPayload) -> Self {
        ConnectionSecret {
            username: payload.username,
            password: payload.password,
            options: payload.options,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SecretFile {
    version: u8,
    key_id: String,
    entries: BTreeMap<String, String>,
}

struct Inner {
    key: [u8; 32],
    key_id: String,
    secrets: HashMap<String, ConnectionSecret>,
}

/// Almacén de secretos cifrado con AES-256-GCM y respaldado en disco.
pub struct EncryptedFileSecretStore {
    path: PathBuf,
    inner: RwLock<Inner>,
}

impl EncryptedFileSecretStore {
    /// Abre (o inicializa) el almacén cifrado. Si el fichero ya existe, valida
    /// que la clave maestra sea la misma con la que se cifró.
    pub fn open(path: impl Into<PathBuf>, master_key: &str) -> Result<Self, SecureStoreError> {
        let path = path.into();
        let key = derive_key(master_key);
        let identifier = key_id(&key);
        let secrets = if path.exists() {
            let raw = fs::read(&path).map_err(|source| SecureStoreError::Io {
                path: path.display().to_string(),
                source,
            })?;
            let file: SecretFile = serde_json::from_slice(&raw)
                .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
            if file.key_id != identifier {
                return Err(SecureStoreError::WrongKey);
            }
            let mut secrets = HashMap::with_capacity(file.entries.len());
            for (reference, blob) in file.entries {
                let plaintext = decrypt(&key, &blob)?;
                let payload: SecretPayload = serde_json::from_slice(&plaintext)
                    .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
                secrets.insert(reference, payload.into());
            }
            secrets
        } else {
            HashMap::new()
        };
        Ok(Self {
            path,
            inner: RwLock::new(Inner {
                key,
                key_id: identifier,
                secrets,
            }),
        })
    }

    /// Rota la clave maestra: descifra con la clave actual (ya en memoria) y
    /// vuelve a cifrar todos los secretos con la nueva clave.
    pub async fn rotate_key(&self, new_master_key: &str) -> Result<String, SecureStoreError> {
        let mut inner = self.inner.write().await;
        inner.key = derive_key(new_master_key);
        inner.key_id = key_id(&inner.key);
        self.write_file(&inner)?;
        Ok(inner.key_id.clone())
    }

    /// Huella de la clave activa.
    pub async fn active_key_id(&self) -> String {
        self.inner.read().await.key_id.clone()
    }

    fn write_file(&self, inner: &Inner) -> Result<(), SecureStoreError> {
        let mut entries = BTreeMap::new();
        for (reference, secret) in &inner.secrets {
            let payload = SecretPayload::from(secret);
            let plaintext = serde_json::to_vec(&payload)
                .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
            entries.insert(reference.clone(), encrypt(&inner.key, &plaintext)?);
        }
        let file = SecretFile {
            version: SECRET_FILE_VERSION,
            key_id: inner.key_id.clone(),
            entries,
        };
        let serialized = serde_json::to_vec_pretty(&file)
            .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
        write_private(&self.path, &serialized)
    }
}

#[async_trait]
impl SecretStore for EncryptedFileSecretStore {
    async fn resolve(&self, reference: &str) -> Result<ConnectionSecret, ConnectionManagerError> {
        self.inner
            .read()
            .await
            .secrets
            .get(reference)
            .cloned()
            .ok_or_else(|| ConnectionManagerError::SecretUnavailable(reference.to_owned()))
    }

    async fn store(
        &self,
        reference: &str,
        secret: ConnectionSecret,
    ) -> Result<(), ConnectionManagerError> {
        let mut inner = self.inner.write().await;
        inner.secrets.insert(reference.to_owned(), secret);
        self.write_file(&inner)?;
        Ok(())
    }

    async fn remove(&self, reference: &str) -> Result<(), ConnectionManagerError> {
        let mut inner = self.inner.write().await;
        if inner.secrets.remove(reference).is_some() {
            self.write_file(&inner)?;
        }
        Ok(())
    }
}

fn encrypt(key: &[u8; 32], plaintext: &[u8]) -> Result<String, SecureStoreError> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| SecureStoreError::Crypto("key"))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce_bytes).map_err(|error| SecureStoreError::Random(error.to_string()))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| SecureStoreError::Crypto("encrypt"))?;
    let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    blob.extend_from_slice(&nonce_bytes);
    blob.extend_from_slice(&ciphertext);
    Ok(STANDARD.encode(blob))
}

fn decrypt(key: &[u8; 32], blob_base64: &str) -> Result<Vec<u8>, SecureStoreError> {
    let blob = STANDARD
        .decode(blob_base64)
        .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
    if blob.len() <= NONCE_LEN {
        return Err(SecureStoreError::Corrupt("bloque cifrado demasiado corto".to_owned()));
    }
    let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| SecureStoreError::Crypto("key"))?;
    cipher
        .decrypt(Nonce::from_slice(nonce_bytes), ciphertext)
        .map_err(|_| SecureStoreError::Crypto("decrypt"))
}

/// Repositorio de perfiles en un fichero JSON. Nunca contiene credenciales.
pub struct FileProfileRepository {
    path: PathBuf,
}

impl FileProfileRepository {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl ProfileRepository for FileProfileRepository {
    async fn load(&self) -> Result<Vec<ConnectionProfile>, ConnectionManagerError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let raw = fs::read(&self.path)
            .map_err(|error| ConnectionManagerError::Persistence(error.to_string()))?;
        serde_json::from_slice(&raw)
            .map_err(|error| ConnectionManagerError::Persistence(error.to_string()))
    }

    async fn save(&self, profiles: &[ConnectionProfile]) -> Result<(), ConnectionManagerError> {
        let serialized = serde_json::to_vec_pretty(profiles)
            .map_err(|error| ConnectionManagerError::Persistence(error.to_string()))?;
        write_private(&self.path, &serialized)?;
        Ok(())
    }
}

/// Auditoría append-only en formato JSON Lines.
pub struct FileAuditSink {
    path: PathBuf,
}

impl FileAuditSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait]
impl AuditSink for FileAuditSink {
    async fn record(&self, entry: AuditEntry) {
        if let Err(error) = append_audit(&self.path, &entry) {
            tracing::warn!(target: "jaiba.audit", %error, "no se pudo registrar la auditoría");
        }
    }
}

fn append_audit(path: &Path, entry: &AuditEntry) -> Result<(), SecureStoreError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| SecureStoreError::Io {
            path: parent.display().to_string(),
            source,
        })?;
    }
    let mut line = serde_json::to_string(entry)
        .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
    line.push('\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|source| SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        })?;
    file.write_all(line.as_bytes())
        .map_err(|source| SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        })
}

/// Escribe un fichero de forma atómica-ish y con permisos restringidos (0600 en
/// Unix) para proteger secretos y perfiles.
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), SecureStoreError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| SecureStoreError::Io {
            path: parent.display().to_string(),
            source,
        })?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes).map_err(|source| SecureStoreError::Io {
        path: temporary.display().to_string(),
        source,
    })?;
    set_owner_only(&temporary)?;
    fs::rename(&temporary, path).map_err(|source| SecureStoreError::Io {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> Result<(), SecureStoreError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
        SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn set_owner_only(_path: &Path) -> Result<(), SecureStoreError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret() -> ConnectionSecret {
        ConnectionSecret {
            username: "dma".to_owned(),
            password: "s3cr3t".to_owned(),
            options: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn secrets_round_trip_encrypted_on_disk() {
        let dir = std::env::temp_dir().join(format!("jaiba-secure-{}", uuid::Uuid::new_v4()));
        let path = dir.join("secrets.enc");
        let store = EncryptedFileSecretStore::open(&path, "clave-maestra").unwrap();
        store.store("memory://a", secret()).await.unwrap();

        let raw = fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("s3cr3t"), "el fichero no debe contener el secreto en claro");
        assert!(!raw.contains("dma"));

        let reopened = EncryptedFileSecretStore::open(&path, "clave-maestra").unwrap();
        let resolved = reopened.resolve("memory://a").await.unwrap();
        assert_eq!(resolved.password, "s3cr3t");
        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn wrong_master_key_is_rejected() {
        let dir = std::env::temp_dir().join(format!("jaiba-secure-{}", uuid::Uuid::new_v4()));
        let path = dir.join("secrets.enc");
        let store = EncryptedFileSecretStore::open(&path, "clave-a").unwrap();
        store.store("memory://a", secret()).await.unwrap();
        assert!(EncryptedFileSecretStore::open(&path, "clave-b").is_err());
        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rotating_the_key_reencrypts_and_reopens() {
        let dir = std::env::temp_dir().join(format!("jaiba-secure-{}", uuid::Uuid::new_v4()));
        let path = dir.join("secrets.enc");
        let store = EncryptedFileSecretStore::open(&path, "clave-a").unwrap();
        store.store("memory://a", secret()).await.unwrap();
        store.rotate_key("clave-b").await.unwrap();

        assert!(EncryptedFileSecretStore::open(&path, "clave-a").is_err());
        let reopened = EncryptedFileSecretStore::open(&path, "clave-b").unwrap();
        assert_eq!(reopened.resolve("memory://a").await.unwrap().password, "s3cr3t");
        fs::remove_dir_all(&dir).ok();
    }
}
