//! Persistencia segura: secretos cifrados con AES-256-GCM, perfiles en disco y
//! auditoría en formato JSON Lines.
//!
//! La clave maestra se deriva con Argon2id (sal aleatoria por fichero) y nunca
//! se guarda en disco. El fichero almacena un *verifier* cifrado (canario) para
//! detectar claves equivocadas sin exponer una huella verificable offline.
//!
//! Los almacenes v1 (SHA-256 sin sal) se migran automáticamente a v2 al abrirlos.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::{Algorithm, Argon2, Params, Version};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use jaiba_plugin_sdk::ConnectionSecret;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::RwLock;
use zeroize::Zeroize;

use crate::{
    AuditEntry, AuditSink, ConnectionManagerError, ConnectionProfile, ProfileRepository,
    SecretStore,
};

const SECRET_FILE_VERSION: u8 = 2;
const LEGACY_SECRET_FILE_VERSION: u8 = 1;
const NONCE_LEN: usize = 12;
const SALT_LEN: usize = 16;
const KEY_LEN: usize = 32;
/// Parámetros OWASP 2023 para Argon2id (≈19 MiB, 2 iteraciones, 1 hilo).
const ARGON2_M_KIB: u32 = 19_456;
const ARGON2_T: u32 = 2;
const ARGON2_P: u32 = 1;
const KDF_NAME: &str = "argon2id";
const VERIFIER_PLAINTEXT: &[u8] = b"jaiba-secret-store-v2";

#[derive(Debug, Error)]
pub enum SecureStoreError {
    #[error("error de entrada/salida en '{path}': {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("fallo criptográfico ({0})")]
    Crypto(&'static str),
    #[error("derivación de clave fallida: {0}")]
    Kdf(String),
    #[error("los secretos guardados se cifraron con otra clave maestra")]
    WrongKey,
    #[error("almacén de secretos corrupto: {0}")]
    Corrupt(String),
    #[error("versión de almacén no soportada: {0}")]
    UnsupportedVersion(u8),
    #[error("fuente de aleatoriedad no disponible: {0}")]
    Random(String),
}

impl From<SecureStoreError> for ConnectionManagerError {
    fn from(error: SecureStoreError) -> Self {
        ConnectionManagerError::Persistence(error.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Argon2ParamsFile {
    m_kib: u32,
    t: u32,
    p: u32,
}

impl Default for Argon2ParamsFile {
    fn default() -> Self {
        Self {
            m_kib: ARGON2_M_KIB,
            t: ARGON2_T,
            p: ARGON2_P,
        }
    }
}

/// Deriva una clave AES-256 con Argon2id a partir de la passphrase y la sal.
fn derive_key_argon2(
    master_key: &str,
    salt: &[u8],
    params: &Argon2ParamsFile,
) -> Result<[u8; KEY_LEN], SecureStoreError> {
    let argon_params = Params::new(params.m_kib, params.t, params.p, Some(KEY_LEN))
        .map_err(|error| SecureStoreError::Kdf(error.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, argon_params);
    let mut key = [0u8; KEY_LEN];
    argon2
        .hash_password_into(master_key.as_bytes(), salt, &mut key)
        .map_err(|error| SecureStoreError::Kdf(error.to_string()))?;
    Ok(key)
}

/// Derivación legada v1 (SHA-256 sin sal). Solo para migración.
fn derive_key_sha256(master_key: &str) -> [u8; KEY_LEN] {
    let digest = Sha256::digest(master_key.as_bytes());
    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&digest);
    key
}

/// Huella de la clave derivada para logs (no se persiste en el fichero).
fn fingerprint(key: &[u8; KEY_LEN]) -> String {
    let digest = Sha256::digest(key);
    hex_prefix(&digest, 8)
}

fn hex_prefix(bytes: &[u8], take: usize) -> String {
    bytes
        .iter()
        .take(take)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn random_bytes(len: usize) -> Result<Vec<u8>, SecureStoreError> {
    let mut buf = vec![0u8; len];
    getrandom::getrandom(&mut buf).map_err(|error| SecureStoreError::Random(error.to_string()))?;
    Ok(buf)
}

fn verify_canary(key: &[u8; KEY_LEN], verifier: &str) -> Result<(), SecureStoreError> {
    let plaintext = decrypt(key, verifier).map_err(|_| SecureStoreError::WrongKey)?;
    if plaintext.as_slice() != VERIFIER_PLAINTEXT {
        return Err(SecureStoreError::WrongKey);
    }
    Ok(())
}

fn make_verifier(key: &[u8; KEY_LEN]) -> Result<String, SecureStoreError> {
    encrypt(key, VERIFIER_PLAINTEXT)
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
    /// Solo v1: huella en claro (oráculo offline). No se escribe en v2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kdf: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    salt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    argon2: Option<Argon2ParamsFile>,
    /// Canario cifrado: demuestra posesión de la clave sin revelar una huella.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verifier: Option<String>,
    entries: BTreeMap<String, String>,
}

struct Inner {
    key: [u8; KEY_LEN],
    salt: Vec<u8>,
    params: Argon2ParamsFile,
    key_id: String,
    secrets: HashMap<String, ConnectionSecret>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.key.zeroize();
        self.salt.zeroize();
    }
}

/// Almacén de secretos cifrado con AES-256-GCM y respaldado en disco.
pub struct EncryptedFileSecretStore {
    path: PathBuf,
    fingerprint: Mutex<String>,
    inner: RwLock<Inner>,
}

impl EncryptedFileSecretStore {
    /// Abre (o inicializa) el almacén cifrado. Si el fichero ya existe, valida
    /// la clave maestra con el canario (v2) o migra desde v1 (SHA-256).
    pub fn open(path: impl Into<PathBuf>, master_key: &str) -> Result<Self, SecureStoreError> {
        let path = path.into();
        if !path.exists() {
            return Self::create_empty(path, master_key);
        }

        let raw = fs::read(&path).map_err(|source| SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let file: SecretFile = serde_json::from_slice(&raw)
            .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;

        match file.version {
            LEGACY_SECRET_FILE_VERSION => Self::open_and_migrate_v1(path, master_key, file),
            SECRET_FILE_VERSION => Self::open_v2(path, master_key, file),
            other => Err(SecureStoreError::UnsupportedVersion(other)),
        }
    }

    fn create_empty(path: PathBuf, master_key: &str) -> Result<Self, SecureStoreError> {
        let salt = random_bytes(SALT_LEN)?;
        let params = Argon2ParamsFile::default();
        let key = derive_key_argon2(master_key, &salt, &params)?;
        let identifier = fingerprint(&key);
        Ok(Self {
            path,
            fingerprint: Mutex::new(identifier.clone()),
            inner: RwLock::new(Inner {
                key,
                salt,
                params,
                key_id: identifier,
                secrets: HashMap::new(),
            }),
        })
    }

    fn open_v2(
        path: PathBuf,
        master_key: &str,
        file: SecretFile,
    ) -> Result<Self, SecureStoreError> {
        let salt_b64 = file
            .salt
            .as_deref()
            .ok_or_else(|| SecureStoreError::Corrupt("falta salt".to_owned()))?;
        let salt = STANDARD
            .decode(salt_b64)
            .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
        if salt.len() < 8 {
            return Err(SecureStoreError::Corrupt("salt demasiado corta".to_owned()));
        }
        let params = file.argon2.unwrap_or_default();
        if file.kdf.as_deref().unwrap_or(KDF_NAME) != KDF_NAME {
            return Err(SecureStoreError::Corrupt(format!(
                "KDF no soportado: {:?}",
                file.kdf
            )));
        }
        let key = derive_key_argon2(master_key, &salt, &params)?;
        let verifier = file
            .verifier
            .as_deref()
            .ok_or_else(|| SecureStoreError::Corrupt("falta verifier".to_owned()))?;
        verify_canary(&key, verifier)?;

        let mut secrets = HashMap::with_capacity(file.entries.len());
        for (reference, blob) in file.entries {
            let plaintext = decrypt(&key, &blob).map_err(|_| SecureStoreError::WrongKey)?;
            let payload: SecretPayload = serde_json::from_slice(&plaintext)
                .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
            secrets.insert(reference, payload.into());
        }

        let identifier = fingerprint(&key);
        Ok(Self {
            path,
            fingerprint: Mutex::new(identifier.clone()),
            inner: RwLock::new(Inner {
                key,
                salt,
                params,
                key_id: identifier,
                secrets,
            }),
        })
    }

    fn open_and_migrate_v1(
        path: PathBuf,
        master_key: &str,
        file: SecretFile,
    ) -> Result<Self, SecureStoreError> {
        let mut key = derive_key_sha256(master_key);
        let legacy_id = fingerprint(&key);
        let stored_id = file
            .key_id
            .as_deref()
            .ok_or_else(|| SecureStoreError::Corrupt("falta key_id en v1".to_owned()))?;
        if stored_id != legacy_id {
            key.zeroize();
            return Err(SecureStoreError::WrongKey);
        }

        let mut secrets = HashMap::with_capacity(file.entries.len());
        for (reference, blob) in &file.entries {
            let plaintext = decrypt(&key, blob).map_err(|_| SecureStoreError::WrongKey)?;
            let payload: SecretPayload = serde_json::from_slice(&plaintext)
                .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
            secrets.insert(reference.clone(), payload.into());
        }
        key.zeroize();

        let salt = random_bytes(SALT_LEN)?;
        let params = Argon2ParamsFile::default();
        let new_key = derive_key_argon2(master_key, &salt, &params)?;
        let identifier = fingerprint(&new_key);
        let inner = Inner {
            key: new_key,
            salt,
            params,
            key_id: identifier.clone(),
            secrets,
        };
        Self::write_file_at(&path, &inner)?;

        tracing::info!(
            target: "jaiba.connections",
            path = %path.display(),
            "almacén de secretos migrado de v1 (SHA-256) a v2 (Argon2id)"
        );
        Ok(Self {
            path,
            fingerprint: Mutex::new(identifier),
            inner: RwLock::new(inner),
        })
    }

    /// Rota la clave maestra: escribe a disco con la nueva clave/sal y solo
    /// entonces actualiza la memoria (evita perder la clave actual si falla I/O).
    pub async fn rotate_key(&self, new_master_key: &str) -> Result<String, SecureStoreError> {
        let mut inner = self.inner.write().await;
        let new_salt = random_bytes(SALT_LEN)?;
        let params = Argon2ParamsFile::default();
        let new_key = derive_key_argon2(new_master_key, &new_salt, &params)?;
        let new_id = fingerprint(&new_key);

        let pending = Inner {
            key: new_key,
            salt: new_salt,
            params,
            key_id: new_id.clone(),
            secrets: inner.secrets.clone(),
        };
        self.write_file(&pending)?;

        *inner = pending;
        if let Ok(mut guard) = self.fingerprint.lock() {
            *guard = new_id.clone();
        }
        Ok(new_id)
    }

    /// Huella de la clave activa (para logs; no se almacena en el fichero).
    pub fn fingerprint(&self) -> String {
        self.fingerprint
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Huella de la clave activa (async, desde el estado interno).
    pub async fn active_key_id(&self) -> String {
        self.inner.read().await.key_id.clone()
    }

    fn write_file(&self, inner: &Inner) -> Result<(), SecureStoreError> {
        Self::write_file_at(&self.path, inner)
    }

    fn write_file_at(path: &Path, inner: &Inner) -> Result<(), SecureStoreError> {
        let mut entries = BTreeMap::new();
        for (reference, secret) in &inner.secrets {
            let payload = SecretPayload::from(secret);
            let plaintext = serde_json::to_vec(&payload)
                .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
            entries.insert(reference.clone(), encrypt(&inner.key, &plaintext)?);
        }
        let file = SecretFile {
            version: SECRET_FILE_VERSION,
            key_id: None,
            kdf: Some(KDF_NAME.to_owned()),
            salt: Some(STANDARD.encode(&inner.salt)),
            argon2: Some(inner.params.clone()),
            verifier: Some(make_verifier(&inner.key)?),
            entries,
        };
        let serialized = serde_json::to_vec_pretty(&file)
            .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
        write_private(path, &serialized)
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
        let previous = inner.secrets.insert(reference.to_owned(), secret);
        if let Err(error) = self.write_file(&inner) {
            match previous {
                Some(previous) => {
                    inner.secrets.insert(reference.to_owned(), previous);
                }
                None => {
                    inner.secrets.remove(reference);
                }
            }
            return Err(error.into());
        }
        Ok(())
    }

    async fn remove(&self, reference: &str) -> Result<(), ConnectionManagerError> {
        let mut inner = self.inner.write().await;
        if let Some(previous) = inner.secrets.remove(reference)
            && let Err(error) = self.write_file(&inner)
        {
            inner.secrets.insert(reference.to_owned(), previous);
            return Err(error.into());
        }
        Ok(())
    }
}

fn encrypt(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<String, SecureStoreError> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| SecureStoreError::Crypto("key"))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce_bytes)
        .map_err(|error| SecureStoreError::Random(error.to_string()))?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| SecureStoreError::Crypto("encrypt"))?;
    let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    blob.extend_from_slice(&nonce_bytes);
    blob.extend_from_slice(&ciphertext);
    Ok(STANDARD.encode(blob))
}

fn decrypt(key: &[u8; KEY_LEN], blob_base64: &str) -> Result<Vec<u8>, SecureStoreError> {
    let blob = STANDARD
        .decode(blob_base64)
        .map_err(|error| SecureStoreError::Corrupt(error.to_string()))?;
    if blob.len() <= NONCE_LEN {
        return Err(SecureStoreError::Corrupt(
            "bloque cifrado demasiado corto".to_owned(),
        ));
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
    let temporary = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
    write_temp_private(&temporary, bytes)?;
    fs::rename(&temporary, path).map_err(|source| {
        let _ = fs::remove_file(&temporary);
        SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        }
    })
}

#[cfg(unix)]
fn write_temp_private(path: &Path, bytes: &[u8]) -> Result<(), SecureStoreError> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        })?;
    file.write_all(bytes)
        .map_err(|source| SecureStoreError::Io {
            path: path.display().to_string(),
            source,
        })?;
    file.sync_all().map_err(|source| SecureStoreError::Io {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(not(unix))]
fn write_temp_private(path: &Path, bytes: &[u8]) -> Result<(), SecureStoreError> {
    fs::write(path, bytes).map_err(|source| SecureStoreError::Io {
        path: path.display().to_string(),
        source,
    })
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
        assert!(
            !raw.contains("s3cr3t"),
            "el fichero no debe contener el secreto en claro"
        );
        assert!(!raw.contains("dma"));
        assert!(raw.contains("\"kdf\": \"argon2id\""));
        assert!(raw.contains("\"verifier\""));
        assert!(!raw.contains("\"key_id\""));

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
        assert!(matches!(
            EncryptedFileSecretStore::open(&path, "clave-b"),
            Err(SecureStoreError::WrongKey)
        ));
        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rotating_the_key_reencrypts_and_reopens() {
        let dir = std::env::temp_dir().join(format!("jaiba-secure-{}", uuid::Uuid::new_v4()));
        let path = dir.join("secrets.enc");
        let store = EncryptedFileSecretStore::open(&path, "clave-a").unwrap();
        store.store("memory://a", secret()).await.unwrap();
        let salt_before: String = {
            let file: SecretFile = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            file.salt.unwrap()
        };
        store.rotate_key("clave-b").await.unwrap();
        let salt_after: String = {
            let file: SecretFile = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            file.salt.unwrap()
        };
        assert_ne!(
            salt_before, salt_after,
            "la rotación debe generar nueva sal"
        );

        assert!(EncryptedFileSecretStore::open(&path, "clave-a").is_err());
        let reopened = EncryptedFileSecretStore::open(&path, "clave-b").unwrap();
        assert_eq!(
            reopened.resolve("memory://a").await.unwrap().password,
            "s3cr3t"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn migrates_v1_sha256_store_to_argon2() {
        let dir = std::env::temp_dir().join(format!("jaiba-secure-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("secrets.enc");
        let master = "clave-legacy";
        let key = derive_key_sha256(master);
        let payload = SecretPayload::from(&secret());
        let plaintext = serde_json::to_vec(&payload).unwrap();
        let blob = encrypt(&key, &plaintext).unwrap();
        let v1 = SecretFile {
            version: 1,
            key_id: Some(fingerprint(&key)),
            kdf: None,
            salt: None,
            argon2: None,
            verifier: None,
            entries: BTreeMap::from([("memory://a".to_owned(), blob)]),
        };
        fs::write(&path, serde_json::to_vec_pretty(&v1).unwrap()).unwrap();

        let store = EncryptedFileSecretStore::open(&path, master).unwrap();
        assert_eq!(
            store.resolve("memory://a").await.unwrap().password,
            "s3cr3t"
        );

        let migrated: SecretFile = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(migrated.version, 2);
        assert!(migrated.salt.is_some());
        assert!(migrated.verifier.is_some());
        assert_eq!(migrated.kdf.as_deref(), Some("argon2id"));
        assert!(migrated.key_id.is_none());

        // Reabrir tras migración.
        let reopened = EncryptedFileSecretStore::open(&path, master).unwrap();
        assert_eq!(
            reopened.resolve("memory://a").await.unwrap().password,
            "s3cr3t"
        );
        fs::remove_dir_all(&dir).ok();
    }
}
