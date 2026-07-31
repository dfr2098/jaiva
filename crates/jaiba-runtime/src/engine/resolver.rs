//! Resolución de conexiones por alias.
//!
//! Un flujo puede referirse a una conexión solo por su alias
//! (`connection: postgres_dma`). En tiempo de arranque, el runtime consulta al
//! Connection Manager (perfiles persistidos + secretos cifrados de la fase 2)
//! para resolver host, puerto, base, usuario, contraseña, SSL, pool y timeout,
//! sin que las credenciales aparezcan nunca en el YAML.

use std::{collections::BTreeSet, path::PathBuf};

use async_trait::async_trait;
use jaiba_connection_manager::{
    ConnectionProfile, EncryptedFileSecretStore, FileProfileRepository, ProfileRepository,
    SecretStore,
};
use jaiba_plugin_sdk::{ConnectionEndpoint, ConnectionSecret, ConnectionType};
use url::Url;

use crate::{config::FlowConfig, error::FlowError};

/// Parámetros concretos de una conexión, ya resueltos a partir de un alias.
#[derive(Debug, Clone)]
pub struct ResolvedConnection {
    /// Tipo que entiende el runtime: `postgres`, `mysql`, `mariadb`, `mongodb`,
    /// `oracle` o `sqlserver`.
    pub connection_type: String,
    /// URL de conexión lista para el driver (credenciales ya incluidas y
    /// codificadas de forma segura).
    pub url: String,
    /// Tamaño máximo del pool.
    pub max_connections: u32,
    /// Timeout de adquisición/conexión en milisegundos.
    pub timeout_ms: u64,
}

/// Fuente capaz de resolver un alias de conexión a sus parámetros concretos.
#[async_trait]
pub trait ConnectionResolver: Send + Sync {
    async fn resolve(&self, alias: &str) -> Result<ResolvedConnection, FlowError>;
}

/// Resolvedor respaldado por los perfiles persistidos y los secretos cifrados
/// del Connection Manager.
pub struct ProfileConnectionResolver {
    profiles: Vec<ConnectionProfile>,
    secrets: EncryptedFileSecretStore,
}

impl ProfileConnectionResolver {
    /// Abre el resolvedor desde una carpeta de datos y una clave maestra.
    pub async fn open(data_dir: PathBuf, master_key: &str) -> Result<Self, FlowError> {
        let profiles = FileProfileRepository::new(data_dir.join("connections.json"))
            .load()
            .await
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        let secrets = EncryptedFileSecretStore::open(data_dir.join("secrets.enc"), master_key)
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        Ok(Self { profiles, secrets })
    }

    /// Construye el resolvedor desde el entorno.
    ///
    /// Devuelve `None` cuando no hay `JAIBA_MASTER_KEY`, de modo que el flujo
    /// sigue usando `database_connections` con variables de entorno.
    pub async fn from_env() -> Result<Option<Self>, FlowError> {
        let Some(master_key) = std::env::var("JAIBA_MASTER_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };
        let data_dir =
            PathBuf::from(std::env::var("JAIBA_DATA_DIR").unwrap_or_else(|_| "data".to_owned()));
        Ok(Some(Self::open(data_dir, &master_key).await?))
    }
}

#[async_trait]
impl ConnectionResolver for ProfileConnectionResolver {
    async fn resolve(&self, alias: &str) -> Result<ResolvedConnection, FlowError> {
        let profile = self
            .profiles
            .iter()
            .find(|profile| profile.name.eq_ignore_ascii_case(alias) || profile.id == alias)
            .ok_or_else(|| {
                FlowError::Configuration(format!(
                    "no existe un perfil de conexión con alias '{alias}' en el Connection Manager"
                ))
            })?;
        let secret = self
            .secrets
            .resolve(&profile.secret_ref)
            .await
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        let (scheme, runtime_type) = scheme_for(&profile.connection_type).ok_or_else(|| {
            FlowError::Configuration(format!(
                "el perfil '{alias}' no es una conexión de base de datos compatible"
            ))
        })?;
        Ok(ResolvedConnection {
            connection_type: runtime_type.to_owned(),
            url: build_url(scheme, &profile.endpoint, &secret)?,
            max_connections: profile.endpoint.pool_max,
            timeout_ms: profile.endpoint.timeout_ms,
        })
    }
}

/// Devuelve `(esquema_url, tipo_runtime)` para una conexión de base de datos.
fn scheme_for(connection_type: &ConnectionType) -> Option<(&'static str, &'static str)> {
    match connection_type {
        ConnectionType::Postgres => Some(("postgres", "postgres")),
        ConnectionType::MySql => Some(("mysql", "mysql")),
        ConnectionType::MariaDb => Some(("mysql", "mariadb")),
        ConnectionType::MongoDb => Some(("mongodb", "mongodb")),
        ConnectionType::Oracle => Some(("oracle", "oracle")),
        ConnectionType::SqlServer => Some(("sqlserver", "sqlserver")),
        _ => None,
    }
}

/// Construye una URL de conexión codificando de forma segura usuario y
/// contraseña (incluidos caracteres especiales).
fn build_url(
    scheme: &str,
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<String, FlowError> {
    let mut url = Url::parse(&format!("{scheme}://{}:{}", endpoint.host, endpoint.port))
        .map_err(|error| FlowError::Configuration(format!("URL de conexión inválida: {error}")))?;
    url.set_username(&secret.username).map_err(|_| {
        FlowError::Configuration("no se pudo fijar el usuario en la URL".to_owned())
    })?;
    url.set_password(Some(&secret.password)).map_err(|_| {
        FlowError::Configuration("no se pudo fijar la contraseña en la URL".to_owned())
    })?;
    if let Some(database) = endpoint
        .database
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        url.set_path(&format!("/{database}"));
    }
    match scheme {
        "postgres" => {
            url.query_pairs_mut()
                .append_pair("sslmode", if endpoint.ssl { "require" } else { "disable" });
        }
        "mysql" if endpoint.ssl => {
            url.query_pairs_mut().append_pair("ssl-mode", "REQUIRED");
        }
        "mongodb" => {
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
        }
        _ => {}
    }
    Ok(url.to_string())
}

/// Recolecta los alias de conexión referenciados por los procesadores que no
/// están definidos como `database_connections` ni `kafka_connections` (es
/// decir, los que deben resolverse contra el Connection Manager).
pub fn referenced_db_aliases(config: &FlowConfig) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    for processor in &config.processors {
        if let Some(connection) = processor
            .config
            .get("connection")
            .and_then(|value| value.as_str())
            && !config.database_connections.contains_key(connection)
            && !config.kafka_connections.contains_key(connection)
        {
            aliases.insert(connection.to_owned());
        }
    }
    aliases.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jaiba_connection_manager::ConnectionProfile;
    use jaiba_plugin_sdk::ConnectionType;
    use std::collections::BTreeMap;

    fn endpoint() -> ConnectionEndpoint {
        ConnectionEndpoint {
            host: "db.internal".to_owned(),
            port: 5432,
            database: Some("dma".to_owned()),
            ssl: true,
            pool_min: 1,
            pool_max: 7,
            timeout_ms: 5_000,
            options: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn resolves_alias_to_url_with_encoded_credentials() {
        let dir = std::env::temp_dir().join(format!("jaiba-resolver-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let secrets =
            EncryptedFileSecretStore::open(dir.join("secrets.enc"), "clave-maestra").unwrap();
        secrets
            .store(
                "secret://pg",
                ConnectionSecret {
                    username: "svc dma".to_owned(),
                    password: "p@ss/word:1".to_owned(),
                    options: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        let profile = ConnectionProfile {
            id: "id-1".to_owned(),
            name: "postgres_dma".to_owned(),
            connection_type: ConnectionType::Postgres,
            endpoint: endpoint(),
            secret_ref: "secret://pg".to_owned(),
            tags: Vec::new(),
        };
        FileProfileRepository::new(dir.join("connections.json"))
            .save(std::slice::from_ref(&profile))
            .await
            .unwrap();

        let resolver = ProfileConnectionResolver::open(dir.clone(), "clave-maestra")
            .await
            .unwrap();
        let resolved = resolver.resolve("postgres_dma").await.unwrap();
        assert_eq!(resolved.connection_type, "postgres");
        assert_eq!(resolved.max_connections, 7);
        // Las credenciales con caracteres especiales quedan codificadas.
        assert!(resolved.url.starts_with("postgres://svc%20dma:"));
        assert!(resolved.url.contains("@db.internal:5432/dma"));
        assert!(resolved.url.contains("sslmode=require"));
        assert!(!resolved.url.contains("p@ss/word:1"));

        // También resuelve por id y falla claramente con alias inexistente.
        assert!(resolver.resolve("id-1").await.is_ok());
        assert!(resolver.resolve("desconocido").await.is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn builds_mongodb_url_with_auth_source_pool_and_timeout() {
        let mut mongodb = endpoint();
        mongodb.port = 27_017;
        mongodb.database = Some("pruebas".to_owned());
        mongodb.ssl = false;
        mongodb
            .options
            .insert("auth_source".to_owned(), "admin".to_owned());
        let url = build_url(
            "mongodb",
            &mongodb,
            &ConnectionSecret {
                username: "admin user".to_owned(),
                password: "p@ss/word".to_owned(),
                options: BTreeMap::new(),
            },
        )
        .expect("build MongoDB URL");

        assert!(url.starts_with("mongodb://admin%20user:"));
        assert!(url.contains("@db.internal:27017/pruebas"));
        assert!(url.contains("authSource=admin"));
        assert!(url.contains("tls=false"));
        assert!(url.contains("maxPoolSize=7"));
        assert!(url.contains("serverSelectionTimeoutMS=5000"));
        assert!(!url.contains("p@ss/word"));
    }
}
