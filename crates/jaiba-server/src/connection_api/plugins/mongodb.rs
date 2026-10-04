//! Plugin de conexión MongoDB (`mongodb-driver`).

use std::time::Instant;

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, ColumnMetadata, CompiledQuery, ConnectionEndpoint, ConnectionPlugin,
    ConnectionSecret, ConnectionTestResult, ConnectionType, DatabaseObject, DatabaseObjectKind,
    DiagnosticCheck, ObjectDescription, PluginDescriptor, PluginError, QuerySpec,
};
use mongodb::{
    Client as MongoClient,
    bson::{Bson, Document, doc},
};
use url::Url;

use super::{description, success};

#[cfg(feature = "mongodb-driver")]
pub(in crate::connection_api) struct MongoDbConnectionPlugin;

#[cfg(feature = "mongodb-driver")]
#[async_trait]
impl ConnectionPlugin for MongoDbConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            id: "jaiba.mongodb".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            display_name: "MongoDB".to_owned(),
            category: "Documental".to_owned(),
            default_port: 27_017,
            capabilities: vec![
                "test".to_owned(),
                "diagnostics".to_owned(),
                "schema_explorer".to_owned(),
            ],
        }
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::MongoDb
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let client = mongodb_client(endpoint, secret).await?;
        let database = mongodb_database(endpoint)?;
        client
            .database(database)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let build_info = client
            .database("admin")
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let version = build_info
            .get_str("version")
            .unwrap_or("MongoDB")
            .to_owned();
        Ok(success(started, version, 1, 0, endpoint.pool_max))
    }

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let client = mongodb_client(endpoint, secret).await?;
        let database_name = mongodb_database(endpoint)?;
        client
            .database(database_name)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let build_info = client
            .database("admin")
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let version_latency = version_started.elapsed().as_millis() as u64;
        let version = build_info
            .get_str("version")
            .unwrap_or("MongoDB")
            .to_owned();

        let metadata_started = Instant::now();
        let visible_objects = client
            .database(database_name)
            .list_collection_names()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .len();
        let metadata_latency = metadata_started.elapsed().as_millis() as u64;

        Ok(vec![
            DiagnosticCheck {
                code: "connectivity".to_owned(),
                label: "Conectividad".to_owned(),
                status: Availability::Available,
                latency_ms: Some(connection_latency),
                details: serde_json::json!({
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "ssl": endpoint.ssl,
                }),
            },
            DiagnosticCheck {
                code: "server_version".to_owned(),
                label: "Versión del servidor".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({ "version": version }),
            },
            DiagnosticCheck {
                code: "metadata_access".to_owned(),
                label: "Acceso a colecciones".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": database_name,
                    "visible_objects": visible_objects,
                }),
            },
        ])
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let client = mongodb_client(endpoint, secret).await?;
        let database_name = schema.unwrap_or(mongodb_database(endpoint)?);
        let mut collections = client
            .database(database_name)
            .list_collection_names()
            .await
            .map_err(|error| PluginError::Exploration(error.to_string()))?
            .into_iter()
            .map(|name| DatabaseObject {
                schema: Some(database_name.to_owned()),
                name,
                kind: DatabaseObjectKind::Collection,
            })
            .collect::<Vec<_>>();
        collections.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(collections)
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let client = mongodb_client(endpoint, secret).await?;
        let database_name = object
            .schema
            .as_deref()
            .unwrap_or(mongodb_database(endpoint)?);
        let sample = client
            .database(database_name)
            .collection::<Document>(&object.name)
            .find_one(doc! {})
            .await
            .map_err(|error| PluginError::Exploration(error.to_string()))?;
        let columns = sample
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(ordinal, (name, value))| ColumnMetadata {
                data_type: mongodb_bson_type(&value).to_owned(),
                nullable: name != "_id",
                ordinal: ordinal as u32 + 1,
                name,
                default_value: None,
            })
            .collect();
        let mut collection = object.clone();
        collection.kind = DatabaseObjectKind::Collection;
        Ok(description(&collection, columns))
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported(
            "constructor de consultas MongoDB".to_owned(),
        ))
    }
}

#[cfg(feature = "mongodb-driver")]
#[derive(Debug, Clone)]
pub(in crate::connection_api) struct ParsedMongoUrl {
    pub(in crate::connection_api) host: String,
    pub(in crate::connection_api) port: u16,
    pub(in crate::connection_api) database: Option<String>,
    pub(in crate::connection_api) username: String,
    pub(in crate::connection_api) password: Option<String>,
    pub(in crate::connection_api) auth_source: Option<String>,
    pub(in crate::connection_api) ssl: bool,
}

#[cfg(feature = "mongodb-driver")]
pub(in crate::connection_api) fn parse_mongodb_connection_url(
    raw: &str,
) -> Result<ParsedMongoUrl, String> {
    let url = Url::parse(raw.trim()).map_err(|error| format!("URL MongoDB inválida: {error}"))?;
    match url.scheme() {
        "mongodb" | "mongodb+srv" => {}
        other => {
            return Err(format!(
                "esquema '{other}' no soportado; use mongodb:// o mongodb+srv://"
            ));
        }
    }
    let host = url
        .host_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "la URL MongoDB debe incluir host".to_owned())?
        .to_owned();
    let port = url.port().unwrap_or(27_017);
    let database = {
        let path = url.path().trim_matches('/');
        if path.is_empty() {
            None
        } else {
            Some(path.to_owned())
        }
    };
    let username = url.username().to_owned();
    let password = url.password().map(str::to_owned);
    let mut auth_source = None;
    let mut ssl = url.scheme() == "mongodb+srv";
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "authSource" => auth_source = Some(value.into_owned()),
            "tls" | "ssl" => {
                ssl = matches!(value.as_ref(), "true" | "1" | "yes");
            }
            _ => {}
        }
    }
    Ok(ParsedMongoUrl {
        host,
        port,
        database,
        username,
        password,
        auth_source,
        ssl,
    })
}

#[cfg(feature = "mongodb-driver")]
pub(in crate::connection_api) fn apply_credentials_to_mongo_url(
    raw: &str,
    username: &str,
    password: &str,
) -> Result<String, String> {
    let mut url =
        Url::parse(raw.trim()).map_err(|error| format!("URL MongoDB inválida: {error}"))?;
    url.set_username(username)
        .map_err(|_| "usuario MongoDB inválido en la URL".to_owned())?;
    url.set_password(Some(password))
        .map_err(|_| "contraseña MongoDB inválida en la URL".to_owned())?;
    Ok(url.into())
}

#[cfg(feature = "mongodb-driver")]
pub(in crate::connection_api) async fn mongodb_client(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<MongoClient, PluginError> {
    MongoClient::with_uri_str(mongodb_url(endpoint, secret)?)
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))
}

#[cfg(feature = "mongodb-driver")]
fn mongodb_url(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<String, PluginError> {
    if let Some(stored) = secret
        .options
        .get("connection_url")
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        return apply_credentials_to_mongo_url(stored, &secret.username, &secret.password)
            .map_err(PluginError::Configuration);
    }

    let mut url = Url::parse(&format!("mongodb://{}:{}", endpoint.host, endpoint.port))
        .map_err(|error| PluginError::Configuration(error.to_string()))?;
    url.set_username(&secret.username)
        .map_err(|_| PluginError::Configuration("usuario MongoDB inválido".to_owned()))?;
    url.set_password(Some(&secret.password))
        .map_err(|_| PluginError::Configuration("contraseña MongoDB inválida".to_owned()))?;
    if let Some(database) = endpoint.database.as_deref() {
        url.set_path(database);
    }
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
    Ok(url.into())
}

#[cfg(feature = "mongodb-driver")]
pub(in crate::connection_api) fn mongodb_database(
    endpoint: &ConnectionEndpoint,
) -> Result<&str, PluginError> {
    endpoint
        .database
        .as_deref()
        .filter(|database| !database.trim().is_empty())
        .ok_or_else(|| PluginError::Configuration("la base MongoDB es obligatoria".to_owned()))
}

#[cfg(feature = "mongodb-driver")]
fn mongodb_bson_type(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "document",
        Bson::Boolean(_) => "boolean",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
        Bson::Int32(_) => "int32",
        Bson::Int64(_) => "int64",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binary",
        Bson::ObjectId(_) => "object_id",
        Bson::DateTime(_) => "datetime",
        Bson::Symbol(_) => "symbol",
        Bson::Decimal128(_) => "decimal128",
        Bson::Undefined => "undefined",
        Bson::MaxKey => "max_key",
        Bson::MinKey => "min_key",
        Bson::DbPointer(_) => "db_pointer",
    }
}

#[cfg(all(test, feature = "mongodb-driver"))]
mod mongo_url_unit_tests {
    use super::*;

    #[test]
    fn parses_mongodb_url_with_auth_source() {
        let parsed = parse_mongodb_connection_url(
            "mongodb://dma_test:s3cret@127.0.0.1:27018/dma_test?authSource=admin&tls=false",
        )
        .expect("parse");
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 27_018);
        assert_eq!(parsed.database.as_deref(), Some("dma_test"));
        assert_eq!(parsed.username, "dma_test");
        assert_eq!(parsed.password.as_deref(), Some("s3cret"));
        assert_eq!(parsed.auth_source.as_deref(), Some("admin"));
        assert!(!parsed.ssl);
    }

    #[test]
    fn parses_mongodb_srv_as_tls() {
        let parsed = parse_mongodb_connection_url(
            "mongodb+srv://app:pass@cluster0.example.net/prod?retryWrites=true",
        )
        .expect("parse srv");
        assert_eq!(parsed.host, "cluster0.example.net");
        assert_eq!(parsed.port, 27_017);
        assert!(parsed.ssl);
        assert_eq!(parsed.database.as_deref(), Some("prod"));
    }

    #[test]
    fn rejects_non_mongo_scheme() {
        let error = parse_mongodb_connection_url("postgres://u:p@h/db").expect_err("reject");
        assert!(error.contains("mongodb://"));
    }
}
