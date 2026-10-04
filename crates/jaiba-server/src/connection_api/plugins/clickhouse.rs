//! Plugin de conexión ClickHouse (`clickhouse-driver`).

use std::time::Instant;

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, CompiledQuery, ConnectionEndpoint, ConnectionPlugin, ConnectionSecret,
    ConnectionTestResult, ConnectionType, DatabaseObject, DiagnosticCheck, ObjectDescription,
    PluginDescriptor, PluginError, QuerySpec,
};
use url::Url;

use super::success;

#[cfg(feature = "clickhouse-driver")]
/// ClickHouse por HTTP: prueba, diagnóstico y explorador. No compila consultas
/// visuales.
pub(in crate::connection_api) struct ClickHouseConnectionPlugin;

#[cfg(feature = "clickhouse-driver")]
#[async_trait]
impl ConnectionPlugin for ClickHouseConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            id: "jaiba.clickhouse".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            display_name: "ClickHouse".to_owned(),
            category: "Analytics".to_owned(),
            default_port: 8123,
            // MVP sink: connectivity only (no query builder / schema explorer yet).
            capabilities: vec!["test".to_owned(), "diagnostics".to_owned()],
        }
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::ClickHouse
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let writer = clickhouse_writer(endpoint, secret)?;
        let version = writer
            .ping()
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        Ok(success(started, version, 1, 0, endpoint.pool_max))
    }

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let writer = clickhouse_writer(endpoint, secret)?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;
        let version_started = Instant::now();
        let version = writer
            .ping()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let version_latency = version_started.elapsed().as_millis() as u64;
        Ok(vec![
            DiagnosticCheck {
                code: "connectivity".to_owned(),
                label: "Conectividad".to_owned(),
                status: Availability::Available,
                latency_ms: Some(connection_latency),
                details: serde_json::json!({
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "database": endpoint.database,
                }),
            },
            DiagnosticCheck {
                code: "version".to_owned(),
                label: "Versión".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({ "version": version }),
            },
        ])
    }

    async fn list_objects(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
        _schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        Err(PluginError::Unsupported("schema_explorer".to_owned()))
    }

    async fn describe_object(
        &self,
        _endpoint: &ConnectionEndpoint,
        _secret: &ConnectionSecret,
        _object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        Err(PluginError::Unsupported("schema_explorer".to_owned()))
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("query_builder".to_owned()))
    }
}

#[cfg(feature = "clickhouse-driver")]
/// Cliente HTTP(S) de ClickHouse con credenciales en la URL; sin base usa
/// `default`.
fn clickhouse_writer(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<jaiba_runtime::connectors::ClickHouseWriter, PluginError> {
    let scheme = if endpoint.ssl { "https" } else { "http" };
    let database = endpoint
        .database
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    let mut url = Url::parse(&format!(
        "{scheme}://{}:{}/{}",
        endpoint.host, endpoint.port, database
    ))
    .map_err(|error| PluginError::Configuration(format!("URL ClickHouse inválida: {error}")))?;
    url.set_username(&secret.username).map_err(|_| {
        PluginError::Configuration("no se pudo fijar el usuario ClickHouse".to_owned())
    })?;
    url.set_password(Some(&secret.password)).map_err(|_| {
        PluginError::Configuration("no se pudo fijar la contraseña ClickHouse".to_owned())
    })?;
    jaiba_runtime::connectors::ClickHouseWriter::from_url(url.as_str())
        .map_err(|error| PluginError::Configuration(error.to_string()))
}
