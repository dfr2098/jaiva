//! Plugin de conexión Oracle (`oracle-driver`).

use std::time::Instant;

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, ColumnMetadata, CompiledQuery, ConnectionEndpoint, ConnectionPlugin,
    ConnectionSecret, ConnectionTestResult, ConnectionType, DatabaseObject, DatabaseObjectKind,
    DiagnosticCheck, ObjectDescription, PluginDescriptor, PluginError, QuerySpec,
};

use super::{description, descriptor, success, with_schemas};

#[cfg(feature = "oracle-driver")]
/// Oracle: prueba, diagnóstico y explorador. No compila consultas visuales.
pub(in crate::connection_api) struct OracleConnectionPlugin;

#[cfg(feature = "oracle-driver")]
#[async_trait]
impl ConnectionPlugin for OracleConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.oracle", "Oracle", 1521, false, false)
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::Oracle
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let connection = oracle_connect(&endpoint, &secret)?;
            let version = connection
                .query_row_as::<String>("SELECT banner FROM v$version WHERE ROWNUM = 1", &[])
                .map_err(|error| PluginError::Connection(error.to_string()))?;
            Ok(success(started, version, 1, 0, endpoint.pool_max))
        })
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))?
    }

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        tokio::task::spawn_blocking(move || {
            let connected_at = Instant::now();
            let connection = oracle_connect(&endpoint, &secret)?;
            let connection_latency = connected_at.elapsed().as_millis() as u64;

            let version_started = Instant::now();
            let version = connection
                .query_row_as::<String>("SELECT banner FROM v$version WHERE ROWNUM = 1", &[])
                .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
            let version_latency = version_started.elapsed().as_millis() as u64;

            let metadata_started = Instant::now();
            let owner = secret.username.to_uppercase();
            let visible_objects = connection
                .query_row_as::<i64>(
                    "SELECT COUNT(*) FROM all_objects WHERE owner = :1",
                    &[&owner],
                )
                .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
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
                        "service": endpoint.database,
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
                    label: "Acceso a metadatos".to_owned(),
                    status: Availability::Available,
                    latency_ms: Some(metadata_latency),
                    details: serde_json::json!({
                        "owner": owner,
                        "visible_objects": visible_objects,
                    }),
                },
            ])
        })
        .await
        .map_err(|error| PluginError::Diagnostic(error.to_string()))?
    }

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        let schema = schema.map(str::to_owned);
        let include_schemas = schema.is_none();
        tokio::task::spawn_blocking(move || {
            let connection = oracle_connect(&endpoint, &secret)?;
            let owner = schema.unwrap_or_else(|| secret.username.to_uppercase());
            let rows = connection
                .query(
                    "SELECT owner, object_name, object_type FROM all_objects \
                     WHERE owner = :1 AND object_type IN ('TABLE', 'VIEW') \
                     ORDER BY object_name",
                    &[&owner],
                )
                .map_err(|error| PluginError::Exploration(error.to_string()))?;
            let objects = rows
                .map(|row| {
                    let row = row.map_err(|error| PluginError::Exploration(error.to_string()))?;
                    let object_type: String = row.get(2).map_err(oracle_exploration)?;
                    Ok(DatabaseObject {
                        schema: Some(row.get(0).map_err(oracle_exploration)?),
                        name: row.get(1).map_err(oracle_exploration)?,
                        kind: if object_type == "VIEW" {
                            DatabaseObjectKind::View
                        } else {
                            DatabaseObjectKind::Table
                        },
                    })
                })
                .collect::<Result<Vec<_>, PluginError>>()?;
            Ok(with_schemas(objects, include_schemas))
        })
        .await
        .map_err(|error| PluginError::Exploration(error.to_string()))?
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let endpoint = endpoint.clone();
        let secret = secret.clone();
        let object = object.clone();
        tokio::task::spawn_blocking(move || {
            let connection = oracle_connect(&endpoint, &secret)?;
            let owner = object
                .schema
                .clone()
                .unwrap_or_else(|| secret.username.to_uppercase());
            let rows = connection
                .query(
                    "SELECT column_name, data_type, nullable, column_id, data_default \
                 FROM all_tab_columns WHERE owner = :1 AND table_name = :2 ORDER BY column_id",
                    &[&owner, &object.name],
                )
                .map_err(oracle_exploration)?;
            let columns = rows
                .map(|row| {
                    let row = row.map_err(oracle_exploration)?;
                    Ok(ColumnMetadata {
                        name: row.get(0).map_err(oracle_exploration)?,
                        data_type: row.get(1).map_err(oracle_exploration)?,
                        nullable: row.get::<_, String>(2).map_err(oracle_exploration)? == "Y",
                        ordinal: row.get::<_, u32>(3).map_err(oracle_exploration)?,
                        default_value: row.get(4).ok(),
                    })
                })
                .collect::<Result<Vec<_>, PluginError>>()?;
            Ok(description(&object, columns))
        })
        .await
        .map_err(|error| PluginError::Exploration(error.to_string()))?
    }

    fn compile_query(&self, _specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        Err(PluginError::Unsupported("constructor SQL".to_owned()))
    }
}

#[cfg(feature = "oracle-driver")]
/// Conecta por `host:puerto/servicio` (el servicio va en `database`).
pub(in crate::connection_api) fn oracle_connect(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<oracle::Connection, PluginError> {
    let service = endpoint.database.as_deref().unwrap_or("");
    oracle::Connection::connect(
        &secret.username,
        &secret.password,
        format!("{}:{}/{}", endpoint.host, endpoint.port, service),
    )
    .map_err(oracle_connection)
}

#[cfg(feature = "oracle-driver")]
/// Error de conexión; si falta Instant Client (`DPI-1047`) dice cómo instalarlo.
fn oracle_connection(error: oracle::Error) -> PluginError {
    let message = error.to_string();
    if message.contains("DPI-1047") {
        return PluginError::Connection(format!(
            "{message}. Instala Oracle Instant Client de 64 bits y agrega su directorio a \
             LD_LIBRARY_PATH antes de iniciar Jaiba"
        ));
    }
    PluginError::Connection(message)
}

#[cfg(feature = "oracle-driver")]
fn oracle_exploration(error: oracle::Error) -> PluginError {
    PluginError::Exploration(error.to_string())
}
