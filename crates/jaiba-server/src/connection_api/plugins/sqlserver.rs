//! Plugin de conexión SQL Server (`sqlserver-driver`, tiberius).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, ColumnMetadata, CompiledQuery, ConnectionEndpoint, ConnectionPlugin,
    ConnectionSecret, ConnectionTestResult, ConnectionType, DatabaseObject, DatabaseObjectKind,
    DiagnosticCheck, ObjectDescription, PluginDescriptor, PluginError, QuerySpec,
};
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::{description, descriptor, success, with_schemas};

#[cfg(feature = "sqlserver-driver")]
/// SQL Server: prueba, diagnóstico, explorador y compilación de `QuerySpec`
/// hacia el nodo `query_sqlserver`.
pub(in crate::connection_api) struct SqlServerConnectionPlugin;

#[cfg(feature = "sqlserver-driver")]
type SqlServerClient = Client<Compat<TcpStream>>;

#[cfg(feature = "sqlserver-driver")]
#[async_trait]
impl ConnectionPlugin for SqlServerConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.sqlserver", "SQL Server", 1433, true, true)
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::SqlServer
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let row = client
            .simple_query("SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))")
            .await
            .map_err(sqlserver_connection)?
            .into_row()
            .await
            .map_err(sqlserver_connection)?
            .ok_or_else(|| PluginError::Connection("SQL Server no devolvió versión".to_owned()))?;
        Ok(success(
            started,
            row.get::<&str, _>(0).unwrap_or("SQL Server").to_owned(),
            1,
            0,
            endpoint.pool_max,
        ))
    }

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let version_row = client
            .simple_query(
                "SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), \
                        CAST(SERVERPROPERTY('Edition') AS nvarchar(128))",
            )
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .into_row()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .ok_or_else(|| PluginError::Diagnostic("versión no disponible".to_owned()))?;
        let version = version_row.get::<&str, _>(0).unwrap_or_default().to_owned();
        let edition = version_row.get::<&str, _>(1).unwrap_or_default().to_owned();
        let version_latency = version_started.elapsed().as_millis() as u64;

        let metadata_started = Instant::now();
        let metadata_row = client
            .simple_query(
                "SELECT CAST(DB_NAME() AS nvarchar(128)), COUNT_BIG(*) \
                 FROM sys.objects WHERE is_ms_shipped = 0",
            )
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .into_row()
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?
            .ok_or_else(|| PluginError::Diagnostic("metadatos no disponibles".to_owned()))?;
        let database = metadata_row
            .get::<&str, _>(0)
            .unwrap_or_default()
            .to_owned();
        let visible_objects = metadata_row.get::<i64, _>(1).unwrap_or_default();
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
                    "encrypted": endpoint.ssl,
                }),
            },
            DiagnosticCheck {
                code: "server_version".to_owned(),
                label: "Versión del servidor".to_owned(),
                status: Availability::Available,
                latency_ms: Some(version_latency),
                details: serde_json::json!({
                    "version": version,
                    "edition": edition,
                }),
            },
            DiagnosticCheck {
                code: "metadata_access".to_owned(),
                label: "Acceso a metadatos".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": database,
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
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let selected = schema.unwrap_or("");
        let rows = client
            .query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES \
                 WHERE (@P1 = '' OR TABLE_SCHEMA = @P1) ORDER BY TABLE_SCHEMA, TABLE_NAME",
                &[&selected],
            )
            .await
            .map_err(sqlserver_exploration)?
            .into_first_result()
            .await
            .map_err(sqlserver_exploration)?;
        Ok(with_schemas(
            rows.into_iter()
                .map(|row| {
                    let object_type = row.get::<&str, _>(2).unwrap_or("BASE TABLE");
                    DatabaseObject {
                        schema: row.get::<&str, _>(0).map(str::to_owned),
                        name: row.get::<&str, _>(1).unwrap_or_default().to_owned(),
                        kind: if object_type == "VIEW" {
                            DatabaseObjectKind::View
                        } else {
                            DatabaseObjectKind::Table
                        },
                    }
                })
                .collect(),
            schema.is_none(),
        ))
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let mut client = sqlserver_connect(endpoint, secret).await?;
        let schema = object.schema.as_deref().unwrap_or("dbo");
        let rows = client
            .query(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION, COLUMN_DEFAULT \
                 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 \
                 ORDER BY ORDINAL_POSITION",
                &[&schema, &object.name.as_str()],
            )
            .await
            .map_err(sqlserver_exploration)?
            .into_first_result()
            .await
            .map_err(sqlserver_exploration)?;
        let columns = rows
            .into_iter()
            .map(|row| ColumnMetadata {
                name: row.get::<&str, _>(0).unwrap_or_default().to_owned(),
                data_type: row.get::<&str, _>(1).unwrap_or_default().to_owned(),
                nullable: row.get::<&str, _>(2) == Some("YES"),
                ordinal: row.get::<i32, _>(3).unwrap_or_default() as u32,
                default_value: row.get::<&str, _>(4).map(str::to_owned),
            })
            .collect();
        Ok(description(object, columns))
    }

    fn compile_query(&self, specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        let mut compiled =
            crate::sql_builder::compile(specification, crate::sql_builder::Dialect::SqlServer)?;
        // Rows become JSON objects in the runtime; no SQL wrapper is required.
        compiled.processor_type = Some("query_sqlserver".to_owned());
        compiled.execution_statement = Some(compiled.statement.clone());
        Ok(compiled)
    }
}

#[cfg(feature = "sqlserver-driver")]
/// Conecta con autenticación SQL y `timeout_ms` en el TCP. Sin `ssl` acepta
/// cualquier certificado.
pub(in crate::connection_api) async fn sqlserver_connect(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<SqlServerClient, PluginError> {
    let mut config = Config::new();
    config.host(&endpoint.host);
    config.port(endpoint.port);
    if let Some(database) = endpoint.database.as_deref() {
        config.database(database);
    }
    config.authentication(AuthMethod::sql_server(&secret.username, &secret.password));
    if !endpoint.ssl {
        config.trust_cert();
    }
    let tcp = tokio::time::timeout(
        Duration::from_millis(endpoint.timeout_ms),
        TcpStream::connect(config.get_addr()),
    )
    .await
    .map_err(|_| PluginError::Connection("timeout conectando a SQL Server".to_owned()))?
    .map_err(sqlserver_connection)?;
    tcp.set_nodelay(true).map_err(sqlserver_connection)?;
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(sqlserver_connection)
}

#[cfg(feature = "sqlserver-driver")]
fn sqlserver_connection(error: impl std::fmt::Display) -> PluginError {
    PluginError::Connection(error.to_string())
}

#[cfg(feature = "sqlserver-driver")]
fn sqlserver_exploration(error: impl std::fmt::Display) -> PluginError {
    PluginError::Exploration(error.to_string())
}

#[cfg(all(test, feature = "sqlserver-driver"))]
mod sqlserver_compile_tests {
    use super::*;
    use jaiba_plugin_sdk::{FilterOperator, QueryFilter, QuerySource, QuerySpec};
    use serde_json::Value;

    #[test]
    fn sqlserver_plugin_wires_query_sqlserver_node() {
        let compiled = SqlServerConnectionPlugin
            .compile_query(&QuerySpec {
                source: QuerySource {
                    schema: Some("dbo".to_owned()),
                    table: "items".to_owned(),
                },
                columns: vec!["id".to_owned()],
                joins: vec![],
                filters: vec![QueryFilter {
                    field: "active".to_owned(),
                    operator: FilterOperator::Eq,
                    value: Value::Bool(true),
                }],
                group_by: vec![],
                order_by: vec![],
                limit: Some(5),
            })
            .expect("compile sqlserver query");

        assert_eq!(
            compiled.statement,
            "SELECT TOP (5) [id] FROM [dbo].[items] WHERE [active] = @P1"
        );
        assert_eq!(compiled.parameters, vec![Value::Bool(true)]);
        assert_eq!(compiled.processor_type.as_deref(), Some("query_sqlserver"));
        assert_eq!(
            compiled.execution_statement.as_deref(),
            Some(compiled.statement.as_str())
        );
        let descriptor = SqlServerConnectionPlugin.descriptor();
        assert!(descriptor.capabilities.iter().any(|c| c == "query_builder"));
        assert!(descriptor.capabilities.iter().any(|c| c == "query_node"));
    }
}
