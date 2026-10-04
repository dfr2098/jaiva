//! Plugin de conexión MySQL / MariaDB (sqlx).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, ColumnMetadata, CompiledQuery, ConnectionEndpoint, ConnectionPlugin,
    ConnectionSecret, ConnectionTestResult, ConnectionType, DatabaseObject, DatabaseObjectKind,
    DiagnosticCheck, IndexMetadata, KeyMetadata, ObjectDescription, PluginDescriptor, PluginError,
    QuerySpec,
};
use sqlx::Row;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode};

use super::{descriptor, exploration_error, split_columns, success, with_schemas};

/// MySQL y MariaDB comparten implementación; `connection_type` decide cuál se
/// registra.
pub(in crate::connection_api) struct MySqlConnectionPlugin {
    pub(in crate::connection_api) connection_type: ConnectionType,
}

#[async_trait]
impl ConnectionPlugin for MySqlConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.mysql", "MySQL / MariaDB", 3306, true, true)
    }

    fn connection_type(&self) -> ConnectionType {
        self.connection_type.clone()
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let mut options = MySqlConnectOptions::new()
            .host(&endpoint.host)
            .port(endpoint.port)
            .username(&secret.username)
            .password(&secret.password)
            .ssl_mode(if endpoint.ssl {
                MySqlSslMode::Required
            } else {
                MySqlSslMode::Disabled
            });
        if let Some(database) = endpoint.database.as_deref() {
            options = options.database(database);
        }
        let pool = MySqlPoolOptions::new()
            .min_connections(endpoint.pool_min)
            .max_connections(endpoint.pool_max)
            .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
            .connect_with(options)
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let version = sqlx::query_scalar::<_, String>("SELECT version()")
            .fetch_one(&pool)
            .await
            .map_err(|error| PluginError::Connection(error.to_string()))?;
        let result = success(
            started,
            version,
            pool.size(),
            pool.num_idle() as u32,
            endpoint.pool_max,
        );
        pool.close().await;
        Ok(result)
    }

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError> {
        let connected_at = Instant::now();
        let pool = mysql_pool(endpoint, secret).await?;
        let connection_latency = connected_at.elapsed().as_millis() as u64;

        let version_started = Instant::now();
        let version = sqlx::query_scalar::<_, String>("SELECT VERSION()")
            .fetch_one(&pool)
            .await
            .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let version_latency = version_started.elapsed().as_millis() as u64;

        let metadata_started = Instant::now();
        let visible_objects = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = DATABASE()",
        )
        .fetch_one(&pool)
        .await
        .map_err(|error| PluginError::Diagnostic(error.to_string()))?;
        let metadata_latency = metadata_started.elapsed().as_millis() as u64;
        pool.close().await;

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
                label: "Acceso a metadatos".to_owned(),
                status: Availability::Available,
                latency_ms: Some(metadata_latency),
                details: serde_json::json!({
                    "database": endpoint.database,
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
        let pool = mysql_pool(endpoint, secret).await?;
        let selected = schema.or(endpoint.database.as_deref());
        let tables = sqlx::query(
            "SELECT CAST(table_schema AS CHAR) AS table_schema, \
                    CAST(table_name AS CHAR) AS table_name, \
                    CAST(table_type AS CHAR) AS table_type FROM information_schema.tables \
             WHERE (? IS NULL OR table_schema = ?) AND table_schema NOT IN \
             ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY table_schema, table_name",
        )
        .bind(selected)
        .bind(selected)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let routines = sqlx::query(
            "SELECT CAST(routine_schema AS CHAR) AS routine_schema, \
                    CAST(routine_name AS CHAR) AS routine_name, \
                    CAST(routine_type AS CHAR) AS routine_type FROM information_schema.routines \
             WHERE (? IS NULL OR routine_schema = ?) AND routine_schema NOT IN \
             ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY routine_schema, routine_name",
        )
        .bind(selected)
        .bind(selected)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        pool.close().await;
        let mut objects: Vec<DatabaseObject> = tables
            .into_iter()
            .map(|row| DatabaseObject {
                schema: Some(row.get("table_schema")),
                name: row.get("table_name"),
                kind: if row.get::<String, _>("table_type") == "VIEW" {
                    DatabaseObjectKind::View
                } else {
                    DatabaseObjectKind::Table
                },
            })
            .collect();
        objects.extend(routines.into_iter().map(|row| DatabaseObject {
            schema: Some(row.get("routine_schema")),
            name: row.get("routine_name"),
            kind: if row.get::<String, _>("routine_type") == "PROCEDURE" {
                DatabaseObjectKind::Procedure
            } else {
                DatabaseObjectKind::Function
            },
        }));
        Ok(with_schemas(objects, schema.is_none()))
    }

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError> {
        let schema_name = object.schema.as_deref().or(endpoint.database.as_deref());
        let pool = mysql_pool(endpoint, secret).await?;
        let column_rows = sqlx::query(
            "SELECT CAST(column_name AS CHAR) AS column_name, \
                    CAST(column_type AS CHAR) AS column_type, \
                    CAST(is_nullable AS CHAR) AS is_nullable, \
                    ordinal_position AS ordinal_position, \
                    CAST(column_default AS CHAR) AS column_default \
             FROM information_schema.columns WHERE table_schema = ? AND table_name = ? \
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let key_rows = sqlx::query(
            "SELECT CAST(tc.constraint_name AS CHAR) AS constraint_name, \
                    CAST(tc.constraint_type AS CHAR) AS constraint_type, \
                    CAST(GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) AS CHAR) AS columns \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON tc.constraint_name = kcu.constraint_name \
              AND tc.table_schema = kcu.table_schema \
              AND tc.table_name = kcu.table_name \
             WHERE tc.table_schema = ? AND tc.table_name = ? \
             GROUP BY tc.constraint_name, tc.constraint_type ORDER BY tc.constraint_type",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let index_rows = sqlx::query(
            "SELECT CAST(index_name AS CHAR) AS index_name, non_unique AS non_unique, \
                    CAST(GROUP_CONCAT(column_name ORDER BY seq_in_index) AS CHAR) AS columns \
             FROM information_schema.statistics WHERE table_schema = ? AND table_name = ? \
             GROUP BY index_name, non_unique ORDER BY index_name",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        pool.close().await;
        Ok(ObjectDescription {
            object: object.clone(),
            columns: column_rows
                .into_iter()
                .map(|row| ColumnMetadata {
                    name: row.get("column_name"),
                    data_type: row.get("column_type"),
                    nullable: row.get::<String, _>("is_nullable") == "YES",
                    ordinal: row
                        .try_get::<u64, _>("ordinal_position")
                        .map(|value| value as u32)
                        .or_else(|_| row.try_get::<u32, _>("ordinal_position"))
                        .unwrap_or(0),
                    default_value: row.try_get("column_default").ok(),
                })
                .collect(),
            keys: key_rows
                .into_iter()
                .map(|row| KeyMetadata {
                    name: row.get("constraint_name"),
                    kind: row.get("constraint_type"),
                    columns: split_columns(row.try_get::<String, _>("columns").ok()),
                })
                .collect(),
            indexes: index_rows
                .into_iter()
                .map(|row| IndexMetadata {
                    name: row.get("index_name"),
                    columns: split_columns(row.try_get::<String, _>("columns").ok()),
                    unique: row
                        .try_get::<i64, _>("non_unique")
                        .or_else(|_| row.try_get::<i32, _>("non_unique").map(i64::from))
                        .unwrap_or(1)
                        == 0,
                })
                .collect(),
        })
    }

    fn compile_query(&self, specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        let mut compiled =
            crate::sql_builder::compile(specification, crate::sql_builder::Dialect::MySql)?;
        // Rows become JSON objects in the runtime; no SQL wrapper is required.
        compiled.processor_type = Some("query_mysql".to_owned());
        compiled.execution_statement = Some(compiled.statement.clone());
        Ok(compiled)
    }
}

/// Pool de una conexión para probar o explorar.
pub(in crate::connection_api) async fn mysql_pool(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<sqlx::MySqlPool, PluginError> {
    let mut options = MySqlConnectOptions::new()
        .host(&endpoint.host)
        .port(endpoint.port)
        .username(&secret.username)
        .password(&secret.password)
        .ssl_mode(if endpoint.ssl {
            MySqlSslMode::Required
        } else {
            MySqlSslMode::Disabled
        });
    if let Some(database) = endpoint.database.as_deref() {
        options = options.database(database);
    }
    MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
        .connect_with(options)
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))
}
