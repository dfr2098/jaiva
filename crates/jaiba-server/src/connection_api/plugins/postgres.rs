//! Plugin de conexión PostgreSQL (sqlx).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    Availability, ColumnMetadata, CompiledQuery, ConnectionEndpoint, ConnectionPlugin,
    ConnectionSecret, ConnectionTestResult, ConnectionType, DatabaseObject, DatabaseObjectKind,
    DiagnosticCheck, IndexMetadata, KeyMetadata, ObjectDescription, PluginDescriptor, PluginError,
    QuerySpec,
};
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};

use super::{descriptor, exploration_error, split_columns, success, with_schemas};

/// PostgreSQL: prueba, diagnóstico, explorador y compilación de `QuerySpec`.
pub(in crate::connection_api) struct PostgresConnectionPlugin;

#[async_trait]
impl ConnectionPlugin for PostgresConnectionPlugin {
    fn descriptor(&self) -> PluginDescriptor {
        descriptor("jaiba.postgres", "PostgreSQL", 5432, true, true)
    }

    fn connection_type(&self) -> ConnectionType {
        ConnectionType::Postgres
    }

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError> {
        let started = Instant::now();
        let options = PgConnectOptions::new()
            .host(&endpoint.host)
            .port(endpoint.port)
            .username(&secret.username)
            .password(&secret.password)
            .database(endpoint.database.as_deref().unwrap_or("postgres"))
            .ssl_mode(if endpoint.ssl {
                PgSslMode::Require
            } else {
                PgSslMode::Disable
            });
        let pool = PgPoolOptions::new()
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
        let pool = postgres_pool(endpoint, secret).await?;
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
             WHERE table_schema = current_schema()",
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
        let pool = postgres_pool(endpoint, secret).await?;
        let tables = sqlx::query(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables \
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
             AND ($1::text IS NULL OR table_schema = $1) ORDER BY table_schema, table_name",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let sequences = sqlx::query(
            "SELECT sequence_schema, sequence_name FROM information_schema.sequences \
             WHERE ($1::text IS NULL OR sequence_schema = $1) \
             ORDER BY sequence_schema, sequence_name",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let routines = sqlx::query(
            "SELECT routine_schema, routine_name, routine_type FROM information_schema.routines \
             WHERE routine_schema NOT IN ('pg_catalog', 'information_schema') \
             AND ($1::text IS NULL OR routine_schema = $1) ORDER BY routine_schema, routine_name",
        )
        .bind(schema)
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
        objects.extend(sequences.into_iter().map(|row| DatabaseObject {
            schema: Some(row.get("sequence_schema")),
            name: row.get("sequence_name"),
            kind: DatabaseObjectKind::Sequence,
        }));
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
        let schema_name = object.schema.as_deref().unwrap_or("public");
        let pool = postgres_pool(endpoint, secret).await?;
        let column_rows = sqlx::query(
            "SELECT column_name, data_type, is_nullable, ordinal_position, column_default \
             FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let key_rows = sqlx::query(
            "SELECT tc.constraint_name, tc.constraint_type, \
                    string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) AS columns \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON tc.constraint_name = kcu.constraint_name \
              AND tc.table_schema = kcu.table_schema \
             WHERE tc.table_schema = $1 AND tc.table_name = $2 \
               AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE') \
             GROUP BY tc.constraint_name, tc.constraint_type ORDER BY tc.constraint_type",
        )
        .bind(schema_name)
        .bind(&object.name)
        .fetch_all(&pool)
        .await
        .map_err(exploration_error)?;
        let index_rows = sqlx::query(
            "SELECT i.relname AS index_name, ix.indisunique AS is_unique, \
                    array_to_string(array_agg(a.attname ORDER BY x.ord), ',') AS columns \
             FROM pg_index ix \
             JOIN pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_class t ON t.oid = ix.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             JOIN unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ord) ON true \
             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum \
             WHERE n.nspname = $1 AND t.relname = $2 \
             GROUP BY i.relname, ix.indisunique ORDER BY i.relname",
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
                    data_type: row.get("data_type"),
                    nullable: row.get::<String, _>("is_nullable") == "YES",
                    ordinal: row.get::<i32, _>("ordinal_position") as u32,
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
                    unique: row.get::<bool, _>("is_unique"),
                })
                .collect(),
        })
    }

    fn compile_query(&self, specification: &QuerySpec) -> Result<CompiledQuery, PluginError> {
        let mut compiled =
            crate::sql_builder::compile(specification, crate::sql_builder::Dialect::Postgres)?;
        compiled.processor_type = Some("query_postgres".to_owned());
        compiled.execution_statement = Some(format!(
            "SELECT to_jsonb(t) AS record FROM (\n{}\n) AS t",
            compiled.statement
        ));
        Ok(compiled)
    }
}

/// Pool de una conexión para probar o explorar; sin base usa `postgres`.
pub(in crate::connection_api) async fn postgres_pool(
    endpoint: &ConnectionEndpoint,
    secret: &ConnectionSecret,
) -> Result<sqlx::PgPool, PluginError> {
    let options = PgConnectOptions::new()
        .host(&endpoint.host)
        .port(endpoint.port)
        .username(&secret.username)
        .password(&secret.password)
        .database(endpoint.database.as_deref().unwrap_or("postgres"))
        .ssl_mode(if endpoint.ssl {
            PgSslMode::Require
        } else {
            PgSslMode::Disable
        });
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(endpoint.timeout_ms))
        .connect_with(options)
        .await
        .map_err(|error| PluginError::Connection(error.to_string()))
}
