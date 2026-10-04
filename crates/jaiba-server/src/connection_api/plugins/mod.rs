//! Un plugin `ConnectionPlugin` por motor de base de datos y los helpers que
//! comparten (descriptor, diagnóstico y armado de metadatos).

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use jaiba_plugin_sdk::{
    Availability, ConnectionTestResult, DatabaseObject, DatabaseObjectKind, PluginDescriptor,
    PluginError, PoolStatus,
};
#[cfg(any(
    feature = "mongodb-driver",
    feature = "oracle-driver",
    feature = "sqlserver-driver"
))]
use jaiba_plugin_sdk::{ColumnMetadata, ObjectDescription};

#[cfg(feature = "clickhouse-driver")]
pub(super) mod clickhouse;
#[cfg(feature = "mongodb-driver")]
pub(super) mod mongodb;
pub(super) mod mysql;
#[cfg(feature = "oracle-driver")]
pub(super) mod oracle;
pub(super) mod postgres;
#[cfg(feature = "sqlserver-driver")]
pub(super) mod sqlserver;

fn exploration_error(error: sqlx::Error) -> PluginError {
    PluginError::Exploration(error.to_string())
}

#[cfg(any(
    feature = "mongodb-driver",
    feature = "oracle-driver",
    feature = "sqlserver-driver"
))]
fn description(object: &DatabaseObject, columns: Vec<ColumnMetadata>) -> ObjectDescription {
    ObjectDescription {
        object: object.clone(),
        columns,
        keys: Vec::new(),
        indexes: Vec::new(),
    }
}

/// Divide una lista de columnas separadas por comas (proveniente de `string_agg` /
/// `GROUP_CONCAT`) en nombres individuales, descartando entradas vacías.
fn split_columns(value: Option<String>) -> Vec<String> {
    value
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn with_schemas(mut objects: Vec<DatabaseObject>, include: bool) -> Vec<DatabaseObject> {
    if !include {
        return objects;
    }
    let mut schemas = objects
        .iter()
        .filter_map(|object| object.schema.clone())
        .collect::<Vec<_>>();
    schemas.sort();
    schemas.dedup();
    let mut result = schemas
        .into_iter()
        .map(|name| DatabaseObject {
            schema: None,
            name,
            kind: DatabaseObjectKind::Schema,
        })
        .collect::<Vec<_>>();
    result.append(&mut objects);
    result
}

fn descriptor(
    id: &str,
    name: &str,
    default_port: u16,
    query_builder: bool,
    query_node: bool,
) -> PluginDescriptor {
    let mut capabilities = vec![
        "test".to_owned(),
        "diagnostics".to_owned(),
        "schema_explorer".to_owned(),
    ];
    if query_builder {
        capabilities.push("query_builder".to_owned());
    }
    if query_node {
        capabilities.push("query_node".to_owned());
    }
    PluginDescriptor {
        id: id.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        display_name: name.to_owned(),
        category: "SQL".to_owned(),
        default_port,
        capabilities,
    }
}

fn success(
    started: Instant,
    version: String,
    active: u32,
    idle: u32,
    maximum: u32,
) -> ConnectionTestResult {
    ConnectionTestResult {
        availability: Availability::Available,
        latency_ms: started.elapsed().as_millis() as u64,
        version: Some(version),
        pool: Some(PoolStatus {
            active,
            idle,
            maximum,
        }),
        tested_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
        message: Some("Conexión validada".to_owned()),
    }
}
