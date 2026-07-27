//! Contratos sin dependencias del runtime para extensiones de Jaiba.
//!
//! Los plugins oficiales pueden compilarse como crates. Los plugins externos
//! podrán implementar estos mismos contratos mediante un adaptador de proceso
//! aislado o WebAssembly, evitando depender de la ABI de Rust.

use std::{collections::BTreeMap, fmt};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("plugin configuration error: {0}")]
    Configuration(String),
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("diagnostic failed: {0}")]
    Diagnostic(String),
    #[error("schema exploration failed: {0}")]
    Exploration(String),
    #[error("processor failed: {0}")]
    Processing(String),
    #[error("capability '{0}' is not supported")]
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    Oracle,
    Postgres,
    SqlServer,
    #[serde(rename = "mysql")]
    MySql,
    MariaDb,
    Kafka,
    OpcUa,
    Rest,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionEndpoint {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    #[serde(default)]
    pub ssl: bool,
    #[serde(default = "default_pool_min")]
    pub pool_min: u32,
    #[serde(default = "default_pool_max")]
    pub pool_max: u32,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

fn default_pool_min() -> u32 {
    1
}

fn default_pool_max() -> u32 {
    10
}

fn default_timeout() -> u64 {
    10_000
}

/// Secret resolved by a server-side SecretStore. It is intentionally not
/// serializable and its Debug implementation never reveals values.
#[derive(Clone)]
pub struct ConnectionSecret {
    pub username: String,
    pub password: String,
    pub options: BTreeMap<String, String>,
}

impl fmt::Debug for ConnectionSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionSecret")
            .field("username", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .field("options", &self.options.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Unknown,
    Testing,
    Available,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatus {
    pub active: u32,
    pub idle: u32,
    pub maximum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTestResult {
    pub availability: Availability,
    pub latency_ms: u64,
    pub version: Option<String>,
    pub pool: Option<PoolStatus>,
    pub tested_at: i64,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticCheck {
    pub code: String,
    pub label: String,
    pub status: Availability,
    pub latency_ms: Option<u64>,
    pub details: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseObjectKind {
    Schema,
    Table,
    View,
    Procedure,
    Function,
    Sequence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseObject {
    pub schema: Option<String>,
    pub name: String,
    pub kind: DatabaseObjectKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal: u32,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    pub name: String,
    pub kind: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDescription {
    pub object: DatabaseObject,
    pub columns: Vec<ColumnMetadata>,
    pub keys: Vec<KeyMetadata>,
    pub indexes: Vec<IndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySource {
    pub schema: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Eq,
    NotEq,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    Contains,
    StartsWith,
    In,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOrder {
    pub field: String,
    pub direction: SortDirection,
}

/// Neutral query generated by the UI. A database plugin is responsible for
/// quoting identifiers and producing bound parameters for its dialect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub source: QuerySource,
    pub columns: Vec<String>,
    #[serde(default)]
    pub filters: Vec<QueryFilter>,
    #[serde(default)]
    pub order_by: Vec<QueryOrder>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledQuery {
    pub statement: String,
    pub parameters: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDescriptor {
    pub id: String,
    pub version: String,
    pub display_name: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum PluginPacket {
    Records(Vec<Value>),
    Bytes {
        content: Vec<u8>,
        media_type: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct PluginExecutionContext {
    pub flow_id: String,
    pub processor_id: String,
    pub attributes: BTreeMap<String, String>,
}

#[async_trait]
pub trait ConnectionPlugin: Send + Sync {
    fn descriptor(&self) -> PluginDescriptor;
    fn connection_type(&self) -> ConnectionType;

    async fn test(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<ConnectionTestResult, PluginError>;

    async fn diagnose(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
    ) -> Result<Vec<DiagnosticCheck>, PluginError>;

    async fn list_objects(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        schema: Option<&str>,
    ) -> Result<Vec<DatabaseObject>, PluginError>;

    async fn describe_object(
        &self,
        endpoint: &ConnectionEndpoint,
        secret: &ConnectionSecret,
        object: &DatabaseObject,
    ) -> Result<ObjectDescription, PluginError>;

    fn compile_query(&self, specification: &QuerySpec) -> Result<CompiledQuery, PluginError>;
}

#[async_trait]
pub trait ProcessorPlugin: Send + Sync {
    fn descriptor(&self) -> PluginDescriptor;

    async fn execute(
        &self,
        configuration: &Value,
        packet: PluginPacket,
        context: &PluginExecutionContext,
    ) -> Result<Vec<PluginPacket>, PluginError>;
}

#[cfg(test)]
mod tests {
    use super::ConnectionType;

    #[test]
    fn mysql_uses_the_public_api_identifier() {
        assert_eq!(
            serde_json::to_string(&ConnectionType::MySql).unwrap(),
            "\"mysql\""
        );
        assert_eq!(
            serde_json::from_str::<ConnectionType>("\"mysql\"").unwrap(),
            ConnectionType::MySql
        );
    }
}
