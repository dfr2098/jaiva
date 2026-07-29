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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

/// A join between the main source and another table. `left`/`right` are the
/// columns compared with equality; the plugin validates and quotes both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryJoin {
    pub kind: JoinKind,
    pub source: QuerySource,
    pub left: String,
    pub right: String,
}

/// Neutral query generated by the UI. A database plugin is responsible for
/// quoting identifiers and producing bound parameters for its dialect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub source: QuerySource,
    pub columns: Vec<String>,
    #[serde(default)]
    pub joins: Vec<QueryJoin>,
    #[serde(default)]
    pub filters: Vec<QueryFilter>,
    #[serde(default)]
    pub group_by: Vec<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PluginPacket {
    Records(Vec<Value>),
    Bytes {
        content: Vec<u8>,
        media_type: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Versioned JSON protocol used across the process boundary for external
/// plugins. Each envelope is encoded as one JSON line; no Rust ABI is exposed.
pub mod isolated {
    use std::{
        io::{BufRead, BufReader, Write},
        process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    };

    use serde::{Deserialize, Serialize, de::DeserializeOwned};
    use serde_json::Value;

    use super::{PluginDescriptor, PluginExecutionContext, PluginPacket};

    pub const PROTOCOL_VERSION: u16 = 1;
    pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "snake_case")]
    pub enum RequestOperation {
        Describe,
        Execute,
        Shutdown,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PluginRequest {
        pub protocol_version: u16,
        pub request_id: String,
        pub operation: RequestOperation,
        #[serde(default)]
        pub configuration: Value,
        pub packet: Option<PluginPacket>,
        pub context: Option<PluginExecutionContext>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PluginResponse {
        pub protocol_version: u16,
        pub request_id: String,
        pub result: Option<PluginResult>,
        pub error: Option<ProtocolError>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    pub enum PluginResult {
        Descriptor { descriptor: PluginDescriptor },
        Packets { packets: Vec<PluginPacket> },
        Shutdown,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ProtocolError {
        pub code: String,
        pub message: String,
        #[serde(default)]
        pub retryable: bool,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum FrameError {
        #[error("isolated plugin frame exceeds {MAX_FRAME_BYTES} bytes")]
        TooLarge,
        #[error("isolated plugin I/O failed: {0}")]
        Io(#[from] std::io::Error),
        #[error("isolated plugin sent invalid JSON: {0}")]
        Json(#[from] serde_json::Error),
        #[error("unsupported plugin protocol version {received}; expected {PROTOCOL_VERSION}")]
        Version { received: u16 },
        #[error("plugin response must contain exactly one of result or error")]
        InvalidResponse,
        #[error("plugin response id '{received}' does not match request '{expected}'")]
        MismatchedRequest { expected: String, received: String },
    }

    pub trait VersionedEnvelope {
        fn protocol_version(&self) -> u16;
    }

    impl VersionedEnvelope for PluginRequest {
        fn protocol_version(&self) -> u16 {
            self.protocol_version
        }
    }

    impl VersionedEnvelope for PluginResponse {
        fn protocol_version(&self) -> u16 {
            self.protocol_version
        }
    }

    pub fn write_frame<T: Serialize>(
        writer: &mut impl Write,
        message: &T,
    ) -> Result<(), FrameError> {
        let encoded = serde_json::to_vec(message)?;
        if encoded.len() > MAX_FRAME_BYTES {
            return Err(FrameError::TooLarge);
        }
        writer.write_all(&encoded)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    pub fn read_frame<T: DeserializeOwned + VersionedEnvelope>(
        reader: &mut impl BufRead,
    ) -> Result<Option<T>, FrameError> {
        let mut encoded = Vec::new();
        loop {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                break;
            }
            let chunk_length = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |position| position + 1);
            if encoded.len() + chunk_length > MAX_FRAME_BYTES + 1 {
                return Err(FrameError::TooLarge);
            }
            encoded.extend_from_slice(&available[..chunk_length]);
            reader.consume(chunk_length);
            if encoded.ends_with(b"\n") {
                break;
            }
        }
        if encoded.is_empty() {
            return Ok(None);
        }
        if encoded.len() > MAX_FRAME_BYTES + 1 || !encoded.ends_with(b"\n") {
            return Err(FrameError::TooLarge);
        }
        encoded.pop();
        let message: T = serde_json::from_slice(&encoded)?;
        if message.protocol_version() != PROTOCOL_VERSION {
            return Err(FrameError::Version {
                received: message.protocol_version(),
            });
        }
        Ok(Some(message))
    }

    pub fn validate_response(response: &PluginResponse) -> Result<(), FrameError> {
        if response.result.is_some() == response.error.is_some() {
            return Err(FrameError::InvalidResponse);
        }
        Ok(())
    }

    /// Host-side transport for an executable plugin connected only through
    /// stdin/stdout. The caller chooses the executable from a trusted catalog.
    pub struct IsolatedPluginProcess {
        child: Child,
        stdin: ChildStdin,
        stdout: BufReader<ChildStdout>,
    }

    impl IsolatedPluginProcess {
        pub fn spawn(executable: &str, arguments: &[String]) -> Result<Self, FrameError> {
            let mut child = Command::new(executable)
                .args(arguments)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?;
            let stdin = child.stdin.take().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "plugin stdin unavailable")
            })?;
            let stdout = child.stdout.take().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "plugin stdout unavailable")
            })?;
            Ok(Self {
                child,
                stdin,
                stdout: BufReader::new(stdout),
            })
        }

        pub fn transact(&mut self, request: &PluginRequest) -> Result<PluginResponse, FrameError> {
            write_frame(&mut self.stdin, request)?;
            let response = read_frame::<PluginResponse>(&mut self.stdout)?.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "plugin stopped before responding",
                )
            })?;
            validate_response(&response)?;
            if response.request_id != request.request_id {
                return Err(FrameError::MismatchedRequest {
                    expected: request.request_id.clone(),
                    received: response.request_id,
                });
            }
            Ok(response)
        }

        pub fn wait(mut self) -> Result<std::process::ExitStatus, FrameError> {
            Ok(self.child.wait()?)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::{
        ConnectionType,
        isolated::{
            FrameError, PROTOCOL_VERSION, PluginRequest, RequestOperation, read_frame, write_frame,
        },
    };
    use serde_json::json;

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

    #[test]
    fn isolated_protocol_round_trips_one_bounded_json_frame() {
        let request = PluginRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "request-1".to_owned(),
            operation: RequestOperation::Execute,
            configuration: json!({"batch": 10}),
            packet: None,
            context: None,
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let decoded = read_frame::<PluginRequest>(&mut BufReader::new(Cursor::new(bytes)))
            .unwrap()
            .unwrap();
        assert_eq!(decoded.request_id, "request-1");
        assert_eq!(decoded.operation, RequestOperation::Execute);
    }

    #[test]
    fn isolated_protocol_rejects_an_incompatible_version() {
        let bytes = br#"{"protocol_version":99,"request_id":"x","operation":"describe","configuration":{},"packet":null,"context":null}
"#;
        assert!(matches!(
            read_frame::<PluginRequest>(&mut BufReader::new(Cursor::new(bytes))),
            Err(FrameError::Version { received: 99 })
        ));
    }
}
