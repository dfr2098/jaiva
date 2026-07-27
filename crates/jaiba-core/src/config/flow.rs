use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct FlowConfig {
    pub id: String,
    #[serde(default)]
    pub parameters: HashMap<String, String>,
    #[serde(default)]
    pub database_connections: HashMap<String, DatabaseConnectionConfig>,
    #[serde(default)]
    pub kafka_connections: HashMap<String, KafkaConnectionConfig>,
    #[serde(default)]
    pub engine: EngineConfig,
    pub processors: Vec<ProcessorConfig>,
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConnectionConfig {
    pub brokers_env: String,
    #[serde(default = "default_kafka_client_id")]
    pub client_id: String,
    #[serde(default = "default_kafka_security_protocol")]
    pub security_protocol: String,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    pub sasl_username_env: Option<String>,
    pub sasl_password_env: Option<String>,
    #[serde(default = "default_kafka_message_timeout")]
    pub message_timeout_ms: u64,
}

fn default_kafka_client_id() -> String {
    "jaiva".to_owned()
}
fn default_kafka_security_protocol() -> String {
    "PLAINTEXT".to_owned()
}
fn default_kafka_message_timeout() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConnectionConfig {
    #[serde(rename = "type")]
    pub connection_type: String,
    pub url_env: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_state_file")]
    pub state_file: PathBuf,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub repository: RepositoryConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub workers: WorkerConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: default_queue_capacity(),
            max_concurrency: default_concurrency(),
            state_file: default_state_file(),
            memory: MemoryConfig::default(),
            repository: RepositoryConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            admin: AdminConfig::default(),
            workers: WorkerConfig::default(),
        }
    }
}

/// Limits for work that must not occupy Tokio's asynchronous I/O workers.
///
/// A value of zero selects an automatic limit based on the logical CPUs that
/// are visible to this process (and therefore respects container CPU limits).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WorkerConfig {
    #[serde(default)]
    pub cpu_threads: usize,
    #[serde(default)]
    pub blocking_threads: usize,
}

impl WorkerConfig {
    pub fn resolved(&self) -> ResolvedWorkerConfig {
        let available = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        ResolvedWorkerConfig {
            available_parallelism: available,
            cpu_threads: if self.cpu_threads == 0 {
                (available / 2).max(1)
            } else {
                self.cpu_threads
            },
            blocking_threads: if self.blocking_threads == 0 {
                (available / 4).max(2).min(available.max(1))
            } else {
                self.blocking_threads
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedWorkerConfig {
    pub available_parallelism: usize,
    pub cpu_threads: usize,
    pub blocking_threads: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShutdownConfig {
    #[serde(default = "default_drain_timeout")]
    pub drain_timeout_seconds: u64,
    #[serde(default = "default_true")]
    pub force_after_timeout: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout_seconds: default_drain_timeout(),
            force_after_timeout: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_circuit_open_seconds")]
    pub open_seconds: u64,
    #[serde(default = "default_half_open_requests")]
    pub half_open_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: default_failure_threshold(),
            open_seconds: default_circuit_open_seconds(),
            half_open_requests: default_half_open_requests(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdminConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub authentication: AdminAuthentication,
    #[serde(default = "default_admin_token_env")]
    pub token_env: String,
    #[serde(default = "default_request_limit")]
    pub max_request_body_bytes: usize,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            authentication: AdminAuthentication::Bearer,
            token_env: default_admin_token_env(),
            max_request_body_bytes: default_request_limit(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAuthentication {
    None,
    #[default]
    Bearer,
}

fn default_drain_timeout() -> u64 {
    60
}
fn default_failure_threshold() -> u32 {
    5
}
fn default_circuit_open_seconds() -> u64 {
    30
}
fn default_half_open_requests() -> u32 {
    1
}
fn default_admin_token_env() -> String {
    "JAIBA_ADMIN_TOKEN".to_owned()
}
fn default_request_limit() -> usize {
    1024 * 1024
}

/// Persistent execution log settings.
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_log_directory")]
    pub directory: PathBuf,
    #[serde(default)]
    pub rotation: LogRotation,
    #[serde(default = "default_log_retention")]
    pub retention_hours: u64,
    #[serde(default = "default_log_cleanup_interval")]
    pub cleanup_interval_seconds: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            directory: default_log_directory(),
            rotation: LogRotation::Daily,
            retention_hours: default_log_retention(),
            cleanup_interval_seconds: default_log_cleanup_interval(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRotation {
    Hourly,
    #[default]
    Daily,
    Never,
}

fn default_true() -> bool {
    true
}
fn default_log_directory() -> PathBuf {
    ".jaiva/logs".into()
}
fn default_log_retention() -> u64 {
    24 * 30
}
fn default_log_cleanup_interval() -> u64 {
    3600
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_repository_database")]
    pub database_path: PathBuf,
    #[serde(default = "default_content_path")]
    pub content_path: PathBuf,
    #[serde(default = "default_abandoned_seconds")]
    pub abandoned_after_seconds: u64,
    #[serde(default = "default_completed_retention")]
    pub completed_retention_hours: u64,
    #[serde(default = "default_provenance_retention")]
    pub provenance_retention_hours: u64,
}

impl Default for RepositoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            database_path: default_repository_database(),
            content_path: default_content_path(),
            abandoned_after_seconds: default_abandoned_seconds(),
            completed_retention_hours: default_completed_retention(),
            provenance_retention_hours: default_provenance_retention(),
        }
    }
}

fn default_repository_database() -> PathBuf {
    ".jaiva/repository.db".into()
}

fn default_content_path() -> PathBuf {
    ".jaiva/content".into()
}

fn default_abandoned_seconds() -> u64 {
    0
}

fn default_completed_retention() -> u64 {
    24
}

fn default_provenance_retention() -> u64 {
    24 * 90
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    #[serde(default = "default_memory_percent")]
    pub maximum_percent: u8,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            maximum_percent: default_memory_percent(),
        }
    }
}

fn default_memory_percent() -> u8 {
    70
}

fn default_queue_capacity() -> usize {
    100
}

fn default_concurrency() -> usize {
    4
}

fn default_state_file() -> PathBuf {
    ".jaiva/state.json".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessorConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub processor_type: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub scheduling: SchedulingConfig,
    #[serde(default)]
    pub simulation: SimulationConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SimulationConfig {
    #[serde(default)]
    pub mode: DataExecutionMode,
    #[serde(default)]
    pub options: Value,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            mode: DataExecutionMode::Real,
            options: Value::Object(Default::default()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataExecutionMode {
    #[default]
    Real,
    Mock,
    Replay,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    #[serde(default)]
    pub maximum_attempts: u32,
    #[serde(default = "default_retry_delay")]
    pub initial_delay_ms: u64,
    #[serde(default = "default_retry_max_delay")]
    pub maximum_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            maximum_attempts: 0,
            initial_delay_ms: default_retry_delay(),
            maximum_delay_ms: default_retry_max_delay(),
        }
    }
}

fn default_retry_delay() -> u64 {
    250
}

fn default_retry_max_delay() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulingConfig {
    #[serde(default = "default_concurrent_tasks")]
    pub concurrent_tasks: usize,
    pub maximum_in_flight: Option<usize>,
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    #[serde(default)]
    pub ordering: OrderingMode,
    pub partition_by: Option<String>,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            concurrent_tasks: default_concurrent_tasks(),
            maximum_in_flight: None,
            timeout_ms: None,
            execution_mode: ExecutionMode::Auto,
            ordering: OrderingMode::Unordered,
            partition_by: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    #[default]
    Auto,
    AsyncIo,
    BlockingIo,
    Cpu,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    #[default]
    Unordered,
    Preserve,
    Partitioned,
}

fn default_concurrent_tasks() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionConfig {
    pub from: String,
    pub relationship: String,
    pub to: String,
    #[serde(default)]
    pub queue: QueueConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    #[serde(default = "default_queue_capacity")]
    pub capacity: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            capacity: default_queue_capacity(),
        }
    }
}

impl FlowConfig {
    pub fn processor_map(&self) -> HashMap<&str, &ProcessorConfig> {
        self.processors
            .iter()
            .map(|processor| (processor.id.as_str(), processor))
            .collect()
    }
}
