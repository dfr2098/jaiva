use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlowError {
    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("processor '{processor_id}' failed: {message}")]
    Processor {
        processor_id: String,
        message: String,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),

    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error("flow queue reached its configured capacity of {capacity} packets")]
    Backpressure { capacity: usize },

    #[error("server error: {0}")]
    Server(String),

    #[error("processor output channel closed")]
    ChannelClosed,

    #[error(
        "packet requires {packet_bytes} bytes but the configured memory budget is {budget_bytes}"
    )]
    PacketTooLarge {
        packet_bytes: u64,
        budget_bytes: u64,
    },

    #[error(
        "packet memory capacity exhausted while reserving {packet_bytes} bytes; reduce packet size, branching or concurrent flows, or increase the budget"
    )]
    MemoryCapacity { packet_bytes: u64 },

    #[error("repository error: {0}")]
    Repository(String),

    #[error("database connector error: {0}")]
    DatabaseConnector(String),

    /// The database refused the statement or its data (syntax, constraint,
    /// type). Repeating the same packet cannot succeed.
    #[error("database rejected the request: {0}")]
    DatabaseRejected(String),

    #[error("message connector error: {0}")]
    MessageConnector(String),

    #[error("circuit breaker '{connection}' is open")]
    CircuitOpen { connection: String },
}

impl FlowError {
    /// Whether the engine should retry the packet after this error.
    pub fn is_retryable(&self) -> bool {
        !matches!(
            self,
            Self::PacketTooLarge { .. } | Self::DatabaseRejected(_)
        )
    }
}
