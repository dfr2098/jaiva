use std::{collections::HashMap, sync::Arc};

use super::{CircuitBreakers, ConnectionManager, DomainMemoryHandle, FlowMetrics, StateStore};

#[derive(Debug, Clone)]
pub struct ProcessorContext {
    pub flow_id: String,
    pub processor_id: String,
    pub parameters: Arc<HashMap<String, String>>,
    pub connections: ConnectionManager,
    pub metrics: FlowMetrics,
    pub state: StateStore,
    pub circuits: CircuitBreakers,
    /// Jaiba Memory Engine (None si `engine.domain_memory.enabled` es false).
    pub domain_memory: Option<DomainMemoryHandle>,
}
