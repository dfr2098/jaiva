use std::{collections::HashMap, sync::Arc};

use super::{CircuitBreakers, ConnectionManager, FlowMetrics, StateStore};

#[derive(Debug, Clone)]
pub struct ProcessorContext {
    pub flow_id: String,
    pub processor_id: String,
    pub parameters: Arc<HashMap<String, String>>,
    pub connections: ConnectionManager,
    pub metrics: FlowMetrics,
    pub state: StateStore,
    pub circuits: CircuitBreakers,
}
