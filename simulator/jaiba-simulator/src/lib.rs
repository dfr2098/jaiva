//! Selección de fuentes de datos para simulación sin alterar el DAG.

use async_trait::async_trait;
pub use jaiba_core::config::DataExecutionMode as ExecutionMode;
use jaiba_plugin_sdk::PluginPacket;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationRequest {
    pub flow_id: String,
    pub processor_id: String,
    pub mode: ExecutionMode,
    #[serde(default)]
    pub options: Value,
}

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("mode {0:?} is not configured")]
    MissingProvider(ExecutionMode),
    #[error("simulation source failed: {0}")]
    Source(String),
}

#[async_trait]
pub trait SimulationSource: Send + Sync {
    fn mode(&self) -> ExecutionMode;

    async fn packets(
        &self,
        request: &SimulationRequest,
    ) -> Result<Vec<PluginPacket>, SimulationError>;
}

/// A replay reference points at provenance; packet content remains in the
/// runtime repository and is never embedded in the flow YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReference {
    pub source_flow_id: String,
    pub packet_ids: Vec<String>,
}
