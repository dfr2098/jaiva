use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub struct GenerateRecords {
    records: Vec<Value>,
}

#[derive(Deserialize)]
struct GenerateConfig {
    #[serde(default)]
    records: Vec<Value>,
}

impl GenerateRecords {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: GenerateConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        Ok(Self {
            records: config.records,
        })
    }
}

#[async_trait]
impl Processor for GenerateRecords {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        _context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        packet.content = crate::engine::PacketContent::Records(self.records.clone());
        packet
            .attributes
            .insert("record.count".to_owned(), self.records.len().to_string());
        output.success(packet).await
    }
}
