use std::{fs, path::PathBuf};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

pub struct WriteFile {
    path: PathBuf,
}

#[derive(Deserialize)]
struct WriteFileConfig {
    path: PathBuf,
}

impl WriteFile {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: WriteFileConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        Ok(Self { path: config.path })
    }
}

#[async_trait]
impl Processor for WriteFile {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::BlockingIo
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let bytes = match &packet.content {
            PacketContent::Encoded { bytes, .. } => bytes,
            PacketContent::Records(_) => {
                return Err(FlowError::Processor {
                    processor_id: context.processor_id.clone(),
                    message: "write_file requires encoded content".to_owned(),
                });
            }
        };
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, bytes)?;
        output.success(packet).await
    }
}
