use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub struct RenameFields {
    fields: HashMap<String, String>,
}

#[derive(Deserialize)]
struct RenameConfig {
    fields: HashMap<String, String>,
}

impl RenameFields {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: RenameConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        Ok(Self {
            fields: config.fields,
        })
    }
}

#[async_trait]
impl Processor for RenameFields {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet
            .records_mut()
            .map_err(|message| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message,
            })?;
        for record in records {
            let object = record.as_object_mut().ok_or_else(|| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: "rename_fields expects JSON objects".to_owned(),
            })?;

            for (old_name, new_name) in &self.fields {
                if let Some(value) = object.remove(old_name) {
                    object.insert(new_name.clone(), value);
                }
            }
        }

        output.success(packet).await
    }
}
