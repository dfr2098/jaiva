use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub struct LoadCheckpoint {
    key: String,
    attribute: String,
    default: Option<String>,
}

pub struct SaveCheckpoint {
    key: String,
    attribute: String,
}

#[derive(Deserialize)]
struct LoadConfig {
    key: String,
    #[serde(default = "default_attribute")]
    attribute: String,
    default: Option<String>,
}

#[derive(Deserialize)]
struct SaveConfig {
    key: String,
    #[serde(default = "default_attribute")]
    attribute: String,
}

fn default_attribute() -> String {
    "checkpoint.value".to_owned()
}

impl LoadCheckpoint {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: LoadConfig = parse(value)?;
        Ok(Self {
            key: config.key,
            attribute: config.attribute,
            default: config.default,
        })
    }
}

impl SaveCheckpoint {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: SaveConfig = parse(value)?;
        Ok(Self {
            key: config.key,
            attribute: config.attribute,
        })
    }
}

fn parse<T>(value: &Value) -> Result<T, FlowError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(value.clone())
        .map_err(|error| FlowError::Configuration(error.to_string()))
}

#[async_trait]
impl Processor for LoadCheckpoint {
    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        if let Some(value) = context
            .state
            .get(&self.key)
            .or_else(|| self.default.clone())
        {
            packet.attributes.insert(self.attribute.clone(), value);
        }
        output.success(packet).await
    }
}

#[async_trait]
impl Processor for SaveCheckpoint {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::BlockingIo
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let value = packet
            .attributes
            .get(&self.attribute)
            .ok_or_else(|| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: format!("attribute '{}' does not exist", self.attribute),
            })?;
        context.state.set(&self.key, value)?;
        output.success(packet).await
    }
}
