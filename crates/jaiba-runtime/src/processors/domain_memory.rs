use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

pub struct MemoryUpsert {
    class: String,
    id_attribute: String,
    value_attribute: Option<String>,
}

pub struct MemoryGet {
    class: String,
    id_attribute: String,
    attribute: String,
    /// Si true, escribe JSON `null` cuando no hay valor; si false, omite el atributo.
    miss_as_null: bool,
}

pub struct MemoryRemove {
    class: String,
    id_attribute: String,
}

#[derive(Deserialize)]
struct UpsertConfig {
    class: String,
    id_attribute: String,
    #[serde(default)]
    value_attribute: Option<String>,
}

#[derive(Deserialize)]
struct GetConfig {
    class: String,
    id_attribute: String,
    #[serde(default = "default_get_attribute")]
    attribute: String,
    #[serde(default)]
    miss_as_null: bool,
}

#[derive(Deserialize)]
struct RemoveConfig {
    class: String,
    id_attribute: String,
}

fn default_get_attribute() -> String {
    "memory.value".to_owned()
}

impl MemoryUpsert {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: UpsertConfig = parse(value)?;
        Ok(Self {
            class: config.class,
            id_attribute: config.id_attribute,
            value_attribute: config.value_attribute,
        })
    }
}

impl MemoryGet {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: GetConfig = parse(value)?;
        Ok(Self {
            class: config.class,
            id_attribute: config.id_attribute,
            attribute: config.attribute,
            miss_as_null: config.miss_as_null,
        })
    }
}

impl MemoryRemove {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: RemoveConfig = parse(value)?;
        Ok(Self {
            class: config.class,
            id_attribute: config.id_attribute,
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

fn require_domain_memory(
    context: &ProcessorContext,
) -> Result<&crate::engine::DomainMemoryHandle, FlowError> {
    context.domain_memory.as_ref().ok_or_else(|| {
        FlowError::Configuration(
            "processor requires engine.domain_memory.enabled: true and a valid policy_file"
                .to_owned(),
        )
    })
}

fn packet_id(
    packet: &DataPacket,
    id_attribute: &str,
    processor_id: &str,
) -> Result<String, FlowError> {
    if let Some(value) = packet.attributes.get(id_attribute) {
        return Ok(value.clone());
    }
    if let PacketContent::Records(records) = &packet.content
        && let Some(Value::Object(map)) = records.first()
        && let Some(value) = map.get(id_attribute)
    {
        return Ok(match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        });
    }
    Err(FlowError::Processor {
        processor_id: processor_id.to_owned(),
        message: format!("id '{id_attribute}' not found in packet attributes or first record"),
    })
}

fn packet_value(
    packet: &DataPacket,
    value_attribute: Option<&str>,
    processor_id: &str,
) -> Result<Value, FlowError> {
    if let Some(attr) = value_attribute {
        let raw = packet
            .attributes
            .get(attr)
            .ok_or_else(|| FlowError::Processor {
                processor_id: processor_id.to_owned(),
                message: format!("attribute '{attr}' does not exist"),
            })?;
        return serde_json::from_str(raw).or_else(|_| Ok(Value::String(raw.clone())));
    }
    match &packet.content {
        PacketContent::Records(records) if !records.is_empty() => Ok(records[0].clone()),
        _ => Err(FlowError::Processor {
            processor_id: processor_id.to_owned(),
            message: "memory_upsert needs value_attribute or non-empty records content".to_owned(),
        }),
    }
}

#[async_trait]
impl Processor for MemoryUpsert {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let handle = require_domain_memory(context)?;
        let id = packet_id(&packet, &self.id_attribute, &context.processor_id)?;
        let value = packet_value(
            &packet,
            self.value_attribute.as_deref(),
            &context.processor_id,
        )?;
        {
            let mut mm = handle.lock()?;
            mm.upsert_keyed(&self.class, &id, value)
                .map_err(|error| FlowError::Processor {
                    processor_id: context.processor_id.clone(),
                    message: error.to_string(),
                })?;
        }
        output.success(packet).await
    }
}

#[async_trait]
impl Processor for MemoryGet {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let handle = require_domain_memory(context)?;
        let id = packet_id(&packet, &self.id_attribute, &context.processor_id)?;
        let value = {
            let mut mm = handle.lock()?;
            mm.get_keyed(&self.class, &id)
        };
        match value {
            Some(value) => {
                packet.attributes.insert(
                    self.attribute.clone(),
                    serde_json::to_string(&value).map_err(|error| FlowError::Processor {
                        processor_id: context.processor_id.clone(),
                        message: error.to_string(),
                    })?,
                );
            }
            None if self.miss_as_null => {
                packet
                    .attributes
                    .insert(self.attribute.clone(), "null".to_owned());
            }
            None => {}
        }
        output.success(packet).await
    }
}

#[async_trait]
impl Processor for MemoryRemove {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let handle = require_domain_memory(context)?;
        let id = packet_id(&packet, &self.id_attribute, &context.processor_id)?;
        {
            let mut mm = handle.lock()?;
            let _ = mm.remove(&format!("{}:{id}", self.class));
        }
        output.success(packet).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{CircuitBreakerConfig, MemoryConfig},
        engine::{
            CircuitBreakers, ConnectionManager, DomainMemoryHandle, FlowMetrics, MemoryLimiter,
            OutputSender, ProcessorEmission, StateStore,
        },
    };
    use jaiba_memory::MemoryManager;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::mpsc;

    fn context_with_jme() -> ProcessorContext {
        let yaml = include_str!("../../../../examples/jme-hot-policy.yaml");
        let mm = MemoryManager::from_yaml(yaml).unwrap();
        let state_path = std::env::temp_dir().join(format!(
            "jaiba-jme-state-{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        ProcessorContext {
            flow_id: "test".into(),
            processor_id: "mem".into(),
            parameters: Arc::new(HashMap::new()),
            connections: ConnectionManager::default(),
            metrics: FlowMetrics::default(),
            state: StateStore::load(state_path).unwrap(),
            circuits: CircuitBreakers::new(CircuitBreakerConfig {
                enabled: false,
                ..CircuitBreakerConfig::default()
            })
            .unwrap(),
            domain_memory: Some(DomainMemoryHandle::new(mm, FlowMetrics::default())),
        }
    }

    #[tokio::test]
    async fn upsert_get_roundtrip() {
        let ctx = context_with_jme();
        let upsert = MemoryUpsert {
            class: "carrier".into(),
            id_attribute: "carrier.id".into(),
            value_attribute: Some("carrier.json".into()),
        };
        let get = MemoryGet {
            class: "carrier".into(),
            id_attribute: "carrier.id".into(),
            attribute: "memory.value".into(),
            miss_as_null: false,
        };
        let (tx, mut rx) = mpsc::channel::<ProcessorEmission>(4);
        let memory = MemoryLimiter::detect(
            &MemoryConfig {
                maximum_percent: 42,
            },
            ctx.metrics.clone(),
        )
        .unwrap();
        let out = OutputSender::new(tx, "mem", memory, ctx.metrics.clone());

        let mut packet = DataPacket::empty();
        packet.attributes.insert("carrier.id".into(), "A12".into());
        packet
            .attributes
            .insert("carrier.json".into(), r#"{"lane":3}"#.into());
        upsert.execute(packet, &ctx, &out).await.unwrap();
        let _ = rx.recv().await;

        let mut packet = DataPacket::empty();
        packet.attributes.insert("carrier.id".into(), "A12".into());
        get.execute(packet, &ctx, &out).await.unwrap();
        let emission = rx.recv().await.expect("emission");
        let value = emission
            .packet
            .attributes
            .get("memory.value")
            .expect("attr");
        assert!(value.contains("lane"));
    }
}
