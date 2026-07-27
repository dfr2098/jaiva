use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

type KafkaMessage = (Vec<u8>, Option<String>);

/// Publishes every record (or one encoded payload) after broker acknowledgement.
pub struct PublishKafka {
    connection: String,
    topic: String,
    key_field: Option<String>,
    key_attribute: Option<String>,
    queue_timeout_ms: u64,
}

#[derive(Deserialize)]
struct PublishKafkaConfig {
    connection: String,
    topic: String,
    key_field: Option<String>,
    key_attribute: Option<String>,
    #[serde(default = "default_queue_timeout")]
    queue_timeout_ms: u64,
}

fn default_queue_timeout() -> u64 {
    5_000
}

impl PublishKafka {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: PublishKafkaConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.topic.trim().is_empty() {
            return Err(FlowError::Configuration(
                "publish_kafka topic cannot be empty".to_owned(),
            ));
        }
        if config.queue_timeout_ms == 0 {
            return Err(FlowError::Configuration(
                "publish_kafka queue_timeout_ms must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            connection: config.connection,
            topic: config.topic,
            key_field: config.key_field,
            key_attribute: config.key_attribute,
            queue_timeout_ms: config.queue_timeout_ms,
        })
    }

    fn messages(&self, packet: &DataPacket) -> Result<Vec<KafkaMessage>, FlowError> {
        match &packet.content {
            PacketContent::Records(records) => records
                .iter()
                .map(|record| {
                    let key = self
                        .key_field
                        .as_ref()
                        .and_then(|field| record.as_object()?.get(field))
                        .map(json_key)
                        .transpose()?;
                    let payload = serde_json::to_vec(record)
                        .map_err(|error| FlowError::Configuration(error.to_string()))?;
                    Ok((payload, key))
                })
                .collect(),
            PacketContent::Encoded { bytes, .. } => {
                let key = self
                    .key_attribute
                    .as_ref()
                    .and_then(|name| packet.attributes.get(name))
                    .cloned();
                Ok(vec![(bytes.clone(), key)])
            }
        }
    }

    async fn publish(
        &self,
        producer: &FutureProducer,
        messages: &[(Vec<u8>, Option<String>)],
    ) -> Result<(i32, i64), FlowError> {
        let mut last = (-1, -1);
        for (payload, key) in messages {
            let mut record = FutureRecord::to(&self.topic).payload(payload);
            if let Some(key) = key {
                record = record.key(key);
            }
            let delivery = producer
                .send(record, Duration::from_millis(self.queue_timeout_ms))
                .await
                .map_err(|(error, _)| FlowError::MessageConnector(error.to_string()))?;
            last = (delivery.partition, delivery.offset);
        }
        Ok(last)
    }
}

#[async_trait]
impl Processor for PublishKafka {
    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let producer = context.connections.kafka(&self.connection)?;
        let circuit = format!("kafka:{}", self.connection);
        if let Err(error) = context.circuits.permit(&circuit) {
            context.metrics.circuit_rejected();
            return Err(error);
        }
        let messages = self.messages(&packet)?;
        let bytes = messages
            .iter()
            .map(|(payload, _)| payload.len() as u64)
            .sum();
        let started = Instant::now();
        let (partition, offset) = match self.publish(producer, &messages).await {
            Ok(delivery) => delivery,
            Err(error) => {
                context.circuits.failure(&circuit);
                context
                    .metrics
                    .set_circuits_open(context.circuits.open_count());
                context.metrics.kafka_publish_error();
                return Err(error);
            }
        };
        context.circuits.success(&circuit);
        context
            .metrics
            .set_circuits_open(context.circuits.open_count());
        context
            .metrics
            .kafka_published(messages.len() as u64, bytes);
        packet
            .attributes
            .insert("kafka.connection".to_owned(), self.connection.clone());
        packet
            .attributes
            .insert("kafka.topic".to_owned(), self.topic.clone());
        packet
            .attributes
            .insert("kafka.messages".to_owned(), messages.len().to_string());
        packet
            .attributes
            .insert("kafka.partition".to_owned(), partition.to_string());
        packet
            .attributes
            .insert("kafka.offset".to_owned(), offset.to_string());
        packet.attributes.insert(
            "kafka.duration_ms".to_owned(),
            started.elapsed().as_millis().to_string(),
        );
        output.success(packet).await
    }
}

fn json_key(value: &Value) -> Result<String, FlowError> {
    Ok(match value {
        Value::String(value) => value.clone(),
        Value::Null => return Ok(String::new()),
        scalar @ (Value::Bool(_) | Value::Number(_)) => scalar.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map_err(|error| FlowError::Configuration(error.to_string()))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_one_keyed_message_per_record() {
        let processor = PublishKafka::from_config(&serde_json::json!({
            "connection": "main",
            "topic": "events",
            "key_field": "id"
        }))
        .unwrap();
        let packet = DataPacket::with_records(vec![
            serde_json::json!({"id": 1, "name": "Ada"}),
            serde_json::json!({"id": 2, "name": "Linus"}),
        ]);
        let messages = processor.messages(&packet).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].1.as_deref(), Some("1"));
        assert_eq!(messages[1].1.as_deref(), Some("2"));
    }

    #[test]
    fn rejects_empty_topic() {
        assert!(
            PublishKafka::from_config(&serde_json::json!({
                "connection": "main",
                "topic": ""
            }))
            .is_err()
        );
    }
}
