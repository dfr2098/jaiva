//! Fuente Kafka (`consume_kafka`).
//!
//! - Auto-commit desactivado; commit tras emitir por `success` o `failure`
//!   (at-least-once; un mensaje envenenado no bloquea el grupo).
//! - Un ciclo de ejecución hace poll hasta `max_poll_messages` o `max_idle_ms`
//!   sin mensajes nuevos (útil con agendas / una pasada CLI).
//! - Errores de transporte al unirse al grupo se toleran mientras no se haya
//!   consumido nada y no expire el idle.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer},
    message::BorrowedMessage,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

use super::kafka_support::{is_transient_join, kafka_error, opens_circuit};

/// Lee un lote de mensajes Kafka y emite un paquete por mensaje.
pub struct ConsumeKafka {
    /// Nombre en `kafka_connections` / Connection Manager.
    connection: String,
    topic: String,
    group_id: String,
    /// Solo aplica si el grupo aún no tiene offsets (`earliest` | `latest`).
    auto_offset_reset: String,
    max_poll_messages: usize,
    /// Tope de espera por cada `recv`.
    max_poll_ms: u64,
    /// Sin mensajes nuevos en este intervalo → fin del ciclo.
    max_idle_ms: u64,
    decode: DecodeMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DecodeMode {
    #[default]
    Json,
    Bytes,
}

#[derive(Deserialize)]
struct ConsumeKafkaConfig {
    connection: String,
    topic: String,
    group_id: String,
    #[serde(default = "default_offset_reset")]
    auto_offset_reset: String,
    #[serde(default = "default_max_poll_messages")]
    max_poll_messages: usize,
    #[serde(default = "default_max_poll_ms")]
    max_poll_ms: u64,
    #[serde(default = "default_max_idle_ms")]
    max_idle_ms: u64,
    #[serde(default)]
    decode: DecodeMode,
}

fn default_offset_reset() -> String {
    "earliest".to_owned()
}
fn default_max_poll_messages() -> usize {
    100
}
fn default_max_poll_ms() -> u64 {
    1_000
}
fn default_max_idle_ms() -> u64 {
    2_000
}

impl ConsumeKafka {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: ConsumeKafkaConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.topic.trim().is_empty() {
            return Err(FlowError::Configuration(
                "consume_kafka topic cannot be empty".to_owned(),
            ));
        }
        if config.group_id.trim().is_empty() {
            return Err(FlowError::Configuration(
                "consume_kafka group_id cannot be empty".to_owned(),
            ));
        }
        if config.max_poll_messages == 0 {
            return Err(FlowError::Configuration(
                "consume_kafka max_poll_messages must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            connection: config.connection,
            topic: config.topic,
            group_id: config.group_id,
            auto_offset_reset: config.auto_offset_reset,
            max_poll_messages: config.max_poll_messages,
            max_poll_ms: config.max_poll_ms.max(1),
            max_idle_ms: config.max_idle_ms.max(1),
            decode: config.decode,
        })
    }

    /// `json`: objeto → un registro; arreglo → N registros. `bytes`: Encoded.
    fn decode_payload(&self, payload: &[u8]) -> Result<PacketContent, FlowError> {
        match self.decode {
            DecodeMode::Bytes => Ok(PacketContent::Encoded {
                bytes: payload.to_vec(),
                media_type: "application/octet-stream".to_owned(),
            }),
            DecodeMode::Json => {
                let value: Value = serde_json::from_slice(payload).map_err(|error| {
                    FlowError::MessageRejected(format!("invalid JSON from Kafka: {error}"))
                })?;
                let records = match value {
                    Value::Array(items) => items,
                    other => vec![other],
                };
                Ok(PacketContent::Records(records))
            }
        }
    }

    fn annotate(
        &self,
        packet: &mut DataPacket,
        message: &BorrowedMessage<'_>,
        started: Instant,
        record_count: usize,
    ) {
        packet
            .attributes
            .insert("kafka.connection".to_owned(), self.connection.clone());
        packet
            .attributes
            .insert("kafka.topic".to_owned(), self.topic.clone());
        packet
            .attributes
            .insert("kafka.group_id".to_owned(), self.group_id.clone());
        packet.attributes.insert(
            "kafka.partition".to_owned(),
            message.partition().to_string(),
        );
        packet
            .attributes
            .insert("kafka.offset".to_owned(), message.offset().to_string());
        packet
            .attributes
            .insert("kafka.messages".to_owned(), "1".to_owned());
        packet
            .attributes
            .insert("record.count".to_owned(), record_count.to_string());
        if let Some(key) = message.key() {
            packet.attributes.insert(
                "kafka.key".to_owned(),
                String::from_utf8_lossy(key).into_owned(),
            );
        }
        packet.attributes.insert(
            "kafka.duration_ms".to_owned(),
            started.elapsed().as_millis().to_string(),
        );
    }

    fn fail_circuit(context: &ProcessorContext, circuit: &str) {
        context.circuits.failure(circuit);
        context
            .metrics
            .set_circuits_open(context.circuits.open_count());
        context.metrics.kafka_consume_error();
    }
}

#[async_trait]
impl Processor for ConsumeKafka {
    async fn execute(
        &self,
        // Fuente: el paquete de arranque del grafo se ignora.
        _packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let endpoint = context.connections.kafka_endpoint(&self.connection)?;
        let circuit = format!("kafka:{}", self.connection);
        if let Err(error) = context.circuits.permit(&circuit) {
            context.metrics.circuit_rejected();
            return Err(error);
        }

        let consumer = match endpoint.create_consumer(&self.group_id, &self.auto_offset_reset) {
            Ok(consumer) => consumer,
            Err(error) => {
                Self::fail_circuit(context, &circuit);
                return Err(error);
            }
        };
        if let Err(error) = consumer.subscribe(&[&self.topic]) {
            Self::fail_circuit(context, &circuit);
            return Err(kafka_error(error));
        }

        let started = Instant::now();
        let mut consumed = 0u64;
        let mut bytes = 0u64;
        let mut idle_deadline = Instant::now() + Duration::from_millis(self.max_idle_ms);

        while consumed < self.max_poll_messages as u64 {
            let wait = idle_deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            let poll_wait = wait.min(Duration::from_millis(self.max_poll_ms));
            match tokio::time::timeout(poll_wait, consumer.recv()).await {
                Ok(Ok(message)) => {
                    let payload = message.payload().unwrap_or(&[]);
                    bytes += payload.len() as u64;
                    let content = match self.decode_payload(payload) {
                        Ok(content) => content,
                        Err(error) => {
                            // Mensaje envenenado: va a `failure`, se confirma el
                            // offset y el ciclo sigue (no abre el circuit).
                            let mut packet = DataPacket::empty();
                            packet.content = PacketContent::Encoded {
                                bytes: payload.to_vec(),
                                media_type: "application/octet-stream".to_owned(),
                            };
                            self.annotate(&mut packet, &message, started, 0);
                            packet
                                .attributes
                                .insert("error.kind".to_owned(), "poison".to_owned());
                            packet
                                .attributes
                                .insert("error.message".to_owned(), error.to_string());
                            if let Err(emit_error) = output.emit("failure", packet).await {
                                Self::fail_circuit(context, &circuit);
                                return Err(emit_error);
                            }
                            if let Err(commit_error) =
                                consumer.commit_message(&message, CommitMode::Async)
                            {
                                let mapped = kafka_error(commit_error);
                                if opens_circuit(&mapped) {
                                    Self::fail_circuit(context, &circuit);
                                } else {
                                    context.metrics.kafka_consume_error();
                                }
                                return Err(mapped);
                            }
                            consumed += 1;
                            idle_deadline =
                                Instant::now() + Duration::from_millis(self.max_idle_ms);
                            continue;
                        }
                    };
                    let record_count = match &content {
                        PacketContent::Records(records) => records.len(),
                        PacketContent::Encoded { .. } => 1,
                    };
                    let mut packet = DataPacket::empty();
                    packet.content = content;
                    self.annotate(&mut packet, &message, started, record_count);

                    if let Err(error) = output.success(packet).await {
                        Self::fail_circuit(context, &circuit);
                        return Err(error);
                    }

                    // Commit tras emisión exitosa (at-least-once).
                    if let Err(error) = consumer.commit_message(&message, CommitMode::Async) {
                        let mapped = kafka_error(error);
                        if opens_circuit(&mapped) {
                            Self::fail_circuit(context, &circuit);
                        } else {
                            context.metrics.kafka_consume_error();
                        }
                        return Err(mapped);
                    }

                    consumed += 1;
                    idle_deadline = Instant::now() + Duration::from_millis(self.max_idle_ms);
                }
                Ok(Err(error)) => {
                    // Durante el join/metadata inicial librdkafka puede reportar
                    // fallos de transporte transitorios; seguir hasta idle.
                    if is_transient_join(&error) && consumed == 0 {
                        continue;
                    }
                    let mapped = kafka_error(error);
                    if opens_circuit(&mapped) {
                        Self::fail_circuit(context, &circuit);
                    } else {
                        context.metrics.kafka_consume_error();
                    }
                    return Err(mapped);
                }
                Err(_) => {
                    // Timeout de poll: si aún no hay mensajes, respetar idle.
                    if consumed == 0 {
                        continue;
                    }
                    break;
                }
            }
        }

        context.circuits.success(&circuit);
        context
            .metrics
            .set_circuits_open(context.circuits.open_count());
        if consumed > 0 {
            context.metrics.kafka_consumed(consumed, bytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_topic_and_group() {
        assert!(
            ConsumeKafka::from_config(&serde_json::json!({
                "connection": "dma",
                "topic": "",
                "group_id": "g"
            }))
            .is_err()
        );
        assert!(
            ConsumeKafka::from_config(&serde_json::json!({
                "connection": "dma",
                "topic": "t",
                "group_id": ""
            }))
            .is_err()
        );
    }

    #[test]
    fn decodes_json_object_as_single_record() {
        let processor = ConsumeKafka::from_config(&serde_json::json!({
            "connection": "dma",
            "topic": "events",
            "group_id": "jaiva-readers"
        }))
        .unwrap();
        match processor
            .decode_payload(br#"{"id":1,"name":"Ada"}"#)
            .unwrap()
        {
            PacketContent::Records(records) => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0]["name"], "Ada");
            }
            other => panic!("unexpected content: {other:?}"),
        }
    }

    #[test]
    fn invalid_json_is_message_rejected() {
        let processor = ConsumeKafka::from_config(&serde_json::json!({
            "connection": "dma",
            "topic": "events",
            "group_id": "jaiva-readers"
        }))
        .unwrap();
        let error = processor.decode_payload(b"not-json").unwrap_err();
        assert!(matches!(error, FlowError::MessageRejected(_)));
        assert!(!error.is_retryable());
    }
}

#[cfg(all(test, feature = "kafka-driver"))]
#[path = "consume_kafka_live_tests.rs"]
mod live_tests;
