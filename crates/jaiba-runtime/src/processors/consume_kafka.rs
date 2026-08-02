//! Fuente Kafka (`consume_kafka`).
//!
//! - Auto-commit desactivado; commit tras `output.success` (at-least-once MVP).
//! - Un ciclo de ejecución hace poll hasta `max_poll_messages` o `max_idle_ms`
//!   sin mensajes nuevos (útil con agendas / una pasada CLI).
//! - Errores de transporte al unirse al grupo se toleran mientras no se haya
//!   consumido nada y no expire el idle.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer},
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DecodeMode {
    Json,
    Bytes,
}

impl Default for DecodeMode {
    fn default() -> Self {
        Self::Json
    }
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
                    FlowError::MessageConnector(format!("invalid JSON from Kafka: {error}"))
                })?;
                let records = match value {
                    Value::Array(items) => items,
                    other => vec![other],
                };
                Ok(PacketContent::Records(records))
            }
        }
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
                context.circuits.failure(&circuit);
                context
                    .metrics
                    .set_circuits_open(context.circuits.open_count());
                context.metrics.kafka_consume_error();
                return Err(error);
            }
        };
        if let Err(error) = consumer.subscribe(&[&self.topic]) {
            context.circuits.failure(&circuit);
            context
                .metrics
                .set_circuits_open(context.circuits.open_count());
            context.metrics.kafka_consume_error();
            return Err(FlowError::MessageConnector(error.to_string()));
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
                            context.circuits.failure(&circuit);
                            context
                                .metrics
                                .set_circuits_open(context.circuits.open_count());
                            context.metrics.kafka_consume_error();
                            return Err(error);
                        }
                    };
                    let record_count = match &content {
                        PacketContent::Records(records) => records.len(),
                        PacketContent::Encoded { .. } => 1,
                    };
                    let mut packet = DataPacket::empty();
                    packet.content = content;
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

                    if let Err(error) = output.success(packet).await {
                        context.circuits.failure(&circuit);
                        context
                            .metrics
                            .set_circuits_open(context.circuits.open_count());
                        context.metrics.kafka_consume_error();
                        return Err(error);
                    }

                    // MVP: commit tras emisión exitosa (at-least-once).
                    if let Err(error) = consumer.commit_message(&message, CommitMode::Async) {
                        context.circuits.failure(&circuit);
                        context
                            .metrics
                            .set_circuits_open(context.circuits.open_count());
                        context.metrics.kafka_consume_error();
                        return Err(FlowError::MessageConnector(error.to_string()));
                    }

                    consumed += 1;
                    idle_deadline = Instant::now() + Duration::from_millis(self.max_idle_ms);
                }
                Ok(Err(error)) => {
                    // Durante el join/metadata inicial librdkafka puede reportar
                    // fallos de transporte transitorios; seguir hasta idle.
                    let text = error.to_string();
                    let transient = text.contains("BrokerTransportFailure")
                        || text.contains("AllBrokerConnectionsDown")
                        || text.contains("Timed out")
                        || text.contains("Local: Wait timed out");
                    if transient && consumed == 0 {
                        continue;
                    }
                    context.circuits.failure(&circuit);
                    context
                        .metrics
                        .set_circuits_open(context.circuits.open_count());
                    context.metrics.kafka_consume_error();
                    return Err(FlowError::MessageConnector(text));
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
}

#[cfg(all(test, feature = "kafka-driver"))]
mod phase8_kafka_tests {
    use std::{
        env,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use rdkafka::{
        ClientConfig,
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        client::DefaultClientContext,
    };

    use crate::{
        engine::{DataPacket, FlowEngine, OutputSender, Processor, ProcessorContext},
        error::FlowError,
        processors::default_registry,
    };

    /// Destino de prueba: acumula paquetes emitidos por `consume_kafka`.
    struct CaptureSink {
        packets: Arc<Mutex<Vec<DataPacket>>>,
    }

    #[async_trait]
    impl Processor for CaptureSink {
        async fn execute(
            &self,
            packet: DataPacket,
            _: &ProcessorContext,
            _: &OutputSender,
        ) -> Result<(), FlowError> {
            self.packets
                .lock()
                .expect("capture poisoned")
                .push(packet);
            Ok(())
        }
    }

    async fn ensure_topic(brokers: &str, topic: &str) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("broker.address.family", "v4")
            .create()
            .expect("create Kafka admin client");
        let creation = admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new()
                    .operation_timeout(Some(Duration::from_secs(10)))
                    .request_timeout(Some(Duration::from_secs(10))),
            )
            .await
            .expect("create integration topic");
        assert!(
            creation.iter().all(|result| result.as_ref().is_ok()),
            "Kafka topic creation failed: {creation:?}"
        );
    }

    async fn delete_topic(brokers: &str, topic: &str) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("broker.address.family", "v4")
            .create()
            .expect("create Kafka admin client");
        let deletion = admin
            .delete_topics(
                &[topic],
                &AdminOptions::new()
                    .operation_timeout(Some(Duration::from_secs(10)))
                    .request_timeout(Some(Duration::from_secs(10))),
            )
            .await
            .expect("delete integration topic");
        assert!(
            deletion.iter().all(|result| result.as_ref().is_ok()),
            "Kafka topic deletion failed: {deletion:?}"
        );
    }

    /// Publica con `publish_kafka` y consume con el procesador `consume_kafka`.
    #[tokio::test]
    async fn kafka_real_consume_kafka_processor() {
        let Ok(brokers) = env::var("JAIBA_TEST_KAFKA_BROKERS") else {
            eprintln!("skipping real Kafka test: JAIBA_TEST_KAFKA_BROKERS is not set");
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let topic = format!("jaiva.phase-8.consume.{suffix}");
        let group = format!("jaiva-phase-8-consume-{suffix}");
        ensure_topic(&brokers, &topic).await;

        let publish_yaml = format!(
            r#"
id: phase8-publish
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-phase8-publish
    security_protocol: PLAINTEXT
    message_timeout_ms: 15000
processors:
  - id: records
    type: generate_records
    config:
      records:
        - batch_id: phase8-a
          status: created
        - batch_id: phase8-b
          status: completed
  - id: publish
    type: publish_kafka
    config:
      connection: integration
      topic: {topic}
      key_field: batch_id
      queue_timeout_ms: 5000
connections:
  - from: records
    relationship: success
    to: publish
"#
        );
        let publish_summary = FlowEngine::new(serde_yaml::from_str(&publish_yaml).unwrap())
            .unwrap()
            .run()
            .await
            .expect("publish flow");
        assert_eq!(publish_summary.failed, 0);
        assert!(publish_summary.emitted >= 1);

        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut registry = default_registry();
        let sink = Arc::new(CaptureSink {
            packets: captured.clone(),
        });
        registry.register("capture_sink", move |_| Ok(sink.clone()));

        let consume_yaml = format!(
            r#"
id: phase8-consume
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-phase8-consume
    security_protocol: PLAINTEXT
    message_timeout_ms: 15000
processors:
  - id: consume
    type: consume_kafka
    config:
      connection: integration
      topic: {topic}
      group_id: {group}
      auto_offset_reset: earliest
      max_poll_messages: 20
      max_poll_ms: 1000
      max_idle_ms: 12000
      decode: json
  - id: capture
    type: capture_sink
connections:
  - from: consume
    relationship: success
    to: capture
"#
        );
        let consume_summary = FlowEngine::new(serde_yaml::from_str(&consume_yaml).unwrap())
            .unwrap()
            .with_registry(registry)
            .run()
            .await
            .expect("consume flow");
        assert_eq!(consume_summary.failed, 0);

        let packets = captured.lock().expect("capture poisoned");
        assert_eq!(packets.len(), 2, "expected two Kafka messages as packets");
        for packet in packets.iter() {
            assert!(packet.attributes.contains_key("kafka.offset"));
            assert_eq!(
                packet.attributes.get("kafka.topic").map(String::as_str),
                Some(topic.as_str())
            );
            let records = packet.records().expect("records payload");
            assert_eq!(records.len(), 1);
        }
        drop(packets);
        delete_topic(&brokers, &topic).await;
    }

    /// Smoke de rendimiento: 100 mensajes publish→consume en < 30s.
    #[tokio::test]
    async fn kafka_throughput_smoke() {
        let Ok(brokers) = env::var("JAIBA_TEST_KAFKA_BROKERS") else {
            eprintln!("skipping real Kafka test: JAIBA_TEST_KAFKA_BROKERS is not set");
            return;
        };
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let topic = format!("jaiva.phase-8.thru.{suffix}");
        let group = format!("jaiva-phase-8-thru-{suffix}");
        ensure_topic(&brokers, &topic).await;

        let records = (0..100)
            .map(|index| format!("        - event_id: e-{index}\n          n: {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let publish_yaml = format!(
            r#"
id: phase8-thru-publish
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-phase8-thru-pub
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
processors:
  - id: records
    type: generate_records
    config:
      records:
{records}
  - id: publish
    type: publish_kafka
    config:
      connection: integration
      topic: {topic}
      key_field: event_id
      queue_timeout_ms: 10000
connections:
  - from: records
    relationship: success
    to: publish
"#
        );

        let started = Instant::now();
        FlowEngine::new(serde_yaml::from_str(&publish_yaml).unwrap())
            .unwrap()
            .run()
            .await
            .expect("throughput publish");

        let captured = Arc::new(Mutex::new(Vec::new()));
        let mut registry = default_registry();
        let sink = Arc::new(CaptureSink {
            packets: captured.clone(),
        });
        registry.register("capture_sink", move |_| Ok(sink.clone()));
        let consume_yaml = format!(
            r#"
id: phase8-thru-consume
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-phase8-thru-cons
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
processors:
  - id: consume
    type: consume_kafka
    config:
      connection: integration
      topic: {topic}
      group_id: {group}
      auto_offset_reset: earliest
      max_poll_messages: 100
      max_poll_ms: 1000
      max_idle_ms: 20000
      decode: json
  - id: capture
    type: capture_sink
connections:
  - from: consume
    relationship: success
    to: capture
"#
        );
        let summary = FlowEngine::new(serde_yaml::from_str(&consume_yaml).unwrap())
            .unwrap()
            .with_registry(registry)
            .run()
            .await
            .expect("throughput consume");
        let elapsed = started.elapsed();
        assert_eq!(summary.failed, 0);
        assert_eq!(captured.lock().expect("capture").len(), 100);
        assert!(
            elapsed < Duration::from_secs(30),
            "throughput smoke exceeded 30s: {elapsed:?}"
        );
        delete_topic(&brokers, &topic).await;
    }

    /// Broker inalcanzable: `publish_kafka` falla de forma controlada (sin panic).
    #[tokio::test]
    async fn kafka_fail_broker_is_controlled() {
        if env::var("JAIBA_TEST_KAFKA_FAIL_BROKER").is_err() {
            eprintln!("skipping real Kafka test: JAIBA_TEST_KAFKA_FAIL_BROKER is not set");
            return;
        }
        let yaml = r#"
id: phase8-fail-broker
kafka_connections:
  down:
    brokers_env: JAIBA_TEST_KAFKA_FAIL_BROKER
    client_id: jaiva-phase8-fail
    security_protocol: PLAINTEXT
    message_timeout_ms: 2000
processors:
  - id: records
    type: generate_records
    config:
      records:
        - id: 1
  - id: publish
    type: publish_kafka
    config:
      connection: down
      topic: jaiva.phase-8.unreachable
      queue_timeout_ms: 2000
    retry:
      maximum_attempts: 0
      initial_delay_ms: 1
      maximum_delay_ms: 1
connections:
  - from: records
    relationship: success
    to: publish
"#;
        match FlowEngine::new(serde_yaml::from_str(yaml).unwrap())
            .unwrap()
            .run()
            .await
        {
            Ok(summary) => assert!(
                summary.failed >= 1 || summary.kafka_publish_errors >= 1,
                "expected controlled Kafka failure, got {summary:?}"
            ),
            Err(error) => {
                let text = error.to_string();
                assert!(
                    text.contains("Kafka")
                        || text.contains("broker")
                        || text.contains("Message")
                        || text.contains("transport")
                        || text.contains("connect")
                        || text.contains("timeout")
                        || text.contains("Timeout"),
                    "unexpected fail-broker error: {text}"
                );
            }
        }
    }
}
