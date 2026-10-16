//! Helpers compartidos por las pruebas reales de Kafka.

use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
};

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub(super) fn brokers() -> Option<String> {
    match env::var("JAIBA_TEST_KAFKA_BROKERS") {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ if env::var("JAIBA_TEST_KAFKA_REQUIRED").is_ok() => {
            panic!("JAIBA_TEST_KAFKA_BROKERS is required when JAIBA_TEST_KAFKA_REQUIRED is set")
        }
        _ => {
            eprintln!("skipping real Kafka test: JAIBA_TEST_KAFKA_BROKERS is not set");
            None
        }
    }
}

pub(super) fn unique(prefix: &str) -> String {
    format!("{prefix}.{}", uuid::Uuid::new_v4().simple())
}

pub(super) async fn ensure_topic(brokers: &str, topic: &str) {
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
        creation.iter().all(|result| {
            result.is_ok()
                || matches!(
                    result,
                    Err((_, rdkafka::error::RDKafkaErrorCode::TopicAlreadyExists))
                )
        }),
        "Kafka topic creation failed: {creation:?}"
    );
}

pub(super) async fn delete_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("broker.address.family", "v4")
        .create()
        .expect("create Kafka admin client");
    let _ = admin
        .delete_topics(
            &[topic],
            &AdminOptions::new()
                .operation_timeout(Some(Duration::from_secs(10)))
                .request_timeout(Some(Duration::from_secs(10))),
        )
        .await;
}

/// Destino de prueba: acumula paquetes emitidos por `consume_kafka`.
pub(super) struct CaptureSink {
    pub packets: Arc<Mutex<Vec<DataPacket>>>,
}

#[async_trait]
impl Processor for CaptureSink {
    async fn execute(
        &self,
        packet: DataPacket,
        _: &ProcessorContext,
        _: &OutputSender,
    ) -> Result<(), FlowError> {
        self.packets.lock().expect("capture poisoned").push(packet);
        Ok(())
    }
}
