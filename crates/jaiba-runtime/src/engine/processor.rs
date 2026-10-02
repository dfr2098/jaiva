use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::config::ExecutionMode;
use crate::error::FlowError;

use super::{DataPacket, FlowMetrics, MemoryLimiter, MemoryReservation, ProcessorContext};

/// Packet emitted by a processor together with its routing relationship.
pub struct ProcessorEmission {
    pub processor_id: String,
    pub relationship: String,
    pub packet: DataPacket,
    pub(crate) _reservation: MemoryReservation,
}

/// Bounded output channel supplied to each processor.
///
/// Sending waits when channel or memory capacity is exhausted. Acceptance by
/// this channel does not mean a downstream database has committed the packet.
#[derive(Clone)]
pub struct OutputSender {
    sender: mpsc::Sender<ProcessorEmission>,
    processor_id: String,
    memory: MemoryLimiter,
    metrics: FlowMetrics,
    emitted_records: Arc<AtomicU64>,
    routed_relationships: Option<Arc<HashSet<String>>>,
    holds_input: bool,
}

impl OutputSender {
    pub(crate) fn new(
        sender: mpsc::Sender<ProcessorEmission>,
        processor_id: impl Into<String>,
        memory: MemoryLimiter,
        metrics: FlowMetrics,
    ) -> Self {
        Self {
            sender,
            processor_id: processor_id.into(),
            memory,
            metrics,
            emitted_records: Arc::new(AtomicU64::new(0)),
            routed_relationships: None,
            holds_input: false,
        }
    }

    /// Declara qué relaciones tienen una arista de salida. Las emisiones
    /// terminales se contabilizan localmente y no ocupan el canal global.
    pub(crate) fn with_routed_relationships(mut self, relationships: HashSet<String>) -> Self {
        self.routed_relationships = Some(Arc::new(relationships));
        self
    }

    pub(crate) fn with_input_reservation(mut self, holds_input: bool) -> Self {
        self.holds_input = holds_input;
        self
    }

    pub(crate) fn reserve_working_copy(
        &self,
        packet: &DataPacket,
    ) -> Result<Option<MemoryReservation>, FlowError> {
        if self.holds_input {
            self.memory.try_reserve(packet.estimated_size()).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Emits a packet through an arbitrary relationship.
    pub async fn emit(
        &self,
        relationship: impl Into<String>,
        packet: DataPacket,
    ) -> Result<(), FlowError> {
        let relationship = relationship.into();
        let records = packet
            .records()
            .map(|records| records.len() as u64)
            .unwrap_or(1);
        if self
            .routed_relationships
            .as_ref()
            .is_some_and(|relationships| !relationships.contains(&relationship))
        {
            if relationship != "failure" {
                self.emitted_records.fetch_add(records, Ordering::Relaxed);
                self.metrics.processor_records(&self.processor_id, records);
            }
            return Ok(());
        }
        let reservation = if self.holds_input {
            self.memory.try_reserve(packet.estimated_size())?
        } else {
            self.memory.reserve(packet.estimated_size()).await?
        };
        self.sender
            .send(ProcessorEmission {
                processor_id: self.processor_id.clone(),
                relationship: relationship.clone(),
                packet,
                _reservation: reservation,
            })
            .await
            .map_err(|_| FlowError::ChannelClosed)?;
        if relationship != "failure" {
            self.emitted_records.fetch_add(records, Ordering::Relaxed);
            self.metrics.processor_records(&self.processor_id, records);
        }
        Ok(())
    }

    /// Emits a packet through the conventional `success` relationship.
    pub async fn success(&self, packet: DataPacket) -> Result<(), FlowError> {
        self.emit("success", packet).await
    }

    pub(crate) fn emitted_records(&self) -> u64 {
        self.emitted_records.load(Ordering::Relaxed)
    }
}

/// Executable behavior within a Jaiva flow.
///
/// Implementations should emit packets as soon as they become available rather
/// than collecting an entire source in memory.
#[async_trait]
pub trait Processor: Send + Sync {
    /// Preferred executor. Flow YAML can override this for custom processors.
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::AsyncIo
    }

    /// Processes one packet and streams zero or more outputs.
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError>;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn input_holder_fails_instead_of_waiting_for_its_own_memory() {
        let metrics = FlowMetrics::default();
        let memory = MemoryLimiter::from_budget(65536, metrics.clone());
        let input = memory.reserve(1).await.unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let output = OutputSender::new(sender, "transform", memory, metrics.clone())
            .with_input_reservation(true);
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            output.success(DataPacket::empty()),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(FlowError::MemoryCapacity { .. })));
        assert!(matches!(
            output.reserve_working_copy(&DataPacket::empty()),
            Err(FlowError::MemoryCapacity { .. })
        ));
        drop(input);
        assert_eq!(metrics.summary().memory_used_bytes, 0);
    }

    #[tokio::test]
    async fn bounded_output_waits_until_capacity_is_available() {
        let (sender, mut receiver) = mpsc::channel(1);
        let metrics = crate::engine::FlowMetrics::default();
        let memory = MemoryLimiter::detect(
            &crate::config::MemoryConfig {
                maximum_percent: 42,
            },
            metrics.clone(),
        )
        .unwrap();
        let output = OutputSender::new(sender, "source", memory, metrics);
        output.success(DataPacket::empty()).await.unwrap();

        let blocked_output = output.clone();
        let blocked_send =
            tokio::spawn(async move { blocked_output.success(DataPacket::empty()).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked_send.is_finished());

        receiver.recv().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), blocked_send)
            .await
            .expect("send should resume")
            .expect("task should complete")
            .expect("send should succeed");
    }
}
