use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
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
    pub(crate) reservation: MemoryReservation,
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
        let reservation = self.memory.reserve(packet.estimated_size()).await?;
        self.sender
            .send(ProcessorEmission {
                processor_id: self.processor_id.clone(),
                relationship: relationship.clone(),
                packet,
                reservation,
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
