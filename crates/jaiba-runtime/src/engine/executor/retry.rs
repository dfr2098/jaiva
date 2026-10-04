use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::time::sleep;
use tracing::{info, warn};

use crate::{
    config::RetryConfig,
    engine::{
        DataPacket, LocalPacketRepository, OutputSender, PacketRepository, Processor,
        ProcessorContext, ProvenanceEvent,
    },
    error::FlowError,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_with_retry(
    processor: Arc<dyn Processor>,
    mut packet: DataPacket,
    context: ProcessorContext,
    retry: RetryConfig,
    timeout_ms: Option<u64>,
    output: OutputSender,
    repository: Option<LocalPacketRepository>,
    queue_id: Option<String>,
) -> Result<Option<(String, u32)>, FlowError> {
    loop {
        let started = Instant::now();
        let input_records = packet
            .records()
            .map(|records| records.len() as u64)
            .unwrap_or(1);
        if let (Some(repository), Some(queue_id)) = (&repository, queue_id.as_deref()) {
            let _ = repository
                .record_event(
                    queue_id,
                    ProvenanceEvent::ProcessingStarted,
                    serde_json::json!({
                        "attempt": packet.attempt,
                        "input_bytes": packet.estimated_size()
                    }),
                )
                .await;
        }
        info!(
            flow_id = %context.flow_id,
            processor_id = %context.processor_id,
            packet_id = %packet.id,
            attempt = packet.attempt,
            "executing processor"
        );

        let outcome = match output.reserve_working_copy(&packet) {
            Err(error) => Err(error),
            Ok(working_copy) => {
                let execution = processor.execute(packet.clone(), &context, &output);
                let outcome = match timeout_ms {
                    Some(milliseconds) => {
                        tokio::time::timeout(Duration::from_millis(milliseconds), execution)
                            .await
                            .map_err(|_| FlowError::Processor {
                                processor_id: context.processor_id.clone(),
                                message: format!("execution exceeded timeout of {milliseconds} ms"),
                            })
                            .and_then(|result| result)
                    }
                    None => execution.await,
                };
                drop(working_copy);
                outcome
            }
        };

        match outcome {
            Err(error @ FlowError::PacketTooLarge { .. }) => return Err(error),
            Ok(()) => {
                if output.emitted_records() == 0 {
                    context
                        .metrics
                        .processor_records(&context.processor_id, input_records);
                }
                context.metrics.processed();
                context
                    .metrics
                    .processor_finished(&context.processor_id, started.elapsed(), true);
                if let (Some(repository), Some(queue_id)) = (&repository, queue_id.as_deref()) {
                    let _ = repository
                        .record_event(
                            queue_id,
                            ProvenanceEvent::Processed,
                            serde_json::json!({
                                "attempt": packet.attempt,
                                "duration_ms": started.elapsed().as_millis()
                            }),
                        )
                        .await;
                }
                return Ok(None);
            }
            Err(error) if packet.attempt < retry.maximum_attempts => {
                packet.attempt += 1;
                context.metrics.retried();
                let multiplier = 2_u64.saturating_pow(packet.attempt.saturating_sub(1));
                let delay = retry
                    .initial_delay_ms
                    .saturating_mul(multiplier)
                    .min(retry.maximum_delay_ms);
                warn!(
                    processor_id = %context.processor_id,
                    packet_id = %packet.id,
                    attempt = packet.attempt,
                    delay_ms = delay,
                    error = %error,
                    "retrying processor"
                );
                if let (Some(repository), Some(queue_id)) = (&repository, queue_id.as_deref()) {
                    let _ = repository
                        .record_event(
                            queue_id,
                            ProvenanceEvent::Retried,
                            serde_json::json!({
                                "attempt": packet.attempt,
                                "duration_ms": started.elapsed().as_millis(),
                                "delay_ms": delay,
                                "error": error.to_string()
                            }),
                        )
                        .await;
                }
                sleep(Duration::from_millis(delay)).await;
            }
            Err(error) => {
                context.metrics.failed();
                context
                    .metrics
                    .processor_finished(&context.processor_id, started.elapsed(), false);
                let error_message = error.to_string();
                warn!(
                    processor_id = %context.processor_id,
                    packet_id = %packet.id,
                    attempt = packet.attempt,
                    error = %error_message,
                    "processor failed"
                );
                packet
                    .attributes
                    .insert("error.processor".to_owned(), context.processor_id.clone());
                packet
                    .attributes
                    .insert("error.message".to_owned(), error_message.clone());
                let attempt = packet.attempt;
                if let Err(send_error) = output.emit("failure", packet).await {
                    warn!(
                        processor_id = %context.processor_id,
                        error = %send_error,
                        "could not route failed packet"
                    );
                }
                return Ok(Some((error_message, attempt)));
            }
        }
    }
}
