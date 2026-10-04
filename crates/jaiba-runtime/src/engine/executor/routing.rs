use std::collections::{HashMap, VecDeque};

use tracing::warn;

use crate::{
    config::{ConnectionConfig, ProcessorConfig},
    engine::{
        FlowMetrics, LocalPacketRepository, MemoryLimiter, PacketRepository, ProcessorEmission,
        ProvenanceEvent,
    },
    error::FlowError,
};

use super::{RouteOutcome, WorkItem, is_source_seed, metrics_sync::sync_repository_metrics};

#[allow(clippy::too_many_arguments)]
pub(super) async fn route_emission(
    emission: &ProcessorEmission,
    pending: &mut VecDeque<WorkItem>,
    connections: &[ConnectionConfig],
    global_capacity: usize,
    metrics: &FlowMetrics,
    definitions: &HashMap<String, ProcessorConfig>,
    active_per_processor: &HashMap<String, usize>,
    repository: Option<&LocalPacketRepository>,
    flow_id: &str,
    memory: &MemoryLimiter,
) -> Result<RouteOutcome, FlowError> {
    let next = outgoing(connections, &emission.processor_id, &emission.relationship);
    if next.is_empty() {
        warn!(
            processor_id = emission.processor_id,
            relationship = emission.relationship,
            "packet reached the end of the flow"
        );
        return Ok(RouteOutcome::Routed);
    }

    // Source seeds (no connection) are bounded by the processor count and must
    // not consume queue capacity, or many sources deadlock the router.
    let queued = pending.iter().filter(|item| !is_source_seed(item)).count();
    if queued.saturating_add(next.len()) > global_capacity {
        return Ok(RouteOutcome::QueueFull);
    }
    for connection in &next {
        let edge_size = pending
            .iter()
            .filter(|item| item.connection.as_deref() == Some(connection_id(connection).as_str()))
            .count();
        if edge_size >= connection.queue.capacity {
            return Ok(RouteOutcome::QueueFull);
        }
        if let Some(maximum) = definitions
            .get(&connection.to)
            .and_then(|definition| definition.scheduling.maximum_in_flight)
        {
            let queued = pending
                .iter()
                .filter(|item| item.processor_id == connection.to)
                .count();
            let active = active_per_processor
                .get(&connection.to)
                .copied()
                .unwrap_or_default();
            let incoming = next
                .iter()
                .filter(|candidate| candidate.to == connection.to)
                .count();
            if queued.saturating_add(active).saturating_add(incoming) > maximum {
                return Ok(RouteOutcome::QueueFull);
            }
        }
    }

    // Reserve every branch before cloning or publishing any of them.
    let mut reservations = Vec::with_capacity(next.len());
    for _ in &next {
        match memory.try_reserve(emission.packet.estimated_size()) {
            Ok(reservation) => reservations.push(reservation),
            Err(error @ FlowError::MemoryCapacity { .. }) => {
                return Ok(RouteOutcome::MemoryFull(error));
            }
            Err(error) => return Err(error),
        }
    }
    for (connection, reservation) in next.into_iter().zip(reservations) {
        let queue_id = if let Some(repository) = repository {
            let queue_id = repository
                .enqueue(
                    flow_id,
                    &connection.to,
                    &emission.relationship,
                    &emission.packet,
                )
                .await?;
            repository
                .record_event(
                    &queue_id,
                    ProvenanceEvent::Routed,
                    serde_json::json!({
                        "source_processor": connection.from,
                        "destination_processor": connection.to,
                        "relationship": emission.relationship,
                        "packet_bytes": emission.packet.estimated_size()
                    }),
                )
                .await?;
            Some(queue_id)
        } else {
            None
        };
        pending.push_back(WorkItem {
            processor_id: connection.to.clone(),
            packet: emission.packet.clone(),
            connection: Some(connection_id(connection)),
            reservation: Some(reservation),
            queue_id,
        });
        metrics.emitted(1);
    }
    if let Some(repository) = repository {
        sync_repository_metrics(repository, metrics).await?;
    }
    Ok(RouteOutcome::Routed)
}

pub(super) fn connection_id(connection: &ConnectionConfig) -> String {
    format!(
        "{}.{}.{}",
        connection.from, connection.relationship, connection.to
    )
}

fn outgoing<'a>(
    connections: &'a [ConnectionConfig],
    processor_id: &str,
    relationship: &str,
) -> Vec<&'a ConnectionConfig> {
    connections
        .iter()
        .filter(|connection| {
            connection.from == processor_id && connection.relationship == relationship
        })
        .collect()
}
