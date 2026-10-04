use std::collections::{HashMap, VecDeque};

use crate::{
    config::{ConnectionConfig, OrderingMode, ProcessorConfig},
    engine::{FlowMetrics, LocalPacketRepository, PacketRepository},
    error::FlowError,
};

use super::{WorkItem, routing::connection_id};

pub(super) async fn sync_repository_metrics(
    repository: &LocalPacketRepository,
    metrics: &FlowMetrics,
) -> Result<(), FlowError> {
    let stats = repository.stats().await?;
    metrics.set_repository(
        stats.pending,
        stats.running,
        stats.dead_letter,
        stats.content_bytes,
    );
    Ok(())
}

pub(super) fn sync_processor_metrics(
    metrics: &FlowMetrics,
    definitions: &HashMap<String, ProcessorConfig>,
    pending: &VecDeque<WorkItem>,
    active: &HashMap<String, usize>,
) {
    for (id, definition) in definitions {
        let queue_depth = pending
            .iter()
            .filter(|item| item.processor_id == *id)
            .count();
        let concurrency_limit = match definition.scheduling.ordering {
            OrderingMode::Preserve => 1,
            OrderingMode::Unordered | OrderingMode::Partitioned => {
                definition.scheduling.concurrent_tasks
            }
        };
        metrics.set_processor_load(
            id,
            queue_depth,
            active.get(id).copied().unwrap_or_default(),
            concurrency_limit,
        );
    }
}

pub(super) fn empty_connection_queues(
    connections: &[ConnectionConfig],
) -> HashMap<String, (u64, u64)> {
    connections
        .iter()
        .map(|connection| (connection_id(connection), (0, 0)))
        .collect()
}

pub(super) fn sync_connection_metrics(
    metrics: &FlowMetrics,
    connections: &[ConnectionConfig],
    pending: &VecDeque<WorkItem>,
) {
    let mut queues = empty_connection_queues(connections);
    for item in pending {
        let Some(connection) = item.connection.as_ref() else {
            continue;
        };
        let queue = queues.entry(connection.clone()).or_default();
        queue.0 = queue.0.saturating_add(1);
        queue.1 = queue.1.saturating_add(item.packet.estimated_size() as u64);
    }
    metrics.set_connection_queues(queues);
}
