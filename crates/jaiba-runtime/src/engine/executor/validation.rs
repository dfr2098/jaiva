use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::{
    config::{ConnectionConfig, FlowConfig, OrderingMode},
    error::FlowError,
};

pub(super) fn processor_downstream_depths(config: &FlowConfig) -> HashMap<String, usize> {
    fn visit(
        processor: &str,
        connections: &[ConnectionConfig],
        memo: &mut HashMap<String, usize>,
        visiting: &mut HashSet<String>,
    ) -> usize {
        if let Some(depth) = memo.get(processor) {
            return *depth;
        }
        if !visiting.insert(processor.to_owned()) {
            return 0;
        }
        let depth = connections
            .iter()
            .filter(|connection| connection.from == processor)
            .map(|connection| {
                1_usize.saturating_add(visit(&connection.to, connections, memo, visiting))
            })
            .max()
            .unwrap_or_default();
        visiting.remove(processor);
        memo.insert(processor.to_owned(), depth);
        depth
    }

    let mut depths = HashMap::new();
    for processor in &config.processors {
        visit(
            &processor.id,
            &config.connections,
            &mut depths,
            &mut HashSet::new(),
        );
    }
    depths
}

pub(super) fn resolve_processor_parameters(config: &mut FlowConfig) -> Result<(), FlowError> {
    for processor in &mut config.processors {
        interpolate_value(&mut processor.config, &config.parameters)?;
    }
    Ok(())
}

fn interpolate_value(
    value: &mut Value,
    parameters: &HashMap<String, String>,
) -> Result<(), FlowError> {
    match value {
        Value::String(text) => {
            for (name, replacement) in parameters {
                *text = text.replace(&format!("${{{name}}}"), replacement);
            }
            resolve_environment_placeholders(text)?;
            if text.contains("${") {
                return Err(FlowError::Configuration(format!(
                    "unresolved parameter in '{text}'"
                )));
            }
        }
        Value::Array(items) => {
            for item in items {
                interpolate_value(item, parameters)?;
            }
        }
        Value::Object(object) => {
            for item in object.values_mut() {
                interpolate_value(item, parameters)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_environment_placeholders(text: &mut String) -> Result<(), FlowError> {
    while let Some(start) = text.find("${env:") {
        let name_start = start + "${env:".len();
        let relative_end = text[name_start..].find('}').ok_or_else(|| {
            FlowError::Configuration(format!("invalid environment placeholder in '{text}'"))
        })?;
        let end = name_start + relative_end;
        let name = &text[name_start..end];
        let value = std::env::var(name).map_err(|_| {
            FlowError::Configuration(format!(
                "environment variable '{name}' required by processor configuration is missing"
            ))
        })?;
        text.replace_range(start..=end, &value);
    }
    Ok(())
}

pub(super) fn validate(config: &FlowConfig) -> Result<(), FlowError> {
    if config.processors.is_empty() {
        return Err(FlowError::Configuration(
            "the flow must contain at least one processor".to_owned(),
        ));
    }
    if config.engine.queue_capacity == 0 || config.engine.max_concurrency == 0 {
        return Err(FlowError::Configuration(
            "engine capacities must be greater than zero".to_owned(),
        ));
    }
    {
        let mut fanout: HashMap<(&str, &str), usize> = HashMap::new();
        for connection in &config.connections {
            *fanout
                .entry((connection.from.as_str(), connection.relationship.as_str()))
                .or_default() += 1;
        }
        if let Some(max_fanout) = fanout.values().copied().max()
            && config.engine.queue_capacity < max_fanout
        {
            return Err(FlowError::Configuration(format!(
                "engine.queue_capacity ({}) must be at least the maximum fan-out per relationship ({max_fanout})",
                config.engine.queue_capacity
            )));
        }
    }
    if config.engine.shutdown.drain_timeout_seconds == 0 {
        return Err(FlowError::Configuration(
            "shutdown drain_timeout_seconds must be greater than zero".to_owned(),
        ));
    }
    if config.engine.admin.max_request_body_bytes == 0 {
        return Err(FlowError::Configuration(
            "admin max_request_body_bytes must be greater than zero".to_owned(),
        ));
    }
    if config.engine.admin.enabled
        && config.engine.admin.authentication == crate::config::AdminAuthentication::Bearer
        && config.engine.admin.token_env.trim().is_empty()
    {
        return Err(FlowError::Configuration(
            "admin token_env cannot be empty when the administrative API is enabled".to_owned(),
        ));
    }
    if config.engine.workers.cpu_threads > 1024 || config.engine.workers.blocking_threads > 1024 {
        return Err(FlowError::Configuration(
            "worker thread limits cannot exceed 1024".to_owned(),
        ));
    }

    let mut ids = HashSet::new();
    for processor in &config.processors {
        if processor.simulation.mode != crate::config::DataExecutionMode::Real {
            return Err(FlowError::Configuration(format!(
                "processor '{}' uses {:?} data mode; execute it through jaiba-simulator",
                processor.id, processor.simulation.mode
            )));
        }
        if processor.scheduling.concurrent_tasks == 0 {
            return Err(FlowError::Configuration(format!(
                "processor '{}' must allow at least one concurrent task",
                processor.id
            )));
        }
        if processor.scheduling.maximum_in_flight == Some(0) {
            return Err(FlowError::Configuration(format!(
                "processor '{}' maximum_in_flight must be greater than zero",
                processor.id
            )));
        }
        if processor
            .scheduling
            .maximum_in_flight
            .is_some_and(|maximum| maximum < processor.scheduling.concurrent_tasks)
        {
            return Err(FlowError::Configuration(format!(
                "processor '{}' maximum_in_flight cannot be lower than concurrent_tasks",
                processor.id
            )));
        }
        if processor.scheduling.ordering == OrderingMode::Partitioned
            && processor
                .scheduling
                .partition_by
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(FlowError::Configuration(format!(
                "processor '{}' uses partitioned ordering but has no partition_by selector",
                processor.id
            )));
        }
        if !ids.insert(processor.id.as_str()) {
            return Err(FlowError::Configuration(format!(
                "duplicate processor id '{}'",
                processor.id
            )));
        }
    }

    for connection in &config.connections {
        if connection.queue.capacity == 0 {
            return Err(FlowError::Configuration(
                "connection queue capacity must be greater than zero".to_owned(),
            ));
        }
        if !ids.contains(connection.from.as_str()) || !ids.contains(connection.to.as_str()) {
            return Err(FlowError::Configuration(format!(
                "connection '{} -> {}' references an unknown processor",
                connection.from, connection.to
            )));
        }
    }

    let incoming: HashSet<&str> = config
        .connections
        .iter()
        .map(|connection| connection.to.as_str())
        .collect();
    if config
        .processors
        .iter()
        .all(|processor| incoming.contains(processor.id.as_str()))
    {
        return Err(FlowError::Configuration(
            "the flow has no starting processor; check for cycles".to_owned(),
        ));
    }
    let required_pipeline_slots = processor_downstream_depths(config)
        .values()
        .copied()
        .max()
        .unwrap_or_default()
        .saturating_add(1);
    if config.engine.max_concurrency < required_pipeline_slots {
        return Err(FlowError::Configuration(format!(
            "engine max_concurrency must be at least {required_pipeline_slots} for the longest streaming path"
        )));
    }
    Ok(())
}
