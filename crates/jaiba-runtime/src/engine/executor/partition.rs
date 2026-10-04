use serde_json::Value;

use crate::{engine::DataPacket, error::FlowError};

pub(super) fn packet_partition_key(
    packet: &DataPacket,
    selector: &str,
    processor_id: &str,
) -> Result<String, FlowError> {
    let attribute = selector.strip_prefix("attribute.").unwrap_or(selector);
    if let Some(value) = packet.attributes.get(attribute) {
        return Ok(value.clone());
    }
    let records = packet.records().map_err(|message| FlowError::Processor {
        processor_id: processor_id.to_owned(),
        message: format!("cannot resolve partition '{selector}': {message}"),
    })?;
    let mut values = records.iter().map(|record| {
        record
            .get(selector)
            .map(canonical_partition_value)
            .ok_or_else(|| FlowError::Processor {
                processor_id: processor_id.to_owned(),
                message: format!("partition field '{selector}' is missing"),
            })
    });
    let first = values
        .next()
        .transpose()?
        .ok_or_else(|| FlowError::Processor {
            processor_id: processor_id.to_owned(),
            message: format!("cannot partition an empty packet by '{selector}'"),
        })?;
    for value in values {
        if value? != first {
            return Err(FlowError::Processor {
                processor_id: processor_id.to_owned(),
                message: format!(
                    "packet contains multiple values for partition field '{selector}'; split it before this processor"
                ),
            });
        }
    }
    Ok(first)
}

fn canonical_partition_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}
