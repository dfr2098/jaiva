use std::{collections::BTreeMap, time::Instant};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    connectors::{WriteMode, WriteRequest},
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

/// Writes record packets through a database-independent writer connection.
pub struct PutDatabase {
    connection: String,
    request: WriteRequest,
}

#[derive(Deserialize)]
struct PutDatabaseConfig {
    connection: String,
    table: String,
    mode: WriteMode,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    columns: BTreeMap<String, String>,
    #[serde(default)]
    conflict_columns: Vec<String>,
}

fn default_batch_size() -> usize {
    1_000
}

impl PutDatabase {
    /// Parses static writer configuration.
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: PutDatabaseConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        Ok(Self {
            connection: config.connection,
            request: WriteRequest {
                table: config.table,
                mode: config.mode,
                columns: config.columns,
                conflict_columns: config.conflict_columns,
                batch_size: config.batch_size,
            },
        })
    }
}

#[async_trait]
impl Processor for PutDatabase {
    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let writer = context.connections.writer(&self.connection)?;
        let circuit = format!("database:{}", self.connection);
        if let Err(error) = context.circuits.permit(&circuit) {
            context.metrics.circuit_rejected();
            return Err(error);
        }
        writer.validate(&self.request)?;
        let records = packet.records().map_err(|message| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        })?;
        let started = Instant::now();
        let summary = match writer.write(&self.request, records).await {
            Ok(summary) => summary,
            Err(error) => {
                context.circuits.failure(&circuit);
                context
                    .metrics
                    .set_circuits_open(context.circuits.open_count());
                // A writer returns an error only after its transaction has been
                // rolled back or failed to commit.
                context.metrics.database_write_error();
                context.metrics.database_rollback();
                return Err(error);
            }
        };
        context.circuits.success(&circuit);
        context
            .metrics
            .set_circuits_open(context.circuits.open_count());
        let elapsed_ms = started.elapsed().as_millis() as u64;
        context
            .metrics
            .database_write(summary.rows, summary.batches, elapsed_ms);

        packet
            .attributes
            .insert("write.rows".to_owned(), summary.rows.to_string());
        packet
            .attributes
            .insert("write.batches".to_owned(), summary.batches.to_string());
        packet
            .attributes
            .insert("write.connection".to_owned(), self.connection.clone());
        packet.attributes.insert(
            "write.database_type".to_owned(),
            format!("{:?}", writer.kind()).to_lowercase(),
        );
        packet
            .attributes
            .insert("write.duration_ms".to_owned(), elapsed_ms.to_string());
        output.success(packet).await
    }
}
