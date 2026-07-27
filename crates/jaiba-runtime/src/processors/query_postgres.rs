use async_trait::async_trait;
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub struct QueryPostgres {
    connection: String,
    query: String,
    batch_size: usize,
}

#[derive(Deserialize)]
struct QueryPostgresConfig {
    connection: String,
    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_batch_size() -> usize {
    1_000
}

impl QueryPostgres {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: QueryPostgresConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;

        if config.query.trim().is_empty() {
            return Err(FlowError::Configuration(
                "query_postgres requires a non-empty query".to_owned(),
            ));
        }

        Ok(Self {
            connection: config.connection,
            query: config.query,
            batch_size: config.batch_size.max(1),
        })
    }
}

#[async_trait]
impl Processor for QueryPostgres {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let pool = context.connections.postgres(&self.connection)?;
        let mut rows = sqlx::query_scalar::<_, Value>(&self.query).fetch(pool);
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut batch_number = 0_u64;

        while let Some(record) = rows.try_next().await? {
            batch.push(record);
            if batch.len() == self.batch_size {
                output
                    .success(make_packet(
                        &packet,
                        std::mem::take(&mut batch),
                        batch_number,
                    ))
                    .await?;
                batch = Vec::with_capacity(self.batch_size);
                batch_number += 1;
            }
        }

        if !batch.is_empty() || batch_number == 0 {
            output
                .success(make_packet(&packet, batch, batch_number))
                .await?;
        }

        Ok(())
    }
}

fn make_packet(template: &DataPacket, records: Vec<Value>, batch_number: u64) -> DataPacket {
    let mut packet = DataPacket::with_records(records);
    packet.attributes.clone_from(&template.attributes);
    packet
        .attributes
        .insert("source.database".to_owned(), "postgresql".to_owned());
    packet
        .attributes
        .insert("batch.number".to_owned(), batch_number.to_string());
    packet.attributes.insert(
        "record.count".to_owned(),
        packet.records().expect("records packet").len().to_string(),
    );
    packet
}
