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
    parameters: Vec<Value>,
    batch_size: usize,
}

#[derive(Deserialize)]
struct QueryPostgresConfig {
    connection: String,
    query: String,
    /// Parámetros ligados (`$1`, `$2`, …). El constructor visual genera SQL
    /// parametrizado, por lo que los valores nunca se interpolan en el texto.
    #[serde(default)]
    parameters: Vec<Value>,
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
        for parameter in &config.parameters {
            validate_parameter(parameter)?;
        }

        Ok(Self {
            connection: config.connection,
            query: config.query,
            parameters: config.parameters,
            batch_size: config.batch_size.max(1),
        })
    }
}

fn validate_parameter(value: &Value) -> Result<(), FlowError> {
    match value {
        Value::Null => Err(FlowError::Configuration(
            "query_postgres does not accept untyped null parameters; use IS NULL".to_owned(),
        )),
        Value::Number(number) if number.as_i64().is_none() && number.as_f64().is_none() => {
            Err(FlowError::Configuration(format!(
                "query_postgres numeric parameter is outside the supported range: {number}"
            )))
        }
        Value::Number(number) if number.as_u64().is_some_and(|value| value > i64::MAX as u64) => {
            Err(FlowError::Configuration(format!(
                "query_postgres integer parameter exceeds PostgreSQL BIGINT: {number}"
            )))
        }
        _ => Ok(()),
    }
}

/// Liga un valor JSON al tipo escalar SQL adecuado. Las listas/objetos se
/// envían como JSONB para no romper la ejecución si aparecen.
fn bind_json<'q>(
    query: sqlx::query::QueryScalar<'q, sqlx::Postgres, Value, sqlx::postgres::PgArguments>,
    value: &'q Value,
) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, Value, sqlx::postgres::PgArguments> {
    match value {
        Value::Null => unreachable!("null parameters are rejected during configuration"),
        Value::Bool(flag) => query.bind(*flag),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                query.bind(integer)
            } else {
                query.bind(number.as_f64().unwrap_or_default())
            }
        }
        Value::String(text) => query.bind(text.as_str()),
        other => query.bind(other.clone()),
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
        let mut query = sqlx::query_scalar::<_, Value>(&self.query);
        for parameter in &self.parameters {
            query = bind_json(query, parameter);
        }
        let mut rows = query.fetch(pool);
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_untyped_null_parameters() {
        let error = QueryPostgres::from_config(&json!({
            "connection": "main",
            "query": "SELECT jsonb_build_object('id', id) FROM items WHERE id = $1",
            "parameters": [null]
        }))
        .err()
        .expect("null must be rejected");
        assert!(error.to_string().contains("untyped null"));
    }

    #[test]
    fn rejects_integers_larger_than_postgres_bigint() {
        let error = QueryPostgres::from_config(&json!({
            "connection": "main",
            "query": "SELECT jsonb_build_object('id', id) FROM items WHERE id = $1",
            "parameters": [u64::MAX]
        }))
        .err()
        .expect("oversized integer must be rejected");
        assert!(error.to_string().contains("BIGINT"));
    }
}
