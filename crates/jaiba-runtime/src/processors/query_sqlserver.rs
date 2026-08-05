use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

/// Lectura SQL Server (Tiberius) que emite un paquete JSON por lote.
pub struct QuerySqlServer {
    connection: String,
    query: String,
    parameters: Vec<Value>,
    batch_size: usize,
}

#[derive(Deserialize)]
struct QuerySqlServerConfig {
    connection: String,
    query: String,
    /// Parámetros ligados (`@P1`, `@P2`, …).
    #[serde(default)]
    parameters: Vec<Value>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_batch_size() -> usize {
    1_000
}

impl QuerySqlServer {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: QuerySqlServerConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        let query = config.query.trim();
        let keyword = query
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        if !matches!(keyword.as_str(), "SELECT" | "WITH") {
            return Err(FlowError::Configuration(
                "query_sqlserver accepts only SELECT or WITH queries".to_owned(),
            ));
        }
        for parameter in &config.parameters {
            validate_parameter(parameter)?;
        }
        Ok(Self {
            connection: config.connection,
            query: query.to_owned(),
            parameters: config.parameters,
            batch_size: config.batch_size.max(1),
        })
    }
}

fn validate_parameter(value: &Value) -> Result<(), FlowError> {
    match value {
        Value::Null => Err(FlowError::Configuration(
            "query_sqlserver does not accept untyped null parameters; use IS NULL".to_owned(),
        )),
        Value::Number(number) if number.as_i64().is_none() && number.as_f64().is_none() => {
            Err(FlowError::Configuration(format!(
                "query_sqlserver numeric parameter is outside the supported range: {number}"
            )))
        }
        Value::Number(number) if number.as_u64().is_some_and(|value| value > i64::MAX as u64) => {
            Err(FlowError::Configuration(format!(
                "query_sqlserver integer parameter exceeds signed BIGINT: {number}"
            )))
        }
        _ => Ok(()),
    }
}

#[async_trait]
impl Processor for QuerySqlServer {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let batches = context
            .connections
            .sqlserver(&self.connection)?
            .query_json_batches(&self.query, &self.parameters, self.batch_size)
            .await?;
        for (batch_number, records) in batches.into_iter().enumerate() {
            let mut result = DataPacket::with_records(records);
            result.attributes.clone_from(&packet.attributes);
            result
                .attributes
                .insert("source.database".to_owned(), "sqlserver".to_owned());
            result
                .attributes
                .insert("batch.number".to_owned(), batch_number.to_string());
            result.attributes.insert(
                "record.count".to_owned(),
                result.records().expect("records packet").len().to_string(),
            );
            output.success(result).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_select_and_normalizes_batch_size() {
        let processor = QuerySqlServer::from_config(&json!({
            "connection": "mssql",
            "query": "  SELECT TOP (1) id FROM dbo.items",
            "batch_size": 0
        }))
        .unwrap();
        assert_eq!(processor.query, "SELECT TOP (1) id FROM dbo.items");
        assert_eq!(processor.batch_size, 1);
    }

    #[test]
    fn rejects_non_query_statements() {
        let error = QuerySqlServer::from_config(&json!({
            "connection": "mssql",
            "query": "DELETE FROM dbo.items"
        }))
        .err()
        .expect("writes must be rejected");
        assert!(error.to_string().contains("SELECT or WITH"));
    }

    #[test]
    fn rejects_untyped_null_parameters() {
        let error = QuerySqlServer::from_config(&json!({
            "connection": "mssql",
            "query": "SELECT id FROM dbo.items WHERE id = @P1",
            "parameters": [null]
        }))
        .err()
        .expect("null must be rejected");
        assert!(error.to_string().contains("untyped null"));
    }
}
