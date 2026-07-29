use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

/// Read-only Oracle source that emits one record packet per configured batch.
///
/// The connector performs blocking OCI work outside Tokio's async workers;
/// this processor handles validation and packet metadata.
pub struct QueryOracle {
    connection: String,
    query: String,
    batch_size: usize,
}

#[derive(Deserialize)]
struct QueryOracleConfig {
    /// Connection alias declared in YAML or resolved by Connection Manager.
    connection: String,
    /// Read-only SQL. DML and DDL are rejected before opening the connection.
    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

fn default_batch_size() -> usize {
    1_000
}

impl QueryOracle {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: QueryOracleConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        let query = config.query.trim();
        // Early safety guard rather than a full SQL parser. OCI validates the
        // complete statement when the connector executes it.
        let keyword = query
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        if !matches!(keyword.as_str(), "SELECT" | "WITH") {
            return Err(FlowError::Configuration(
                "query_oracle accepts only SELECT or WITH queries".to_owned(),
            ));
        }
        Ok(Self {
            connection: config.connection,
            query: query.to_owned(),
            batch_size: config.batch_size.max(1),
        })
    }
}

#[async_trait]
impl Processor for QueryOracle {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let batches = context
            .connections
            .oracle(&self.connection)?
            .query_json_batches(&self.query, self.batch_size)
            .await?;
        for (batch_number, records) in batches.into_iter().enumerate() {
            let mut result = DataPacket::with_records(records);
            // Preserve correlation attributes and add source metadata used by
            // provenance, metrics and downstream diagnostics.
            result.attributes.clone_from(&packet.attributes);
            result
                .attributes
                .insert("source.database".to_owned(), "oracle".to_owned());
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
        let processor = QueryOracle::from_config(&json!({
            "connection": "oracle",
            "query": "  SELECT 1 FROM DUAL",
            "batch_size": 0
        }))
        .unwrap();
        assert_eq!(processor.query, "SELECT 1 FROM DUAL");
        assert_eq!(processor.batch_size, 1);
    }

    #[test]
    fn rejects_non_query_statements() {
        let error = QueryOracle::from_config(&json!({
            "connection": "oracle",
            "query": "DELETE FROM customers"
        }))
        .err()
        .expect("writes must be rejected");
        assert!(error.to_string().contains("SELECT or WITH"));
    }
}
