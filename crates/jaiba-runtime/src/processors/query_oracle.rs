use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    connectors::{ORACLE_DEFAULT_CALL_TIMEOUT, OracleQueryOptions},
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

/// Read-only Oracle source that emits one record packet per configured batch.
///
/// Rows are streamed: the connector fetches on Tokio's blocking pool and only
/// a couple of batches are buffered ahead of the flow, so large results do not
/// need to fit in memory. A retry after a mid-stream failure re-reads the query
/// from the start (at-least-once delivery).
pub struct QueryOracle {
    connection: String,
    query: String,
    batch_size: usize,
    call_timeout: Option<Duration>,
}

#[derive(Deserialize)]
struct QueryOracleConfig {
    /// Connection alias declared in YAML or resolved by Connection Manager.
    connection: String,
    /// Read-only SQL. DML and DDL are rejected before opening the connection.
    query: String,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    /// Limit for each round trip to Oracle; `0` disables it.
    #[serde(default = "default_call_timeout_ms")]
    call_timeout_ms: u64,
}

fn default_batch_size() -> usize {
    1_000
}

fn default_call_timeout_ms() -> u64 {
    ORACLE_DEFAULT_CALL_TIMEOUT.as_millis() as u64
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
            call_timeout: (config.call_timeout_ms > 0)
                .then(|| Duration::from_millis(config.call_timeout_ms)),
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
        let mut batches = context.connections.oracle(&self.connection)?.query_stream(
            &self.query,
            OracleQueryOptions {
                batch_size: self.batch_size,
                call_timeout: self.call_timeout,
            },
        );
        let mut batch_number = 0_u64;
        while let Some(records) = batches.next_batch().await {
            let mut result = DataPacket::with_records(records?);
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
            batch_number += 1;
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
        assert_eq!(processor.call_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn call_timeout_zero_disables_the_limit() {
        let processor = QueryOracle::from_config(&json!({
            "connection": "oracle",
            "query": "SELECT 1 FROM DUAL",
            "call_timeout_ms": 0
        }))
        .unwrap();
        assert_eq!(processor.call_timeout, None);
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
