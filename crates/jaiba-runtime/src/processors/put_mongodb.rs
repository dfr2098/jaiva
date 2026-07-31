use std::time::Instant;

use async_trait::async_trait;
use mongodb::bson::Document;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::mongodb_support::{default_database, json_document, value_at_path};

/// Destino MongoDB. `upsert` reemplaza el documento completo que coincide con
/// los campos clave y resulta idempotente cuando dichas claves son estables.
pub struct PutMongoDb {
    connection: String,
    collection: String,
    mode: MongoWriteMode,
    key_fields: Vec<String>,
    batch_size: usize,
    ordered: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MongoWriteMode {
    Insert,
    Upsert,
}

#[derive(Deserialize)]
struct PutMongoDbConfig {
    connection: String,
    collection: String,
    #[serde(default)]
    mode: MongoWriteMode,
    #[serde(default = "default_key_fields")]
    key_fields: Vec<String>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    #[serde(default = "default_ordered")]
    ordered: bool,
}

impl Default for MongoWriteMode {
    fn default() -> Self {
        Self::Insert
    }
}

fn default_key_fields() -> Vec<String> {
    vec!["_id".to_owned()]
}

fn default_batch_size() -> usize {
    1_000
}

fn default_ordered() -> bool {
    true
}

impl PutMongoDb {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: PutMongoDbConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.connection.trim().is_empty() {
            return Err(FlowError::Configuration(
                "put_mongodb requires a connection".to_owned(),
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(FlowError::Configuration(
                "put_mongodb requires a collection".to_owned(),
            ));
        }
        if config.batch_size == 0 {
            return Err(FlowError::Configuration(
                "put_mongodb batch_size must be greater than zero".to_owned(),
            ));
        }
        if matches!(config.mode, MongoWriteMode::Upsert)
            && (config.key_fields.is_empty()
                || config
                    .key_fields
                    .iter()
                    .any(|field| field.trim().is_empty()))
        {
            return Err(FlowError::Configuration(
                "put_mongodb upsert requires non-empty key_fields".to_owned(),
            ));
        }

        Ok(Self {
            connection: config.connection,
            collection: config.collection,
            mode: config.mode,
            key_fields: config.key_fields,
            batch_size: config.batch_size,
            ordered: config.ordered,
        })
    }
}

#[async_trait]
impl Processor for PutMongoDb {
    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let client = context.connections.mongodb(&self.connection)?;
        let database = default_database(client)?;
        let collection = database.collection::<Document>(&self.collection);
        let circuit = format!("mongodb:{}", self.connection);
        if let Err(error) = context.circuits.permit(&circuit) {
            context.metrics.circuit_rejected();
            return Err(error);
        }

        let records = packet.records().map_err(|message| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        })?;
        let documents = records
            .iter()
            .enumerate()
            .map(|(index, record)| json_document(record, &format!("put_mongodb record {index}")))
            .collect::<Result<Vec<_>, _>>()?;
        let started = Instant::now();
        let result = match self.mode {
            MongoWriteMode::Insert => self.insert_documents(&collection, &documents).await,
            MongoWriteMode::Upsert => self.upsert_documents(&collection, &documents).await,
        };
        let written = match result {
            Ok(written) => written,
            Err(error) => {
                context.circuits.failure(&circuit);
                context
                    .metrics
                    .set_circuits_open(context.circuits.open_count());
                context.metrics.database_write_error();
                return Err(error);
            }
        };
        context.circuits.success(&circuit);
        context
            .metrics
            .set_circuits_open(context.circuits.open_count());

        let batches = documents.len().div_ceil(self.batch_size) as u64;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        context.metrics.database_write(written, batches, elapsed_ms);
        packet
            .attributes
            .insert("write.rows".to_owned(), written.to_string());
        packet
            .attributes
            .insert("write.batches".to_owned(), batches.to_string());
        packet
            .attributes
            .insert("write.connection".to_owned(), self.connection.clone());
        packet
            .attributes
            .insert("write.database_type".to_owned(), "mongodb".to_owned());
        packet
            .attributes
            .insert("write.collection".to_owned(), self.collection.clone());
        packet.attributes.insert(
            "write.mode".to_owned(),
            match self.mode {
                MongoWriteMode::Insert => "insert",
                MongoWriteMode::Upsert => "upsert",
            }
            .to_owned(),
        );
        packet
            .attributes
            .insert("write.duration_ms".to_owned(), elapsed_ms.to_string());
        output.success(packet).await
    }
}

impl PutMongoDb {
    async fn insert_documents(
        &self,
        collection: &mongodb::Collection<Document>,
        documents: &[Document],
    ) -> Result<u64, FlowError> {
        let mut inserted = 0_u64;
        for batch in documents.chunks(self.batch_size) {
            if batch.is_empty() {
                continue;
            }
            let result = collection
                .insert_many(batch.to_vec())
                .ordered(self.ordered)
                .await
                .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?;
            inserted += result.inserted_ids.len() as u64;
        }
        Ok(inserted)
    }

    async fn upsert_documents(
        &self,
        collection: &mongodb::Collection<Document>,
        documents: &[Document],
    ) -> Result<u64, FlowError> {
        let mut written = 0_u64;
        for batch in documents.chunks(self.batch_size) {
            for document in batch {
                let filter = self.upsert_filter(document)?;
                let result = collection
                    .replace_one(filter, document.clone())
                    .upsert(true)
                    .await
                    .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?;
                written += result.modified_count + u64::from(result.upserted_id.is_some());
                if result.matched_count > 0 && result.modified_count == 0 {
                    // A retry may replace a document with identical content; it still
                    // counts as a successfully handled record.
                    written += 1;
                }
            }
        }
        Ok(written)
    }

    fn upsert_filter(&self, document: &Document) -> Result<Document, FlowError> {
        let mut filter = Document::new();
        for field in &self.key_fields {
            let value = value_at_path(document, field).ok_or_else(|| {
                FlowError::Configuration(format!(
                    "put_mongodb upsert record is missing key field '{field}'"
                ))
            })?;
            filter.insert(field, value.clone());
        }
        Ok(filter)
    }
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, doc};
    use serde_json::json;

    use super::*;

    #[test]
    fn upsert_builds_filter_from_nested_keys() {
        let processor = PutMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "mode": "upsert",
            "key_fields": ["tenant", "customer.id"]
        }))
        .expect("valid writer");
        let filter = processor
            .upsert_filter(&doc! {
                "tenant": "north",
                "customer": { "id": 42_i32 }
            })
            .expect("build filter");
        assert_eq!(filter.get_str("tenant"), Ok("north"));
        assert_eq!(filter.get("customer.id"), Some(&Bson::Int32(42)));
    }

    #[test]
    fn upsert_rejects_missing_keys() {
        let processor = PutMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "mode": "upsert",
            "key_fields": ["external_id"]
        }))
        .expect("valid writer");
        let error = processor
            .upsert_filter(&doc! { "name": "Ada" })
            .expect_err("missing key must fail");
        assert!(error.to_string().contains("external_id"));
    }
}
