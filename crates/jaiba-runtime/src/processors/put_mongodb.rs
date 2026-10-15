use std::time::{Duration, Instant};

use async_trait::async_trait;
use mongodb::{
    Collection, Database,
    bson::{Bson, Document, RawDocumentBuf, doc},
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::mongodb_support::{
    call_timeout, default_call_timeout_ms, default_database, is_transient_code, record_document,
    value_at_path, with_call_timeout,
};

/// Bytes of documents per `update` command. The server limit for a command is
/// 16 MiB plus 16 KiB of overhead; the margin covers filters and field names.
const UPSERT_COMMAND_BYTES: usize = 15 * 1024 * 1024;

/// Destino MongoDB. `upsert` reemplaza el documento completo que coincide con
/// los campos clave y resulta idempotente cuando dichas claves son estables.
pub struct PutMongoDb {
    connection: String,
    collection: String,
    mode: MongoWriteMode,
    key_fields: Vec<String>,
    batch_size: usize,
    ordered: bool,
    call_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MongoWriteMode {
    #[default]
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
    /// Espera máxima por cada lote escrito; 0 = sin límite.
    #[serde(default = "default_call_timeout_ms")]
    call_timeout_ms: u64,
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
            call_timeout: call_timeout(config.call_timeout_ms),
        })
    }

    /// Escribe todos los registros y devuelve `(documentos, lotes)`.
    pub(super) async fn write_records(
        &self,
        database: &Database,
        records: &[Value],
    ) -> Result<(u64, u64), FlowError> {
        let documents = records
            .iter()
            .enumerate()
            .map(|(index, record)| record_document(record, &format!("put_mongodb record {index}")))
            .collect::<Result<Vec<_>, _>>()?;
        let collection = database.collection::<Document>(&self.collection);
        match self.mode {
            MongoWriteMode::Insert => self.insert_documents(&collection, &documents).await,
            MongoWriteMode::Upsert => self.upsert_documents(database, &documents).await,
        }
    }

    async fn insert_documents(
        &self,
        collection: &Collection<Document>,
        documents: &[Document],
    ) -> Result<(u64, u64), FlowError> {
        let mut inserted = 0_u64;
        let mut batches = 0_u64;
        for batch in documents.chunks(self.batch_size) {
            let result = with_call_timeout(
                self.call_timeout,
                collection
                    .insert_many(batch)
                    .ordered(self.ordered)
                    .into_future(),
            )
            .await?;
            inserted += result.inserted_ids.len() as u64;
            batches += 1;
        }
        Ok((inserted, batches))
    }

    /// Un comando `update` por lote con una entrada `{q, u, upsert: true}` por
    /// documento: un viaje al servidor por lote en lugar de uno por documento.
    async fn upsert_documents(
        &self,
        database: &Database,
        documents: &[Document],
    ) -> Result<(u64, u64), FlowError> {
        let mut written = 0_u64;
        let mut batches = 0_u64;
        let mut updates = Vec::with_capacity(self.batch_size.min(documents.len()));
        let mut first_index = 0;
        let mut bytes = 0;
        for (index, document) in documents.iter().enumerate() {
            let filter = self.upsert_filter(document, index)?;
            let size = document_size(document)? + document_size(&filter)?;
            if !updates.is_empty()
                && (updates.len() == self.batch_size || bytes + size > UPSERT_COMMAND_BYTES)
            {
                written += self
                    .run_upsert(database, std::mem::take(&mut updates), first_index)
                    .await?;
                batches += 1;
                first_index = index;
                bytes = 0;
            }
            bytes += size;
            updates.push(Bson::Document(doc! {
                "q": filter,
                "u": document.clone(),
                "upsert": true,
            }));
        }
        if !updates.is_empty() {
            written += self.run_upsert(database, updates, first_index).await?;
            batches += 1;
        }
        Ok((written, batches))
    }

    async fn run_upsert(
        &self,
        database: &Database,
        updates: Vec<Bson>,
        first_index: usize,
    ) -> Result<u64, FlowError> {
        let command = doc! {
            "update": self.collection.as_str(),
            "updates": updates,
            "ordered": self.ordered,
        };
        let reply = with_call_timeout(
            self.call_timeout,
            database.run_command(command).into_future(),
        )
        .await?;
        upsert_reply(&reply, first_index)
    }

    fn upsert_filter(&self, document: &Document, index: usize) -> Result<Document, FlowError> {
        let mut filter = Document::new();
        for field in &self.key_fields {
            let value = value_at_path(document, field).ok_or_else(|| {
                FlowError::DatabaseRejected(format!(
                    "put_mongodb upsert record {index} is missing key field '{field}'"
                ))
            })?;
            filter.insert(field, value.clone());
        }
        Ok(filter)
    }
}

fn document_size(document: &Document) -> Result<usize, FlowError> {
    RawDocumentBuf::from_document(document)
        .map(|raw| raw.as_bytes().len())
        .map_err(|error| FlowError::DatabaseRejected(format!("put_mongodb record: {error}")))
}

/// Interpreta la respuesta de `update`: `n` cuenta documentos coincidentes más
/// insertados. `writeErrors` llega con `ok: 1`, así que se revisa aquí.
fn upsert_reply(reply: &Document, first_index: usize) -> Result<u64, FlowError> {
    if let Ok(errors) = reply.get_array("writeErrors")
        && !errors.is_empty()
    {
        let codes = errors
            .iter()
            .filter_map(Bson::as_document)
            .map(|error| error.get_i32("code").unwrap_or_default())
            .collect::<Vec<_>>();
        let first = errors.first().and_then(Bson::as_document);
        let index = first
            .and_then(|error| error.get_i32("index").ok())
            .unwrap_or_default() as usize;
        let message = format!(
            "put_mongodb upsert: {} write error(s); record {}: code {} ({})",
            errors.len(),
            first_index + index,
            codes.first().copied().unwrap_or_default(),
            first
                .and_then(|error| error.get_str("errmsg").ok())
                .unwrap_or("unknown error"),
        );
        return Err(if codes.iter().all(|code| is_transient_code(*code)) {
            FlowError::DatabaseConnector(message)
        } else {
            FlowError::DatabaseRejected(message)
        });
    }
    if let Ok(error) = reply.get_document("writeConcernError") {
        return Err(FlowError::DatabaseConnector(format!(
            "put_mongodb upsert write concern error: code {} ({})",
            error.get_i32("code").unwrap_or_default(),
            error.get_str("errmsg").unwrap_or("unknown error"),
        )));
    }
    let count = match reply.get("n") {
        Some(Bson::Int32(count)) => i64::from(*count),
        Some(Bson::Int64(count)) => *count,
        _ => 0,
    };
    Ok(count.max(0) as u64)
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
        let circuit = format!("mongodb:{}", self.connection);
        if let Err(error) = context.circuits.permit(&circuit) {
            context.metrics.circuit_rejected();
            return Err(error);
        }

        let records = packet.records().map_err(|message| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        })?;
        let started = Instant::now();
        let (written, batches) = match self.write_records(&database, records).await {
            Ok(written) => written,
            Err(error) => {
                // A rejected document proves the server is reachable; only
                // connectivity-style failures should open the circuit.
                if error.is_retryable() {
                    context.circuits.failure(&circuit);
                } else {
                    context.circuits.success(&circuit);
                }
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
            .upsert_filter(
                &doc! {
                    "tenant": "north",
                    "customer": { "id": 42_i32 }
                },
                0,
            )
            .expect("build filter");
        assert_eq!(filter.get_str("tenant"), Ok("north"));
        assert_eq!(filter.get("customer.id"), Some(&Bson::Int32(42)));
    }

    #[test]
    fn upsert_rejects_missing_keys_without_retry() {
        let processor = PutMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "mode": "upsert",
            "key_fields": ["external_id"]
        }))
        .expect("valid writer");
        let error = processor
            .upsert_filter(&doc! { "name": "Ada" }, 3)
            .expect_err("missing key must fail");
        assert!(error.to_string().contains("external_id"));
        assert!(error.to_string().contains("record 3"));
        assert!(!error.is_retryable());
    }

    #[test]
    fn upsert_reply_counts_and_classifies_write_errors() {
        assert_eq!(
            upsert_reply(&doc! { "n": 3, "nModified": 1, "ok": 1 }, 0).expect("count"),
            3
        );

        let duplicate = upsert_reply(
            &doc! {
                "n": 1,
                "writeErrors": [{ "index": 1, "code": 11000, "errmsg": "E11000 duplicate key" }],
                "ok": 1,
            },
            1000,
        )
        .expect_err("duplicate key");
        assert!(!duplicate.is_retryable());
        assert!(duplicate.to_string().contains("record 1001"));

        let conflict = upsert_reply(
            &doc! { "n": 0, "writeErrors": [{ "index": 0, "code": 112, "errmsg": "WriteConflict" }], "ok": 1 },
            0,
        )
        .expect_err("write conflict");
        assert!(conflict.is_retryable());

        let concern = upsert_reply(
            &doc! { "n": 2, "writeConcernError": { "code": 64, "errmsg": "waiting for replication timed out" }, "ok": 1 },
            0,
        )
        .expect_err("write concern");
        assert!(concern.is_retryable());
    }

    #[test]
    fn reads_call_timeout_with_zero_as_unlimited() {
        let writer = PutMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "call_timeout_ms": 0
        }))
        .expect("valid writer");
        assert_eq!(writer.call_timeout, None);
    }
}

#[cfg(test)]
#[path = "put_mongodb_live_tests.rs"]
mod live_tests;
