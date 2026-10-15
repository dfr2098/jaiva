use std::time::Duration;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::{
    Collection,
    bson::{Bson, Document},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::mongodb_support::{
    JsonFormat, call_timeout, default_call_timeout_ms, default_database, document_json,
    json_document, with_call_timeout,
};

/// Fuente MongoDB con lectura streaming y memoria acotada por `batch_size`.
pub struct QueryMongoDb {
    connection: String,
    collection: String,
    filter: Document,
    projection: Option<Document>,
    sort: Option<Document>,
    skip: u64,
    limit: Option<i64>,
    batch_size: usize,
    call_timeout: Option<Duration>,
    max_time: Option<Duration>,
    json_format: JsonFormat,
}

#[derive(Deserialize)]
struct QueryMongoDbConfig {
    connection: String,
    collection: String,
    #[serde(default = "empty_object")]
    filter: Value,
    #[serde(default)]
    projection: Option<Value>,
    #[serde(default)]
    sort: Option<Value>,
    #[serde(default)]
    skip: u64,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    /// Espera máxima por cada respuesta del servidor (`find`/`getMore`); 0 = sin límite.
    #[serde(default = "default_call_timeout_ms")]
    call_timeout_ms: u64,
    /// `maxTimeMS` del servidor: tiempo total de ejecución del cursor; 0 = sin límite.
    #[serde(default)]
    max_time_ms: u64,
    #[serde(default)]
    json_format: JsonFormat,
}

fn empty_object() -> Value {
    json!({})
}

fn default_batch_size() -> usize {
    1_000
}

impl QueryMongoDb {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: QueryMongoDbConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.connection.trim().is_empty() {
            return Err(FlowError::Configuration(
                "query_mongodb requires a connection".to_owned(),
            ));
        }
        if config.collection.trim().is_empty() {
            return Err(FlowError::Configuration(
                "query_mongodb requires a collection".to_owned(),
            ));
        }
        if config.batch_size == 0 {
            return Err(FlowError::Configuration(
                "query_mongodb batch_size must be greater than zero".to_owned(),
            ));
        }
        if config.limit.is_some_and(|limit| limit <= 0) {
            return Err(FlowError::Configuration(
                "query_mongodb limit must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            connection: config.connection,
            collection: config.collection,
            filter: json_document(&config.filter, "query_mongodb filter")?,
            projection: config
                .projection
                .as_ref()
                .map(|value| json_document(value, "query_mongodb projection"))
                .transpose()?,
            sort: config
                .sort
                .as_ref()
                .map(|value| json_document(value, "query_mongodb sort"))
                .transpose()?,
            skip: config.skip,
            limit: config.limit,
            batch_size: config.batch_size,
            call_timeout: call_timeout(config.call_timeout_ms),
            max_time: call_timeout(config.max_time_ms),
            json_format: config.json_format,
        })
    }

    /// Lee el cursor y emite un paquete cada `batch_size` documentos. `comment`
    /// identifica la operación en `currentOp` y en el profiler del servidor.
    pub(super) async fn stream_documents(
        &self,
        collection: &Collection<Document>,
        comment: &str,
        packet: &DataPacket,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let driver_batch_size = u32::try_from(self.batch_size).map_err(|_| {
            FlowError::Configuration("query_mongodb batch_size exceeds u32".to_owned())
        })?;

        let mut find = collection
            .find(self.filter.clone())
            .skip(self.skip)
            .batch_size(driver_batch_size)
            .comment(Bson::String(comment.to_owned()));
        if let Some(projection) = &self.projection {
            find = find.projection(projection.clone());
        }
        if let Some(sort) = &self.sort {
            find = find.sort(sort.clone());
        }
        if let Some(limit) = self.limit {
            find = find.limit(limit);
        }
        if let Some(max_time) = self.max_time {
            find = find.max_time(max_time);
        }

        let mut cursor = with_call_timeout(self.call_timeout, find.into_future()).await?;
        let mut records = Vec::with_capacity(self.batch_size);
        let mut batch_number = 0_u64;

        while let Some(document) = with_call_timeout(self.call_timeout, cursor.try_next()).await? {
            records.push(document_json(document, self.json_format));
            if records.len() == self.batch_size {
                output
                    .success(make_packet(
                        packet,
                        std::mem::take(&mut records),
                        &self.collection,
                        batch_number,
                    ))
                    .await?;
                records = Vec::with_capacity(self.batch_size);
                batch_number += 1;
            }
        }

        if !records.is_empty() || batch_number == 0 {
            output
                .success(make_packet(packet, records, &self.collection, batch_number))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Processor for QueryMongoDb {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let client = context.connections.mongodb(&self.connection)?;
        let database = default_database(client)?;
        let collection = database.collection::<Document>(&self.collection);
        let comment = format!("jaiba:{}", context.processor_id);
        self.stream_documents(&collection, &comment, &packet, output)
            .await
    }
}

fn make_packet(
    template: &DataPacket,
    records: Vec<Value>,
    collection: &str,
    batch_number: u64,
) -> DataPacket {
    let mut packet = DataPacket::with_records(records);
    packet.attributes.clone_from(&template.attributes);
    packet
        .attributes
        .insert("source.database".to_owned(), "mongodb".to_owned());
    packet
        .attributes
        .insert("source.collection".to_owned(), collection.to_owned());
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
    use mongodb::bson::{Bson, doc};
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_filter_projection_sort_and_limits() {
        let processor = QueryMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "filter": { "active": true, "age": { "$gte": 18 } },
            "projection": { "name": 1, "active": 1 },
            "sort": { "name": 1 },
            "skip": 2,
            "limit": 10,
            "batch_size": 50
        }))
        .expect("valid query config");

        assert_eq!(processor.filter.get_bool("active"), Ok(true));
        assert_eq!(
            processor
                .filter
                .get_document("age")
                .expect("age predicate")
                .get("$gte"),
            Some(&Bson::Int32(18))
        );
        assert_eq!(processor.projection, Some(doc! { "name": 1, "active": 1 }));
        assert_eq!(processor.sort, Some(doc! { "name": 1 }));
        assert_eq!(processor.limit, Some(10));
    }

    #[test]
    fn rejects_non_object_filter() {
        let error = QueryMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "filter": []
        }))
        .err()
        .expect("array filter must fail");
        assert!(error.to_string().contains("must be a JSON object"));
    }

    #[test]
    fn reads_timeouts_and_json_format() {
        let defaults = QueryMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers"
        }))
        .expect("defaults");
        assert_eq!(defaults.call_timeout, Some(Duration::from_secs(300)));
        assert_eq!(defaults.max_time, None);
        assert_eq!(defaults.json_format, JsonFormat::Relaxed);

        let custom = QueryMongoDb::from_config(&json!({
            "connection": "mongo",
            "collection": "customers",
            "call_timeout_ms": 0,
            "max_time_ms": 1500,
            "json_format": "canonical"
        }))
        .expect("custom");
        assert_eq!(custom.call_timeout, None);
        assert_eq!(custom.max_time, Some(Duration::from_millis(1500)));
        assert_eq!(custom.json_format, JsonFormat::Canonical);
    }
}

#[cfg(test)]
#[path = "query_mongodb_live_tests.rs"]
pub(crate) mod live_tests;
