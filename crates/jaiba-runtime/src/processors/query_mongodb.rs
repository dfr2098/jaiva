use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::Document;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::mongodb_support::{default_database, document_json, json_document};

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
        })
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
        let driver_batch_size = u32::try_from(self.batch_size).map_err(|_| {
            FlowError::Configuration("query_mongodb batch_size exceeds u32".to_owned())
        })?;

        let mut find = collection
            .find(self.filter.clone())
            .skip(self.skip)
            .batch_size(driver_batch_size);
        if let Some(projection) = &self.projection {
            find = find.projection(projection.clone());
        }
        if let Some(sort) = &self.sort {
            find = find.sort(sort.clone());
        }
        if let Some(limit) = self.limit {
            find = find.limit(limit);
        }

        let mut cursor = find
            .await
            .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?;
        let mut records = Vec::with_capacity(self.batch_size);
        let mut batch_number = 0_u64;

        while let Some(document) = cursor
            .try_next()
            .await
            .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?
        {
            records.push(document_json(document));
            if records.len() == self.batch_size {
                output
                    .success(make_packet(
                        &packet,
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
                .success(make_packet(
                    &packet,
                    records,
                    &self.collection,
                    batch_number,
                ))
                .await?;
        }
        Ok(())
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
    use std::{env, fs};

    use mongodb::bson::{Bson, doc};
    use serde_json::json;
    use uuid::Uuid;

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

    /// Prueba opt-in de la fase 2: lee documentos filtrados y los carga con
    /// upsert en otra colección. Se ejecuta dos veces para comprobar idempotencia.
    #[tokio::test]
    async fn mongodb_real_query_to_upsert_flow_is_idempotent() {
        let Ok(url) = env::var("JAIBA_TEST_MONGODB_URL") else {
            eprintln!("skipping MongoDB flow test: JAIBA_TEST_MONGODB_URL is not set");
            return;
        };
        let client = mongodb::Client::with_uri_str(&url)
            .await
            .expect("connect to MongoDB");
        let database = default_database(&client).expect("default MongoDB database");
        let suffix = Uuid::new_v4().simple().to_string();
        let source_name = format!("jaiba_phase_2_source_{suffix}");
        let target_name = format!("jaiba_phase_2_target_{suffix}");
        let source = database.collection::<Document>(&source_name);
        let target = database.collection::<Document>(&target_name);
        source
            .insert_many(vec![
                doc! { "_id": "customer-1", "name": "Ada", "active": true },
                doc! { "_id": "customer-2", "name": "Grace", "active": true },
                doc! { "_id": "customer-3", "name": "Hidden", "active": false },
            ])
            .await
            .expect("seed source collection");

        let state_dir = env::temp_dir().join(format!("jaiba-mongodb-phase-2-{suffix}"));
        fs::create_dir_all(&state_dir).expect("create state directory");
        let state_file = state_dir
            .join("state.json")
            .to_string_lossy()
            .replace('\\', "/");
        let yaml = format!(
            r#"
id: mongodb-phase-2
database_connections:
  mongo:
    type: mongodb
    url_env: JAIBA_TEST_MONGODB_URL
    max_connections: 2
engine:
  state_file: "{state_file}"
  repository:
    enabled: false
processors:
  - id: read
    type: query_mongodb
    config:
      connection: mongo
      collection: {source_name}
      filter:
        active: true
      sort:
        _id: 1
      batch_size: 1
  - id: write
    type: put_mongodb
    config:
      connection: mongo
      collection: {target_name}
      mode: upsert
      key_fields: [_id]
      batch_size: 2
connections:
  - from: read
    relationship: success
    to: write
"#
        );
        let config: jaiba_core::config::FlowConfig =
            serde_yaml::from_str(&yaml).expect("parse MongoDB flow");

        for _ in 0..2 {
            let summary = crate::engine::FlowEngine::new(config.clone())
                .expect("build MongoDB flow")
                .run()
                .await
                .expect("run MongoDB flow");
            assert_eq!(summary.failed, 0);
        }

        assert_eq!(
            target
                .count_documents(doc! {})
                .await
                .expect("count target documents"),
            2
        );
        assert_eq!(
            target
                .find_one(doc! { "_id": "customer-1" })
                .await
                .expect("read target")
                .expect("customer exists")
                .get_str("name"),
            Ok("Ada")
        );

        source.drop().await.expect("drop source collection");
        target.drop().await.expect("drop target collection");
        fs::remove_dir_all(state_dir).ok();
    }
}
