//! `query_mongodb` against a real MongoDB server.
//!
//! `JAIBA_TEST_MONGODB_URL` (with a default database, e.g.
//! `mongodb://jaiba:<password>@127.0.0.1:27017/jaiba?authSource=admin`)
//! enables these tests; without it they are skipped unless
//! `JAIBA_TEST_MONGODB_REQUIRED` is set, in which case they fail.

use std::{
    env,
    time::{Duration, Instant},
};

use futures_util::TryStreamExt;
use mongodb::{Client, Database, bson::doc};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::engine::{FlowMetrics, MemoryLimiter, ProcessorEmission};

pub(crate) async fn live_database() -> Option<(Client, Database)> {
    let Ok(url) = env::var("JAIBA_TEST_MONGODB_URL") else {
        assert!(
            env::var_os("JAIBA_TEST_MONGODB_REQUIRED").is_none(),
            "JAIBA_TEST_MONGODB_REQUIRED is set but JAIBA_TEST_MONGODB_URL is missing"
        );
        eprintln!("skipping live MongoDB test: JAIBA_TEST_MONGODB_URL is not set");
        return None;
    };
    let client = Client::with_uri_str(&url).await.expect("MongoDB client");
    let database = client
        .default_database()
        .expect("JAIBA_TEST_MONGODB_URL must include a database");
    Some((client, database))
}

pub(crate) fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

async fn running_operations(client: &Client, comment: &str) -> Vec<Document> {
    client
        .database("admin")
        .aggregate([
            doc! { "$currentOp": { "allUsers": true } },
            doc! { "$match": { "$or": [
                { "command.comment": comment },
                { "cursor.originatingCommand.comment": comment },
            ] } },
        ])
        .await
        .expect("$currentOp")
        .try_collect::<Vec<_>>()
        .await
        .expect("read $currentOp")
}

/// Waits until an operation tagged with `comment` shows up in `$currentOp`.
async fn wait_for_operation(client: &Client, comment: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while running_operations(client, comment).await.is_empty() {
        assert!(
            Instant::now() < deadline,
            "operation '{comment}' never started"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits until no operation tagged with `comment` is running on the server.
pub(crate) async fn assert_operation_stops(client: &Client, comment: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let running = running_operations(client, comment).await;
        if running.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "operation '{comment}' is still running: {running:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) fn output(capacity: usize) -> (OutputSender, mpsc::Receiver<ProcessorEmission>) {
    let metrics = FlowMetrics::default();
    let memory = MemoryLimiter::from_budget(256 * 1024 * 1024, metrics.clone());
    let (sender, receiver) = mpsc::channel(capacity);
    (
        OutputSender::new(sender, "query", memory, metrics),
        receiver,
    )
}

pub(crate) fn query(collection: &str, extra: Value) -> QueryMongoDb {
    let mut config = json!({ "connection": "live", "collection": collection });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    QueryMongoDb::from_config(&config).unwrap()
}

pub(crate) async fn read_all(
    database: &Database,
    processor: &QueryMongoDb,
) -> Result<Vec<Vec<Value>>, FlowError> {
    let collection = database.collection::<Document>(&processor.collection);
    let (output, mut receiver) = output(4);
    let reader = async {
        let result = processor
            .stream_documents(
                &collection,
                "jaiba:live-read",
                &DataPacket::empty(),
                &output,
            )
            .await;
        drop(output);
        result
    };
    let collector = async {
        let mut batches = Vec::new();
        while let Some(emission) = receiver.recv().await {
            batches.push(emission.packet.records().unwrap().to_vec());
        }
        batches
    };
    let (result, batches) = tokio::join!(reader, collector);
    result.map(|()| batches)
}

/// Source document written from canonical Extended JSON so every BSON type is
/// explicit.
pub(crate) fn typed_document() -> Document {
    json_document(
        &json!({
            "_id": { "$oid": "65f1a2b3c4d5e6f708192a3b" },
            "int": { "$numberInt": "42" },
            "small_long": { "$numberLong": "7" },
            "big_long": { "$numberLong": "9007199254740993" },
            "double": { "$numberDouble": "0.1" },
            "whole_double": { "$numberDouble": "2.0" },
            "nan": { "$numberDouble": "NaN" },
            "decimal": { "$numberDecimal": "12345678901234.5678" },
            "date": { "$date": { "$numberLong": "1760350830123" } },
            "old_date": { "$date": { "$numberLong": "-62135596800000" } },
            "binary": { "$binary": { "base64": "AAEC/w==", "subType": "00" } },
            "uuid": { "$binary": { "base64": "OyQRAeK7QlWMr0E2xWapYg==", "subType": "04" } },
            "timestamp": { "$timestamp": { "t": 1760350830, "i": 3 } },
            "regex": { "$regularExpression": { "pattern": "^a", "options": "i" } },
            "flag": true,
            "nothing": null,
            "text": "ñandú",
            "nested": { "list": [1, "two", { "three": { "$numberLong": "3" } }] },
            "min": { "$minKey": 1 },
            "max": { "$maxKey": 1 }
        }),
        "typed document",
    )
    .unwrap()
}

/// Exact BSON comparison where `NaN` equals itself.
pub(crate) fn assert_same_document(actual: Document, expected: Document) {
    assert_eq!(
        document_json(actual, JsonFormat::Canonical),
        document_json(expected, JsonFormat::Canonical)
    );
}

#[tokio::test]
async fn mongodb_live_reads_types_as_extended_json() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let name = unique_name("jaiba_types");
    let collection = database.collection::<Document>(&name);
    collection.insert_one(typed_document()).await.unwrap();

    let relaxed = read_all(&database, &query(&name, json!({}))).await.unwrap();
    let document = &relaxed[0][0];
    assert_eq!(
        document["_id"],
        json!({ "$oid": "65f1a2b3c4d5e6f708192a3b" })
    );
    assert_eq!(document["int"], json!(42));
    assert_eq!(document["small_long"], json!(7));
    assert_eq!(document["big_long"], json!(9_007_199_254_740_993_i64));
    assert_eq!(document["double"], json!(0.1));
    assert_eq!(document["whole_double"], json!(2.0));
    assert_eq!(document["nan"], json!({ "$numberDouble": "NaN" }));
    assert_eq!(
        document["decimal"],
        json!({ "$numberDecimal": "12345678901234.5678" })
    );
    assert_eq!(
        document["date"],
        json!({ "$date": "2025-10-13T10:20:30.123Z" })
    );
    assert_eq!(
        document["old_date"],
        json!({ "$date": { "$numberLong": "-62135596800000" } })
    );
    assert_eq!(
        document["binary"],
        json!({ "$binary": { "base64": "AAEC/w==", "subType": "00" } })
    );
    assert_eq!(
        document["uuid"],
        json!({ "$binary": { "base64": "OyQRAeK7QlWMr0E2xWapYg==", "subType": "04" } })
    );
    assert_eq!(
        document["timestamp"],
        json!({ "$timestamp": { "t": 1_760_350_830_u32, "i": 3 } })
    );
    assert_eq!(
        document["regex"],
        json!({ "$regularExpression": { "pattern": "^a", "options": "i" } })
    );
    assert_eq!(document["flag"], json!(true));
    assert_eq!(document["nothing"], Value::Null);
    assert_eq!(document["text"], json!("ñandú"));
    assert_eq!(
        document["nested"],
        json!({ "list": [1, "two", { "three": 3 }] })
    );
    assert_eq!(document["min"], json!({ "$minKey": 1 }));
    assert_eq!(document["max"], json!({ "$maxKey": 1 }));

    let canonical = read_all(
        &database,
        &query(&name, json!({ "json_format": "canonical" })),
    )
    .await
    .unwrap();
    let document = &canonical[0][0];
    assert_eq!(document["small_long"], json!({ "$numberLong": "7" }));
    assert_eq!(document["int"], json!({ "$numberInt": "42" }));
    assert_same_document(
        json_document(document, "canonical").unwrap(),
        typed_document(),
    );

    let plain = read_all(&database, &query(&name, json!({ "json_format": "plain" })))
        .await
        .unwrap();
    let document = &plain[0][0];
    assert_eq!(document["_id"], json!("65f1a2b3c4d5e6f708192a3b"));
    assert_eq!(document["small_long"], json!(7));
    assert_eq!(document["big_long"], json!(9_007_199_254_740_993_i64));
    assert_eq!(document["double"], json!(0.1));
    assert_eq!(document["nan"], json!("NaN"));
    assert_eq!(document["decimal"], json!("12345678901234.5678"));
    assert_eq!(document["date"], json!("2025-10-13T10:20:30.123Z"));
    assert_eq!(document["old_date"], json!("0001-01-01T00:00:00.000Z"));
    assert_eq!(document["binary"], json!("AAEC/w=="));
    assert_eq!(
        document["uuid"],
        json!("3b241101-e2bb-4255-8caf-4136c566a962")
    );
    assert_eq!(
        document["nested"],
        json!({ "list": [1, "two", { "three": 3 }] })
    );
    assert_eq!(
        document["timestamp"],
        json!({ "$timestamp": { "t": 1_760_350_830_u32, "i": 3 } })
    );

    collection.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_streams_in_bounded_batches() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let name = unique_name("jaiba_stream");
    let collection = database.collection::<Document>(&name);
    let documents = (0..25_000_i32)
        .map(|id| doc! { "_id": id, "payload": format!("row-{id}") })
        .collect::<Vec<_>>();
    collection.insert_many(documents).await.unwrap();

    let batches = read_all(
        &database,
        &query(&name, json!({ "batch_size": 1000, "sort": { "_id": 1 } })),
    )
    .await
    .unwrap();
    assert_eq!(batches.len(), 25);
    assert!(batches.iter().all(|batch| batch.len() == 1000));
    assert_eq!(
        batches[24][999],
        json!({ "_id": 24_999, "payload": "row-24999" })
    );

    collection.drop().await.unwrap();
}

async fn slow_collection(database: &Database) -> (String, Collection<Document>) {
    let name = unique_name("jaiba_slow");
    let collection = database.collection::<Document>(&name);
    collection.insert_one(doc! { "_id": 1 }).await.unwrap();
    (name, collection)
}

const SLOW_FILTER: &str = "sleep(20000) || true";

#[tokio::test]
async fn mongodb_live_cancelled_read_stops_on_the_server() {
    let Some((client, database)) = live_database().await else {
        return;
    };
    let (name, collection) = slow_collection(&database).await;
    let processor = query(&name, json!({ "filter": { "$where": SLOW_FILTER } }));
    let comment = unique_name("jaiba_cancel");
    let (output, _receiver) = output(1);
    let packet = DataPacket::empty();
    let reader = processor.stream_documents(&collection, &comment, &packet, &output);
    tokio::select! {
        result = reader => panic!("the slow read must not finish: {result:?}"),
        () = wait_for_operation(&client, &comment) => {}
    }
    // The reader future was dropped mid-call (same as a node timeout or a
    // stopped flow).
    assert_operation_stops(&client, &comment).await;
    collection.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_call_timeout_is_retryable_and_stops_the_read() {
    let Some((client, database)) = live_database().await else {
        return;
    };
    let (name, collection) = slow_collection(&database).await;
    let processor = query(
        &name,
        json!({ "filter": { "$where": SLOW_FILTER }, "call_timeout_ms": 500 }),
    );
    let comment = unique_name("jaiba_timeout");
    let (output, _receiver) = output(1);
    let started = Instant::now();
    let error = processor
        .stream_documents(&collection, &comment, &DataPacket::empty(), &output)
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(matches!(error, FlowError::DatabaseConnector(_)), "{error}");
    assert_operation_stops(&client, &comment).await;
    collection.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_max_time_is_enforced_by_the_server() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let (name, collection) = slow_collection(&database).await;
    let processor = query(
        &name,
        json!({ "filter": { "$where": SLOW_FILTER }, "max_time_ms": 300 }),
    );
    let started = Instant::now();
    let error = read_all(&database, &processor).await.unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(error.is_retryable(), "{error}");
    assert!(error.to_string().contains("MaxTimeMSExpired"), "{error}");
    collection.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_rejects_invalid_queries_without_retry() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let name = unique_name("jaiba_bad_query");
    let error = read_all(
        &database,
        &query(&name, json!({ "filter": { "$bogus": 1 } })),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, FlowError::DatabaseRejected(_)), "{error}");
}

#[tokio::test]
async fn mongodb_real_query_to_upsert_flow_is_idempotent() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
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
    std::fs::create_dir_all(&state_dir).expect("create state directory");
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

    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 2);
    assert_eq!(
        target
            .find_one(doc! { "_id": "customer-1" })
            .await
            .unwrap()
            .expect("customer exists")
            .get_str("name"),
        Ok("Ada")
    );

    source.drop().await.unwrap();
    target.drop().await.unwrap();
    std::fs::remove_dir_all(state_dir).ok();
}
