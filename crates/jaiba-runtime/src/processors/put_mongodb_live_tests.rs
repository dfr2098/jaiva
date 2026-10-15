//! `put_mongodb` against a real MongoDB server (see
//! `query_mongodb_live_tests.rs` for the environment variables).

use std::time::{Duration, Instant};

use mongodb::{Client, bson::doc, options::ClientOptions};
use serde_json::{Value, json};

use super::*;
use crate::processors::query_mongodb::live_tests::{
    assert_same_document, live_database, query, read_all, typed_document, unique_name,
};

fn writer(collection: &str, extra: Value) -> PutMongoDb {
    let mut config = json!({ "connection": "live", "collection": collection });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    PutMongoDb::from_config(&config).unwrap()
}

fn customers(count: usize, version: u32) -> Vec<Value> {
    (0..count)
        .map(|id| {
            json!({
                "tenant": "north",
                "customer": { "id": id },
                "name": format!("customer-{id}"),
                "version": version,
            })
        })
        .collect()
}

#[tokio::test]
async fn mongodb_live_upserts_in_batches_and_is_idempotent() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let name = unique_name("jaiba_upsert");
    let collection = database.collection::<Document>(&name);
    let writer = writer(
        &name,
        json!({ "mode": "upsert", "key_fields": ["tenant", "customer.id"], "batch_size": 1000 }),
    );

    let (written, batches) = writer
        .write_records(&database, &customers(2500, 1))
        .await
        .unwrap();
    assert_eq!((written, batches), (2500, 3));
    let (written, batches) = writer
        .write_records(&database, &customers(2500, 2))
        .await
        .unwrap();
    assert_eq!((written, batches), (2500, 3));

    assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 2500);
    assert_eq!(
        collection
            .count_documents(doc! { "version": 2 })
            .await
            .unwrap(),
        2500
    );
    collection.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_splits_large_upserts_below_the_command_limit() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let name = unique_name("jaiba_large");
    let collection = database.collection::<Document>(&name);
    let body = "x".repeat(2 * 1024 * 1024);
    let records = (0..20)
        .map(|id| json!({ "_id": id, "body": body }))
        .collect::<Vec<_>>();
    let writer = writer(&name, json!({ "mode": "upsert" }));

    let (written, batches) = writer.write_records(&database, &records).await.unwrap();
    assert_eq!(written, 20);
    assert_eq!(batches, 3, "7 documents of 2 MiB per 15 MiB command");
    assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 20);
    collection.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_canonical_copy_keeps_every_type() {
    let Some((_client, database)) = live_database().await else {
        return;
    };
    let source_name = unique_name("jaiba_copy_source");
    let target_name = unique_name("jaiba_copy_target");
    let source = database.collection::<Document>(&source_name);
    let target = database.collection::<Document>(&target_name);
    source.insert_one(typed_document()).await.unwrap();

    let batches = read_all(
        &database,
        &query(&source_name, json!({ "json_format": "canonical" })),
    )
    .await
    .unwrap();
    writer(&target_name, json!({ "mode": "upsert" }))
        .write_records(&database, &batches[0])
        .await
        .unwrap();
    assert_same_document(
        target.find_one(doc! {}).await.unwrap().unwrap(),
        typed_document(),
    );

    // Relaxed JSON keeps values but narrows small Int64 to Int32.
    let batches = read_all(&database, &query(&source_name, json!({})))
        .await
        .unwrap();
    writer(&target_name, json!({ "mode": "upsert" }))
        .write_records(&database, &batches[0])
        .await
        .unwrap();
    let relaxed = target.find_one(doc! {}).await.unwrap().unwrap();
    assert_eq!(relaxed.get("small_long"), Some(&Bson::Int32(7)));
    assert_eq!(
        relaxed.get("big_long"),
        Some(&Bson::Int64(9_007_199_254_740_993))
    );

    source.drop().await.unwrap();
    target.drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_live_classifies_errors() {
    let Some((client, database)) = live_database().await else {
        return;
    };
    let name = unique_name("jaiba_errors");
    let collection = database.collection::<Document>(&name);
    database
        .run_command(doc! {
            "create": &name,
            "validator": { "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": { "name": { "bsonType": "string" } },
            } },
        })
        .await
        .unwrap();
    database
        .run_command(doc! {
            "createIndexes": &name,
            "indexes": [{ "key": { "email": 1 }, "name": "email_unique", "unique": true }],
        })
        .await
        .unwrap();

    let insert = writer(&name, json!({}));
    insert
        .write_records(
            &database,
            &[json!({ "_id": 1, "name": "Ada", "email": "a@x" })],
        )
        .await
        .unwrap();

    let duplicate = insert
        .write_records(
            &database,
            &[json!({ "_id": 1, "name": "Ada", "email": "b@x" })],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(duplicate, FlowError::DatabaseRejected(_)),
        "{duplicate}"
    );
    assert!(duplicate.to_string().contains("11000"), "{duplicate}");

    let invalid = insert
        .write_records(&database, &[json!({ "_id": 2, "name": 5 })])
        .await
        .unwrap_err();
    assert!(
        matches!(invalid, FlowError::DatabaseRejected(_)),
        "{invalid}"
    );
    assert!(invalid.to_string().contains("121"), "{invalid}");

    // Ordered upsert: the first record is written, the second violates the
    // unique email index and stops the batch.
    let upsert = writer(&name, json!({ "mode": "upsert" }));
    let conflict = upsert
        .write_records(
            &database,
            &[
                json!({ "_id": 3, "name": "Grace", "email": "g@x" }),
                json!({ "_id": 4, "name": "Hedy", "email": "a@x" }),
                json!({ "_id": 5, "name": "Joan", "email": "j@x" }),
            ],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(conflict, FlowError::DatabaseRejected(_)),
        "{conflict}"
    );
    assert!(conflict.to_string().contains("record 1"), "{conflict}");
    assert_eq!(collection.count_documents(doc! {}).await.unwrap(), 2);

    let missing_key = writer(&name, json!({ "mode": "upsert", "key_fields": ["code"] }))
        .write_records(&database, &[json!({ "name": "Ada" })])
        .await
        .unwrap_err();
    assert!(!missing_key.is_retryable(), "{missing_key}");

    let bad_record = insert
        .write_records(&database, &[json!({ "_id": { "$oid": "nope" } })])
        .await
        .unwrap_err();
    assert!(!bad_record.is_retryable(), "{bad_record}");

    // Wrong password: rejected, retrying cannot fix it.
    drop(client);
    let url = std::env::var("JAIBA_TEST_MONGODB_URL").unwrap();
    let mut options = ClientOptions::parse(&url).await.unwrap();
    if let Some(credential) = options.credential.as_mut() {
        credential.password = Some("wrong-password".to_owned());
        let wrong = Client::with_options(options).unwrap();
        let error = writer(&name, json!({}))
            .write_records(
                &wrong.database(database.name()),
                &[json!({ "_id": 9, "name": "x" })],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, FlowError::DatabaseRejected(_)), "{error}");
    }

    // Unreachable server: retryable.
    let mut options = ClientOptions::parse("mongodb://127.0.0.1:1/jaiba")
        .await
        .unwrap();
    options.server_selection_timeout = Some(Duration::from_millis(300));
    let unreachable = Client::with_options(options).unwrap();
    let started = Instant::now();
    let error = writer(&name, json!({}))
        .write_records(&unreachable.database("jaiba"), &[json!({ "_id": 9 })])
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(matches!(error, FlowError::DatabaseConnector(_)), "{error}");

    collection.drop().await.unwrap();
}
