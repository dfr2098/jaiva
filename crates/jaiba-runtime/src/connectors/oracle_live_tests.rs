//! Tests against a real Oracle database. They are skipped unless
//! `JAIBA_TEST_ORACLE_PASSWORD` is set and need the Oracle Client libraries
//! (see `docs/oracle.md`).

use std::{collections::BTreeMap, time::Duration};

use serde_json::{Value, json};

use super::*;

fn live_url() -> Option<String> {
    let password = std::env::var("JAIBA_TEST_ORACLE_PASSWORD").ok()?;
    let host = std::env::var("JAIBA_TEST_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("JAIBA_TEST_ORACLE_PORT").unwrap_or_else(|_| "11521".to_owned());
    let service =
        std::env::var("JAIBA_TEST_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
    let username =
        std::env::var("JAIBA_TEST_ORACLE_USER").unwrap_or_else(|_| "dma_test".to_owned());
    let mut url = Url::parse(&format!("oracle://{host}:{port}/{service}")).ok()?;
    url.set_username(&username).ok()?;
    url.set_password(Some(&password)).ok()?;
    Some(url.into())
}

fn live_writer(options: OraclePoolOptions) -> Option<OracleWriter> {
    let Some(url) = live_url() else {
        assert!(
            std::env::var_os("JAIBA_TEST_ORACLE_REQUIRED").is_none(),
            "JAIBA_TEST_ORACLE_REQUIRED is set but JAIBA_TEST_ORACLE_PASSWORD is missing"
        );
        eprintln!("skipping real Oracle test: JAIBA_TEST_ORACLE_PASSWORD is not set");
        return None;
    };
    Some(
        OracleWriter::from_url(&url)
            .unwrap()
            .with_pool_options(options),
    )
}

async fn execute(writer: &OracleWriter, sql: &str) {
    let sql = sql.to_owned();
    writer
        .run_blocking(None, move |connection| {
            connection.execute(&sql, &[]).map_err(oracle_error)?;
            connection.commit().map_err(oracle_error)
        })
        .await
        .unwrap();
}

async fn recreate_table(writer: &OracleWriter, table: &str, columns: &str) {
    execute(
        writer,
        &format!(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE'; \
             EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        ),
    )
    .await;
    execute(writer, &format!("CREATE TABLE {table} ({columns})")).await;
}

async fn read_all(writer: &OracleWriter, query: &str, batch_size: usize) -> Vec<Vec<Value>> {
    let mut stream = writer.query_stream(
        query,
        OracleQueryOptions {
            batch_size,
            call_timeout: Some(Duration::from_secs(60)),
        },
    );
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch().await {
        batches.push(batch.unwrap());
    }
    batches
}

#[tokio::test]
async fn oracle_live_maps_column_types_to_json() {
    let Some(writer) = live_writer(OraclePoolOptions::default()) else {
        return;
    };
    recreate_table(
        &writer,
        "JAIBA_IT_TYPES",
        "id NUMBER(10), amount NUMBER(12,2), ratio NUMBER, huge NUMBER(38), \
         bd BINARY_DOUBLE, bf BINARY_FLOAT, d DATE, ts TIMESTAMP(6), \
         tstz TIMESTAMP(3) WITH TIME ZONE, label VARCHAR2(20), body CLOB, \
         bin RAW(4), blob_col BLOB",
    )
    .await;
    execute(
        &writer,
        "INSERT INTO JAIBA_IT_TYPES VALUES (1, 12.34, 0.1, \
         12345678901234567890123456789012345678, 1.5, 0.1, \
         DATE '2026-10-13' + 37230/86400, TIMESTAMP '2026-10-13 10:20:30.123456', \
         TIMESTAMP '2026-10-13 10:20:30.5 -06:00', 'ñandú', TO_CLOB(RPAD('x', 4000, 'x')) || RPAD('y', 4000, 'y'), \
         HEXTORAW('FF007F80'), HEXTORAW('666F6F'))",
    )
    .await;
    execute(&writer, "INSERT INTO JAIBA_IT_TYPES (id) VALUES (2)").await;

    let batches = read_all(&writer, "SELECT * FROM JAIBA_IT_TYPES ORDER BY id", 10).await;
    assert_eq!(batches.len(), 1);
    let rows = &batches[0];
    let full = rows[0].as_object().unwrap();
    assert_eq!(full["id"], json!(1));
    assert_eq!(full["amount"], json!(12.34));
    assert_eq!(full["ratio"], json!(0.1));
    assert_eq!(
        full["huge"],
        json!("12345678901234567890123456789012345678")
    );
    assert_eq!(full["bd"], json!(1.5));
    assert_eq!(full["bf"], json!(0.1));
    assert_eq!(full["d"], json!("2026-10-13T10:20:30"));
    assert_eq!(full["ts"], json!("2026-10-13T10:20:30.123456"));
    assert_eq!(full["tstz"], json!("2026-10-13T10:20:30.500-06:00"));
    assert_eq!(full["label"], json!("ñandú"));
    assert_eq!(full["body"].as_str().unwrap().len(), 8000);
    assert_eq!(full["bin"], json!("/wB/gA=="));
    assert_eq!(full["blob_col"], json!("Zm9v"));

    let empty = rows[1].as_object().unwrap();
    assert_eq!(empty["id"], json!(2));
    for (column, value) in empty.iter().filter(|(column, _)| *column != "id") {
        assert_eq!(value, &Value::Null, "column {column} should be NULL");
    }
}

#[tokio::test]
async fn oracle_live_maps_native_boolean_and_json() {
    let Some(writer) = live_writer(OraclePoolOptions::default()) else {
        return;
    };
    let major = writer
        .run_blocking(None, |connection| {
            let (version, _) = connection.server_version().map_err(oracle_error)?;
            Ok(version.major())
        })
        .await
        .unwrap();
    if major < 23 {
        eprintln!("skipping BOOLEAN/JSON test: Oracle {major} has no native types");
        return;
    }
    let rows = read_all(
        &writer,
        "SELECT TRUE AS yes, FALSE AS no, CAST(NULL AS BOOLEAN) AS unknown, \
         JSON_SERIALIZE(JSON('{\"a\": [1, 2.5, \"x\"]}') RETURNING CLOB) AS doc FROM DUAL",
        1,
    )
    .await;
    assert_eq!(
        rows[0][0],
        json!({"yes": true, "no": false, "unknown": null, "doc": "{\"a\":[1,2.5,\"x\"]}"})
    );

    let mut stream = writer.query_stream(
        "SELECT JSON('{\"a\": 1}') AS doc FROM DUAL",
        OracleQueryOptions {
            batch_size: 1,
            call_timeout: None,
        },
    );
    let error = stream.next_batch().await.unwrap().unwrap_err();
    assert!(matches!(error, FlowError::DatabaseRejected(_)), "{error}");
    assert!(error.to_string().contains("JSON_SERIALIZE"), "{error}");
}

#[tokio::test]
async fn oracle_live_streams_batches_and_releases_cancelled_sessions() {
    // One session only: the second read succeeds only if dropping the first
    // stream returned its session to the pool.
    let Some(writer) = live_writer(OraclePoolOptions {
        max_connections: 1,
        acquire_timeout: Duration::from_secs(10),
    }) else {
        return;
    };
    let query = "SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= 25000";
    let batches = read_all(&writer, query, 1_000).await;
    assert_eq!(batches.len(), 25);
    assert!(batches.iter().all(|batch| batch.len() == 1_000));
    assert_eq!(batches[24][999]["n"], json!(25_000));

    let mut stream = writer.query_stream(
        "SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= 5000000",
        OracleQueryOptions {
            batch_size: 100,
            call_timeout: None,
        },
    );
    assert_eq!(stream.next_batch().await.unwrap().unwrap().len(), 100);
    drop(stream);

    let empty = read_all(&writer, "SELECT 1 AS one FROM DUAL WHERE 1 = 0", 10).await;
    assert_eq!(empty, vec![Vec::<Value>::new()]);
}

#[tokio::test]
async fn oracle_live_call_timeout_is_retryable() {
    let Some(writer) = live_writer(OraclePoolOptions::default()) else {
        return;
    };
    let mut stream = writer.query_stream(
        "SELECT COUNT(*) AS c FROM all_objects a, all_objects b, all_objects c",
        OracleQueryOptions {
            batch_size: 1,
            call_timeout: Some(Duration::from_millis(500)),
        },
    );
    let error = stream.next_batch().await.unwrap().unwrap_err();
    assert!(error.is_retryable(), "{error}");
    assert!(matches!(error, FlowError::DatabaseConnector(_)), "{error}");
}

#[tokio::test]
async fn oracle_live_classifies_errors() {
    let Some(writer) = live_writer(OraclePoolOptions::default()) else {
        return;
    };
    let mut stream = writer.query_stream(
        "SELECT * FROM jaiba_table_that_does_not_exist",
        OracleQueryOptions {
            batch_size: 10,
            call_timeout: None,
        },
    );
    let error = stream.next_batch().await.unwrap().unwrap_err();
    assert!(matches!(error, FlowError::DatabaseRejected(_)), "{error}");
    assert!(error.to_string().contains("ORA-00942"), "{error}");

    let unreachable = OracleWriter::from_url("oracle://user:secret@127.0.0.1:1/NOPE")
        .unwrap()
        .with_pool_options(OraclePoolOptions {
            max_connections: 1,
            acquire_timeout: Duration::from_secs(2),
        });
    let mut stream = unreachable.query_stream(
        "SELECT 1 FROM DUAL",
        OracleQueryOptions {
            batch_size: 1,
            call_timeout: None,
        },
    );
    let error = stream.next_batch().await.unwrap().unwrap_err();
    assert!(error.is_retryable(), "{error}");
}

fn write_request(table: &str, mode: WriteMode, batch_size: usize) -> WriteRequest {
    WriteRequest {
        table: table.to_owned(),
        mode,
        columns: BTreeMap::from([
            ("id".to_owned(), "ID".to_owned()),
            ("name".to_owned(), "NAME".to_owned()),
            ("amount".to_owned(), "AMOUNT".to_owned()),
            ("created".to_owned(), "CREATED".to_owned()),
            ("seen".to_owned(), "SEEN".to_owned()),
            ("notes".to_owned(), "NOTES".to_owned()),
        ]),
        conflict_columns: vec!["ID".to_owned()],
        batch_size,
    }
}

async fn count_rows(writer: &OracleWriter, table: &str) -> i64 {
    let batches = read_all(writer, &format!("SELECT COUNT(*) AS c FROM {table}"), 1).await;
    batches[0][0]["c"].as_i64().unwrap()
}

#[tokio::test]
async fn oracle_live_writes_with_array_dml_merge_and_rollback() {
    let Some(writer) = live_writer(OraclePoolOptions::default()) else {
        return;
    };
    let table = "JAIBA_IT_WRITE";
    recreate_table(
        &writer,
        table,
        "id NUMBER PRIMARY KEY, name VARCHAR2(50), amount NUMBER(12,2), created DATE, \
         seen TIMESTAMP(3) WITH TIME ZONE, notes CLOB",
    )
    .await;

    let long_note = "n".repeat(MAX_VARCHAR_BIND_BYTES + 10);
    let records = (1..=2_500)
        .map(|id| {
            json!({
                // The first row starts with NULLs so types come from the
                // whole chunk, not from the first value.
                "id": id,
                "name": if id == 1 { Value::Null } else { json!(format!("name-{id}")) },
                "amount": if id == 1 { Value::Null } else { json!(12.34) },
                "created": "2026-10-13T10:20:30",
                "seen": "2026-10-13T10:20:30.250-06:00",
                "notes": if id == 7 { json!(long_note) } else { json!({"id": id}) },
            })
        })
        .collect::<Vec<_>>();
    let summary = writer
        .write(&write_request(table, WriteMode::Insert, 1_000), &records)
        .await
        .unwrap();
    assert_eq!(summary.rows, 2_500);
    assert_eq!(summary.batches, 3);
    assert_eq!(count_rows(&writer, table).await, 2_500);

    let rows = read_all(
        &writer,
        &format!(
            "SELECT id, name, amount, created, seen, LENGTH(notes) AS notes_length \
             FROM {table} WHERE id IN (1, 2, 7) ORDER BY id"
        ),
        10,
    )
    .await;
    assert_eq!(
        rows[0],
        vec![
            json!({"id": 1, "name": null, "amount": null, "created": "2026-10-13T10:20:30",
                   "seen": "2026-10-13T10:20:30.250-06:00", "notes_length": 8}),
            json!({"id": 2, "name": "name-2", "amount": 12.34, "created": "2026-10-13T10:20:30",
                   "seen": "2026-10-13T10:20:30.250-06:00", "notes_length": 8}),
            json!({"id": 7, "name": "name-7", "amount": 12.34, "created": "2026-10-13T10:20:30",
                   "seen": "2026-10-13T10:20:30.250-06:00", "notes_length": MAX_VARCHAR_BIND_BYTES + 10}),
        ]
    );

    let upserts = vec![
        json!({"id": 2, "name": "updated", "amount": 99.5, "created": "2026-01-01",
               "seen": null, "notes": "merged"}),
        json!({"id": 9_000, "name": "inserted", "amount": 1, "created": "2026-01-02T03:04:05",
               "seen": null, "notes": null}),
    ];
    let summary = writer
        .write(&write_request(table, WriteMode::Upsert, 1_000), &upserts)
        .await
        .unwrap();
    assert_eq!(summary.rows, 2);
    assert_eq!(count_rows(&writer, table).await, 2_501);
    let merged = read_all(
        &writer,
        &format!("SELECT id, name, amount, created FROM {table} WHERE id IN (2, 9000) ORDER BY id"),
        10,
    )
    .await;
    assert_eq!(
        merged[0],
        vec![
            json!({"id": 2, "name": "updated", "amount": 99.5, "created": "2026-01-01T00:00:00"}),
            json!({"id": 9000, "name": "inserted", "amount": 1, "created": "2026-01-02T03:04:05"}),
        ]
    );

    // A duplicate key in the second chunk must roll back the first chunk too.
    let conflicting = vec![
        json!({"id": 10_001, "name": "a", "amount": 1, "created": null, "seen": null, "notes": null}),
        json!({"id": 10_002, "name": "b", "amount": 1, "created": null, "seen": null, "notes": null}),
        json!({"id": 1, "name": "dup", "amount": 1, "created": null, "seen": null, "notes": null}),
    ];
    let error = writer
        .write(&write_request(table, WriteMode::Insert, 2), &conflicting)
        .await
        .unwrap_err();
    assert!(matches!(error, FlowError::DatabaseRejected(_)), "{error}");
    assert!(error.to_string().contains("ORA-00001"), "{error}");
    assert!(!error.is_retryable());
    assert_eq!(count_rows(&writer, table).await, 2_501);
}
