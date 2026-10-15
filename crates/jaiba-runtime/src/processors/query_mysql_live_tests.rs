//! `query_mysql` against a real MySQL or MariaDB server (see
//! `connectors/mysql_live_tests.rs` for the environment variables).

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sqlx::MySqlPool;
use tokio::sync::mpsc;

use super::*;
use crate::{
    connectors::mysql::live_tests::{assert_statement_stops, execute, is_mariadb, live_pool},
    engine::{FlowMetrics, MemoryLimiter, ProcessorEmission},
};

fn output(capacity: usize) -> (OutputSender, mpsc::Receiver<ProcessorEmission>) {
    let metrics = FlowMetrics::default();
    let memory = MemoryLimiter::from_budget(256 * 1024 * 1024, metrics.clone());
    let (sender, receiver) = mpsc::channel(capacity);
    (
        OutputSender::new(sender, "query", memory, metrics),
        receiver,
    )
}

fn processor(query: &str, extra: Value) -> QueryMysql {
    let mut config = json!({ "connection": "live", "query": query });
    config
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    QueryMysql::from_config(&config).unwrap()
}

async fn read_all(pool: &MySqlPool, processor: &QueryMysql) -> Vec<Vec<Value>> {
    let (output, mut receiver) = output(4);
    let mut session = MySqlSession::acquire(pool).await.unwrap();
    let reader = async {
        processor
            .stream_rows(&mut session, &DataPacket::empty(), &output)
            .await
            .unwrap();
        drop(output);
    };
    let collector = async {
        let mut batches = Vec::new();
        while let Some(emission) = receiver.recv().await {
            batches.push(emission.packet.records().unwrap().to_vec());
        }
        batches
    };
    let ((), batches) = tokio::join!(reader, collector);
    session.finish();
    batches
}

/// 0..rows without recursion limits (`cte_max_recursion_depth` is 1000).
fn sequence_query(rows: u32, marker: &str) -> String {
    let digits = "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 \
                  UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 \
                  UNION ALL SELECT 8 UNION ALL SELECT 9)";
    format!(
        "SELECT a.d + 10 * b.d + 100 * c.d + 1000 * e.d + 10000 * f.d AS n, \
         '{marker}' AS marker \
         FROM {digits} a, {digits} b, {digits} c, {digits} e, {digits} f \
         WHERE a.d + 10 * b.d + 100 * c.d + 1000 * e.d + 10000 * f.d < {rows} ORDER BY n"
    )
}

#[tokio::test]
async fn mysql_live_maps_column_types_to_json() {
    let Some(pool) = live_pool(2).await else {
        return;
    };
    let mariadb = is_mariadb(&pool).await;
    execute(&pool, "DROP TABLE IF EXISTS jaiba_it_types").await;
    execute(
        &pool,
        "CREATE TABLE jaiba_it_types (id INT PRIMARY KEY, flag BOOLEAN, big BIGINT UNSIGNED, \
         amount DECIMAL(10,2), huge DECIMAL(38,10), f FLOAT, dbl DOUBLE, d DATE, \
         t TIME(3), t2 TIME, dt DATETIME(6), ts TIMESTAMP(3) NULL, label VARCHAR(20), body MEDIUMTEXT, \
         bin VARBINARY(8), blob_col BLOB, bits BIT(8), doc JSON, kind ENUM('a','b'), \
         code VARCHAR(10) COLLATE utf8mb4_bin, shape GEOMETRY)",
    )
    .await;
    execute(
        &pool,
        "INSERT INTO jaiba_it_types VALUES (1, TRUE, 18446744073709551615, 12.50, \
         1234567890123456789012345678.0123456789, 0.1, 1.5, '2026-10-13', '-838:59:58.5', '07:05:09', \
         '2026-10-13 10:20:30.123456', '2026-10-13 10:20:30.5', 'ñandú', REPEAT('x', 70000), \
         X'FF007F80', 'foo', b'00000101', '{\"a\": [1, 2]}', 'b', 'Key-1', ST_GeomFromText('POINT(1 2)'))",
    )
    .await;
    execute(&pool, "INSERT INTO jaiba_it_types (id) VALUES (2)").await;

    let batches = read_all(
        &pool,
        &processor("SELECT * FROM jaiba_it_types ORDER BY id", json!({})),
    )
    .await;
    assert_eq!(batches.len(), 1);
    let full = batches[0][0].as_object().unwrap();
    assert_eq!(full["id"], json!(1));
    assert_eq!(full["flag"], json!(true));
    assert_eq!(full["big"], json!(18_446_744_073_709_551_615_u64));
    assert_eq!(full["amount"], json!(12.5));
    assert_eq!(
        full["huge"],
        json!("1234567890123456789012345678.0123456789")
    );
    assert_eq!(full["f"], json!(0.1));
    assert_eq!(full["dbl"], json!(1.5));
    assert_eq!(full["d"], json!("2026-10-13"));
    assert_eq!(full["t"], json!("-838:59:58.5"));
    assert_eq!(full["t2"], json!("07:05:09"));
    assert_eq!(full["dt"], json!("2026-10-13T10:20:30.123456"));
    assert_eq!(full["ts"], json!("2026-10-13T10:20:30.500"));
    assert_eq!(full["label"], json!("ñandú"));
    assert_eq!(full["body"].as_str().unwrap().len(), 70_000);
    assert_eq!(full["bin"], json!("/wB/gA=="));
    assert_eq!(full["blob_col"], json!("foo"));
    assert_eq!(full["code"], json!("Key-1"));
    // SRID (4 bytes, 0) + WKB point (1.0, 2.0).
    assert_eq!(full["shape"], json!("AAAAAAEBAAAAAAAAAAAA8D8AAAAAAAAAQA=="));
    assert_eq!(full["bits"], json!(5));
    if mariadb {
        // MariaDB stores JSON as LONGTEXT and reports it as text.
        assert_eq!(full["doc"], json!("{\"a\": [1, 2]}"));
    } else {
        assert_eq!(full["doc"], json!({ "a": [1, 2] }));
    }
    assert_eq!(full["kind"], json!("b"));

    let empty = batches[0][1].as_object().unwrap();
    for (column, value) in empty.iter().filter(|(column, _)| *column != "id") {
        assert_eq!(value, &Value::Null, "column {column} should be NULL");
    }
    execute(&pool, "DROP TABLE jaiba_it_types").await;
}

#[tokio::test]
async fn mysql_live_streams_in_bounded_batches() {
    let Some(pool) = live_pool(2).await else {
        return;
    };
    let batches = read_all(
        &pool,
        &processor(
            &sequence_query(25_000, "jaiba_stream"),
            json!({ "batch_size": 1000 }),
        ),
    )
    .await;
    assert_eq!(batches.len(), 25);
    assert!(batches.iter().all(|batch| batch.len() == 1000));
    let last = batches.last().unwrap().last().unwrap();
    assert_eq!(last["n"], json!(24_999));
}

#[tokio::test]
async fn mysql_live_cancelled_stream_stops_the_query_and_frees_the_pool() {
    let Some(pool) = live_pool(1).await else {
        return;
    };
    let processor = processor(
        &sequence_query(100_000, "jaiba_cancel_marker"),
        json!({ "batch_size": 100 }),
    );
    let (output, mut receiver) = output(1);
    let reader = async {
        let mut session = MySqlSession::acquire(&pool).await.unwrap();
        processor
            .stream_rows(&mut session, &DataPacket::empty(), &output)
            .await
    };
    tokio::select! {
        _ = reader => panic!("the reader must block on the full output channel"),
        first = receiver.recv() => assert!(first.is_some()),
    }
    // The reader future was dropped mid-stream (same as a node timeout).
    let started = Instant::now();
    let mut session = MySqlSession::acquire(&pool).await.unwrap();
    let one: i64 = sqlx::query_scalar("SELECT 1")
        .fetch_one(session.connection())
        .await
        .unwrap();
    assert_eq!(one, 1);
    session.finish();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_statement_stops(&pool, "jaiba_cancel_marker").await;
}

#[tokio::test]
async fn mysql_live_call_timeout_is_retryable_and_kills_the_query() {
    let Some(pool) = live_pool(2).await else {
        return;
    };
    let processor = processor(
        "SELECT SLEEP(20) AS slept, 'jaiba_timeout_marker' AS marker",
        json!({ "call_timeout_ms": 500 }),
    );
    let (output, _receiver) = output(1);
    let started = Instant::now();
    let mut session = MySqlSession::acquire(&pool).await.unwrap();
    let error = processor
        .stream_rows(&mut session, &DataPacket::empty(), &output)
        .await
        .unwrap_err();
    drop(session);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(matches!(error, FlowError::DatabaseConnector(_)), "{error}");
    assert!(error.is_retryable());
    assert_statement_stops(&pool, "jaiba_timeout_marker").await;
}
