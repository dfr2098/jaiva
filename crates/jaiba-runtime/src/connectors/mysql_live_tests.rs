//! Tests against a real MySQL or MariaDB server. They are skipped unless
//! `JAIBA_TEST_MYSQL_URL` is set (`mysql://user:password@host:port/db`);
//! `JAIBA_TEST_MYSQL_REQUIRED=1` turns a missing URL into a failure.

use std::{collections::BTreeMap, time::Duration};

use serde_json::{Value, json};
use sqlx::mysql::MySqlPoolOptions;

use super::*;

pub(crate) async fn live_pool(max_connections: u32) -> Option<MySqlPool> {
    let Ok(url) = std::env::var("JAIBA_TEST_MYSQL_URL") else {
        assert!(
            std::env::var_os("JAIBA_TEST_MYSQL_REQUIRED").is_none(),
            "JAIBA_TEST_MYSQL_REQUIRED is set but JAIBA_TEST_MYSQL_URL is missing"
        );
        eprintln!("skipping real MySQL test: JAIBA_TEST_MYSQL_URL is not set");
        return None;
    };
    Some(
        MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&url)
            .await
            .expect("connect to the MySQL integration database"),
    )
}

pub(crate) async fn execute(pool: &MySqlPool, sql: &str) {
    sqlx::query(sql).execute(pool).await.unwrap();
}

pub(crate) async fn is_mariadb(pool: &MySqlPool) -> bool {
    let version: String = sqlx::query_scalar("SELECT VERSION()")
        .fetch_one(pool)
        .await
        .unwrap();
    version.contains("MariaDB")
}

/// Waits until no statement containing `marker` is running on the server.
pub(crate) async fn assert_statement_stops(pool: &MySqlPool, marker: &str) {
    let pattern = format!("%{marker}%");
    for _ in 0..50 {
        let running: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.processlist \
             WHERE command = 'Query' AND info LIKE ? AND id <> CONNECTION_ID()",
        )
        .bind(&pattern)
        .fetch_one(pool)
        .await
        .unwrap();
        if running == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("statement '{marker}' is still running on the server");
}

fn request(table: &str, mode: WriteMode, batch_size: usize) -> WriteRequest {
    WriteRequest {
        table: table.to_owned(),
        mode,
        columns: BTreeMap::from([
            ("id".to_owned(), "id".to_owned()),
            ("name".to_owned(), "name".to_owned()),
            ("amount".to_owned(), "amount".to_owned()),
        ]),
        conflict_columns: vec!["id".to_owned()],
        batch_size,
    }
}

fn rows(range: std::ops::Range<i64>, prefix: &str) -> Vec<Value> {
    range
        .map(|id| json!({ "id": id, "name": format!("{prefix}-{id}"), "amount": "12345678901234.56" }))
        .collect()
}

async fn count(pool: &MySqlPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn mysql_live_writes_insert_upsert_and_rolls_back_whole_packet() {
    let Some(pool) = live_pool(4).await else {
        return;
    };
    execute(&pool, "DROP TABLE IF EXISTS jaiba_it_write").await;
    execute(
        &pool,
        "CREATE TABLE jaiba_it_write (id BIGINT PRIMARY KEY, name VARCHAR(40) NOT NULL, \
         amount DECIMAL(16,2) NOT NULL)",
    )
    .await;
    let writer = MySqlWriter::new(pool.clone(), DatabaseKind::MySql).unwrap();

    let inserted = writer
        .write(
            &request("jaiba_it_write", WriteMode::Insert, 1000),
            &rows(0..2500, "a"),
        )
        .await
        .unwrap();
    assert_eq!((inserted.rows, inserted.batches), (2500, 3));

    let upserted = writer
        .write(
            &request("jaiba_it_write", WriteMode::Upsert, 1000),
            &rows(2000..3000, "b"),
        )
        .await
        .unwrap();
    assert_eq!(upserted.rows, 1000);
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM jaiba_it_write").await,
        3000
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM jaiba_it_write WHERE name LIKE 'b-%'"
        )
        .await,
        1000
    );
    let amount: String =
        sqlx::query_scalar("SELECT CAST(amount AS CHAR) FROM jaiba_it_write WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(amount, "12345678901234.56");

    // The NULL lands in the second sub-batch: the first one must roll back too.
    let mut poisoned = rows(5000..6500, "c");
    poisoned[1200]["name"] = Value::Null;
    let error = writer
        .write(
            &request("jaiba_it_write", WriteMode::Insert, 1000),
            &poisoned,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, FlowError::DatabaseRejected(_)), "{error}");
    assert!(!error.is_retryable());
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM jaiba_it_write WHERE id >= 5000"
        )
        .await,
        0
    );
    execute(&pool, "DROP TABLE jaiba_it_write").await;
}

#[tokio::test]
async fn mysql_live_classifies_errors() {
    let Some(pool) = live_pool(4).await else {
        return;
    };
    execute(&pool, "DROP TABLE IF EXISTS jaiba_it_errors").await;
    execute(
        &pool,
        "CREATE TABLE jaiba_it_errors (id BIGINT PRIMARY KEY, name VARCHAR(40) NOT NULL, \
         amount DECIMAL(16,2) NOT NULL)",
    )
    .await;
    let writer = MySqlWriter::new(pool.clone(), DatabaseKind::MySql).unwrap();
    let insert = request("jaiba_it_errors", WriteMode::Insert, 100);
    writer.write(&insert, &rows(0..1, "a")).await.unwrap();

    let duplicate = writer.write(&insert, &rows(0..1, "a")).await.unwrap_err();
    assert!(
        matches!(duplicate, FlowError::DatabaseRejected(_)),
        "{duplicate}"
    );
    let too_long = writer
        .write(
            &insert,
            &[json!({ "id": 9, "name": "x".repeat(80), "amount": 1 })],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(too_long, FlowError::DatabaseRejected(_)),
        "{too_long}"
    );
    let missing_table = writer
        .write(
            &request("jaiba_it_missing_table", WriteMode::Insert, 100),
            &rows(0..1, "a"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(missing_table, FlowError::DatabaseRejected(_)),
        "{missing_table}"
    );
    let syntax = mysql_error(sqlx::query("SELEC 1").execute(&pool).await.unwrap_err());
    assert!(matches!(syntax, FlowError::DatabaseRejected(_)), "{syntax}");

    // A lock wait timeout is about the moment, not the data: retryable.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM jaiba_it_errors WHERE id = 0 FOR UPDATE")
        .fetch_all(&mut *holder)
        .await
        .unwrap();
    let mut waiter = pool.acquire().await.unwrap();
    sqlx::query("SET SESSION innodb_lock_wait_timeout = 1")
        .execute(&mut *waiter)
        .await
        .unwrap();
    let lock_wait = mysql_error(
        sqlx::query("UPDATE jaiba_it_errors SET name = 'z' WHERE id = 0")
            .execute(&mut *waiter)
            .await
            .unwrap_err(),
    );
    assert!(
        matches!(lock_wait, FlowError::DatabaseConnector(_)),
        "{lock_wait}"
    );
    assert!(lock_wait.is_retryable());
    holder.rollback().await.unwrap();
    drop(waiter);
    execute(&pool, "DROP TABLE jaiba_it_errors").await;
}

#[tokio::test]
async fn mysql_live_write_timeout_kills_the_statement_and_rolls_back() {
    let Some(pool) = live_pool(4).await else {
        return;
    };
    execute(&pool, "DROP TABLE IF EXISTS jaiba_it_write_timeout").await;
    execute(
        &pool,
        "CREATE TABLE jaiba_it_write_timeout (id BIGINT PRIMARY KEY, name VARCHAR(40) NOT NULL, \
         amount DECIMAL(16,2) NOT NULL)",
    )
    .await;
    execute(
        &pool,
        "INSERT INTO jaiba_it_write_timeout VALUES (1, 'held', 1)",
    )
    .await;

    // Another transaction holds the row, so the upsert waits on its lock.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM jaiba_it_write_timeout WHERE id = 1 FOR UPDATE")
        .fetch_all(&mut *holder)
        .await
        .unwrap();
    let writer = MySqlWriter::new(pool.clone(), DatabaseKind::MySql)
        .unwrap()
        .with_call_timeout(Some(Duration::from_millis(500)));
    let started = std::time::Instant::now();
    let error = writer
        .write(
            &request("jaiba_it_write_timeout", WriteMode::Upsert, 100),
            &[
                json!({ "id": 2, "name": "new", "amount": 2 }),
                json!({ "id": 1, "name": "jaiba_write_timeout", "amount": 3 }),
            ],
        )
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(matches!(error, FlowError::DatabaseConnector(_)), "{error}");
    assert!(error.is_retryable());
    assert_statement_stops(&pool, "jaiba_it_write_timeout").await;
    holder.commit().await.unwrap();

    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM jaiba_it_write_timeout").await,
        1,
        "the timed-out packet must not leave rows behind"
    );
    execute(&pool, "DROP TABLE jaiba_it_write_timeout").await;
}
