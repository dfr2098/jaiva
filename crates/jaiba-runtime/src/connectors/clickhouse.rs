//! ClickHouse sink writer (HTTP JSONEachRow inserts).
//!
//! Mapa humano: `docs/connectivity/02-conexiones.md`.
//! Feature: `clickhouse-driver`. YAML `type: clickhouse` →
//! `engine/connections.rs` → este writer vía `put_database`.
//!
//! MVP: insert-only via `put_database`. Upsert is rejected — ClickHouse
//! deduplication belongs to table engines (e.g. ReplacingMergeTree), not
//! classical ON CONFLICT.
//!
//! The official `clickhouse` crate is used for connectivity checks. Dynamic
//! Jaiba records are inserted over the HTTP interface with `FORMAT JSONEachRow`
//! (typed `Row` inserts cannot map arbitrary JSON column sets).

use async_trait::async_trait;
use clickhouse::Client;
use serde_json::{Map, Value};
use url::Url;

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
};

/// ClickHouse HTTP writer.
#[derive(Clone)]
pub struct ClickHouseWriter {
    client: Client,
    http: reqwest::Client,
    base_url: String,
    user: String,
    password: String,
    database: String,
}

impl ClickHouseWriter {
    /// Parses `http(s)://user:password@host:8123/database` or `clickhouse://…`.
    pub fn from_url(value: &str) -> Result<Self, FlowError> {
        let url = Url::parse(value).map_err(|error| {
            FlowError::Configuration(format!("invalid ClickHouse URL: {error}"))
        })?;
        // `url` crate cannot always swap clickhouse→http in place; map scheme here.
        let scheme = match url.scheme() {
            "http" | "https" => url.scheme(),
            "clickhouse" => "http",
            other => {
                return Err(FlowError::Configuration(format!(
                    "ClickHouse URL must use http://, https://, or clickhouse:// (got '{other}')"
                )));
            }
        };
        let host = url
            .host_str()
            .ok_or_else(|| FlowError::Configuration("ClickHouse URL requires a host".to_owned()))?;
        let port = url.port().unwrap_or(8123);
        let database = {
            let path = url.path().trim_start_matches('/');
            if path.is_empty() {
                "default".to_owned()
            } else {
                path.to_owned()
            }
        };
        let user = if url.username().is_empty() {
            "default".to_owned()
        } else {
            urlencoding_decode(url.username())
        };
        let password = url.password().map(urlencoding_decode).unwrap_or_default();
        let base_url = format!("{scheme}://{host}:{port}");
        let client = Client::default()
            .with_url(&base_url)
            .with_user(&user)
            .with_password(&password)
            .with_database(&database);
        let http = reqwest::Client::builder()
            .build()
            .map_err(|error| FlowError::Configuration(format!("HTTP client: {error}")))?;

        Ok(Self {
            client,
            http,
            base_url,
            user,
            password,
            database,
        })
    }

    /// Lightweight connectivity check (`SELECT version()`).
    pub async fn ping(&self) -> Result<String, FlowError> {
        let version: String = self
            .client
            .query("SELECT version()")
            .fetch_one()
            .await
            .map_err(connector_error)?;
        Ok(version)
    }

    fn effective_batch_size(&self, request: &WriteRequest) -> usize {
        request.batch_size.clamp(1, 100_000)
    }

    fn insert_sql(&self, request: &WriteRequest) -> Result<String, FlowError> {
        let dialect = self.kind().identifier_dialect();
        let table = quote_qualified_identifier(&request.table, dialect)?;
        let columns: Vec<String> = request
            .columns
            .values()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<_, _>>()?;
        Ok(format!(
            "INSERT INTO {table} ({}) FORMAT JSONEachRow",
            columns.join(", ")
        ))
    }

    fn records_to_json_each_row(
        request: &WriteRequest,
        records: &[Value],
    ) -> Result<Vec<u8>, FlowError> {
        let mut body = String::new();
        for record in records {
            let object = record.as_object().ok_or_else(|| {
                FlowError::Configuration("put_database requires object records".to_owned())
            })?;
            let mut row = Map::new();
            for (source_field, destination_column) in &request.columns {
                let value = object.get(source_field).ok_or_else(|| {
                    FlowError::Configuration(format!(
                        "record is missing mapped field '{source_field}'"
                    ))
                })?;
                row.insert(destination_column.clone(), value.clone());
            }
            body.push_str(
                &serde_json::to_string(&Value::Object(row)).map_err(|error| {
                    FlowError::DatabaseConnector(format!("JSONEachRow encode failed: {error}"))
                })?,
            );
            body.push('\n');
        }
        Ok(body.into_bytes())
    }

    async fn http_insert(&self, sql: &str, payload: Vec<u8>) -> Result<(), FlowError> {
        let mut url =
            Url::parse(&format!("{}/", self.base_url.trim_end_matches('/'))).map_err(|error| {
                FlowError::Configuration(format!("invalid ClickHouse base URL: {error}"))
            })?;
        url.query_pairs_mut()
            .append_pair("database", &self.database)
            .append_pair("query", sql);

        let response = self
            .http
            .post(url)
            .basic_auth(&self.user, Some(&self.password))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .await
            .map_err(connector_error)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(FlowError::DatabaseConnector(format!(
                "ClickHouse INSERT failed ({status}): {body}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl DatabaseWriter for ClickHouseWriter {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::ClickHouse
    }

    fn capabilities(&self) -> WriteCapabilities {
        WriteCapabilities {
            transactions: false,
            bulk_insert: true,
            native_upsert: false,
            maximum_parameters: None,
            returning: false,
        }
    }

    fn validate(&self, request: &WriteRequest) -> Result<(), FlowError> {
        if request.mode == WriteMode::Upsert {
            return Err(FlowError::Configuration(
                "ClickHouse put_database supports mode=insert only (no classical upsert); \
                 use a ReplacingMergeTree / dedup table if needed"
                    .to_owned(),
            ));
        }
        validate_write_request(request, self.kind())?;
        Ok(())
    }

    async fn write(
        &self,
        request: &WriteRequest,
        records: &[Value],
    ) -> Result<WriteSummary, FlowError> {
        self.validate(request)?;
        if records.is_empty() {
            return Ok(WriteSummary::default());
        }

        let batch_size = self.effective_batch_size(request);
        let sql = self.insert_sql(request)?;
        let mut summary = WriteSummary::default();

        for records_batch in records.chunks(batch_size) {
            let payload = Self::records_to_json_each_row(request, records_batch)?;
            self.http_insert(&sql, payload).await?;
            summary.rows += records_batch.len() as u64;
            summary.batches += 1;
        }

        Ok(summary)
    }
}

fn connector_error(error: impl ToString) -> FlowError {
    FlowError::DatabaseConnector(error.to_string())
}

fn urlencoding_decode(value: &str) -> String {
    percent_encoding_decode(value).unwrap_or_else(|| value.to_owned())
}

fn percent_encoding_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).ok()?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn request(mode: WriteMode) -> WriteRequest {
        WriteRequest {
            table: "broder.events".to_owned(),
            mode,
            columns: BTreeMap::from([
                ("event_type".to_owned(), "event_type".to_owned()),
                ("conveyor".to_owned(), "conveyor".to_owned()),
            ]),
            conflict_columns: vec![],
            batch_size: 1000,
        }
    }

    #[test]
    fn parses_http_url() {
        let writer =
            ClickHouseWriter::from_url("http://default:secret@127.0.0.1:8123/broder").unwrap();
        assert_eq!(writer.database, "broder");
        assert_eq!(writer.user, "default");
        assert_eq!(writer.password, "secret");
    }

    #[test]
    fn parses_clickhouse_scheme_as_http() {
        let writer =
            ClickHouseWriter::from_url("clickhouse://user:pass@ch.local:8123/analytics").unwrap();
        assert_eq!(writer.database, "analytics");
        assert!(writer.base_url.starts_with("http://"));
    }

    #[test]
    fn rejects_upsert() {
        let writer = ClickHouseWriter::from_url("http://127.0.0.1:8123/default").unwrap();
        let err = writer.validate(&request(WriteMode::Upsert)).unwrap_err();
        assert!(err.to_string().contains("insert only"));
    }

    #[test]
    fn builds_insert_sql() {
        let writer = ClickHouseWriter::from_url("http://127.0.0.1:8123/default").unwrap();
        let sql = writer.insert_sql(&request(WriteMode::Insert)).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `broder`.`events` (`conveyor`, `event_type`) FORMAT JSONEachRow"
        );
    }

    #[test]
    fn encodes_json_each_row_with_destination_columns() {
        let req = request(WriteMode::Insert);
        let records = vec![serde_json::json!({
            "event_type": "PALLET_MISALIGNED",
            "conveyor": "TP04"
        })];
        let body = ClickHouseWriter::records_to_json_each_row(&req, &records).unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let row: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(row["event_type"], "PALLET_MISALIGNED");
        assert_eq!(row["conveyor"], "TP04");
    }

    #[test]
    fn plan_is_multi_row_insert() {
        let writer = ClickHouseWriter::from_url("http://127.0.0.1:8123/default").unwrap();
        let plan = writer.plan(&request(WriteMode::Insert)).unwrap();
        assert_eq!(plan.database, DatabaseKind::ClickHouse);
        assert!(!plan.transactional);
        assert_eq!(
            plan.strategy.as_str(),
            super::super::WriteStrategy::MultiRowInsert.as_str()
        );
    }

    /// Opt-in: `JAIBA_TEST_CLICKHOUSE_URL=http://default:@127.0.0.1:8123/default`
    #[tokio::test]
    async fn real_clickhouse_ping_when_url_set() {
        let Ok(url) = std::env::var("JAIBA_TEST_CLICKHOUSE_URL") else {
            eprintln!("skip: JAIBA_TEST_CLICKHOUSE_URL not set");
            return;
        };
        let writer = ClickHouseWriter::from_url(&url).expect("url");
        let version = writer.ping().await.expect("ping");
        assert!(!version.is_empty(), "expected ClickHouse version string");
    }
}
