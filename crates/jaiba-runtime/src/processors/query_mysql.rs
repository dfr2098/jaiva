use std::time::Duration;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::{Map, Value};
use sqlx::mysql::{MySqlRow, types::MySqlTime};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use crate::{
    connectors::{
        MYSQL_DEFAULT_CALL_TIMEOUT,
        mysql::{MySqlSession, mysql_error, with_call_timeout},
        values::{base64_encode, float_value, number_value},
    },
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub struct QueryMysql {
    connection: String,
    query: String,
    parameters: Vec<Value>,
    batch_size: usize,
    call_timeout: Option<Duration>,
}

#[derive(Deserialize)]
struct QueryMysqlConfig {
    connection: String,
    query: String,
    /// Parámetros ligados (`?`). El constructor visual genera SQL
    /// parametrizado, por lo que los valores nunca se interpolan en el texto.
    #[serde(default)]
    parameters: Vec<Value>,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
    /// Espera máxima por cada respuesta de MySQL (la siguiente fila); 0 = sin límite.
    #[serde(default = "default_call_timeout_ms")]
    call_timeout_ms: u64,
}

fn default_batch_size() -> usize {
    1_000
}

fn default_call_timeout_ms() -> u64 {
    MYSQL_DEFAULT_CALL_TIMEOUT.as_millis() as u64
}

impl QueryMysql {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: QueryMysqlConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;

        if config.query.trim().is_empty() {
            return Err(FlowError::Configuration(
                "query_mysql requires a non-empty query".to_owned(),
            ));
        }
        for parameter in &config.parameters {
            validate_parameter(parameter)?;
        }

        Ok(Self {
            connection: config.connection,
            query: config.query,
            parameters: config.parameters,
            batch_size: config.batch_size.max(1),
            call_timeout: (config.call_timeout_ms > 0)
                .then(|| Duration::from_millis(config.call_timeout_ms)),
        })
    }

    async fn stream_rows(
        &self,
        session: &mut MySqlSession,
        packet: &DataPacket,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let mut query = sqlx::query(&self.query);
        for parameter in &self.parameters {
            query = bind_json(query, parameter);
        }
        let mut rows = query.fetch(session.connection());
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut batch_number = 0_u64;

        while let Some(row) = with_call_timeout(self.call_timeout, rows.try_next()).await? {
            batch.push(mysql_row_to_json(&row)?);
            if batch.len() == self.batch_size {
                output
                    .success(make_packet(
                        packet,
                        std::mem::take(&mut batch),
                        batch_number,
                    ))
                    .await?;
                batch = Vec::with_capacity(self.batch_size);
                batch_number += 1;
            }
        }

        if !batch.is_empty() || batch_number == 0 {
            output
                .success(make_packet(packet, batch, batch_number))
                .await?;
        }
        Ok(())
    }
}

fn validate_parameter(value: &Value) -> Result<(), FlowError> {
    match value {
        Value::Null => Err(FlowError::Configuration(
            "query_mysql does not accept untyped null parameters; use IS NULL".to_owned(),
        )),
        Value::Number(number) if number.as_i64().is_none() && number.as_f64().is_none() => {
            Err(FlowError::Configuration(format!(
                "query_mysql numeric parameter is outside the supported range: {number}"
            )))
        }
        Value::Number(number) if number.as_u64().is_some_and(|value| value > i64::MAX as u64) => {
            Err(FlowError::Configuration(format!(
                "query_mysql integer parameter exceeds signed BIGINT: {number}"
            )))
        }
        _ => Ok(()),
    }
}

fn bind_json<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &'q Value,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        Value::Null => unreachable!("null parameters are rejected during configuration"),
        Value::Bool(flag) => query.bind(*flag),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                query.bind(integer)
            } else if let Some(unsigned) = number.as_u64() {
                query.bind(unsigned)
            } else {
                query.bind(number.as_f64().unwrap_or_default())
            }
        }
        Value::String(text) => query.bind(text.as_str()),
        other => query.bind(sqlx::types::Json(other.clone())),
    }
}

#[async_trait]
impl Processor for QueryMysql {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let pool = context.connections.mysql(&self.connection)?;
        let mut session = MySqlSession::acquire(pool).await?;
        // On error or cancellation the session is dropped unfinished, which
        // stops the statement on the server and discards the connection.
        self.stream_rows(&mut session, &packet, output).await?;
        session.finish();
        Ok(())
    }
}

fn make_packet(template: &DataPacket, records: Vec<Value>, batch_number: u64) -> DataPacket {
    let mut packet = DataPacket::with_records(records);
    packet.attributes.clone_from(&template.attributes);
    packet
        .attributes
        .insert("source.database".to_owned(), "mysql".to_owned());
    packet
        .attributes
        .insert("batch.number".to_owned(), batch_number.to_string());
    packet.attributes.insert(
        "record.count".to_owned(),
        packet.records().expect("records packet").len().to_string(),
    );
    packet
}

fn mysql_row_to_json(row: &MySqlRow) -> Result<Value, FlowError> {
    let mut object = Map::with_capacity(row.len());
    for column in row.columns() {
        let value = mysql_column_value(row, column.ordinal(), column.type_info().name())
            .map_err(mysql_error)?;
        object.insert(column.name().to_owned(), value);
    }
    Ok(Value::Object(object))
}

fn mysql_column_value(row: &MySqlRow, index: usize, type_name: &str) -> Result<Value, sqlx::Error> {
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(Value::Null);
    }

    match type_name {
        "BOOLEAN" => Ok(Value::Bool(row.try_get::<bool, _>(index)?)),
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" | "YEAR" | "TINYINT UNSIGNED"
        | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            Ok(Value::from(row.try_get::<i64, _>(index)?))
        }
        "BIGINT UNSIGNED" => Ok(Value::from(row.try_get::<u64, _>(index)?)),
        // Shortest decimal text of the f32, so FLOAT 0.1 stays 0.1.
        "FLOAT" => {
            let value = row.try_get::<f32, _>(index)?;
            Ok(float_value(
                value.to_string().parse().unwrap_or(f64::from(value)),
            ))
        }
        "DOUBLE" => Ok(float_value(row.try_get::<f64, _>(index)?)),
        // sqlx stores DECIMAL as a textual buffer; decode past Type::compatible.
        "DECIMAL" => Ok(number_value(&decode_mysql_text(raw)?)),
        "JSON" => {
            let sqlx::types::Json(value) = row.try_get::<sqlx::types::Json<Value>, _>(index)?;
            Ok(value)
        }
        "BIT" => match row.try_get::<u64, _>(index) {
            Ok(value) => Ok(Value::from(value)),
            Err(_) => Ok(Value::String(base64_encode(decode_mysql_bytes(raw)?))),
        },
        // MySQL also flags text with a `_bin` collation (and MariaDB JSON) as
        // binary and sqlx does not expose the charset, so valid UTF-8 stays text.
        "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => {
            let bytes = decode_mysql_bytes(raw)?;
            Ok(Value::String(match std::str::from_utf8(bytes) {
                Ok(text) => text.to_owned(),
                Err(_) => base64_encode(bytes),
            }))
        }
        "GEOMETRY" => Ok(Value::String(base64_encode(decode_mysql_bytes(raw)?))),
        "DATE" => {
            let value = row.try_get::<chrono::NaiveDate, _>(index)?;
            Ok(Value::String(value.format("%Y-%m-%d").to_string()))
        }
        "TIME" => Ok(Value::String(time_text(
            &row.try_get::<MySqlTime, _>(index)?,
        ))),
        "DATETIME" => {
            let value = row.try_get::<chrono::NaiveDateTime, _>(index)?;
            Ok(Value::String(
                value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
            ))
        }
        // The session runs in UTC (sqlx sets time_zone = '+00:00').
        "TIMESTAMP" => {
            let value = row.try_get::<chrono::DateTime<chrono::Utc>, _>(index)?;
            Ok(Value::String(
                value.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
            ))
        }
        _ => match row.try_get::<String, _>(index) {
            Ok(text) => Ok(Value::String(text)),
            Err(_) => Ok(Value::String(decode_mysql_text(raw)?)),
        },
    }
}

/// `TIME` is a duration in MySQL (`-838:59:59` to `838:59:59`).
fn time_text(value: &MySqlTime) -> String {
    // `MySqlTime::is_negative` is inverted in sqlx 0.8.6; the sign is not.
    let sign = if value.sign().is_negative() { "-" } else { "" };
    let mut text = format!(
        "{sign}{:02}:{:02}:{:02}",
        value.hours(),
        value.minutes(),
        value.seconds()
    );
    if value.microseconds() > 0 {
        let fraction = format!("{:06}", value.microseconds());
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    text
}

fn decode_mysql_text<'r>(raw: sqlx::mysql::MySqlValueRef<'r>) -> Result<String, sqlx::Error> {
    use sqlx::decode::Decode;
    <&'r str as Decode<'r, sqlx::MySql>>::decode(raw)
        .map(str::to_owned)
        .map_err(sqlx::Error::Decode)
}

fn decode_mysql_bytes<'r>(raw: sqlx::mysql::MySqlValueRef<'r>) -> Result<&'r [u8], sqlx::Error> {
    use sqlx::decode::Decode;
    <&'r [u8] as Decode<'r, sqlx::MySql>>::decode(raw).map_err(sqlx::Error::Decode)
}

#[cfg(test)]
#[path = "query_mysql_live_tests.rs"]
mod live_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_untyped_null_parameters() {
        let error = QueryMysql::from_config(&json!({
            "connection": "main",
            "query": "SELECT id FROM items WHERE id = ?",
            "parameters": [null]
        }))
        .err()
        .expect("null must be rejected");
        assert!(error.to_string().contains("untyped null"));
    }

    #[test]
    fn rejects_integers_larger_than_signed_bigint() {
        let error = QueryMysql::from_config(&json!({
            "connection": "main",
            "query": "SELECT id FROM items WHERE id = ?",
            "parameters": [u64::MAX]
        }))
        .err()
        .expect("oversized integer must be rejected");
        assert!(error.to_string().contains("BIGINT"));
    }

    #[test]
    fn normalizes_zero_batch_size() {
        let processor = QueryMysql::from_config(&json!({
            "connection": "main",
            "query": "SELECT 1",
            "batch_size": 0
        }))
        .unwrap();
        assert_eq!(processor.batch_size, 1);
    }

    #[test]
    fn reads_call_timeout_with_zero_as_unlimited() {
        let default = QueryMysql::from_config(&json!({
            "connection": "main",
            "query": "SELECT 1"
        }))
        .unwrap();
        assert_eq!(default.call_timeout, Some(MYSQL_DEFAULT_CALL_TIMEOUT));
        let unlimited = QueryMysql::from_config(&json!({
            "connection": "main",
            "query": "SELECT 1",
            "call_timeout_ms": 0
        }))
        .unwrap();
        assert_eq!(unlimited.call_timeout, None);
    }
}
