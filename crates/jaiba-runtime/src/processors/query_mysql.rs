use async_trait::async_trait;
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::{Map, Number, Value};
use sqlx::mysql::MySqlRow;
use sqlx::{Column, Row, TypeInfo, ValueRef};

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

pub struct QueryMysql {
    connection: String,
    query: String,
    parameters: Vec<Value>,
    batch_size: usize,
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
}

fn default_batch_size() -> usize {
    1_000
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
        })
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
        let mut query = sqlx::query(&self.query);
        for parameter in &self.parameters {
            query = bind_json(query, parameter);
        }
        let mut rows = query.fetch(pool);
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut batch_number = 0_u64;

        while let Some(row) = rows.try_next().await? {
            batch.push(mysql_row_to_json(&row)?);
            if batch.len() == self.batch_size {
                output
                    .success(make_packet(
                        &packet,
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
                .success(make_packet(&packet, batch, batch_number))
                .await?;
        }

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
        let value = mysql_column_value(row, column.ordinal(), column.type_info().name())?;
        object.insert(column.name().to_owned(), value);
    }
    Ok(Value::Object(object))
}

fn mysql_column_value(row: &MySqlRow, index: usize, type_name: &str) -> Result<Value, FlowError> {
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
        "FLOAT" | "DOUBLE" => {
            let number = row.try_get::<f64, _>(index)?;
            Number::from_f64(number).map(Value::Number).ok_or_else(|| {
                FlowError::DatabaseConnector(format!(
                    "MySQL returned a non-finite number in column {index}"
                ))
            })
        }
        "DECIMAL" => {
            // sqlx stores DECIMAL as a textual buffer; decode past Type::compatible.
            let text = decode_mysql_text(raw)?;
            if let Ok(integer) = text.parse::<i64>() {
                Ok(Value::from(integer))
            } else if let Ok(float) = text.parse::<f64>() {
                Ok(Number::from_f64(float)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(text)))
            } else {
                Ok(Value::String(text))
            }
        }
        "JSON" => {
            let sqlx::types::Json(value) = row.try_get::<sqlx::types::Json<Value>, _>(index)?;
            Ok(value)
        }
        "BIT" => match row.try_get::<u64, _>(index) {
            Ok(value) => Ok(Value::from(value)),
            Err(_) => Ok(bytes_to_json(decode_mysql_bytes(raw)?)),
        },
        "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" | "GEOMETRY" => {
            match row.try_get::<Vec<u8>, _>(index) {
                Ok(bytes) => Ok(bytes_to_json(bytes)),
                Err(_) => Ok(bytes_to_json(decode_mysql_bytes(raw)?)),
            }
        }
        "DATE" => {
            let value = row.try_get::<chrono::NaiveDate, _>(index)?;
            Ok(Value::String(value.format("%Y-%m-%d").to_string()))
        }
        "TIME" => {
            let value = row.try_get::<chrono::NaiveTime, _>(index)?;
            Ok(Value::String(value.format("%H:%M:%S%.f").to_string()))
        }
        "DATETIME" => {
            let value = row.try_get::<chrono::NaiveDateTime, _>(index)?;
            Ok(Value::String(
                value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            ))
        }
        "TIMESTAMP" => {
            let value = row.try_get::<chrono::DateTime<chrono::Utc>, _>(index)?;
            Ok(Value::String(
                value.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
            ))
        }
        _ => match row.try_get::<String, _>(index) {
            Ok(text) => Ok(Value::String(text)),
            Err(_) => match row.try_get::<Vec<u8>, _>(index) {
                Ok(bytes) => Ok(bytes_to_json(bytes)),
                Err(_) => Ok(Value::String(decode_mysql_text(raw)?)),
            },
        },
    }
}

fn decode_mysql_text<'r>(raw: sqlx::mysql::MySqlValueRef<'r>) -> Result<String, FlowError> {
    use sqlx::decode::Decode;
    <&'r str as Decode<'r, sqlx::MySql>>::decode(raw)
        .map(str::to_owned)
        .map_err(|error| FlowError::DatabaseConnector(error.to_string()))
}

fn decode_mysql_bytes<'r>(raw: sqlx::mysql::MySqlValueRef<'r>) -> Result<Vec<u8>, FlowError> {
    use sqlx::decode::Decode;
    <&'r [u8] as Decode<'r, sqlx::MySql>>::decode(raw)
        .map(|bytes| bytes.to_vec())
        .map_err(|error| FlowError::DatabaseConnector(error.to_string()))
}

fn bytes_to_json(bytes: Vec<u8>) -> Value {
    match String::from_utf8(bytes) {
        Ok(text) => Value::String(text),
        Err(error) => Value::String(base64_encode(error.as_bytes())),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        output.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        output.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

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
    fn encodes_binary_as_base64() {
        assert_eq!(base64_encode(&[0xff, 0x00, 0x01]), "/wAB");
    }
}
