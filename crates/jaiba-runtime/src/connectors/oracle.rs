use std::sync::Arc;

use async_trait::async_trait;
use oracle::sql_type::OracleType;
use oracle::sql_type::ToSql;
use serde_json::Map;
use serde_json::Value;
use url::Url;

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
};

#[derive(Debug, Clone)]
struct OracleSettings {
    username: String,
    password: String,
    connect_string: String,
}

/// Oracle writer backed by rust-oracle and ODPI-C.
///
/// Oracle calls are blocking, so writes execute through Tokio's blocking pool.
#[derive(Clone)]
pub struct OracleWriter {
    settings: Arc<OracleSettings>,
}

impl OracleWriter {
    /// Parses `oracle://user:password@host:port/service` settings.
    pub fn from_url(value: &str) -> Result<Self, FlowError> {
        let url = Url::parse(value)
            .map_err(|error| FlowError::Configuration(format!("invalid Oracle URL: {error}")))?;
        if url.scheme() != "oracle" {
            return Err(FlowError::Configuration(
                "Oracle URL must use oracle://".to_owned(),
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| FlowError::Configuration("Oracle URL requires a host".to_owned()))?;
        let service = url.path().trim_start_matches('/');
        if service.is_empty() || service.contains('/') {
            return Err(FlowError::Configuration(
                "Oracle URL requires one service name".to_owned(),
            ));
        }
        let username = url.username();
        let password = url.password().unwrap_or_default();
        if username.is_empty() || password.is_empty() {
            return Err(FlowError::Configuration(
                "Oracle URL requires username and password".to_owned(),
            ));
        }
        let port = url.port().unwrap_or(1521);
        Ok(Self {
            settings: Arc::new(OracleSettings {
                username: username.to_owned(),
                password: password.to_owned(),
                connect_string: format!("{host}:{port}/{service}"),
            }),
        })
    }

    /// Executes a read-only query and converts rows to JSON object batches.
    ///
    /// Oracle calls are blocking, so the complete fetch runs on Tokio's
    /// blocking pool. Column names are normalized to lowercase so that
    /// unquoted Oracle identifiers map naturally to destination columns.
    pub async fn query_json_batches(
        &self,
        query: &str,
        batch_size: usize,
    ) -> Result<Vec<Vec<Value>>, FlowError> {
        let settings = self.settings.clone();
        let query = query.to_owned();
        let batch_size = batch_size.max(1);
        tokio::task::spawn_blocking(move || {
            let connection = oracle::Connection::connect(
                &settings.username,
                &settings.password,
                &settings.connect_string,
            )
            .map_err(oracle_error)?;
            let rows = connection.query(&query, &[]).map_err(oracle_error)?;
            // Oracle returns unquoted identifiers in uppercase. Normalizing
            // here keeps YAML field mappings portable across destinations.
            let columns = rows
                .column_info()
                .iter()
                .map(|column| {
                    (
                        column.name().to_ascii_lowercase(),
                        column.oracle_type().clone(),
                    )
                })
                .collect::<Vec<_>>();
            let mut batches = Vec::new();
            let mut batch = Vec::with_capacity(batch_size);
            for row in rows {
                let row = row.map_err(oracle_error)?;
                let mut object = Map::with_capacity(columns.len());
                for (index, (name, oracle_type)) in columns.iter().enumerate() {
                    let value = oracle_json_value(&row, index, oracle_type)?;
                    object.insert(name.clone(), value);
                }
                batch.push(Value::Object(object));
                if batch.len() == batch_size {
                    batches.push(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(batch_size),
                    ));
                }
            }
            // Emit one empty batch for an empty result so the downstream graph
            // receives a deterministic completion packet.
            if !batch.is_empty() || batches.is_empty() {
                batches.push(batch);
            }
            Ok(batches)
        })
        .await
        .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?
    }

    fn insert_statement(&self, request: &WriteRequest) -> Result<String, FlowError> {
        let dialect = self.kind().identifier_dialect();
        let table = quote_qualified_identifier(&request.table, dialect)?;
        let columns = request
            .columns
            .values()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        let binds = (1..=columns.len())
            .map(|index| format!(":{index}"))
            .collect::<Vec<_>>();
        Ok(format!(
            "INSERT INTO {table} ({}) VALUES ({})",
            columns.join(", "),
            binds.join(", ")
        ))
    }

    fn merge_statement(&self, request: &WriteRequest) -> Result<String, FlowError> {
        let dialect = self.kind().identifier_dialect();
        let table = quote_qualified_identifier(&request.table, dialect)?;
        let columns = request
            .columns
            .values()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        let source_projection = columns
            .iter()
            .enumerate()
            .map(|(index, column)| format!(":{} AS {column}", index + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let conflicts = request
            .conflict_columns
            .iter()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        let predicate = conflicts
            .iter()
            .map(|column| format!("destination.{column} = source.{column}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let updates = columns
            .iter()
            .filter(|column| !conflicts.contains(column))
            .map(|column| format!("destination.{column} = source.{column}"))
            .collect::<Vec<_>>();
        let mut sql = format!(
            "MERGE INTO {table} destination \
             USING (SELECT {source_projection} FROM DUAL) source \
             ON ({predicate}) "
        );
        if !updates.is_empty() {
            sql.push_str(&format!(
                "WHEN MATCHED THEN UPDATE SET {} ",
                updates.join(", ")
            ));
        }
        sql.push_str(&format!(
            "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            columns.join(", "),
            columns
                .iter()
                .map(|column| format!("source.{column}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        Ok(sql)
    }

    fn statement(&self, request: &WriteRequest) -> Result<String, FlowError> {
        match request.mode {
            WriteMode::Insert => self.insert_statement(request),
            WriteMode::Upsert => self.merge_statement(request),
        }
    }
}

fn oracle_json_value(
    row: &oracle::Row,
    index: usize,
    oracle_type: &OracleType,
) -> Result<Value, FlowError> {
    // Fetch through Oracle's null-aware string representation first. NUMBER is
    // parsed below so integral JSON values remain integral.
    let text = row.get::<_, Option<String>>(index).map_err(oracle_error)?;
    let Some(text) = text else {
        return Ok(Value::Null);
    };
    match oracle_type {
        OracleType::Number(_, _)
        | OracleType::Float(_)
        | OracleType::BinaryFloat
        | OracleType::BinaryDouble
        | OracleType::Int64
        | OracleType::UInt64 => {
            // Prefer integer, then floating point. Values outside JSON's
            // numeric representation remain strings instead of losing data.
            if let Ok(integer) = text.parse::<i64>() {
                Ok(Value::from(integer))
            } else if let Ok(number) = text.parse::<f64>() {
                serde_json::Number::from_f64(number)
                    .map(Value::Number)
                    .ok_or_else(|| {
                        FlowError::DatabaseConnector(format!(
                            "Oracle returned a non-finite number: {text}"
                        ))
                    })
            } else {
                Ok(Value::String(text))
            }
        }
        OracleType::Boolean => {
            text.parse::<bool>()
                .map(Value::Bool)
                .or_else(|_| match text.as_str() {
                    "1" => Ok(Value::Bool(true)),
                    "0" => Ok(Value::Bool(false)),
                    _ => Err(FlowError::DatabaseConnector(format!(
                        "Oracle returned an invalid boolean: {text}"
                    ))),
                })
        }
        OracleType::Json => serde_json::from_str(&text)
            .map_err(|error| FlowError::DatabaseConnector(error.to_string())),
        _ => Ok(Value::String(text)),
    }
}

#[async_trait]
impl DatabaseWriter for OracleWriter {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Oracle
    }

    fn capabilities(&self) -> WriteCapabilities {
        WriteCapabilities {
            transactions: true,
            bulk_insert: false,
            native_upsert: true,
            maximum_parameters: Some(65_535),
            returning: true,
        }
    }

    fn validate(&self, request: &WriteRequest) -> Result<(), FlowError> {
        validate_write_request(request, self.kind())
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
        let settings = self.settings.clone();
        let request = request.clone();
        let records = records.to_vec();
        let sql = self.statement(&request)?;

        tokio::task::spawn_blocking(move || {
            let connection = oracle::Connection::connect(
                &settings.username,
                &settings.password,
                &settings.connect_string,
            )
            .map_err(oracle_error)?;
            let result = (|| {
                let mut summary = WriteSummary::default();
                for batch in records.chunks(request.batch_size) {
                    for record in batch {
                        let object = record.as_object().ok_or_else(|| {
                            FlowError::Configuration(
                                "put_database requires object records".to_owned(),
                            )
                        })?;
                        let values = request
                            .columns
                            .keys()
                            .map(|field| {
                                object
                                    .get(field)
                                    .ok_or_else(|| {
                                        FlowError::Configuration(format!(
                                            "record is missing mapped field '{field}'"
                                        ))
                                    })
                                    .and_then(oracle_bind)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        let parameters = values
                            .iter()
                            .map(|value| value.as_ref())
                            .collect::<Vec<_>>();
                        connection
                            .execute(&sql, &parameters)
                            .map_err(oracle_error)?;
                        summary.rows += 1;
                    }
                    summary.batches += 1;
                }
                connection.commit().map_err(oracle_error)?;
                Ok(summary)
            })();
            if result.is_err() {
                let _ = connection.rollback();
            }
            result
        })
        .await
        .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?
    }
}

fn oracle_bind(value: &Value) -> Result<Box<dyn ToSql>, FlowError> {
    Ok(match value {
        Value::Null => Box::new(Option::<String>::None),
        Value::Bool(value) => Box::new(i32::from(*value)),
        Value::Number(value) if value.is_i64() => Box::new(value.as_i64().unwrap()),
        Value::Number(value) if value.is_u64() => {
            let number = i64::try_from(value.as_u64().unwrap()).map_err(|_| {
                FlowError::Configuration("unsigned integer exceeds Oracle INT64".to_owned())
            })?;
            Box::new(number)
        }
        Value::Number(value) => Box::new(value.as_f64().ok_or_else(|| {
            FlowError::Configuration("number cannot be represented as f64".to_owned())
        })?),
        Value::String(value) => Box::new(value.clone()),
        Value::Array(_) | Value::Object(_) => Box::new(
            serde_json::to_string(value)
                .map_err(|error| FlowError::Configuration(error.to_string()))?,
        ),
    })
}

fn oracle_error(error: oracle::Error) -> FlowError {
    FlowError::DatabaseConnector(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn request(mode: WriteMode) -> WriteRequest {
        WriteRequest {
            table: "DMA_TEST.JAIVA_WRITE_EXAMPLE".to_owned(),
            mode,
            columns: BTreeMap::from([
                ("id".to_owned(), "ID".to_owned()),
                ("name".to_owned(), "NAME".to_owned()),
            ]),
            conflict_columns: vec!["ID".to_owned()],
            batch_size: 1000,
        }
    }

    fn writer() -> OracleWriter {
        OracleWriter::from_url("oracle://user:password@localhost:1521/FREEPDB1").unwrap()
    }

    #[test]
    fn generates_oracle_insert() {
        assert_eq!(
            writer().statement(&request(WriteMode::Insert)).unwrap(),
            "INSERT INTO \"DMA_TEST\".\"JAIVA_WRITE_EXAMPLE\" (\"ID\", \"NAME\") \
             VALUES (:1, :2)"
        );
    }

    #[test]
    fn generates_oracle_merge() {
        let sql = writer().statement(&request(WriteMode::Upsert)).unwrap();
        assert!(sql.contains("MERGE INTO \"DMA_TEST\".\"JAIVA_WRITE_EXAMPLE\""));
        assert!(sql.contains("destination.\"ID\" = source.\"ID\""));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }
}
