use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Map, Number, Value};
use tiberius::{AuthMethod, Client, ColumnData, Config, ToSql};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use url::Url;

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
};

type SqlClient = Client<Compat<TcpStream>>;

/// SQL Server writer using the native TDS protocol.
#[derive(Clone)]
pub struct SqlServerWriter {
    config: Config,
}

impl SqlServerWriter {
    /// Parses `sqlserver://user:password@host:port/database`.
    pub fn from_url(value: &str) -> Result<Self, FlowError> {
        let url = Url::parse(value).map_err(|error| {
            FlowError::Configuration(format!("invalid SQL Server URL: {error}"))
        })?;
        if !matches!(url.scheme(), "sqlserver" | "mssql") {
            return Err(FlowError::Configuration(
                "SQL Server URL must use sqlserver:// or mssql://".to_owned(),
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| FlowError::Configuration("SQL Server URL requires a host".to_owned()))?;
        let database = url.path().trim_start_matches('/');
        let password = url.password().unwrap_or_default();
        if url.username().is_empty() || password.is_empty() || database.is_empty() {
            return Err(FlowError::Configuration(
                "SQL Server URL requires username, password and database".to_owned(),
            ));
        }
        let mut config = Config::new();
        config.host(host);
        config.port(url.port().unwrap_or(1433));
        config.database(database);
        config.authentication(AuthMethod::sql_server(url.username(), password));
        // Solo aceptar certificados no verificados cuando se pide explícitamente
        // (sslmode=disable o TrustServerCertificate=true). Por defecto se valida.
        if sqlserver_trust_cert(&url) {
            config.trust_cert();
        }
        Ok(Self { config })
    }

    async fn connect(&self) -> Result<SqlClient, FlowError> {
        let tcp = TcpStream::connect(self.config.get_addr())
            .await
            .map_err(connector_error)?;
        tcp.set_nodelay(true).map_err(connector_error)?;
        Client::connect(self.config.clone(), tcp.compat_write())
            .await
            .map_err(connector_error)
    }

    /// Ejecuta una consulta de lectura y convierte filas a objetos JSON por lotes.
    ///
    /// Tiberius es async: se hace stream de filas y se agrupan en memoria por
    /// `batch_size`. Un resultado vacío emite un lote vacío (igual que Oracle).
    pub async fn query_json_batches(
        &self,
        query: &str,
        parameters: &[Value],
        batch_size: usize,
    ) -> Result<Vec<Vec<Value>>, FlowError> {
        let batch_size = batch_size.max(1);
        let binds = parameters
            .iter()
            .map(sqlserver_bind)
            .collect::<Result<Vec<_>, _>>()?;
        let bind_refs = binds
            .iter()
            .map(|value| value.as_ref() as &dyn ToSql)
            .collect::<Vec<_>>();

        let mut client = self.connect().await?;
        let mut rows = client
            .query(query, &bind_refs)
            .await
            .map_err(connector_error)?
            .into_row_stream();

        let mut batches = Vec::new();
        let mut batch = Vec::with_capacity(batch_size);
        while let Some(row) = rows.next().await {
            let row = row.map_err(connector_error)?;
            batch.push(sqlserver_row_to_json(&row)?);
            if batch.len() == batch_size {
                batches.push(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(batch_size),
                ));
            }
        }
        if !batch.is_empty() || batches.is_empty() {
            batches.push(batch);
        }
        Ok(batches)
    }

    fn statement(&self, request: &WriteRequest) -> Result<String, FlowError> {
        let dialect = self.kind().identifier_dialect();
        let table = quote_qualified_identifier(&request.table, dialect)?;
        let columns = request
            .columns
            .values()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        let parameters = (1..=columns.len())
            .map(|index| format!("@P{index}"))
            .collect::<Vec<_>>();
        if request.mode == WriteMode::Insert {
            return Ok(format!(
                "INSERT INTO {table} ({}) VALUES ({})",
                columns.join(", "),
                parameters.join(", ")
            ));
        }
        let conflicts = request
            .conflict_columns
            .iter()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        let predicate = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| conflicts.contains(column))
            .map(|(index, column)| format!("{column} = @P{}", index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let updates = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| !conflicts.contains(column))
            .map(|(index, column)| format!("{column} = @P{}", index + 1))
            .collect::<Vec<_>>();
        if updates.is_empty() {
            Ok(format!(
                "IF NOT EXISTS (SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK) WHERE {predicate}) \
                 INSERT INTO {table} ({}) VALUES ({})",
                columns.join(", "),
                parameters.join(", ")
            ))
        } else {
            Ok(format!(
                "UPDATE {table} WITH (UPDLOCK, SERIALIZABLE) SET {} WHERE {predicate}; \
                 IF @@ROWCOUNT = 0 INSERT INTO {table} ({}) VALUES ({})",
                updates.join(", "),
                columns.join(", "),
                parameters.join(", ")
            ))
        }
    }
}

#[async_trait]
impl DatabaseWriter for SqlServerWriter {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::SqlServer
    }

    fn capabilities(&self) -> WriteCapabilities {
        WriteCapabilities {
            transactions: true,
            bulk_insert: false,
            native_upsert: false,
            maximum_parameters: Some(2100),
            returning: false,
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
        let sql = self.statement(request)?;
        let mut client = self.connect().await?;
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(connector_error)?;
        let result = async {
            let mut summary = WriteSummary::default();
            for batch in records.chunks(request.batch_size) {
                for record in batch {
                    let object = record.as_object().ok_or_else(|| {
                        FlowError::Configuration("put_database requires object records".to_owned())
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
                                .and_then(sqlserver_bind)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let parameters = values
                        .iter()
                        .map(|value| value.as_ref() as &dyn ToSql)
                        .collect::<Vec<_>>();
                    client
                        .execute(&sql, &parameters)
                        .await
                        .map_err(connector_error)?;
                    summary.rows += 1;
                }
                summary.batches += 1;
            }
            client
                .simple_query("COMMIT TRANSACTION")
                .await
                .map_err(connector_error)?;
            Ok(summary)
        }
        .await;
        if result.is_err() {
            let _ = client
                .simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
                .await;
        }
        result
    }
}

fn sqlserver_bind(value: &Value) -> Result<Box<dyn ToSql + Send + Sync>, FlowError> {
    Ok(match value {
        Value::Null => Box::new(Option::<String>::None),
        Value::Bool(value) => Box::new(*value),
        Value::Number(value) if value.is_i64() => Box::new(value.as_i64().unwrap()),
        Value::Number(value) if value.is_u64() => {
            Box::new(i64::try_from(value.as_u64().unwrap()).map_err(|_| {
                FlowError::Configuration("unsigned integer exceeds SQL Server INT64".to_owned())
            })?)
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

fn sqlserver_row_to_json(row: &tiberius::Row) -> Result<Value, FlowError> {
    let mut object = Map::with_capacity(row.len());
    for (column, data) in row.cells() {
        object.insert(column.name().to_owned(), column_data_to_json(data)?);
    }
    Ok(Value::Object(object))
}

fn column_data_to_json(data: &ColumnData<'_>) -> Result<Value, FlowError> {
    Ok(match data {
        ColumnData::U8(value) => match value {
            Some(value) => Value::from(*value),
            None => Value::Null,
        },
        ColumnData::I16(value) => match value {
            Some(value) => Value::from(*value),
            None => Value::Null,
        },
        ColumnData::I32(value) => match value {
            Some(value) => Value::from(*value),
            None => Value::Null,
        },
        ColumnData::I64(value) => match value {
            Some(value) => Value::from(*value),
            None => Value::Null,
        },
        ColumnData::F32(value) => match value {
            Some(value) => Number::from_f64(f64::from(*value))
                .map(Value::Number)
                .ok_or_else(|| {
                    FlowError::DatabaseConnector("SQL Server returned a non-finite f32".to_owned())
                })?,
            None => Value::Null,
        },
        ColumnData::F64(value) => match value {
            Some(value) => Number::from_f64(*value).map(Value::Number).ok_or_else(|| {
                FlowError::DatabaseConnector("SQL Server returned a non-finite f64".to_owned())
            })?,
            None => Value::Null,
        },
        ColumnData::Bit(value) => match value {
            Some(value) => Value::Bool(*value),
            None => Value::Null,
        },
        ColumnData::String(value) => match value {
            Some(value) => Value::String(value.to_string()),
            None => Value::Null,
        },
        ColumnData::Guid(value) => match value {
            Some(value) => Value::String(value.to_string()),
            None => Value::Null,
        },
        ColumnData::Binary(value) => match value {
            Some(bytes) => match String::from_utf8(bytes.to_vec()) {
                Ok(text) => Value::String(text),
                Err(error) => Value::String(base64_encode(error.as_bytes())),
            },
            None => Value::Null,
        },
        ColumnData::Numeric(value) => match value {
            // Conservar precisión decimal como texto (igual que DECIMAL MySQL).
            Some(value) => Value::String(value.to_string()),
            None => Value::Null,
        },
        ColumnData::Xml(value) => match value {
            Some(value) => Value::String(value.to_string()),
            None => Value::Null,
        },
        // Tiberius sin feature chrono/time no expone Display; Debug conserva
        // los campos TDS (días/fragmentos) de forma estable para hand-off JSON.
        ColumnData::DateTime(value) => match value {
            Some(value) => Value::String(format!("{value:?}")),
            None => Value::Null,
        },
        ColumnData::SmallDateTime(value) => match value {
            Some(value) => Value::String(format!("{value:?}")),
            None => Value::Null,
        },
        ColumnData::Time(value) => match value {
            Some(value) => Value::String(format!("{value:?}")),
            None => Value::Null,
        },
        ColumnData::Date(value) => match value {
            Some(value) => Value::String(format!("{value:?}")),
            None => Value::Null,
        },
        ColumnData::DateTime2(value) => match value {
            Some(value) => Value::String(format!("{value:?}")),
            None => Value::Null,
        },
        ColumnData::DateTimeOffset(value) => match value {
            Some(value) => Value::String(format!("{value:?}")),
            None => Value::Null,
        },
    })
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

fn connector_error(error: impl std::fmt::Display) -> FlowError {
    FlowError::DatabaseConnector(error.to_string())
}

/// Opt-in explícito a aceptar certificados no verificados.
fn sqlserver_trust_cert(url: &Url) -> bool {
    url.query_pairs().any(|(key, value)| {
        let key = key.to_ascii_lowercase();
        let value = value.to_ascii_lowercase();
        match key.as_str() {
            "trustservercertificate" => matches!(value.as_str(), "true" | "1" | "yes"),
            "sslmode" => value == "disable",
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn trusts_certificate_only_when_opted_in() {
        assert!(!sqlserver_trust_cert(
            &Url::parse("sqlserver://sa:p@localhost:1433/db?sslmode=require").unwrap()
        ));
        assert!(sqlserver_trust_cert(
            &Url::parse("sqlserver://sa:p@localhost:1433/db?sslmode=disable").unwrap()
        ));
        assert!(sqlserver_trust_cert(
            &Url::parse("sqlserver://sa:p@localhost:1433/db?TrustServerCertificate=true").unwrap()
        ));
    }

    #[test]
    fn generates_concurrency_safe_upsert() {
        let writer =
            SqlServerWriter::from_url("sqlserver://sa:password@localhost:1433/jaiva_test").unwrap();
        let request = WriteRequest {
            table: "dbo.customers".to_owned(),
            mode: WriteMode::Upsert,
            columns: BTreeMap::from([
                ("id".to_owned(), "id".to_owned()),
                ("name".to_owned(), "name".to_owned()),
            ]),
            conflict_columns: vec!["id".to_owned()],
            batch_size: 100,
        };
        let sql = writer.statement(&request).unwrap();
        assert!(sql.contains("WITH (UPDLOCK, SERIALIZABLE)"));
        assert!(sql.contains("IF @@ROWCOUNT = 0 INSERT"));
        assert!(!sql.contains("MERGE"));
    }

    #[test]
    fn maps_common_column_data_to_json() {
        assert_eq!(
            column_data_to_json(&ColumnData::I32(Some(42))).unwrap(),
            Value::from(42)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(Some(true))).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::String(Some("Ada".into()))).unwrap(),
            Value::String("Ada".to_owned())
        );
        assert_eq!(
            column_data_to_json(&ColumnData::I64(None)).unwrap(),
            Value::Null
        );
        assert_eq!(base64_encode(&[0xff, 0x00, 0x01]), "/wAB");
    }
}
