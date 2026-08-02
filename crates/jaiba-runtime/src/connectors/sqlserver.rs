use async_trait::async_trait;
use serde_json::Value;
use tiberius::{AuthMethod, Client, Config, ToSql};
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
}
