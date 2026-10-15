use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{
    Connection, MySql, MySqlConnection, MySqlPool,
    mysql::{MySqlArguments, MySqlDatabaseError},
    pool::PoolConnection,
    query::Query,
};
use tracing::debug;

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
};

/// Upper bound for one MySQL round trip (a statement, or the wait for the
/// next row of a result) unless a processor sets its own.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Server errors that describe the session or the server, not the request:
/// repeating the same packet later can succeed.
const TRANSIENT_MYSQL_ERRORS: &[u16] = &[
    1040, // ER_CON_COUNT_ERROR: too many connections
    1053, // ER_SERVER_SHUTDOWN
    1152, // ER_ABORTING_CONNECTION
    1158, // ER_NET_READ_ERROR
    1159, // ER_NET_READ_INTERRUPTED
    1160, // ER_NET_ERROR_ON_WRITE
    1161, // ER_NET_WRITE_INTERRUPTED
    1203, // ER_TOO_MANY_USER_CONNECTIONS
    1205, // ER_LOCK_WAIT_TIMEOUT
    1213, // ER_LOCK_DEADLOCK
    1317, // ER_QUERY_INTERRUPTED (KILL QUERY, timeout)
    1614, // ER_XA_RBDEADLOCK
    1927, // ER_CONNECTION_KILLED (MariaDB)
    1969, // ER_STATEMENT_TIMEOUT (MariaDB max_statement_time)
    3024, // ER_QUERY_TIMEOUT (MySQL max_execution_time)
    4031, // ER_CLIENT_INTERACTION_TIMEOUT
];

/// Maps a sqlx error from MySQL/MariaDB to a retryable connector error or a
/// permanent rejection of the statement or its data.
pub(crate) fn mysql_error(error: sqlx::Error) -> FlowError {
    match &error {
        sqlx::Error::Database(database) => {
            let number = database
                .try_downcast_ref::<MySqlDatabaseError>()
                .map(MySqlDatabaseError::number);
            match number {
                Some(number) if TRANSIENT_MYSQL_ERRORS.contains(&number) => {
                    FlowError::DatabaseConnector(error.to_string())
                }
                _ => FlowError::DatabaseRejected(error.to_string()),
            }
        }
        sqlx::Error::Configuration(_) => FlowError::Configuration(error.to_string()),
        sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::Encode(_)
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::InvalidArgument(_) => FlowError::DatabaseRejected(error.to_string()),
        _ => FlowError::DatabaseConnector(error.to_string()),
    }
}

pub(crate) async fn with_call_timeout<T>(
    limit: Option<Duration>,
    work: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, FlowError> {
    let result = match limit {
        Some(limit) => tokio::time::timeout(limit, work).await.map_err(|_| {
            FlowError::DatabaseConnector(format!(
                "MySQL call exceeded the call timeout of {} ms",
                limit.as_millis()
            ))
        })?,
        None => work.await,
    };
    result.map_err(mysql_error)
}

/// A pooled connection whose server thread id is known. If it is dropped
/// before [`MySqlSession::finish`] (timeout, cancelled node, error), the
/// running statement is stopped with `KILL QUERY` from another connection and
/// the connection is closed instead of returning to the pool, which also rolls
/// back an open transaction.
pub(crate) struct MySqlSession {
    pool: MySqlPool,
    connection: Option<PoolConnection<MySql>>,
    connection_id: u64,
    finished: bool,
}

impl MySqlSession {
    pub(crate) async fn acquire(pool: &MySqlPool) -> Result<Self, FlowError> {
        let mut connection = pool.acquire().await.map_err(mysql_error)?;
        let connection_id =
            sqlx::query_scalar::<_, u64>("SELECT CAST(CONNECTION_ID() AS UNSIGNED)")
                .fetch_one(&mut *connection)
                .await
                .map_err(mysql_error)?;
        Ok(Self {
            pool: pool.clone(),
            connection: Some(connection),
            connection_id,
            finished: false,
        })
    }

    pub(crate) fn connection(&mut self) -> &mut MySqlConnection {
        self.connection
            .as_mut()
            .expect("MySQL session connection is present until drop")
    }

    /// Returns the connection to the pool; nothing is left running.
    pub(crate) fn finish(mut self) {
        self.finished = true;
    }
}

impl Drop for MySqlSession {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
        self.connection = None;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let pool = self.pool.clone();
        let connection_id = self.connection_id;
        runtime.spawn(async move {
            // "Unknown thread id" only means the statement already ended.
            if let Err(error) = sqlx::query(&format!("KILL QUERY {connection_id}"))
                .execute(&pool)
                .await
            {
                debug!(connection_id, %error, "MySQL KILL QUERY after cancellation failed");
            }
        });
    }
}

/// Transactional writer shared by MySQL and MariaDB.
#[derive(Clone)]
pub struct MySqlWriter {
    pool: MySqlPool,
    kind: DatabaseKind,
    call_timeout: Option<Duration>,
}

impl MySqlWriter {
    /// Creates a writer for either MySQL or MariaDB.
    pub fn new(pool: MySqlPool, kind: DatabaseKind) -> Result<Self, FlowError> {
        if !matches!(kind, DatabaseKind::MySql | DatabaseKind::MariaDb) {
            return Err(FlowError::Configuration(
                "MySqlWriter requires mysql or mariadb database kind".to_owned(),
            ));
        }
        Ok(Self {
            pool,
            kind,
            call_timeout: Some(DEFAULT_CALL_TIMEOUT),
        })
    }

    /// Upper bound for each statement of a write; `None` disables it.
    pub fn with_call_timeout(mut self, call_timeout: Option<Duration>) -> Self {
        self.call_timeout = call_timeout;
        self
    }

    fn effective_batch_size(&self, request: &WriteRequest) -> usize {
        request
            .batch_size
            .min((65_535 / request.columns.len().max(1)).max(1))
    }

    fn statement(&self, request: &WriteRequest, rows: usize) -> Result<String, FlowError> {
        let dialect = self.kind.identifier_dialect();
        let table = quote_qualified_identifier(&request.table, dialect)?;
        let columns: Vec<String> = request
            .columns
            .values()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<_, _>>()?;
        let row = format!("({})", vec!["?"; columns.len()].join(", "));
        let mut sql = format!(
            "INSERT INTO {table} ({}) VALUES {}",
            columns.join(", "),
            vec![row; rows].join(", ")
        );
        if request.mode == WriteMode::Upsert {
            let updates: Vec<String> = request
                .columns
                .values()
                .filter(|column| !request.conflict_columns.contains(column))
                .map(|column| {
                    quote_identifier(column, dialect)
                        .map(|quoted| format!("{quoted} = VALUES({quoted})"))
                })
                .collect::<Result<_, _>>()?;
            if updates.is_empty() {
                // Portable no-op when every mapped column belongs to the key.
                let conflict = quote_identifier(&request.conflict_columns[0], dialect)?;
                sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {conflict} = {conflict}"));
            } else {
                sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", updates.join(", ")));
            }
        }
        Ok(sql)
    }

    async fn write_in_transaction(
        &self,
        connection: &mut MySqlConnection,
        request: &WriteRequest,
        records: &[Value],
    ) -> Result<WriteSummary, FlowError> {
        // Every sub-batch belongs to one transaction, so any error rolls back
        // the complete packet rather than leaving a partial write.
        let mut transaction = with_call_timeout(self.call_timeout, connection.begin()).await?;
        let mut summary = WriteSummary::default();
        for batch in records.chunks(self.effective_batch_size(request)) {
            let sql = self.statement(request, batch.len())?;
            let mut query = sqlx::query(&sql);
            for record in batch {
                let object = record.as_object().ok_or_else(|| {
                    FlowError::Configuration("put_database requires object records".to_owned())
                })?;
                for source_field in request.columns.keys() {
                    let value = object.get(source_field).ok_or_else(|| {
                        FlowError::Configuration(format!(
                            "record is missing mapped field '{source_field}'"
                        ))
                    })?;
                    query = bind_json(query, value)?;
                }
            }
            with_call_timeout(self.call_timeout, query.execute(&mut *transaction)).await?;
            summary.rows += batch.len() as u64;
            summary.batches += 1;
        }
        with_call_timeout(self.call_timeout, transaction.commit()).await?;
        Ok(summary)
    }
}

#[async_trait]
impl DatabaseWriter for MySqlWriter {
    fn kind(&self) -> DatabaseKind {
        self.kind
    }

    fn capabilities(&self) -> WriteCapabilities {
        WriteCapabilities {
            transactions: true,
            bulk_insert: true,
            native_upsert: true,
            maximum_parameters: Some(65_535),
            returning: false,
        }
    }

    fn validate(&self, request: &WriteRequest) -> Result<(), FlowError> {
        validate_write_request(request, self.kind)
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
        let mut session = MySqlSession::acquire(&self.pool).await?;
        let summary = self
            .write_in_transaction(session.connection(), request, records)
            .await?;
        session.finish();
        Ok(summary)
    }
}

fn bind_json<'query>(
    query: Query<'query, MySql, MySqlArguments>,
    value: &'query Value,
) -> Result<Query<'query, MySql, MySqlArguments>, FlowError> {
    Ok(match value {
        Value::Null => query.bind(Option::<String>::None),
        Value::Bool(value) => query.bind(*value),
        Value::Number(value) if value.is_i64() => query.bind(value.as_i64().unwrap()),
        Value::Number(value) if value.is_u64() => query.bind(value.as_u64().unwrap()),
        Value::Number(value) => query.bind(value.as_f64().ok_or_else(|| {
            FlowError::Configuration("number cannot be represented as f64".to_owned())
        })?),
        Value::String(value) => query.bind(value),
        Value::Array(_) | Value::Object(_) => query.bind(sqlx::types::Json(value)),
    })
}

#[cfg(test)]
#[path = "mysql_live_tests.rs"]
pub(crate) mod live_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sqlx::mysql::MySqlConnectOptions;

    use super::*;

    fn request(mode: WriteMode) -> WriteRequest {
        WriteRequest {
            table: "dma_test.customers".to_owned(),
            mode,
            columns: BTreeMap::from([
                ("id".to_owned(), "customer_id".to_owned()),
                ("name".to_owned(), "customer_name".to_owned()),
            ]),
            conflict_columns: vec!["customer_id".to_owned()],
            batch_size: 1000,
        }
    }

    fn writer(kind: DatabaseKind) -> MySqlWriter {
        let pool = MySqlPool::connect_lazy_with(MySqlConnectOptions::new());
        MySqlWriter::new(pool, kind).unwrap()
    }

    #[tokio::test]
    async fn generates_multi_row_insert() {
        let sql = writer(DatabaseKind::MySql)
            .statement(&request(WriteMode::Insert), 2)
            .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO `dma_test`.`customers` (`customer_id`, `customer_name`) \
             VALUES (?, ?), (?, ?)"
        );
    }

    #[tokio::test]
    async fn generates_mysql_and_mariadb_compatible_upsert() {
        for kind in [DatabaseKind::MySql, DatabaseKind::MariaDb] {
            let sql = writer(kind)
                .statement(&request(WriteMode::Upsert), 1)
                .unwrap();
            assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
            assert!(sql.contains("`customer_name` = VALUES(`customer_name`)"));
        }
    }

    #[test]
    fn transport_failures_are_retryable_and_bad_data_is_not() {
        let io = mysql_error(sqlx::Error::Io(std::io::Error::other("reset")));
        assert!(matches!(io, FlowError::DatabaseConnector(_)));
        assert!(io.is_retryable());
        assert!(matches!(
            mysql_error(sqlx::Error::PoolTimedOut),
            FlowError::DatabaseConnector(_)
        ));
        let decode = mysql_error(sqlx::Error::ColumnNotFound("missing".to_owned()));
        assert!(matches!(decode, FlowError::DatabaseRejected(_)));
        assert!(!decode.is_retryable());
    }
}
