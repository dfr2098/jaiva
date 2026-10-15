use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use async_trait::async_trait;
use oracle::{
    Connection, ErrorKind,
    pool::{GetMode, Pool, PoolBuilder},
    sql_type::{OracleType, Timestamp, ToSql},
};
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use url::Url;

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
    values::{base64_encode, float_value, number_value},
};

/// Upper bound for one OCI round trip unless a processor sets its own.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);
/// Batches fetched ahead of the consumer. Together with `batch_size` this
/// bounds the memory held by a streaming read.
const STREAM_CHANNEL_CAPACITY: usize = 2;
const MAX_FETCH_ARRAY_SIZE: u32 = 10_000;
/// Oracle's maximum VARCHAR2 bind size; larger text is bound as a CLOB.
const MAX_VARCHAR_BIND_BYTES: usize = 32_767;

/// Applied once per new pooled session. It makes string binds for DATE,
/// TIMESTAMP and NUMBER columns accept ISO 8601 and `.` decimals regardless
/// of the server's regional defaults.
const SESSION_SETUP_SQL: &str = "ALTER SESSION SET \
    NLS_DATE_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS' \
    NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS.FF' \
    NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM' \
    NLS_NUMERIC_CHARACTERS = '.,'";

/// ORA codes caused by an unavailable or interrupted session rather than by
/// the statement itself. Every other ORA code is reported as a rejection.
const TRANSIENT_ORA_CODES: &[i32] = &[
    18,    // maximum number of sessions exceeded
    20,    // maximum number of processes exceeded
    51,    // timeout occurred while waiting for a resource
    54,    // resource busy
    60,    // deadlock detected
    1012,  // not logged on
    1013,  // user requested cancel (call timeout or break)
    1033,  // initialization or shutdown in progress
    1034,  // Oracle not available
    1089,  // immediate shutdown in progress
    1090,  // shutdown in progress
    3113,  // end-of-file on communication channel
    3114,  // not connected to Oracle
    3135,  // connection lost contact
    3136,  // inbound connection timed out
    3156,  // OCI call timed out
    12152, // unable to send break message
    12153, // not connected
    12170, // connect timeout occurred
    12514, // listener does not currently know of service
    12516, // listener could not find available handler
    12518, // listener could not hand off client connection
    12519, // no appropriate service handler found
    12520, // listener could not find available handler
    12521, // listener does not currently know of instance
    12526, // listener: all appropriate instances are in restricted mode
    12527, // listener: all instances are in restricted mode or blocking
    12528, // listener: all appropriate instances are blocking
    12537, // connection closed
    12541, // no listener
    12543, // destination host unreachable
    12547, // lost contact
    12560, // protocol adapter error
    12571, // packet writer failure
    24459, // timeout waiting for pool to create new connections
    24496, // timed out waiting for a free connection
    25408, // can not safely replay call
    30006, // resource busy; acquire with WAIT timeout expired
];

/// DPI-1047: the Oracle Client libraries could not be loaded.
const DPI_CLIENT_LIBRARY_MISSING: i32 = 1047;

#[derive(Debug, Clone)]
struct OracleSettings {
    username: String,
    password: String,
    connect_string: String,
}

/// Pool sizing shared by readers and writers of one Oracle connection.
#[derive(Debug, Clone, Copy)]
pub struct OraclePoolOptions {
    pub max_connections: u32,
    /// Maximum wait for a free session before the call fails (retryable).
    pub acquire_timeout: Duration,
}

impl Default for OraclePoolOptions {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }
}

/// Options for one streaming read.
#[derive(Debug, Clone, Copy)]
pub struct OracleQueryOptions {
    /// Rows per emitted batch; also used as fetch array size (capped).
    pub batch_size: usize,
    /// Per round-trip limit; `None` disables it.
    pub call_timeout: Option<Duration>,
}

/// Oracle connector backed by rust-oracle and ODPI-C.
///
/// OCI calls are blocking, so they run on Tokio's blocking pool. Sessions come
/// from a lazily created pool: an unreachable database fails the call (and is
/// retried by the engine) instead of failing flow startup.
#[derive(Clone)]
pub struct OracleWriter {
    settings: Arc<OracleSettings>,
    pool_options: OraclePoolOptions,
    pool: Arc<Mutex<Option<Pool>>>,
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
        let username = percent_decode(url.username())?;
        let password = percent_decode(url.password().unwrap_or_default())?;
        if username.is_empty() || password.is_empty() {
            return Err(FlowError::Configuration(
                "Oracle URL requires username and password".to_owned(),
            ));
        }
        let port = url.port().unwrap_or(1521);
        Ok(Self {
            settings: Arc::new(OracleSettings {
                username,
                password,
                connect_string: format!("{host}:{port}/{service}"),
            }),
            pool_options: OraclePoolOptions::default(),
            pool: Arc::new(Mutex::new(None)),
        })
    }

    pub fn with_pool_options(mut self, options: OraclePoolOptions) -> Self {
        self.pool_options = OraclePoolOptions {
            max_connections: options.max_connections.max(1),
            acquire_timeout: options.acquire_timeout,
        };
        self
    }

    fn pool(&self) -> Result<Pool, FlowError> {
        let mut slot = self.pool.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(pool) = slot.as_ref() {
            return Ok(pool.clone());
        }
        super::oracle_client::ensure_initialized().map_err(oracle_error)?;
        let pool = PoolBuilder::new(
            self.settings.username.clone(),
            self.settings.password.clone(),
            self.settings.connect_string.clone(),
        )
        .max_connections(self.pool_options.max_connections)
        .get_mode(GetMode::TimedWait(self.pool_options.acquire_timeout))
        .build()
        .map_err(oracle_error)?;
        *slot = Some(pool.clone());
        Ok(pool)
    }

    fn acquire(&self, call_timeout: Option<Duration>) -> Result<Connection, FlowError> {
        let connection = self.pool()?.get().map_err(oracle_error)?;
        connection
            .set_call_timeout(call_timeout)
            .map_err(oracle_error)?;
        if connection.is_new_connection() {
            connection
                .execute(SESSION_SETUP_SQL, &[])
                .map_err(oracle_error)?;
        }
        Ok(connection)
    }

    /// Runs `work` on a pooled session in the blocking pool. Dropping the
    /// returned future interrupts the in-flight OCI call.
    async fn run_blocking<T, F>(
        &self,
        call_timeout: Option<Duration>,
        work: F,
    ) -> Result<T, FlowError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, FlowError> + Send + 'static,
    {
        let writer = self.clone();
        let cancel = CancelHandle::default();
        let _guard = cancel.guard();
        tokio::task::spawn_blocking(move || {
            let connection = Arc::new(writer.acquire(call_timeout)?);
            if !cancel.attach(&connection) {
                return Err(cancelled());
            }
            let result = work(&connection);
            cancel.finish();
            result
        })
        .await
        .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?
    }

    /// Streams a read-only query as JSON object batches.
    ///
    /// At most [`STREAM_CHANNEL_CAPACITY`] batches are buffered ahead of the
    /// consumer. Dropping the stream stops the fetch and breaks any in-flight
    /// call. Column names are lowercased so unquoted Oracle identifiers map
    /// naturally to destination fields. An empty result yields one empty batch.
    pub fn query_stream(&self, query: &str, options: OracleQueryOptions) -> OracleRowStream {
        let (sender, receiver) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let cancel = CancelHandle::default();
        let guard = cancel.guard();
        let writer = self.clone();
        let query = query.to_owned();
        let batch_size = options.batch_size.max(1);
        tokio::task::spawn_blocking(move || {
            let result = stream_rows(
                &writer,
                &query,
                batch_size,
                options.call_timeout,
                &cancel,
                &sender,
            );
            cancel.finish();
            if let Err(error) = result {
                let _ = sender.blocking_send(Err(error));
            }
        });
        OracleRowStream {
            receiver,
            _cancel: guard,
        }
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

/// Batches produced by [`OracleWriter::query_stream`].
pub struct OracleRowStream {
    receiver: mpsc::Receiver<Result<Vec<Value>, FlowError>>,
    _cancel: CancelGuard,
}

impl OracleRowStream {
    /// Next batch, or `None` once the result set is exhausted.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<Value>, FlowError>> {
        self.receiver.recv().await
    }
}

fn stream_rows(
    writer: &OracleWriter,
    query: &str,
    batch_size: usize,
    call_timeout: Option<Duration>,
    cancel: &CancelHandle,
    sender: &mpsc::Sender<Result<Vec<Value>, FlowError>>,
) -> Result<(), FlowError> {
    let connection = Arc::new(writer.acquire(call_timeout)?);
    if !cancel.attach(&connection) {
        return Ok(());
    }
    let fetch_size = u32::try_from(batch_size)
        .unwrap_or(MAX_FETCH_ARRAY_SIZE)
        .min(MAX_FETCH_ARRAY_SIZE);
    let mut statement = connection
        .statement(query)
        .fetch_array_size(fetch_size)
        .prefetch_rows(fetch_size)
        .build()
        .map_err(oracle_error)?;
    let rows = statement.query(&[]).map_err(oracle_error)?;
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
    let capacity = batch_size.min(MAX_FETCH_ARRAY_SIZE as usize);
    let mut batch = Vec::with_capacity(capacity);
    let mut emitted = false;
    for row in rows {
        let row = row.map_err(oracle_error)?;
        let mut object = Map::with_capacity(columns.len());
        for (index, (name, oracle_type)) in columns.iter().enumerate() {
            object.insert(name.clone(), oracle_json_value(&row, index, oracle_type)?);
        }
        batch.push(Value::Object(object));
        if batch.len() == batch_size {
            let full = std::mem::replace(&mut batch, Vec::with_capacity(capacity));
            if sender.blocking_send(Ok(full)).is_err() {
                // The consumer went away; stop fetching.
                return Ok(());
            }
            emitted = true;
        }
    }
    if !batch.is_empty() || !emitted {
        let _ = sender.blocking_send(Ok(batch));
    }
    Ok(())
}

/// Shares the session of an in-flight blocking call so the async side can
/// interrupt it (OCIBreak) when its future or stream is dropped.
#[derive(Clone, Default)]
struct CancelHandle {
    state: Arc<Mutex<CancelState>>,
}

#[derive(Default)]
struct CancelState {
    connection: Option<Arc<Connection>>,
    cancelled: bool,
    finished: bool,
}

impl CancelHandle {
    fn guard(&self) -> CancelGuard {
        CancelGuard(self.clone())
    }

    /// Returns `false` when the caller already gave up.
    fn attach(&self, connection: &Arc<Connection>) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.cancelled {
            return false;
        }
        state.connection = Some(connection.clone());
        true
    }

    fn finish(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.finished = true;
        state.connection = None;
    }
}

struct CancelGuard(CancelHandle);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        let connection = {
            let mut state = self.0.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.finished {
                return;
            }
            state.cancelled = true;
            state.connection.take()
        };
        if let Some(connection) = connection {
            // OCIBreak needs a round trip; keep it off the async worker.
            std::thread::spawn(move || {
                let _ = connection.break_execution();
            });
        }
    }
}

fn cancelled() -> FlowError {
    FlowError::DatabaseConnector("Oracle call cancelled".to_owned())
}

fn oracle_json_value(
    row: &oracle::Row,
    index: usize,
    oracle_type: &OracleType,
) -> Result<Value, FlowError> {
    Ok(match oracle_type {
        OracleType::Number(_, _)
        | OracleType::Float(_)
        | OracleType::Int64
        | OracleType::UInt64 => {
            // Fetched as text so precision is decided here, not by ODPI.
            match row.get::<_, Option<String>>(index).map_err(oracle_error)? {
                Some(text) => number_value(&text),
                None => Value::Null,
            }
        }
        OracleType::BinaryFloat => match row.get::<_, Option<f32>>(index).map_err(oracle_error)? {
            // Through the shortest f32 text so 0.1f32 stays 0.1 in JSON.
            Some(value) => float_value(value.to_string().parse().unwrap_or(f64::from(value))),
            None => Value::Null,
        },
        OracleType::BinaryDouble => match row.get::<_, Option<f64>>(index).map_err(oracle_error)? {
            Some(value) => float_value(value),
            None => Value::Null,
        },
        OracleType::Boolean => match row.get::<_, Option<bool>>(index).map_err(oracle_error)? {
            Some(value) => Value::Bool(value),
            None => Value::Null,
        },
        OracleType::Date
        | OracleType::Timestamp(_)
        | OracleType::TimestampTZ(_)
        | OracleType::TimestampLTZ(_) => {
            match row
                .get::<_, Option<Timestamp>>(index)
                .map_err(oracle_error)?
            {
                Some(value) => Value::String(iso_timestamp(&value, oracle_type)),
                None => Value::Null,
            }
        }
        OracleType::Raw(_) | OracleType::LongRaw | OracleType::BLOB => {
            match row.get::<_, Option<Vec<u8>>>(index).map_err(oracle_error)? {
                Some(bytes) => Value::String(base64_encode(&bytes)),
                None => Value::Null,
            }
        }
        _ => match row.get::<_, Option<String>>(index).map_err(oracle_error)? {
            Some(text) => Value::String(text),
            None => Value::Null,
        },
    })
}

/// Maps Oracle NUMBER text to JSON without losing precision: integers when
/// they fit 64 bits, floats only when the decimal round-trips exactly, and the
/// original text otherwise.
fn iso_timestamp(value: &Timestamp, oracle_type: &OracleType) -> String {
    let mut text = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        value.year(),
        value.month(),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    );
    let precision = match oracle_type {
        OracleType::Timestamp(precision)
        | OracleType::TimestampTZ(precision)
        | OracleType::TimestampLTZ(precision) => usize::from(*precision).min(9),
        _ => 0,
    };
    if precision > 0 {
        let nanos = format!("{:09}", value.nanosecond());
        text.push('.');
        text.push_str(&nanos[..precision]);
    }
    if value.with_tz() {
        let offset = value.tz_offset();
        let sign = if offset < 0 { '-' } else { '+' };
        let minutes = offset.unsigned_abs() / 60;
        text.push_str(&format!("{sign}{:02}:{:02}", minutes / 60, minutes % 60));
    }
    text
}

#[async_trait]
impl DatabaseWriter for OracleWriter {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::Oracle
    }

    fn capabilities(&self) -> WriteCapabilities {
        WriteCapabilities {
            transactions: true,
            // Array DML: one round trip per chunk for INSERT and MERGE.
            bulk_insert: true,
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
        let plan = self.plan(request)?;
        if records.is_empty() {
            return Ok(WriteSummary::default());
        }
        let sql = self.statement(request)?;
        let fields = request.columns.keys().cloned().collect::<Vec<_>>();
        let rows = bind_rows(&fields, records)?;
        let chunk_size = plan.effective_batch_size.max(1);

        self.run_blocking(Some(DEFAULT_CALL_TIMEOUT), move |connection| {
            let result = (|| {
                let mut summary = WriteSummary::default();
                for chunk in rows.chunks(chunk_size) {
                    execute_array_dml(connection, &sql, fields.len(), chunk)?;
                    summary.rows += chunk.len() as u64;
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
    }
}

/// A JSON value prepared for binding.
#[derive(Debug, Clone, PartialEq)]
enum BindValue {
    Null,
    Integer(i64),
    Text(String),
}

impl BindValue {
    fn from_json(value: &Value) -> Result<Self, FlowError> {
        Ok(match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Integer(i64::from(*value)),
            Value::Number(number) => match number.as_i64() {
                Some(integer) => Self::Integer(integer),
                // Bound as text so NUMBER receives the exact decimal.
                None => Self::Text(number.to_string()),
            },
            Value::String(text) => Self::Text(text.clone()),
            Value::Array(_) | Value::Object(_) => Self::Text(
                serde_json::to_string(value)
                    .map_err(|error| FlowError::Configuration(error.to_string()))?,
            ),
        })
    }

    fn as_text(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Integer(integer) => Some(integer.to_string()),
            Self::Text(text) => Some(text.clone()),
        }
    }
}

/// One Oracle type per column for the whole chunk: array DML binds a column
/// with a single type, so mixed JSON types are widened to text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindKind {
    Integer,
    Text(u32),
    Clob,
}

impl BindKind {
    fn for_column(rows: &[Vec<BindValue>], column: usize) -> Self {
        let mut integers = 0_usize;
        let mut texts = 0_usize;
        let mut longest = 1_usize;
        for row in rows {
            match &row[column] {
                BindValue::Null => {}
                BindValue::Integer(integer) => {
                    integers += 1;
                    longest = longest.max(integer.to_string().len());
                }
                BindValue::Text(text) => {
                    texts += 1;
                    longest = longest.max(text.len());
                }
            }
        }
        if integers > 0 && texts == 0 {
            Self::Integer
        } else if longest > MAX_VARCHAR_BIND_BYTES {
            Self::Clob
        } else {
            // All-NULL columns bind as VARCHAR2: Oracle converts a NULL
            // VARCHAR2 into any column type, unlike a NULL NUMBER.
            Self::Text(longest as u32)
        }
    }

    fn oracle_type(self) -> OracleType {
        match self {
            Self::Integer => OracleType::Int64,
            Self::Text(length) => OracleType::Varchar2(length),
            Self::Clob => OracleType::CLOB,
        }
    }
}

fn bind_rows(fields: &[String], records: &[Value]) -> Result<Vec<Vec<BindValue>>, FlowError> {
    records
        .iter()
        .map(|record| {
            let object = record.as_object().ok_or_else(|| {
                FlowError::Configuration("put_database requires object records".to_owned())
            })?;
            fields
                .iter()
                .map(|field| {
                    object
                        .get(field)
                        .ok_or_else(|| {
                            FlowError::Configuration(format!(
                                "record is missing mapped field '{field}'"
                            ))
                        })
                        .and_then(BindValue::from_json)
                })
                .collect()
        })
        .collect()
}

fn execute_array_dml(
    connection: &Connection,
    sql: &str,
    columns: usize,
    rows: &[Vec<BindValue>],
) -> Result<(), FlowError> {
    let kinds = (0..columns)
        .map(|column| BindKind::for_column(rows, column))
        .collect::<Vec<_>>();
    let mut batch = connection
        .batch(sql, rows.len())
        .build()
        .map_err(oracle_error)?;
    for (index, kind) in kinds.iter().enumerate() {
        batch
            .set_type(index + 1, &kind.oracle_type())
            .map_err(oracle_error)?;
    }
    for row in rows {
        let values = row
            .iter()
            .zip(&kinds)
            .map(|(value, kind)| -> Box<dyn ToSql> {
                match (kind, value) {
                    (BindKind::Integer, BindValue::Integer(integer)) => Box::new(*integer),
                    (BindKind::Integer, _) => Box::new(Option::<i64>::None),
                    (_, value) => Box::new(value.as_text()),
                }
            })
            .collect::<Vec<_>>();
        let parameters = values
            .iter()
            .map(|value| value.as_ref())
            .collect::<Vec<_>>();
        batch.append_row(&parameters).map_err(oracle_error)?;
    }
    batch.execute().map_err(oracle_error)
}

/// Splits Oracle failures into retryable session problems and permanent
/// rejections of the statement or data.
fn oracle_error(error: oracle::Error) -> FlowError {
    let message = error.to_string();
    if let Some(code) = error.dpi_code() {
        if code == DPI_CLIENT_LIBRARY_MISSING {
            return FlowError::Configuration(format!(
                "{message}. Run `jaiba oracle-client install` (then restart Jaiba on Linux) \
                 or expose an existing Instant Client through LD_LIBRARY_PATH (Linux), \
                 DYLD_LIBRARY_PATH (macOS) or PATH (Windows)"
            ));
        }
        return FlowError::DatabaseConnector(message);
    }
    if message.contains("unsupported Oracle type") {
        // rust-oracle cannot define columns of some 23ai types (JSON, VECTOR).
        return FlowError::DatabaseRejected(format!(
            "{message}. Select the column as text, e.g. JSON_SERIALIZE(col RETURNING CLOB) \
             or FROM_VECTOR(col)"
        ));
    }
    match error.kind() {
        ErrorKind::OciError => match error.db_error().map(oracle::DbError::code) {
            Some(code) if TRANSIENT_ORA_CODES.contains(&code) => {
                FlowError::DatabaseConnector(message)
            }
            Some(_) => FlowError::DatabaseRejected(message),
            None => FlowError::DatabaseConnector(message),
        },
        ErrorKind::BatchErrors
        | ErrorKind::NullValue
        | ErrorKind::ParseError
        | ErrorKind::OutOfRange
        | ErrorKind::InvalidTypeConversion => FlowError::DatabaseRejected(message),
        _ => FlowError::DatabaseConnector(message),
    }
}

/// Connection Manager builds URLs with `Url::set_password`, which
/// percent-encodes reserved characters; OCI needs the raw credentials.
fn percent_decode(value: &str) -> Result<String, FlowError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escaped = (bytes[index] == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match escaped {
            Some(byte) => {
                decoded.push(byte);
                index += 3;
            }
            None => {
                decoded.push(bytes[index]);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| FlowError::Configuration("Oracle URL credentials are not UTF-8".to_owned()))
}

#[cfg(test)]
#[path = "oracle_live_tests.rs"]
mod live_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

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

    #[test]
    fn decodes_percent_encoded_credentials() {
        let mut url = Url::parse("oracle://db:1522/SVC").unwrap();
        url.set_username("app@corp").unwrap();
        url.set_password(Some("p:ss+w&x=y%z")).unwrap();
        let writer = OracleWriter::from_url(url.as_str()).unwrap();
        assert_eq!(writer.settings.username, "app@corp");
        assert_eq!(writer.settings.password, "p:ss+w&x=y%z");
        assert_eq!(writer.settings.connect_string, "db:1522/SVC");
    }

    #[test]
    fn chooses_one_bind_type_per_column() {
        let rows = bind_rows(
            &[
                "a".to_owned(),
                "b".to_owned(),
                "c".to_owned(),
                "d".to_owned(),
            ],
            &[
                json!({"a": null, "b": 1, "c": null, "d": 1.5}),
                json!({"a": 2, "b": "x", "c": null, "d": 2}),
                json!({"a": true, "b": null, "c": null, "d": null}),
            ],
        )
        .unwrap();
        assert_eq!(BindKind::for_column(&rows, 0), BindKind::Integer);
        assert_eq!(BindKind::for_column(&rows, 1), BindKind::Text(1));
        assert_eq!(BindKind::for_column(&rows, 2), BindKind::Text(1));
        assert_eq!(BindKind::for_column(&rows, 3), BindKind::Text(3));
        let long = "x".repeat(MAX_VARCHAR_BIND_BYTES + 1);
        let rows = bind_rows(&["a".to_owned()], &[json!({ "a": long })]).unwrap();
        assert_eq!(BindKind::for_column(&rows, 0), BindKind::Clob);
    }

    #[test]
    fn rejects_records_missing_mapped_fields() {
        let error = bind_rows(&["id".to_owned()], &[json!({"other": 1})]).unwrap_err();
        assert!(error.to_string().contains("missing mapped field 'id'"));
    }
}
