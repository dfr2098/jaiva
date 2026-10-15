//! Database-independent connector contracts and built-in adapters.

#[cfg(feature = "clickhouse-driver")]
mod clickhouse;
mod database;
pub(crate) mod mysql;
#[cfg(feature = "oracle-driver")]
mod oracle;
#[cfg(feature = "oracle-driver")]
pub mod oracle_client;
mod postgres;
#[cfg(feature = "sqlserver-driver")]
mod sqlserver;
pub(crate) mod values;

#[cfg(feature = "clickhouse-driver")]
pub use clickhouse::ClickHouseWriter;
pub use database::{
    DatabaseKind, DatabaseWriter, IdentifierDialect, WriteCapabilities, WriteMode, WritePlan,
    WriteRequest, WriteStrategy, WriteSummary, quote_identifier, quote_qualified_identifier,
};
pub use mysql::{DEFAULT_CALL_TIMEOUT as MYSQL_DEFAULT_CALL_TIMEOUT, MySqlWriter};
#[cfg(feature = "oracle-driver")]
pub use oracle::{
    DEFAULT_CALL_TIMEOUT as ORACLE_DEFAULT_CALL_TIMEOUT, OraclePoolOptions, OracleQueryOptions,
    OracleRowStream, OracleWriter,
};
pub use postgres::PostgresWriter;
#[cfg(feature = "sqlserver-driver")]
pub use sqlserver::SqlServerWriter;
