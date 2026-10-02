//! Database-independent connector contracts and built-in adapters.

#[cfg(feature = "clickhouse-driver")]
mod clickhouse;
mod database;
mod mysql;
#[cfg(feature = "oracle-driver")]
mod oracle;
mod postgres;
#[cfg(feature = "sqlserver-driver")]
mod sqlserver;

#[cfg(feature = "clickhouse-driver")]
pub use clickhouse::ClickHouseWriter;
pub use database::{
    DatabaseKind, DatabaseWriter, IdentifierDialect, WriteCapabilities, WriteMode, WritePlan,
    WriteRequest, WriteStrategy, WriteSummary, quote_identifier, quote_qualified_identifier,
};
pub use mysql::MySqlWriter;
#[cfg(feature = "oracle-driver")]
pub use oracle::OracleWriter;
pub use postgres::PostgresWriter;
#[cfg(feature = "sqlserver-driver")]
pub use sqlserver::SqlServerWriter;
