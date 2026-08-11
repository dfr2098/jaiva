//! Database-independent connector contracts and built-in adapters.

mod database;
#[cfg(feature = "clickhouse-driver")]
mod clickhouse;
mod mysql;
#[cfg(feature = "oracle-driver")]
mod oracle;
mod postgres;
#[cfg(feature = "sqlserver-driver")]
mod sqlserver;

pub use database::{
    DatabaseKind, DatabaseWriter, IdentifierDialect, WriteCapabilities, WriteMode, WritePlan,
    WriteRequest, WriteStrategy, WriteSummary, quote_identifier, quote_qualified_identifier,
};
#[cfg(feature = "clickhouse-driver")]
pub use clickhouse::ClickHouseWriter;
pub use mysql::MySqlWriter;
#[cfg(feature = "oracle-driver")]
pub use oracle::OracleWriter;
pub use postgres::PostgresWriter;
#[cfg(feature = "sqlserver-driver")]
pub use sqlserver::SqlServerWriter;
