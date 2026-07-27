//! Database-independent connector contracts and built-in adapters.

mod database;
mod mysql;
#[cfg(feature = "oracle-driver")]
mod oracle;
mod postgres;
#[cfg(feature = "sqlserver-driver")]
mod sqlserver;

pub use database::{
    DatabaseKind, DatabaseWriter, IdentifierDialect, WriteCapabilities, WriteMode, WriteRequest,
    WriteSummary, quote_identifier, quote_qualified_identifier,
};
pub use mysql::MySqlWriter;
#[cfg(feature = "oracle-driver")]
pub use oracle::OracleWriter;
pub use postgres::PostgresWriter;
#[cfg(feature = "sqlserver-driver")]
pub use sqlserver::SqlServerWriter;
