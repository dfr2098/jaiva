use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::error::FlowError;

/// Supported database families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    PostgreSql,
    MySql,
    MariaDb,
    Oracle,
    SqlServer,
}

/// Identifier quoting rules for a database family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierDialect {
    DoubleQuote,
    Backtick,
    Bracket,
}

impl DatabaseKind {
    /// Stable identifier used in execution plans and packet attributes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PostgreSql => "postgres",
            Self::MySql => "mysql",
            Self::MariaDb => "mariadb",
            Self::Oracle => "oracle",
            Self::SqlServer => "sqlserver",
        }
    }

    /// Returns the identifier dialect used by the database.
    pub fn identifier_dialect(self) -> IdentifierDialect {
        match self {
            Self::PostgreSql | Self::Oracle => IdentifierDialect::DoubleQuote,
            Self::MySql | Self::MariaDb => IdentifierDialect::Backtick,
            Self::SqlServer => IdentifierDialect::Bracket,
        }
    }
}

/// Operation used when writing records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Insert,
    Upsert,
}

impl WriteMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Upsert => "upsert",
        }
    }
}

/// Features and limits declared by a database writer.
#[derive(Debug, Clone, Copy)]
pub struct WriteCapabilities {
    pub transactions: bool,
    pub bulk_insert: bool,
    pub native_upsert: bool,
    pub maximum_parameters: Option<usize>,
    pub returning: bool,
}

/// Concrete loading strategy selected from a writer's declared capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStrategy {
    MultiRowInsert,
    TransactionalRowInsert,
    NativeUpsert,
    TransactionalUpsert,
}

impl WriteStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MultiRowInsert => "multi_row_insert",
            Self::TransactionalRowInsert => "transactional_row_insert",
            Self::NativeUpsert => "native_upsert",
            Self::TransactionalUpsert => "transactional_upsert",
        }
    }
}

/// Explainable plan produced before a database write starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WritePlan {
    pub database: DatabaseKind,
    pub mode: WriteMode,
    pub strategy: WriteStrategy,
    pub requested_batch_size: usize,
    pub effective_batch_size: usize,
    pub transactional: bool,
}

/// Validated request passed to a database writer.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub table: String,
    pub mode: WriteMode,
    /// Mapping from packet field to destination column.
    pub columns: BTreeMap<String, String>,
    /// Destination columns that identify an upsert conflict.
    pub conflict_columns: Vec<String>,
    pub batch_size: usize,
}

/// Result of a committed database write.
#[derive(Debug, Clone, Copy, Default)]
pub struct WriteSummary {
    pub rows: u64,
    pub batches: u64,
}

/// Transactional writer implemented by each database adapter.
#[async_trait]
pub trait DatabaseWriter: Send + Sync {
    /// Identifies the database family.
    fn kind(&self) -> DatabaseKind;

    /// Reports supported operations and driver limits.
    fn capabilities(&self) -> WriteCapabilities;

    /// Validates a request without modifying the destination.
    fn validate(&self, request: &WriteRequest) -> Result<(), FlowError>;

    /// Selects and explains the loading strategy without modifying the destination.
    fn plan(&self, request: &WriteRequest) -> Result<WritePlan, FlowError> {
        self.validate(request)?;
        Ok(build_write_plan(self.kind(), self.capabilities(), request))
    }

    /// Writes every record atomically and returns only after commit.
    async fn write(
        &self,
        request: &WriteRequest,
        records: &[Value],
    ) -> Result<WriteSummary, FlowError>;
}

fn build_write_plan(
    database: DatabaseKind,
    capabilities: WriteCapabilities,
    request: &WriteRequest,
) -> WritePlan {
    let strategy = match (
        request.mode,
        capabilities.bulk_insert,
        capabilities.native_upsert,
    ) {
        (WriteMode::Insert, true, _) => WriteStrategy::MultiRowInsert,
        (WriteMode::Insert, false, _) => WriteStrategy::TransactionalRowInsert,
        (WriteMode::Upsert, _, true) => WriteStrategy::NativeUpsert,
        (WriteMode::Upsert, _, false) => WriteStrategy::TransactionalUpsert,
    };
    let effective_batch_size = if capabilities.bulk_insert {
        capabilities
            .maximum_parameters
            .map(|limit| (limit / request.columns.len().max(1)).max(1))
            .map_or(request.batch_size, |limit| request.batch_size.min(limit))
    } else {
        request.batch_size
    };
    WritePlan {
        database,
        mode: request.mode,
        strategy,
        requested_batch_size: request.batch_size,
        effective_batch_size,
        transactional: capabilities.transactions,
    }
}

/// Quotes a single validated identifier component.
pub fn quote_identifier(value: &str, dialect: IdentifierDialect) -> Result<String, FlowError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(FlowError::Configuration(format!(
            "invalid SQL identifier '{value}'"
        )));
    }
    Ok(match dialect {
        IdentifierDialect::DoubleQuote => format!("\"{value}\""),
        IdentifierDialect::Backtick => format!("`{value}`"),
        IdentifierDialect::Bracket => format!("[{value}]"),
    })
}

/// Quotes a dot-separated catalog/schema/table identifier.
pub fn quote_qualified_identifier(
    value: &str,
    dialect: IdentifierDialect,
) -> Result<String, FlowError> {
    let parts: Vec<&str> = value.split('.').collect();
    if parts.is_empty() || parts.len() > 3 {
        return Err(FlowError::Configuration(format!(
            "invalid qualified SQL identifier '{value}'"
        )));
    }
    parts
        .into_iter()
        .map(|part| quote_identifier(part, dialect))
        .collect::<Result<Vec<_>, _>>()
        .map(|quoted| quoted.join("."))
}

/// Performs validation common to every writer.
pub fn validate_write_request(request: &WriteRequest, kind: DatabaseKind) -> Result<(), FlowError> {
    quote_qualified_identifier(&request.table, kind.identifier_dialect())?;
    if request.columns.is_empty() {
        return Err(FlowError::Configuration(
            "put_database requires at least one column mapping".to_owned(),
        ));
    }
    let mut destinations = HashSet::new();
    for column in request.columns.values() {
        quote_identifier(column, kind.identifier_dialect())?;
        if !destinations.insert(column) {
            return Err(FlowError::Configuration(format!(
                "destination column '{column}' is mapped more than once"
            )));
        }
    }
    if request.batch_size == 0 {
        return Err(FlowError::Configuration(
            "put_database batch_size must be greater than zero".to_owned(),
        ));
    }
    if request.mode == WriteMode::Upsert && request.conflict_columns.is_empty() {
        return Err(FlowError::Configuration(
            "upsert requires conflict_columns".to_owned(),
        ));
    }
    let mut conflicts = HashSet::new();
    for conflict in &request.conflict_columns {
        quote_identifier(conflict, kind.identifier_dialect())?;
        if !conflicts.insert(conflict) {
            return Err(FlowError::Configuration(format!(
                "conflict column '{conflict}' is repeated"
            )));
        }
        if !request.columns.values().any(|column| column == conflict) {
            return Err(FlowError::Configuration(format!(
                "conflict column '{conflict}' is not a mapped destination column"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_each_supported_dialect() {
        assert_eq!(
            quote_qualified_identifier("public.customers", IdentifierDialect::DoubleQuote).unwrap(),
            "\"public\".\"customers\""
        );
        assert_eq!(
            quote_qualified_identifier("sales.customers", IdentifierDialect::Backtick).unwrap(),
            "`sales`.`customers`"
        );
        assert_eq!(
            quote_qualified_identifier("dbo.customers", IdentifierDialect::Bracket).unwrap(),
            "[dbo].[customers]"
        );
    }

    #[test]
    fn rejects_identifier_injection() {
        assert!(
            quote_identifier("customers; DROP TABLE x", IdentifierDialect::DoubleQuote).is_err()
        );
        assert!(quote_qualified_identifier("a.b.c.d", IdentifierDialect::DoubleQuote).is_err());
    }

    fn request(mode: WriteMode, batch_size: usize) -> WriteRequest {
        WriteRequest {
            table: "public.customers".to_owned(),
            mode,
            columns: BTreeMap::from([
                ("id".to_owned(), "id".to_owned()),
                ("name".to_owned(), "name".to_owned()),
            ]),
            conflict_columns: vec!["id".to_owned()],
            batch_size,
        }
    }

    #[test]
    fn plans_parameter_limited_multi_row_insert() {
        let plan = build_write_plan(
            DatabaseKind::PostgreSql,
            WriteCapabilities {
                transactions: true,
                bulk_insert: true,
                native_upsert: true,
                maximum_parameters: Some(10),
                returning: true,
            },
            &request(WriteMode::Insert, 100),
        );
        assert_eq!(plan.strategy, WriteStrategy::MultiRowInsert);
        assert_eq!(plan.effective_batch_size, 5);
        assert!(plan.transactional);
    }

    #[test]
    fn plans_transactional_upsert_when_not_native() {
        let plan = build_write_plan(
            DatabaseKind::SqlServer,
            WriteCapabilities {
                transactions: true,
                bulk_insert: false,
                native_upsert: false,
                maximum_parameters: Some(2_100),
                returning: false,
            },
            &request(WriteMode::Upsert, 1_000),
        );
        assert_eq!(plan.strategy, WriteStrategy::TransactionalUpsert);
        assert_eq!(plan.effective_batch_size, 1_000);
    }
}
