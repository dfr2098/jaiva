use async_trait::async_trait;
use serde_json::Value;
use sqlx::{MySql, MySqlPool, mysql::MySqlArguments, query::Query};

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
};

/// Transactional writer shared by MySQL and MariaDB.
#[derive(Clone)]
pub struct MySqlWriter {
    pool: MySqlPool,
    kind: DatabaseKind,
}

impl MySqlWriter {
    /// Creates a writer for either MySQL or MariaDB.
    pub fn new(pool: MySqlPool, kind: DatabaseKind) -> Result<Self, FlowError> {
        if !matches!(kind, DatabaseKind::MySql | DatabaseKind::MariaDb) {
            return Err(FlowError::Configuration(
                "MySqlWriter requires mysql or mariadb database kind".to_owned(),
            ));
        }
        Ok(Self { pool, kind })
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

        // Every sub-batch belongs to one transaction, so any error rolls back
        // the complete packet rather than leaving a partial write.
        let mut transaction = self.pool.begin().await?;
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
            query.execute(&mut *transaction).await?;
            summary.rows += batch.len() as u64;
            summary.batches += 1;
        }
        transaction.commit().await?;
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
}
