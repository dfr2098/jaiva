use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Postgres, postgres::PgArguments, query::Query};

use crate::{connectors::database::validate_write_request, error::FlowError};

use super::{
    DatabaseKind, DatabaseWriter, WriteCapabilities, WriteMode, WriteRequest, WriteSummary,
    quote_identifier, quote_qualified_identifier,
};

/// PostgreSQL transactional batch writer.
#[derive(Clone)]
pub struct PostgresWriter {
    pool: PgPool,
}

impl PostgresWriter {
    /// Creates a writer backed by a shared SQLx pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn effective_batch_size(&self, request: &WriteRequest) -> usize {
        let columns = request.columns.len().max(1);
        let parameter_limit = self.capabilities().maximum_parameters.unwrap_or(65_535);
        request.batch_size.min((parameter_limit / columns).max(1))
    }

    fn statement(&self, request: &WriteRequest, rows: usize) -> Result<String, FlowError> {
        let dialect = self.kind().identifier_dialect();
        let table = quote_qualified_identifier(&request.table, dialect)?;
        let columns: Vec<String> = request
            .columns
            .values()
            .map(|column| quote_identifier(column, dialect))
            .collect::<Result<_, _>>()?;
        let mut sql = format!("INSERT INTO {table} ({}) VALUES ", columns.join(", "));
        let mut parameter = 1;
        for row_index in 0..rows {
            if row_index > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            for column_index in 0..columns.len() {
                if column_index > 0 {
                    sql.push_str(", ");
                }
                sql.push('$');
                sql.push_str(&parameter.to_string());
                parameter += 1;
            }
            sql.push(')');
        }

        if request.mode == WriteMode::Upsert {
            let conflicts = request
                .conflict_columns
                .iter()
                .map(|column| quote_identifier(column, dialect))
                .collect::<Result<Vec<_>, _>>()?;
            let updates: Vec<String> = request
                .columns
                .values()
                .filter(|column| !request.conflict_columns.contains(column))
                .map(|column| {
                    quote_identifier(column, dialect)
                        .map(|quoted| format!("{quoted} = EXCLUDED.{quoted}"))
                })
                .collect::<Result<_, _>>()?;
            sql.push_str(&format!(" ON CONFLICT ({}) ", conflicts.join(", ")));
            if updates.is_empty() {
                sql.push_str("DO NOTHING");
            } else {
                sql.push_str(&format!("DO UPDATE SET {}", updates.join(", ")));
            }
        }
        Ok(sql)
    }
}

#[async_trait]
impl DatabaseWriter for PostgresWriter {
    fn kind(&self) -> DatabaseKind {
        DatabaseKind::PostgreSql
    }

    fn capabilities(&self) -> WriteCapabilities {
        WriteCapabilities {
            transactions: true,
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
        self.validate(request)?;
        if records.is_empty() {
            return Ok(WriteSummary::default());
        }

        // One transaction covers the full packet. If any sub-batch fails, no
        // earlier sub-batch can be committed accidentally.
        let mut transaction = self.pool.begin().await?;
        let batch_size = self.effective_batch_size(request);
        let mut summary = WriteSummary::default();

        for records_batch in records.chunks(batch_size) {
            let sql = self.statement(request, records_batch.len())?;
            let mut query = sqlx::query(&sql);
            for record in records_batch {
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
            summary.rows += records_batch.len() as u64;
            summary.batches += 1;
        }

        transaction.commit().await?;
        Ok(summary)
    }
}

fn bind_json<'query>(
    query: Query<'query, Postgres, PgArguments>,
    value: &'query Value,
) -> Result<Query<'query, Postgres, PgArguments>, FlowError> {
    Ok(match value {
        Value::Null => query.bind(Option::<String>::None),
        Value::Bool(value) => query.bind(*value),
        Value::Number(value) if value.is_i64() => query.bind(value.as_i64().unwrap()),
        Value::Number(value) if value.is_u64() => {
            let number = i64::try_from(value.as_u64().unwrap()).map_err(|_| {
                FlowError::Configuration("unsigned integer exceeds PostgreSQL BIGINT".to_owned())
            })?;
            query.bind(number)
        }
        Value::Number(value) => query.bind(value.as_f64().ok_or_else(|| {
            FlowError::Configuration("number cannot be represented as f64".to_owned())
        })?),
        Value::String(value) => query.bind(value),
        Value::Array(_) | Value::Object(_) => query.bind(sqlx::types::Json(value)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn request(mode: WriteMode) -> WriteRequest {
        WriteRequest {
            table: "public.customers".to_owned(),
            mode,
            columns: BTreeMap::from([
                ("id".to_owned(), "customer_id".to_owned()),
                ("name".to_owned(), "customer_name".to_owned()),
            ]),
            conflict_columns: vec!["customer_id".to_owned()],
            batch_size: 1000,
        }
    }

    #[tokio::test]
    async fn generates_postgres_insert() {
        let options = sqlx::postgres::PgConnectOptions::new();
        let pool = PgPool::connect_lazy_with(options);
        let writer = PostgresWriter::new(pool);
        let sql = writer.statement(&request(WriteMode::Insert), 2).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"customers\" (\"customer_id\", \"customer_name\") \
             VALUES ($1, $2), ($3, $4)"
        );
    }

    #[tokio::test]
    async fn generates_postgres_upsert() {
        let options = sqlx::postgres::PgConnectOptions::new();
        let pool = PgPool::connect_lazy_with(options);
        let writer = PostgresWriter::new(pool);
        let sql = writer.statement(&request(WriteMode::Upsert), 1).unwrap();
        assert!(sql.contains("ON CONFLICT (\"customer_id\")"));
        assert!(sql.contains("\"customer_name\" = EXCLUDED.\"customer_name\""));
    }

    #[tokio::test]
    async fn reduces_batch_size_to_postgres_parameter_limit() {
        let options = sqlx::postgres::PgConnectOptions::new();
        let pool = PgPool::connect_lazy_with(options);
        let writer = PostgresWriter::new(pool);
        let mut request = request(WriteMode::Insert);
        request.batch_size = 100_000;
        assert_eq!(writer.effective_batch_size(&request), 32_767);
    }
}
