//! Compilación de `QuerySpec` a SQL seguro por dialecto.
//!
//! El objetivo es que la UI jamás construya SQL: envía una especificación
//! neutral y el servidor genera la sentencia. Todos los identificadores se
//! validan contra un patrón estricto y se citan según el dialecto; todos los
//! valores viajan como parámetros ligados (`$1`, `?`), nunca interpolados. Así
//! se evita la inyección tanto por identificadores como por valores.

use jaiba_plugin_sdk::{
    CompiledQuery, FilterOperator, JoinKind, PluginError, QueryFilter, QuerySource, QuerySpec,
    SortDirection,
};
use serde_json::Value;

#[derive(Clone, Copy)]
pub(crate) enum Dialect {
    Postgres,
    MySql,
}

impl Dialect {
    /// Cita un identificador (posiblemente calificado con puntos) validando
    /// cada segmento. Permite `*` como comodín de columnas.
    fn quote(self, identifier: &str) -> Result<String, PluginError> {
        let mut parts = Vec::new();
        for segment in identifier.split('.') {
            let segment = segment.trim();
            if segment == "*" {
                parts.push("*".to_owned());
                continue;
            }
            if !is_valid_identifier(segment) {
                return Err(PluginError::Configuration(format!(
                    "identificador SQL no válido: '{identifier}'"
                )));
            }
            parts.push(match self {
                Dialect::Postgres => format!("\"{segment}\""),
                Dialect::MySql => format!("`{segment}`"),
            });
        }
        if parts.is_empty() {
            return Err(PluginError::Configuration("identificador vacío".to_owned()));
        }
        Ok(parts.join("."))
    }

    fn like_keyword(self) -> &'static str {
        match self {
            Dialect::Postgres => "ILIKE",
            Dialect::MySql => "LIKE",
        }
    }

    fn placeholder(self, index: usize) -> String {
        match self {
            Dialect::Postgres => format!("${index}"),
            Dialect::MySql => "?".to_owned(),
        }
    }
}

fn is_valid_identifier(segment: &str) -> bool {
    let mut chars = segment.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|character| character.is_ascii_alphanumeric() || character == '_' || character == '$')
}

/// Genera un `CompiledQuery` seguro a partir de la especificación neutral.
pub(crate) fn compile(spec: &QuerySpec, dialect: Dialect) -> Result<CompiledQuery, PluginError> {
    if spec.columns.is_empty() {
        return Err(PluginError::Configuration(
            "selecciona al menos una columna".to_owned(),
        ));
    }

    let mut parameters: Vec<Value> = Vec::new();

    let columns = spec
        .columns
        .iter()
        .map(|column| dialect.quote(column))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    let mut statement = format!(
        "SELECT {columns} FROM {}",
        quote_source(dialect, &spec.source)?
    );

    for join in &spec.joins {
        let target = quote_source(dialect, &join.source)?;
        let left = dialect.quote(&join.left)?;
        let right = dialect.quote(&join.right)?;
        let keyword = match join.kind {
            JoinKind::Inner => "INNER JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
            JoinKind::Full => "FULL JOIN",
        };
        statement.push_str(&format!(" {keyword} {target} ON {left} = {right}"));
    }

    if !spec.filters.is_empty() {
        let mut clauses = Vec::with_capacity(spec.filters.len());
        for filter in &spec.filters {
            clauses.push(compile_filter(dialect, filter, &mut parameters)?);
        }
        statement.push_str(" WHERE ");
        statement.push_str(&clauses.join(" AND "));
    }

    if !spec.group_by.is_empty() {
        let group = spec
            .group_by
            .iter()
            .map(|column| dialect.quote(column))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        statement.push_str(&format!(" GROUP BY {group}"));
    }

    if !spec.order_by.is_empty() {
        let order = spec
            .order_by
            .iter()
            .map(|order| {
                let field = dialect.quote(&order.field)?;
                let direction = match order.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                Ok::<_, PluginError>(format!("{field} {direction}"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        statement.push_str(&format!(" ORDER BY {order}"));
    }

    // `limit` es un entero sin signo, por lo que su interpolación es segura.
    if let Some(limit) = spec.limit {
        statement.push_str(&format!(" LIMIT {limit}"));
    }

    Ok(CompiledQuery {
        statement,
        parameters,
    })
}

fn quote_source(dialect: Dialect, source: &QuerySource) -> Result<String, PluginError> {
    match source.schema.as_deref() {
        Some(schema) if !schema.trim().is_empty() => Ok(format!(
            "{}.{}",
            dialect.quote(schema)?,
            dialect.quote(&source.table)?
        )),
        _ => dialect.quote(&source.table),
    }
}

fn compile_filter(
    dialect: Dialect,
    filter: &QueryFilter,
    parameters: &mut Vec<Value>,
) -> Result<String, PluginError> {
    let field = dialect.quote(&filter.field)?;
    if filter.value.is_null()
        && !matches!(
            filter.operator,
            FilterOperator::Eq
                | FilterOperator::NotEq
                | FilterOperator::IsNull
                | FilterOperator::IsNotNull
        )
    {
        return Err(PluginError::Configuration(
            "un valor null sólo admite los operadores igual, distinto, IS NULL o IS NOT NULL"
                .to_owned(),
        ));
    }
    let mut bind = |value: Value| -> String {
        parameters.push(value);
        dialect.placeholder(parameters.len())
    };
    let clause = match filter.operator {
        FilterOperator::Eq if filter.value.is_null() => format!("{field} IS NULL"),
        FilterOperator::NotEq if filter.value.is_null() => format!("{field} IS NOT NULL"),
        FilterOperator::Eq => format!("{field} = {}", bind(filter.value.clone())),
        FilterOperator::NotEq => format!("{field} <> {}", bind(filter.value.clone())),
        FilterOperator::GreaterThan => format!("{field} > {}", bind(filter.value.clone())),
        FilterOperator::GreaterOrEqual => format!("{field} >= {}", bind(filter.value.clone())),
        FilterOperator::LessThan => format!("{field} < {}", bind(filter.value.clone())),
        FilterOperator::LessOrEqual => format!("{field} <= {}", bind(filter.value.clone())),
        FilterOperator::Contains => {
            let pattern = Value::String(format!("%{}%", scalar_string(&filter.value)));
            format!("{field} {} {}", dialect.like_keyword(), bind(pattern))
        }
        FilterOperator::StartsWith => {
            let pattern = Value::String(format!("{}%", scalar_string(&filter.value)));
            format!("{field} {} {}", dialect.like_keyword(), bind(pattern))
        }
        FilterOperator::In => {
            let items = filter.value.as_array().ok_or_else(|| {
                PluginError::Configuration("el operador IN requiere una lista".to_owned())
            })?;
            if items.is_empty() {
                return Err(PluginError::Configuration(
                    "la lista del operador IN no puede estar vacía".to_owned(),
                ));
            }
            if items.iter().any(Value::is_null) {
                return Err(PluginError::Configuration(
                    "la lista del operador IN no puede contener valores null".to_owned(),
                ));
            }
            let placeholders = items
                .iter()
                .map(|item| bind(item.clone()))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{field} IN ({placeholders})")
        }
        FilterOperator::IsNull => format!("{field} IS NULL"),
        FilterOperator::IsNotNull => format!("{field} IS NOT NULL"),
    };
    Ok(clause)
}

fn scalar_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jaiba_plugin_sdk::{QueryJoin, QueryOrder};

    fn source(schema: &str, table: &str) -> QuerySource {
        QuerySource {
            schema: Some(schema.to_owned()),
            table: table.to_owned(),
        }
    }

    #[test]
    fn compiles_a_full_postgres_query() {
        let spec = QuerySpec {
            source: source("public", "orders"),
            columns: vec!["id".to_owned(), "total".to_owned()],
            joins: vec![QueryJoin {
                kind: JoinKind::Left,
                source: source("public", "customers"),
                left: "orders.customer_id".to_owned(),
                right: "customers.id".to_owned(),
            }],
            filters: vec![
                QueryFilter {
                    field: "total".to_owned(),
                    operator: FilterOperator::GreaterThan,
                    value: Value::from(100),
                },
                QueryFilter {
                    field: "status".to_owned(),
                    operator: FilterOperator::In,
                    value: Value::from(vec!["paid", "shipped"]),
                },
            ],
            group_by: vec![],
            order_by: vec![QueryOrder {
                field: "total".to_owned(),
                direction: SortDirection::Desc,
            }],
            limit: Some(50),
        };
        let compiled = compile(&spec, Dialect::Postgres).expect("compila");
        assert_eq!(
            compiled.statement,
            "SELECT \"id\", \"total\" FROM \"public\".\"orders\" \
             LEFT JOIN \"public\".\"customers\" ON \"orders\".\"customer_id\" = \"customers\".\"id\" \
             WHERE \"total\" > $1 AND \"status\" IN ($2, $3) ORDER BY \"total\" DESC LIMIT 50"
        );
        assert_eq!(compiled.parameters.len(), 3);
    }

    #[test]
    fn mysql_uses_backticks_and_question_marks() {
        let spec = QuerySpec {
            source: source("shop", "orders"),
            columns: vec!["*".to_owned()],
            joins: vec![],
            filters: vec![QueryFilter {
                field: "name".to_owned(),
                operator: FilterOperator::Contains,
                value: Value::from("ana"),
            }],
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };
        let compiled = compile(&spec, Dialect::MySql).expect("compila");
        assert_eq!(
            compiled.statement,
            "SELECT * FROM `shop`.`orders` WHERE `name` LIKE ?"
        );
        assert_eq!(compiled.parameters, vec![Value::String("%ana%".to_owned())]);
    }

    #[test]
    fn rejects_injection_in_identifiers() {
        let spec = QuerySpec {
            source: source("public", "orders; DROP TABLE users"),
            columns: vec!["id".to_owned()],
            joins: vec![],
            filters: vec![],
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };
        assert!(compile(&spec, Dialect::Postgres).is_err());
    }

    #[test]
    fn compiles_null_equality_without_an_untyped_parameter() {
        let spec = QuerySpec {
            source: source("public", "orders"),
            columns: vec!["id".to_owned()],
            joins: vec![],
            filters: vec![QueryFilter {
                field: "cancelled_at".to_owned(),
                operator: FilterOperator::Eq,
                value: Value::Null,
            }],
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };
        let compiled = compile(&spec, Dialect::Postgres).expect("compila");
        assert_eq!(
            compiled.statement,
            "SELECT \"id\" FROM \"public\".\"orders\" WHERE \"cancelled_at\" IS NULL"
        );
        assert!(compiled.parameters.is_empty());
    }

    #[test]
    fn rejects_null_inside_an_in_list() {
        let spec = QuerySpec {
            source: source("public", "orders"),
            columns: vec!["id".to_owned()],
            joins: vec![],
            filters: vec![QueryFilter {
                field: "status".to_owned(),
                operator: FilterOperator::In,
                value: Value::Array(vec![Value::String("paid".to_owned()), Value::Null]),
            }],
            group_by: vec![],
            order_by: vec![],
            limit: None,
        };
        assert!(compile(&spec, Dialect::Postgres).is_err());
    }
}
