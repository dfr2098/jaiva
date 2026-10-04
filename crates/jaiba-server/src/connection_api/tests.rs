use std::{collections::BTreeMap, env, fs};

use jaiba_connection_manager::InMemorySecretStore;
use jaiba_plugin_sdk::{
    Availability, FilterOperator, QueryFilter, QueryOrder, QuerySource, SortDirection,
};
#[cfg(feature = "mongodb-driver")]
use mongodb::bson::{Document, doc};
use serde_json::Value;

#[cfg(feature = "mongodb-driver")]
use super::plugins::mongodb::{mongodb_client, mongodb_database};
#[cfg(feature = "oracle-driver")]
use super::plugins::oracle::oracle_connect;
#[cfg(feature = "sqlserver-driver")]
use super::plugins::sqlserver::sqlserver_connect;
use super::plugins::{mysql::mysql_pool, postgres::postgres_pool};
use super::*;

fn mysql_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
    let password = env::var("JAIBA_TEST_MYSQL_PASSWORD").ok()?;
    let host = env::var("JAIBA_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("JAIBA_TEST_MYSQL_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(13_306);
    let database = env::var("JAIBA_TEST_MYSQL_DATABASE").unwrap_or_else(|_| "dma_test".to_owned());
    let username = env::var("JAIBA_TEST_MYSQL_USER").unwrap_or_else(|_| "dma_test".to_owned());
    Some((
        ConnectionEndpoint {
            host,
            port,
            database: Some(database),
            ssl: false,
            pool_min: 1,
            pool_max: 2,
            timeout_ms: 5_000,
            options: BTreeMap::new(),
        },
        ConnectionSecret {
            username,
            password,
            options: BTreeMap::new(),
        },
    ))
}

fn postgres_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
    let password = env::var("JAIBA_TEST_POSTGRES_PASSWORD").ok()?;
    let host = env::var("JAIBA_TEST_POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("JAIBA_TEST_POSTGRES_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(55_432);
    let database = env::var("JAIBA_TEST_POSTGRES_DATABASE").unwrap_or_else(|_| "dma".to_owned());
    let username = env::var("JAIBA_TEST_POSTGRES_USER").unwrap_or_else(|_| "dma".to_owned());
    Some((
        ConnectionEndpoint {
            host,
            port,
            database: Some(database),
            ssl: false,
            pool_min: 1,
            pool_max: 2,
            timeout_ms: 5_000,
            options: BTreeMap::new(),
        },
        ConnectionSecret {
            username,
            password,
            options: BTreeMap::new(),
        },
    ))
}

#[cfg(feature = "mongodb-driver")]
fn mongodb_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
    let password = env::var("JAIBA_TEST_MONGODB_PASSWORD").ok()?;
    let host = env::var("JAIBA_TEST_MONGODB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    // Puerto host típico del contenedor de pruebas (mapeo 27018→27017).
    let port = env::var("JAIBA_TEST_MONGODB_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(27_018);
    let database =
        env::var("JAIBA_TEST_MONGODB_DATABASE").unwrap_or_else(|_| "dma_test".to_owned());
    let username = env::var("JAIBA_TEST_MONGODB_USER").unwrap_or_else(|_| "dma_test".to_owned());
    Some((
        ConnectionEndpoint {
            host,
            port,
            database: Some(database),
            ssl: false,
            pool_min: 1,
            pool_max: 2,
            timeout_ms: 5_000,
            options: BTreeMap::new(),
        },
        ConnectionSecret {
            username,
            password,
            options: BTreeMap::new(),
        },
    ))
}

#[cfg(feature = "oracle-driver")]
fn oracle_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
    let password = env::var("JAIBA_TEST_ORACLE_PASSWORD").ok()?;
    let host = env::var("JAIBA_TEST_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("JAIBA_TEST_ORACLE_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(11_521);
    let service = env::var("JAIBA_TEST_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
    let username = env::var("JAIBA_TEST_ORACLE_USER").unwrap_or_else(|_| "dma_test".to_owned());
    Some((
        ConnectionEndpoint {
            host,
            port,
            database: Some(service),
            ssl: false,
            pool_min: 1,
            pool_max: 1,
            timeout_ms: 10_000,
            options: BTreeMap::new(),
        },
        ConnectionSecret {
            username,
            password,
            options: BTreeMap::new(),
        },
    ))
}

#[cfg(feature = "sqlserver-driver")]
fn sqlserver_test_configuration() -> Option<(ConnectionEndpoint, ConnectionSecret)> {
    let password = env::var("JAIBA_TEST_SQLSERVER_PASSWORD").ok()?;
    let host = env::var("JAIBA_TEST_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("JAIBA_TEST_SQLSERVER_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(11_433);
    let database =
        env::var("JAIBA_TEST_SQLSERVER_DATABASE").unwrap_or_else(|_| "master".to_owned());
    let username = env::var("JAIBA_TEST_SQLSERVER_USER").unwrap_or_else(|_| "sa".to_owned());
    Some((
        ConnectionEndpoint {
            host,
            port,
            database: Some(database),
            ssl: false,
            pool_min: 1,
            pool_max: 1,
            timeout_ms: 10_000,
            options: BTreeMap::new(),
        },
        ConnectionSecret {
            username,
            password,
            options: BTreeMap::new(),
        },
    ))
}

#[cfg(feature = "mongodb-driver")]
#[tokio::test]
async fn mongodb_real_connection_diagnostics_and_collection_metadata() {
    let Some((endpoint, secret)) = mongodb_test_configuration() else {
        eprintln!("skipping real MongoDB test: JAIBA_TEST_MONGODB_PASSWORD is not set");
        return;
    };

    let client = mongodb_client(&endpoint, &secret)
        .await
        .expect("connect to the MongoDB integration database");
    let database = client.database(mongodb_database(&endpoint).expect("database name"));
    let collection = database.collection::<Document>("jaiba_phase_1_probe");
    let _ = collection.drop().await;
    collection
        .insert_one(doc! {
            "_id": "probe-1",
            "amount": 125.50,
            "active": true,
            "note": "Jaiba integration test",
        })
        .await
        .expect("seed MongoDB integration document");

    let secrets = Arc::new(InMemorySecretStore::default());
    secrets.insert("test://mongodb", secret).await;
    let manager = connection_manager(secrets, None, None)
        .await
        .expect("build connection manager");
    let profile = manager
        .create(
            "mongodb_phase_1",
            ConnectionType::MongoDb,
            endpoint.clone(),
            "test://mongodb",
        )
        .await
        .expect("create MongoDB profile");

    let test_result = manager.test(&profile.id).await.expect("test connection");
    assert_eq!(test_result.availability, Availability::Available);
    // MongoDB 7.x / 8.x en entornos de prueba.
    assert!(
        test_result.version.as_deref().is_some_and(|value| {
            value
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .and_then(|major| major.parse::<u32>().ok())
                .is_some_and(|major| (7..=9).contains(&major))
        }),
        "unexpected MongoDB version: {:?}",
        test_result.version
    );

    let diagnostics = manager
        .diagnose(&profile.id)
        .await
        .expect("run MongoDB diagnostics");
    assert_eq!(diagnostics.len(), 3);
    assert!(
        diagnostics
            .iter()
            .all(|check| check.status == Availability::Available)
    );

    let objects = manager
        .list_objects(&profile.id, endpoint.database.as_deref())
        .await
        .expect("list MongoDB collections");
    let probe = objects
        .iter()
        .find(|object| {
            object.name == "jaiba_phase_1_probe" && object.kind == DatabaseObjectKind::Collection
        })
        .expect("probe collection appears in metadata");
    let description = manager
        .describe_object(&profile.id, probe)
        .await
        .expect("describe MongoDB collection");
    assert_eq!(description.object.kind, DatabaseObjectKind::Collection);
    assert!(
        description
            .columns
            .iter()
            .any(|column| column.name == "_id" && column.data_type == "string")
    );
    assert!(
        description
            .columns
            .iter()
            .any(|column| column.name == "active" && column.data_type == "boolean")
    );

    collection
        .drop()
        .await
        .expect("clean MongoDB integration collection");
}

/// Prueba opt-in contra MySQL real. Se omite cuando no se define
/// `JAIBA_TEST_MYSQL_PASSWORD`, por lo que la suite local y CI no necesitan
/// una base externa.
#[tokio::test]
async fn mysql_real_connection_metadata_and_query_compilation() {
    let Some((endpoint, secret)) = mysql_test_configuration() else {
        eprintln!("skipping real MySQL test: JAIBA_TEST_MYSQL_PASSWORD is not set");
        return;
    };

    let pool = mysql_pool(&endpoint, &secret)
        .await
        .expect("connect to the MySQL integration database");
    sqlx::query("DROP TABLE IF EXISTS jaiba_phase_9_3_probe")
        .execute(&pool)
        .await
        .expect("remove stale integration table");
    sqlx::query(
        "CREATE TABLE jaiba_phase_9_3_probe (\
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
            external_id VARCHAR(64) NOT NULL UNIQUE,\
            amount DECIMAL(12,2) NOT NULL,\
            active BOOLEAN NOT NULL DEFAULT TRUE,\
            note VARCHAR(255) NULL,\
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
            INDEX idx_jaiba_probe_active (active)\
        )",
    )
    .execute(&pool)
    .await
    .expect("create integration table");
    sqlx::query(
        "INSERT INTO jaiba_phase_9_3_probe (external_id, amount, active, note) \
         VALUES ('probe-1', 125.50, TRUE, 'Jaiba integration test')",
    )
    .execute(&pool)
    .await
    .expect("seed integration row");
    pool.close().await;

    let secrets = Arc::new(InMemorySecretStore::default());
    secrets.insert("test://mysql", secret).await;
    let manager = connection_manager(secrets, None, None)
        .await
        .expect("build connection manager");
    let profile = manager
        .create(
            "mysql_phase_9_3",
            ConnectionType::MySql,
            endpoint.clone(),
            "test://mysql",
        )
        .await
        .expect("create MySQL profile");

    let test_result = manager.test(&profile.id).await.expect("test connection");
    assert_eq!(test_result.availability, Availability::Available);
    assert!(
        test_result
            .version
            .as_deref()
            .is_some_and(|version| version.starts_with("8."))
    );

    let diagnostics = manager
        .diagnose(&profile.id)
        .await
        .expect("run diagnostics");
    assert!(!diagnostics.is_empty());
    assert!(
        diagnostics
            .iter()
            .all(|check| check.status == Availability::Available)
    );

    let objects = manager
        .list_objects(&profile.id, endpoint.database.as_deref())
        .await
        .expect("list database objects");
    let table = objects
        .iter()
        .find(|object| {
            object.name == "jaiba_phase_9_3_probe" && object.kind == DatabaseObjectKind::Table
        })
        .expect("integration table appears in metadata")
        .clone();

    let description = manager
        .describe_object(&profile.id, &table)
        .await
        .expect("describe integration table");
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "id",
            "external_id",
            "amount",
            "active",
            "note",
            "created_at"
        ]
    );
    assert!(
        description
            .keys
            .iter()
            .any(|key| key.kind == "PRIMARY KEY" && key.columns == ["id"])
    );
    assert!(
        description
            .indexes
            .iter()
            .any(|index| index.name == "idx_jaiba_probe_active")
    );

    let compiled = manager
        .compile_query(
            &profile.id,
            &QuerySpec {
                source: QuerySource {
                    schema: endpoint.database.clone(),
                    table: table.name.clone(),
                },
                columns: vec!["id".to_owned(), "external_id".to_owned()],
                joins: vec![],
                filters: vec![QueryFilter {
                    field: "active".to_owned(),
                    operator: FilterOperator::Eq,
                    value: Value::Bool(true),
                }],
                group_by: vec![],
                order_by: vec![QueryOrder {
                    field: "id".to_owned(),
                    direction: SortDirection::Asc,
                }],
                limit: Some(10),
            },
        )
        .await
        .expect("compile MySQL query");
    assert_eq!(
        compiled.statement,
        "SELECT `id`, `external_id` FROM `dma_test`.`jaiba_phase_9_3_probe` \
         WHERE `active` = ? ORDER BY `id` ASC LIMIT 10"
    );
    assert_eq!(compiled.parameters, vec![Value::Bool(true)]);
    assert_eq!(compiled.processor_type.as_deref(), Some("query_mysql"));
    assert_eq!(
        compiled.execution_statement.as_deref(),
        Some(compiled.statement.as_str())
    );

    let pool = mysql_pool(
        &endpoint,
        &ConnectionSecret {
            username: env::var("JAIBA_TEST_MYSQL_USER").unwrap_or_else(|_| "dma_test".to_owned()),
            password: env::var("JAIBA_TEST_MYSQL_PASSWORD")
                .expect("password remains available during the test"),
            options: BTreeMap::new(),
        },
    )
    .await
    .expect("reconnect for cleanup");
    sqlx::query("DROP TABLE jaiba_phase_9_3_probe")
        .execute(&pool)
        .await
        .expect("clean integration table");
    pool.close().await;
}

/// Prueba opt-in de extremo a extremo contra PostgreSQL real: Connection
/// Manager, compilación SQL y ejecución de `query_postgres`.
#[tokio::test]
async fn postgres_real_connection_query_builder_and_flow_execution() {
    let Some((endpoint, secret)) = postgres_test_configuration() else {
        eprintln!("skipping real PostgreSQL test: JAIBA_TEST_POSTGRES_PASSWORD is not set");
        return;
    };
    if env::var("JAIBA_TEST_POSTGRES_URL").is_err() {
        eprintln!("skipping real PostgreSQL test: JAIBA_TEST_POSTGRES_URL is not set");
        return;
    }

    let pool = postgres_pool(&endpoint, &secret)
        .await
        .expect("connect to the PostgreSQL integration database");
    sqlx::query("DROP TABLE IF EXISTS public.jaiba_phase_9_3_probe")
        .execute(&pool)
        .await
        .expect("remove stale integration table");
    sqlx::query(
        "CREATE TABLE public.jaiba_phase_9_3_probe (\
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\
            external_id VARCHAR(64) NOT NULL UNIQUE,\
            amount NUMERIC(12,2) NOT NULL,\
            active BOOLEAN NOT NULL DEFAULT TRUE,\
            note VARCHAR(255),\
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP\
        )",
    )
    .execute(&pool)
    .await
    .expect("create PostgreSQL integration table");
    sqlx::query(
        "CREATE INDEX idx_jaiba_probe_active \
         ON public.jaiba_phase_9_3_probe (active)",
    )
    .execute(&pool)
    .await
    .expect("create PostgreSQL integration index");
    sqlx::query(
        "INSERT INTO public.jaiba_phase_9_3_probe \
         (external_id, amount, active, note) VALUES \
         ('probe-1', 125.50, TRUE, 'visible'), \
         ('probe-2', 80.00, FALSE, 'filtered')",
    )
    .execute(&pool)
    .await
    .expect("seed PostgreSQL integration rows");
    pool.close().await;

    let secrets = Arc::new(InMemorySecretStore::default());
    secrets.insert("test://postgres", secret.clone()).await;
    let manager = connection_manager(secrets, None, None)
        .await
        .expect("build connection manager");
    let profile = manager
        .create(
            "postgres_phase_9_3",
            ConnectionType::Postgres,
            endpoint.clone(),
            "test://postgres",
        )
        .await
        .expect("create PostgreSQL profile");

    let test_result = manager.test(&profile.id).await.expect("test connection");
    assert_eq!(test_result.availability, Availability::Available);
    assert!(
        test_result
            .version
            .as_deref()
            .is_some_and(|version| version.contains("PostgreSQL 16"))
    );
    let diagnostics = manager
        .diagnose(&profile.id)
        .await
        .expect("run PostgreSQL diagnostics");
    assert_eq!(diagnostics.len(), 3);
    assert!(
        diagnostics
            .iter()
            .all(|check| check.status == Availability::Available)
    );

    let objects = manager
        .list_objects(&profile.id, Some("public"))
        .await
        .expect("list PostgreSQL objects");
    let table = objects
        .iter()
        .find(|object| {
            object.name == "jaiba_phase_9_3_probe" && object.kind == DatabaseObjectKind::Table
        })
        .expect("integration table appears in PostgreSQL metadata")
        .clone();
    let description = manager
        .describe_object(&profile.id, &table)
        .await
        .expect("describe PostgreSQL integration table");
    assert!(
        description
            .keys
            .iter()
            .any(|key| key.kind == "PRIMARY KEY" && key.columns == ["id"])
    );
    assert!(
        description
            .indexes
            .iter()
            .any(|index| index.name == "idx_jaiba_probe_active")
    );

    let compiled = manager
        .compile_query(
            &profile.id,
            &QuerySpec {
                source: QuerySource {
                    schema: Some("public".to_owned()),
                    table: table.name,
                },
                columns: vec![
                    "id".to_owned(),
                    "external_id".to_owned(),
                    "active".to_owned(),
                ],
                joins: vec![],
                filters: vec![QueryFilter {
                    field: "active".to_owned(),
                    operator: FilterOperator::Eq,
                    value: Value::Bool(true),
                }],
                group_by: vec![],
                order_by: vec![QueryOrder {
                    field: "id".to_owned(),
                    direction: SortDirection::Asc,
                }],
                limit: Some(10),
            },
        )
        .await
        .expect("compile PostgreSQL query");
    assert_eq!(compiled.parameters, vec![Value::Bool(true)]);

    let output = format!("/tmp/jaiba-postgres-phase-9-3-{}.json", Uuid::new_v4());
    let wrapped_query = format!(
        "SELECT to_jsonb(t) AS record FROM ({}) AS t",
        compiled.statement
    );
    let flow_yaml = format!(
        r#"
id: postgres-phase-9-3
database_connections:
  integration:
    type: postgres
    url_env: JAIBA_TEST_POSTGRES_URL
    max_connections: 2
engine:
  repository:
    enabled: false
processors:
  - id: read
    type: query_postgres
    config:
      connection: integration
      query: {query}
      parameters: [true]
      batch_size: 100
  - id: encode
    type: encode_json
    config:
      pretty: false
  - id: write
    type: write_file
    config:
      path: {output}
connections:
  - from: read
    relationship: success
    to: encode
  - from: encode
    relationship: success
    to: write
"#,
        query = serde_json::to_string(&wrapped_query).expect("quote query for YAML"),
    );
    let config: jaiba_core::config::FlowConfig =
        serde_yaml::from_str(&flow_yaml).expect("parse integration flow");
    let summary = jaiba_runtime::engine::FlowEngine::new(config)
        .expect("build integration flow")
        .run()
        .await
        .expect("run query_postgres integration flow");
    assert_eq!(summary.failed, 0);
    let records: Value =
        serde_json::from_slice(&fs::read(&output).expect("read query_postgres output"))
            .expect("output is valid JSON");
    assert_eq!(records.as_array().map(Vec::len), Some(1));
    assert_eq!(records[0]["external_id"], "probe-1");
    assert_eq!(records[0]["active"], true);

    fs::remove_file(&output).expect("remove integration output");
    let pool = postgres_pool(&endpoint, &secret)
        .await
        .expect("reconnect to PostgreSQL for cleanup");
    sqlx::query("DROP TABLE public.jaiba_phase_9_3_probe")
        .execute(&pool)
        .await
        .expect("clean PostgreSQL integration table");
    pool.close().await;
}

/// Prueba opt-in contra Oracle Free real. Requiere `oracle-driver`, las
/// bibliotecas de Oracle Client y `JAIBA_TEST_ORACLE_PASSWORD`.
#[cfg(feature = "oracle-driver")]
#[tokio::test]
async fn oracle_real_connection_diagnostics_and_metadata() {
    let Some((endpoint, secret)) = oracle_test_configuration() else {
        eprintln!("skipping real Oracle test: JAIBA_TEST_ORACLE_PASSWORD is not set");
        return;
    };

    let setup_endpoint = endpoint.clone();
    let setup_secret = secret.clone();
    tokio::task::spawn_blocking(move || {
        let connection = oracle_connect(&setup_endpoint, &setup_secret)
            .expect("connect to the Oracle integration database");
        connection
            .execute(
                "BEGIN \
                    EXECUTE IMMEDIATE 'DROP TABLE JAIBA_PHASE_9_3_PROBE PURGE'; \
                 EXCEPTION WHEN OTHERS THEN \
                    IF SQLCODE != -942 THEN RAISE; END IF; \
                 END;",
                &[],
            )
            .expect("remove stale integration table");
        connection
            .execute(
                "CREATE TABLE JAIBA_PHASE_9_3_PROBE (\
                    ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\
                    EXTERNAL_ID VARCHAR2(64) NOT NULL UNIQUE,\
                    AMOUNT NUMBER(12,2) NOT NULL,\
                    ACTIVE NUMBER(1) DEFAULT 1 NOT NULL,\
                    NOTE VARCHAR2(255),\
                    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL\
                )",
                &[],
            )
            .expect("create integration table");
        connection
            .execute(
                "CREATE INDEX IDX_JAIBA_PROBE_ACTIVE \
                 ON JAIBA_PHASE_9_3_PROBE (ACTIVE)",
                &[],
            )
            .expect("create integration index");
        connection
            .execute(
                "INSERT INTO JAIBA_PHASE_9_3_PROBE \
                 (EXTERNAL_ID, AMOUNT, ACTIVE, NOTE) \
                 VALUES ('probe-1', 125.50, 1, 'Jaiba integration test')",
                &[],
            )
            .expect("seed integration row");
        connection.commit().expect("commit integration fixture");
    })
    .await
    .expect("finish Oracle setup task");

    let secrets = Arc::new(InMemorySecretStore::default());
    secrets.insert("test://oracle", secret.clone()).await;
    let manager = connection_manager(secrets, None, None)
        .await
        .expect("build connection manager");
    let profile = manager
        .create(
            "oracle_phase_9_3",
            ConnectionType::Oracle,
            endpoint.clone(),
            "test://oracle",
        )
        .await
        .expect("create Oracle profile");

    let test_result = manager.test(&profile.id).await.expect("test connection");
    assert_eq!(test_result.availability, Availability::Available);
    assert!(
        test_result
            .version
            .as_deref()
            .is_some_and(|version| version.contains("Oracle"))
    );

    let diagnostics = manager
        .diagnose(&profile.id)
        .await
        .expect("run Oracle diagnostics");
    assert_eq!(diagnostics.len(), 3);
    assert!(
        diagnostics
            .iter()
            .all(|check| check.status == Availability::Available)
    );

    let owner = secret.username.to_uppercase();
    let objects = manager
        .list_objects(&profile.id, Some(&owner))
        .await
        .expect("list Oracle objects");
    let table = objects
        .iter()
        .find(|object| {
            object.name == "JAIBA_PHASE_9_3_PROBE" && object.kind == DatabaseObjectKind::Table
        })
        .expect("integration table appears in Oracle metadata")
        .clone();

    let description = manager
        .describe_object(&profile.id, &table)
        .await
        .expect("describe Oracle integration table");
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ID",
            "EXTERNAL_ID",
            "AMOUNT",
            "ACTIVE",
            "NOTE",
            "CREATED_AT"
        ]
    );

    let cleanup_endpoint = endpoint;
    tokio::task::spawn_blocking(move || {
        let connection =
            oracle_connect(&cleanup_endpoint, &secret).expect("reconnect to Oracle for cleanup");
        connection
            .execute("DROP TABLE JAIBA_PHASE_9_3_PROBE PURGE", &[])
            .expect("clean Oracle integration table");
    })
    .await
    .expect("finish Oracle cleanup task");
}

/// Prueba opt-in contra SQL Server real. Requiere `sqlserver-driver` y
/// `JAIBA_TEST_SQLSERVER_PASSWORD`.
#[cfg(feature = "sqlserver-driver")]
#[tokio::test]
async fn sqlserver_real_connection_diagnostics_and_metadata() {
    let Some((endpoint, secret)) = sqlserver_test_configuration() else {
        eprintln!("skipping real SQL Server test: JAIBA_TEST_SQLSERVER_PASSWORD is not set");
        return;
    };

    let mut client = sqlserver_connect(&endpoint, &secret)
        .await
        .expect("connect to the SQL Server integration database");
    client
        .simple_query(
            "IF OBJECT_ID('dbo.JAIBA_PHASE_9_3_PROBE', 'U') IS NOT NULL \
                DROP TABLE dbo.JAIBA_PHASE_9_3_PROBE; \
             CREATE TABLE dbo.JAIBA_PHASE_9_3_PROBE (\
                ID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,\
                EXTERNAL_ID nvarchar(64) NOT NULL UNIQUE,\
                AMOUNT decimal(12,2) NOT NULL,\
                ACTIVE bit NOT NULL CONSTRAINT DF_JAIBA_PROBE_ACTIVE DEFAULT 1,\
                NOTE nvarchar(255) NULL,\
                CREATED_AT datetime2 NOT NULL \
                    CONSTRAINT DF_JAIBA_PROBE_CREATED DEFAULT SYSUTCDATETIME()\
             ); \
             CREATE INDEX IDX_JAIBA_PROBE_ACTIVE \
                ON dbo.JAIBA_PHASE_9_3_PROBE (ACTIVE); \
             INSERT INTO dbo.JAIBA_PHASE_9_3_PROBE \
                (EXTERNAL_ID, AMOUNT, ACTIVE, NOTE) \
                VALUES ('probe-1', 125.50, 1, 'Jaiba integration test');",
        )
        .await
        .expect("prepare SQL Server fixture")
        .into_results()
        .await
        .expect("execute SQL Server fixture");
    drop(client);

    let secrets = Arc::new(InMemorySecretStore::default());
    secrets.insert("test://sqlserver", secret.clone()).await;
    let manager = connection_manager(secrets, None, None)
        .await
        .expect("build connection manager");
    let profile = manager
        .create(
            "sqlserver_phase_9_3",
            ConnectionType::SqlServer,
            endpoint.clone(),
            "test://sqlserver",
        )
        .await
        .expect("create SQL Server profile");

    let test_result = manager.test(&profile.id).await.expect("test connection");
    assert_eq!(test_result.availability, Availability::Available);
    assert!(
        test_result
            .version
            .as_deref()
            .is_some_and(|version| version.starts_with("16."))
    );

    let diagnostics = manager
        .diagnose(&profile.id)
        .await
        .expect("run SQL Server diagnostics");
    assert_eq!(diagnostics.len(), 3);
    assert!(
        diagnostics
            .iter()
            .all(|check| check.status == Availability::Available)
    );

    let objects = manager
        .list_objects(&profile.id, Some("dbo"))
        .await
        .expect("list SQL Server objects");
    let table = objects
        .iter()
        .find(|object| {
            object.name == "JAIBA_PHASE_9_3_PROBE" && object.kind == DatabaseObjectKind::Table
        })
        .expect("integration table appears in SQL Server metadata")
        .clone();

    let description = manager
        .describe_object(&profile.id, &table)
        .await
        .expect("describe SQL Server integration table");
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ID",
            "EXTERNAL_ID",
            "AMOUNT",
            "ACTIVE",
            "NOTE",
            "CREATED_AT"
        ]
    );

    let mut client = sqlserver_connect(&endpoint, &secret)
        .await
        .expect("reconnect to SQL Server for cleanup");
    client
        .simple_query("DROP TABLE dbo.JAIBA_PHASE_9_3_PROBE")
        .await
        .expect("prepare SQL Server cleanup")
        .into_results()
        .await
        .expect("clean SQL Server integration table");
}

/// Valida que un perfil creado solo con URL MongoDB pueda probarse.
#[cfg(feature = "mongodb-driver")]
#[tokio::test]
async fn mongodb_real_connection_from_url() {
    let Some(password) = env::var("JAIBA_TEST_MONGODB_PASSWORD").ok() else {
        eprintln!("skipping real MongoDB URL test: JAIBA_TEST_MONGODB_PASSWORD is not set");
        return;
    };
    let host = env::var("JAIBA_TEST_MONGODB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = env::var("JAIBA_TEST_MONGODB_PORT").unwrap_or_else(|_| "27018".to_owned());
    let database =
        env::var("JAIBA_TEST_MONGODB_DATABASE").unwrap_or_else(|_| "dma_test".to_owned());
    let username = env::var("JAIBA_TEST_MONGODB_USER").unwrap_or_else(|_| "dma_test".to_owned());
    let raw = format!("mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin");

    let input = ConnectionInput {
        name: "mongo_from_url".to_owned(),
        connection_type: ConnectionType::MongoDb,
        host: String::new(),
        port: 0,
        database: None,
        username: String::new(),
        password: None,
        url: Some(raw),
        ssl: false,
        pool_min: 1,
        pool_max: 2,
        timeout_ms: 5_000,
    };
    validate_input(&input, true).expect("URL MongoDB válida");
    let (endpoint, secret) = materialize_connection(&input, None).expect("materializar desde URL");
    assert_eq!(endpoint.host, host);
    assert!(secret.options.contains_key("connection_url"));

    let client = mongodb_client(&endpoint, &secret)
        .await
        .expect("conectar con URL materializada");
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .expect("ping MongoDB vía URL");
}
