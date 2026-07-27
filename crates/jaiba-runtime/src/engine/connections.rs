use std::{collections::HashMap, fmt, sync::Arc};

#[cfg(feature = "kafka-driver")]
use rdkafka::{ClientConfig, producer::FutureProducer};
use sqlx::{MySqlPool, PgPool, mysql::MySqlPoolOptions, postgres::PgPoolOptions};

#[cfg(feature = "oracle-driver")]
use crate::connectors::OracleWriter;
#[cfg(feature = "sqlserver-driver")]
use crate::connectors::SqlServerWriter;
use crate::{
    config::{DatabaseConnectionConfig, KafkaConnectionConfig},
    connectors::{DatabaseKind, DatabaseWriter, MySqlWriter, PostgresWriter},
    error::FlowError,
};

#[derive(Clone, Default)]
pub struct ConnectionManager {
    postgres: Arc<HashMap<String, PgPool>>,
    mysql: Arc<HashMap<String, MySqlPool>>,
    writers: Arc<HashMap<String, Arc<dyn DatabaseWriter>>>,
    #[cfg(feature = "kafka-driver")]
    kafka: Arc<HashMap<String, FutureProducer>>,
}

impl ConnectionManager {
    pub async fn build(
        definitions: &HashMap<String, DatabaseConnectionConfig>,
        kafka_definitions: &HashMap<String, KafkaConnectionConfig>,
    ) -> Result<Self, FlowError> {
        let mut postgres = HashMap::new();
        let mut mysql = HashMap::new();
        let mut writers: HashMap<String, Arc<dyn DatabaseWriter>> = HashMap::new();
        #[cfg(feature = "kafka-driver")]
        let mut kafka = HashMap::new();

        for (name, definition) in definitions {
            let url = std::env::var(&definition.url_env).map_err(|_| {
                FlowError::Configuration(format!(
                    "environment variable '{}' is required by connection '{name}'",
                    definition.url_env
                ))
            })?;
            match definition.connection_type.as_str() {
                "postgres" => {
                    let pool = PgPoolOptions::new()
                        .max_connections(definition.max_connections)
                        .connect(&url)
                        .await?;
                    writers.insert(name.clone(), Arc::new(PostgresWriter::new(pool.clone())));
                    postgres.insert(name.clone(), pool);
                }
                "mysql" | "mariadb" => {
                    let pool = MySqlPoolOptions::new()
                        .max_connections(definition.max_connections)
                        .connect(&url)
                        .await?;
                    let kind = if definition.connection_type == "mysql" {
                        DatabaseKind::MySql
                    } else {
                        DatabaseKind::MariaDb
                    };
                    writers.insert(
                        name.clone(),
                        Arc::new(MySqlWriter::new(pool.clone(), kind)?),
                    );
                    mysql.insert(name.clone(), pool);
                }
                "oracle" => {
                    #[cfg(feature = "oracle-driver")]
                    {
                        writers.insert(name.clone(), Arc::new(OracleWriter::from_url(&url)?));
                    }
                    #[cfg(not(feature = "oracle-driver"))]
                    {
                        return Err(FlowError::Configuration(
                            "Oracle connections require the 'oracle-driver' feature".to_owned(),
                        ));
                    }
                }
                "sqlserver" | "mssql" => {
                    #[cfg(feature = "sqlserver-driver")]
                    {
                        writers.insert(name.clone(), Arc::new(SqlServerWriter::from_url(&url)?));
                    }
                    #[cfg(not(feature = "sqlserver-driver"))]
                    {
                        return Err(FlowError::Configuration(
                            "SQL Server connections require the 'sqlserver-driver' feature"
                                .to_owned(),
                        ));
                    }
                }
                unsupported => {
                    return Err(FlowError::Configuration(format!(
                        "unsupported database connection type '{unsupported}'"
                    )));
                }
            }
        }

        #[cfg(feature = "kafka-driver")]
        for (name, definition) in kafka_definitions {
            if !definition
                .security_protocol
                .eq_ignore_ascii_case("PLAINTEXT")
                || definition.sasl_mechanism.is_some()
                || definition.sasl_username_env.is_some()
                || definition.sasl_password_env.is_some()
            {
                return Err(FlowError::Configuration(
                    "Kafka phase 4.3.1 currently supports only PLAINTEXT; TLS/SASL is planned for the hardened connector"
                        .to_owned(),
                ));
            }
            let brokers = std::env::var(&definition.brokers_env).map_err(|_| {
                FlowError::Configuration(format!(
                    "environment variable '{}' is required by Kafka connection '{name}'",
                    definition.brokers_env
                ))
            })?;
            let mut client = ClientConfig::new();
            client
                .set("bootstrap.servers", brokers)
                .set("client.id", &definition.client_id)
                .set("security.protocol", &definition.security_protocol)
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set(
                    "message.timeout.ms",
                    definition.message_timeout_ms.to_string(),
                );
            let producer = client
                .create()
                .map_err(|error| FlowError::Configuration(error.to_string()))?;
            kafka.insert(name.clone(), producer);
        }
        #[cfg(not(feature = "kafka-driver"))]
        if !kafka_definitions.is_empty() {
            return Err(FlowError::Configuration(
                "Kafka connections require the 'kafka-driver' feature".to_owned(),
            ));
        }

        Ok(Self {
            postgres: Arc::new(postgres),
            mysql: Arc::new(mysql),
            writers: Arc::new(writers),
            #[cfg(feature = "kafka-driver")]
            kafka: Arc::new(kafka),
        })
    }

    pub fn postgres(&self, name: &str) -> Result<&PgPool, FlowError> {
        self.postgres.get(name).ok_or_else(|| {
            FlowError::Configuration(format!("PostgreSQL connection '{name}' does not exist"))
        })
    }

    /// Returns the database-independent writer registered for a connection.
    pub fn writer(&self, name: &str) -> Result<Arc<dyn DatabaseWriter>, FlowError> {
        self.writers.get(name).cloned().ok_or_else(|| {
            FlowError::Configuration(format!(
                "database writer connection '{name}' does not exist"
            ))
        })
    }

    #[cfg(feature = "kafka-driver")]
    pub fn kafka(&self, name: &str) -> Result<&FutureProducer, FlowError> {
        self.kafka.get(name).ok_or_else(|| {
            FlowError::Configuration(format!("Kafka connection '{name}' does not exist"))
        })
    }
}

impl fmt::Debug for ConnectionManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("ConnectionManager");
        debug
            .field("postgres", &self.postgres.keys().collect::<Vec<_>>())
            .field("mysql", &self.mysql.keys().collect::<Vec<_>>())
            .field("writers", &self.writers.keys().collect::<Vec<_>>());
        #[cfg(feature = "kafka-driver")]
        debug.field("kafka", &self.kafka.keys().collect::<Vec<_>>());
        debug.finish()
    }
}
