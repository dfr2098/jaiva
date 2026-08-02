use std::{collections::HashMap, fmt, sync::Arc};

#[cfg(feature = "mongodb-driver")]
use mongodb::Client as MongoClient;
#[cfg(feature = "kafka-driver")]
use rdkafka::{ClientConfig, consumer::StreamConsumer, producer::FutureProducer};
use sqlx::{MySqlPool, PgPool, mysql::MySqlPoolOptions, postgres::PgPoolOptions};

#[cfg(feature = "oracle-driver")]
use crate::connectors::OracleWriter;
#[cfg(feature = "sqlserver-driver")]
use crate::connectors::SqlServerWriter;
use crate::{
    config::{DatabaseConnectionConfig, KafkaConnectionConfig},
    connectors::{DatabaseKind, DatabaseWriter, MySqlWriter, PostgresWriter},
    engine::resolver::ConnectionResolver,
    error::FlowError,
};

#[derive(Clone, Default)]
pub struct ConnectionManager {
    postgres: Arc<HashMap<String, PgPool>>,
    mysql: Arc<HashMap<String, MySqlPool>>,
    #[cfg(feature = "mongodb-driver")]
    mongodb: Arc<HashMap<String, MongoClient>>,
    #[cfg(feature = "oracle-driver")]
    oracle: Arc<HashMap<String, OracleWriter>>,
    writers: Arc<HashMap<String, Arc<dyn DatabaseWriter>>>,
    #[cfg(feature = "kafka-driver")]
    kafka: Arc<HashMap<String, KafkaEndpoint>>,
}

/// Endpoint Kafka compartido: productor idempotente + fábrica de consumidores.
///
/// El productor vive todo el flujo; cada `consume_kafka` crea un consumidor
/// efímero con auto-commit desactivado.
#[cfg(feature = "kafka-driver")]
#[derive(Clone)]
pub struct KafkaEndpoint {
    brokers: String,
    client_id: String,
    security_protocol: String,
    producer: FutureProducer,
}

#[cfg(feature = "kafka-driver")]
impl KafkaEndpoint {
    pub fn producer(&self) -> &FutureProducer {
        &self.producer
    }

    /// Crea un `StreamConsumer` con offsets manuales (`enable.auto.commit=false`).
    ///
    /// `broker.address.family=v4` evita que librdkafka resuelva `localhost` a
    /// `::1` cuando el broker solo escucha en IPv4.
    pub fn create_consumer(
        &self,
        group_id: &str,
        auto_offset_reset: &str,
    ) -> Result<StreamConsumer, FlowError> {
        if group_id.trim().is_empty() {
            return Err(FlowError::Configuration(
                "Kafka consumer group_id cannot be empty".to_owned(),
            ));
        }
        let reset = match auto_offset_reset {
            "earliest" | "latest" => auto_offset_reset,
            other => {
                return Err(FlowError::Configuration(format!(
                    "unsupported auto_offset_reset '{other}' (use earliest|latest)"
                )));
            }
        };
        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &self.brokers)
            .set(
                "client.id",
                format!("{}-consumer-{}", self.client_id, group_id),
            )
            .set("group.id", group_id)
            .set("security.protocol", &self.security_protocol)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", reset)
            .set("enable.partition.eof", "false")
            .set("broker.address.family", "v4");
        client
            .create()
            .map_err(|error| FlowError::Configuration(error.to_string()))
    }
}

impl ConnectionManager {
    pub async fn build(
        definitions: &HashMap<String, DatabaseConnectionConfig>,
        kafka_definitions: &HashMap<String, KafkaConnectionConfig>,
        aliases: &[String],
        resolver: Option<&Arc<dyn ConnectionResolver>>,
    ) -> Result<Self, FlowError> {
        let mut postgres = HashMap::new();
        let mut mysql = HashMap::new();
        #[cfg(feature = "mongodb-driver")]
        let mut mongodb = HashMap::new();
        #[cfg(feature = "oracle-driver")]
        let mut oracle = HashMap::new();
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
            insert_database(
                name,
                &definition.connection_type,
                &url,
                definition.max_connections,
                None,
                &mut postgres,
                &mut mysql,
                #[cfg(feature = "mongodb-driver")]
                &mut mongodb,
                #[cfg(feature = "oracle-driver")]
                &mut oracle,
                &mut writers,
            )
            .await?;
        }

        // Resolución por alias: el YAML solo lleva `connection: <alias>` y el
        // Connection Manager provee host/puerto/base/usuario/contraseña/SSL/pool.
        if !aliases.is_empty() {
            let resolver = resolver.ok_or_else(|| {
                FlowError::Configuration(format!(
                    "el flujo referencia conexiones por alias ({}) pero no hay un Connection Manager configurado; define JAIBA_MASTER_KEY o declara las conexiones en 'database_connections'",
                    aliases.join(", ")
                ))
            })?;
            for alias in aliases {
                let resolved = resolver.resolve(alias).await?;
                insert_database(
                    alias,
                    &resolved.connection_type,
                    &resolved.url,
                    resolved.max_connections,
                    Some(std::time::Duration::from_millis(resolved.timeout_ms)),
                    &mut postgres,
                    &mut mysql,
                    #[cfg(feature = "mongodb-driver")]
                    &mut mongodb,
                    #[cfg(feature = "oracle-driver")]
                    &mut oracle,
                    &mut writers,
                )
                .await?;
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
                    "Kafka currently supports only PLAINTEXT; TLS/SASL will follow in a hardened connector"
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
                .set("bootstrap.servers", &brokers)
                .set("client.id", &definition.client_id)
                .set("security.protocol", &definition.security_protocol)
                // IPv4: evita localhost → ::1 cuando el broker solo escucha en 127.0.0.1.
                .set("broker.address.family", "v4")
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set(
                    "message.timeout.ms",
                    definition.message_timeout_ms.to_string(),
                );
            let producer = client
                .create()
                .map_err(|error| FlowError::Configuration(error.to_string()))?;
            kafka.insert(
                name.clone(),
                KafkaEndpoint {
                    brokers,
                    client_id: definition.client_id.clone(),
                    security_protocol: definition.security_protocol.clone(),
                    producer,
                },
            );
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
            #[cfg(feature = "mongodb-driver")]
            mongodb: Arc::new(mongodb),
            #[cfg(feature = "oracle-driver")]
            oracle: Arc::new(oracle),
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

    #[cfg(feature = "mongodb-driver")]
    pub fn mongodb(&self, name: &str) -> Result<&MongoClient, FlowError> {
        self.mongodb.get(name).ok_or_else(|| {
            FlowError::Configuration(format!("MongoDB connection '{name}' does not exist"))
        })
    }

    #[cfg(feature = "oracle-driver")]
    pub fn oracle(&self, name: &str) -> Result<&OracleWriter, FlowError> {
        self.oracle.get(name).ok_or_else(|| {
            FlowError::Configuration(format!("Oracle connection '{name}' does not exist"))
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
        Ok(self.kafka_endpoint(name)?.producer())
    }

    #[cfg(feature = "kafka-driver")]
    pub fn kafka_endpoint(&self, name: &str) -> Result<&KafkaEndpoint, FlowError> {
        self.kafka.get(name).ok_or_else(|| {
            FlowError::Configuration(format!("Kafka connection '{name}' does not exist"))
        })
    }
}

/// Construye un pool o writer para una conexión de base de datos y lo registra
/// en los mapas correspondientes. Compartido por las conexiones declaradas con
/// `url_env` y por las resueltas por alias desde el Connection Manager.
#[allow(clippy::too_many_arguments)]
async fn insert_database(
    name: &str,
    connection_type: &str,
    url: &str,
    max_connections: u32,
    acquire_timeout: Option<std::time::Duration>,
    postgres: &mut HashMap<String, PgPool>,
    mysql: &mut HashMap<String, MySqlPool>,
    #[cfg(feature = "mongodb-driver")] mongodb: &mut HashMap<String, MongoClient>,
    #[cfg(feature = "oracle-driver")] oracle: &mut HashMap<String, OracleWriter>,
    writers: &mut HashMap<String, Arc<dyn DatabaseWriter>>,
) -> Result<(), FlowError> {
    match connection_type {
        "postgres" => {
            let mut options = PgPoolOptions::new().max_connections(max_connections);
            if let Some(timeout) = acquire_timeout {
                options = options.acquire_timeout(timeout);
            }
            let pool = options.connect(url).await?;
            writers.insert(name.to_owned(), Arc::new(PostgresWriter::new(pool.clone())));
            postgres.insert(name.to_owned(), pool);
        }
        "mysql" | "mariadb" => {
            let mut options = MySqlPoolOptions::new().max_connections(max_connections);
            if let Some(timeout) = acquire_timeout {
                options = options.acquire_timeout(timeout);
            }
            let pool = options.connect(url).await?;
            let kind = if connection_type == "mysql" {
                DatabaseKind::MySql
            } else {
                DatabaseKind::MariaDb
            };
            writers.insert(
                name.to_owned(),
                Arc::new(MySqlWriter::new(pool.clone(), kind)?),
            );
            mysql.insert(name.to_owned(), pool);
        }
        "mongodb" | "mongo" => {
            #[cfg(feature = "mongodb-driver")]
            {
                let client = MongoClient::with_uri_str(url)
                    .await
                    .map_err(|error| FlowError::DatabaseConnector(error.to_string()))?;
                mongodb.insert(name.to_owned(), client);
            }
            #[cfg(not(feature = "mongodb-driver"))]
            {
                let _ = url;
                return Err(FlowError::Configuration(
                    "MongoDB connections require the 'mongodb-driver' feature".to_owned(),
                ));
            }
        }
        "oracle" => {
            #[cfg(feature = "oracle-driver")]
            {
                let connection = OracleWriter::from_url(url)?;
                writers.insert(name.to_owned(), Arc::new(connection.clone()));
                oracle.insert(name.to_owned(), connection);
            }
            #[cfg(not(feature = "oracle-driver"))]
            {
                let _ = url;
                return Err(FlowError::Configuration(
                    "Oracle connections require the 'oracle-driver' feature".to_owned(),
                ));
            }
        }
        "sqlserver" | "mssql" => {
            #[cfg(feature = "sqlserver-driver")]
            {
                writers.insert(name.to_owned(), Arc::new(SqlServerWriter::from_url(url)?));
            }
            #[cfg(not(feature = "sqlserver-driver"))]
            {
                let _ = url;
                return Err(FlowError::Configuration(
                    "SQL Server connections require the 'sqlserver-driver' feature".to_owned(),
                ));
            }
        }
        unsupported => {
            return Err(FlowError::Configuration(format!(
                "unsupported database connection type '{unsupported}'"
            )));
        }
    }
    Ok(())
}

impl fmt::Debug for ConnectionManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("ConnectionManager");
        debug
            .field("postgres", &self.postgres.keys().collect::<Vec<_>>())
            .field("mysql", &self.mysql.keys().collect::<Vec<_>>())
            .field("writers", &self.writers.keys().collect::<Vec<_>>());
        #[cfg(feature = "oracle-driver")]
        debug.field("oracle", &self.oracle.keys().collect::<Vec<_>>());
        #[cfg(feature = "mongodb-driver")]
        debug.field("mongodb", &self.mongodb.keys().collect::<Vec<_>>());
        #[cfg(feature = "kafka-driver")]
        debug.field("kafka", &self.kafka.keys().collect::<Vec<_>>());
        debug.finish()
    }
}
