use std::{future::Future, time::Duration};

use mongodb::{
    Client, Database,
    bson::{Bson, Document, spec::BinarySubtype},
    error::{
        Error as MongoError, ErrorKind, RETRYABLE_ERROR, RETRYABLE_WRITE_ERROR,
        SYSTEM_OVERLOADED_ERROR, TRANSIENT_TRANSACTION_ERROR, WriteFailure,
    },
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    connectors::values::{base64_encode, float_value, number_value},
    error::FlowError,
};

/// Espera máxima por defecto de cada llamada a MongoDB (`find`, `getMore`,
/// escritura de un lote).
pub(crate) const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Server error codes that describe the session, the replica set or a time
/// limit rather than the data: the same request can succeed later. Includes
/// the driver's retryable read/write codes plus lock, interruption and
/// throttling codes.
const TRANSIENT_MONGODB_CODES: [i32; 27] = [
    6,     // HostUnreachable
    7,     // HostNotFound
    24,    // LockTimeout
    43,    // CursorNotFound
    46,    // LockBusy
    50,    // MaxTimeMSExpired
    64,    // WriteConcernFailed
    89,    // NetworkTimeout
    91,    // ShutdownInProgress
    112,   // WriteConflict
    133,   // FailedToSatisfyReadPreference
    134,   // ReadConcernMajorityNotAvailableYet
    175,   // QueryPlanKilled
    189,   // PrimarySteppedDown
    202,   // NetworkInterfaceExceededTimeLimit
    237,   // CursorKilled
    262,   // ExceededTimeLimit
    391,   // ReauthenticationRequired
    9001,  // SocketException
    10058, // LegacyNotPrimary
    10107, // NotWritablePrimary
    11600, // InterruptedAtShutdown
    11601, // Interrupted
    11602, // InterruptedDueToReplStateChange
    13435, // NotPrimaryNoSecondaryOk
    13436, // NotPrimaryOrSecondary
    16500, // RequestRateTooLarge (Azure Cosmos DB)
];

pub(crate) fn is_transient_code(code: i32) -> bool {
    TRANSIENT_MONGODB_CODES.contains(&code)
}

/// Clasifica un error del driver: sesión, red, selección de servidor y códigos
/// transitorios son reintentables (`DatabaseConnector`); datos rechazados
/// (clave duplicada, validación), credenciales y argumentos inválidos son
/// `DatabaseRejected` y van a `failure` sin reintentar.
pub(crate) fn mongodb_error(error: MongoError) -> FlowError {
    let message = error.to_string();
    let labelled_retryable = [
        RETRYABLE_WRITE_ERROR,
        TRANSIENT_TRANSACTION_ERROR,
        RETRYABLE_ERROR,
        SYSTEM_OVERLOADED_ERROR,
    ]
    .iter()
    .any(|label| error.contains_label(label));
    let retryable = labelled_retryable
        || match error.kind.as_ref() {
            ErrorKind::Command(command) => is_transient_code(command.code),
            ErrorKind::Write(WriteFailure::WriteError(write)) => is_transient_code(write.code),
            ErrorKind::Write(WriteFailure::WriteConcernError(_)) => true,
            ErrorKind::InsertMany(insert) => match &insert.write_errors {
                Some(errors) if !errors.is_empty() => {
                    errors.iter().all(|error| is_transient_code(error.code))
                }
                _ => true,
            },
            ErrorKind::InvalidArgument { .. }
            | ErrorKind::Authentication { .. }
            | ErrorKind::BsonDeserialization(_)
            | ErrorKind::BsonSerialization(_)
            | ErrorKind::IncompatibleServer { .. }
            | ErrorKind::InvalidTlsConfig { .. }
            | ErrorKind::SessionsNotSupported => false,
            _ => true,
        };
    if retryable {
        FlowError::DatabaseConnector(message)
    } else {
        FlowError::DatabaseRejected(message)
    }
}

/// Ejecuta una llamada al driver con límite de espera. Al vencer, el futuro se
/// suelta: el driver cierra la conexión y el servidor interrumpe la operación
/// de lectura en curso.
pub(crate) async fn with_call_timeout<T>(
    limit: Option<Duration>,
    work: impl Future<Output = Result<T, MongoError>>,
) -> Result<T, FlowError> {
    let result = match limit {
        Some(limit) => tokio::time::timeout(limit, work).await.map_err(|_| {
            FlowError::DatabaseConnector(format!(
                "MongoDB call exceeded the call timeout of {} ms",
                limit.as_millis()
            ))
        })?,
        None => work.await,
    };
    result.map_err(mongodb_error)
}

pub(super) fn default_call_timeout_ms() -> u64 {
    DEFAULT_CALL_TIMEOUT.as_millis() as u64
}

pub(super) fn call_timeout(milliseconds: u64) -> Option<Duration> {
    (milliseconds > 0).then(|| Duration::from_millis(milliseconds))
}

/// Formato JSON que emite `query_mongodb`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum JsonFormat {
    /// Extended JSON relajado: números legibles, tipos especiales envueltos
    /// (`$oid`, `$date`…). Un `Int64` pequeño vuelve como `Int32` al
    /// escribirse de nuevo en MongoDB.
    #[default]
    Relaxed,
    /// Extended JSON canónico: todo tipo envuelto (`$numberInt`,
    /// `$numberLong`…), copia exacta MongoDB → MongoDB.
    Canonical,
    /// JSON sin envolturas para destinos SQL/CSV: ObjectId en hex, fechas ISO
    /// 8601, Decimal128 exacto, binarios en base64. No conserva los tipos al
    /// volver a MongoDB.
    Plain,
}

/// Devuelve la base indicada en la URI del perfil. Mantener la base en la
/// conexión evita repetir credenciales o nombres de entorno en cada nodo.
pub(super) fn default_database(client: &Client) -> Result<Database, FlowError> {
    client.default_database().ok_or_else(|| {
        FlowError::Configuration(
            "MongoDB connection URI must include a default database".to_owned(),
        )
    })
}

/// Convierte JSON normal o Extended JSON (`$oid`, `$date`, etc.) a BSON sin
/// perder tipos especiales durante un flujo MongoDB → transformación → MongoDB.
pub(super) fn json_document(value: &Value, label: &str) -> Result<Document, FlowError> {
    match Bson::try_from(value.clone())
        .map_err(|error| FlowError::Configuration(format!("{label}: {error}")))?
    {
        Bson::Document(document) => Ok(document),
        _ => Err(FlowError::Configuration(format!(
            "{label} must be a JSON object"
        ))),
    }
}

/// Like [`json_document`] for an incoming record: a record that is not an
/// object or carries invalid Extended JSON is bad data, not configuration, so
/// it goes to `failure` without retries.
pub(super) fn record_document(value: &Value, label: &str) -> Result<Document, FlowError> {
    json_document(value, label).map_err(|error| match error {
        FlowError::Configuration(message) => FlowError::DatabaseRejected(message),
        other => other,
    })
}

/// Convierte BSON a JSON según el formato elegido (ver [`JsonFormat`]).
pub(super) fn document_json(document: Document, format: JsonFormat) -> Value {
    match format {
        JsonFormat::Relaxed => Bson::Document(document).into_relaxed_extjson(),
        JsonFormat::Canonical => Bson::Document(document).into_canonical_extjson(),
        JsonFormat::Plain => plain_json(Bson::Document(document)),
    }
}

fn plain_json(value: Bson) -> Value {
    match value {
        Bson::Document(document) => Value::Object(
            document
                .into_iter()
                .map(|(key, value)| (key, plain_json(value)))
                .collect(),
        ),
        Bson::Array(values) => Value::Array(values.into_iter().map(plain_json).collect()),
        Bson::Int32(value) => Value::from(value),
        Bson::Int64(value) => Value::from(value),
        Bson::Double(value) => float_value(value),
        Bson::Decimal128(value) => number_value(&value.to_string()),
        Bson::ObjectId(value) => Value::String(value.to_hex()),
        Bson::DateTime(value) => chrono::DateTime::from_timestamp_millis(value.timestamp_millis())
            .map(|date| Value::String(date.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
            .unwrap_or_else(|| Value::from(value.timestamp_millis())),
        Bson::Binary(binary) => match (binary.subtype, uuid::Uuid::from_slice(&binary.bytes)) {
            (BinarySubtype::Uuid, Ok(uuid)) => Value::String(uuid.hyphenated().to_string()),
            _ => Value::String(base64_encode(&binary.bytes)),
        },
        Bson::Symbol(value) => Value::String(value),
        Bson::Undefined => Value::Null,
        other => other.into_relaxed_extjson(),
    }
}

/// Obtiene un valor por ruta punteada (`customer.id`) para construir filtros
/// de upsert a partir del documento entrante.
pub(super) fn value_at_path<'a>(document: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut value = document.get(first)?;
    for part in parts {
        value = value.as_document()?.get(part)?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use mongodb::bson::oid::ObjectId;
    use serde_json::json;

    use super::*;

    #[test]
    fn extended_json_round_trip_preserves_object_id() {
        let id = ObjectId::new();
        let value = json!({ "_id": { "$oid": id.to_hex() }, "name": "Ada" });
        let document = json_document(&value, "record").expect("parse Extended JSON");
        assert_eq!(document.get_object_id("_id").expect("object id"), id);
        assert_eq!(document_json(document, JsonFormat::Relaxed), value);
    }

    #[test]
    fn canonical_extended_json_keeps_integer_widths() {
        let document = mongodb::bson::doc! { "small_long": 5_i64, "int": 5_i32, "double": 1.5 };
        let value = document_json(document.clone(), JsonFormat::Canonical);
        assert_eq!(value["small_long"], json!({ "$numberLong": "5" }));
        assert_eq!(
            json_document(&value, "record").expect("parse canonical"),
            document
        );
        let relaxed = document_json(document, JsonFormat::Relaxed);
        assert_eq!(
            json_document(&relaxed, "record")
                .expect("parse relaxed")
                .get("small_long"),
            Some(&Bson::Int32(5))
        );
    }

    #[test]
    fn plain_json_unwraps_special_types() {
        let id = ObjectId::parse_str("65f1a2b3c4d5e6f708192a3b").unwrap();
        let document = mongodb::bson::doc! {
            "_id": id,
            "price": "12.50".parse::<mongodb::bson::Decimal128>().unwrap(),
            "at": mongodb::bson::DateTime::from_millis(1_760_350_830_000),
            "nested": { "id": id },
        };
        assert_eq!(
            document_json(document, JsonFormat::Plain),
            json!({
                "_id": "65f1a2b3c4d5e6f708192a3b",
                "price": 12.5,
                "at": "2025-10-13T10:20:30.000Z",
                "nested": { "id": "65f1a2b3c4d5e6f708192a3b" },
            })
        );
    }

    #[test]
    fn invalid_records_are_rejected_not_retried() {
        let error = record_document(&json!([1, 2]), "record 0").expect_err("array record");
        assert!(!error.is_retryable());
        let error = record_document(&json!({ "_id": { "$oid": "not-hex" } }), "record 1")
            .expect_err("bad ObjectId");
        assert!(!error.is_retryable());
    }

    #[test]
    fn transient_codes_cover_locks_and_elections() {
        for code in [50, 112, 189, 10107, 11600] {
            assert!(is_transient_code(code), "{code}");
        }
        for code in [2, 11000, 121, 13, 26] {
            assert!(!is_transient_code(code), "{code}");
        }
    }

    #[test]
    fn finds_nested_upsert_key() {
        let document = mongodb::bson::doc! {
            "customer": { "id": 42_i32 }
        };
        assert_eq!(
            value_at_path(&document, "customer.id"),
            Some(&Bson::Int32(42))
        );
    }
}
