//! Clasificación de errores Kafka y ayudas compartidas por `publish_kafka` /
//! `consume_kafka`.

use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::error::FlowError;

/// Códigos que no se arreglan reintentando el mismo mensaje o la misma
/// configuración: van a `MessageRejected` (sin circuit breaker ni reintento).
fn is_rejected_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::MessageSizeTooLarge
            | RDKafkaErrorCode::InvalidMessage
            | RDKafkaErrorCode::InvalidMessageSize
            | RDKafkaErrorCode::InvalidRecord
            | RDKafkaErrorCode::InvalidTopic
            | RDKafkaErrorCode::InvalidTimestamp
            | RDKafkaErrorCode::InvalidRequest
            | RDKafkaErrorCode::PolicyViolation
            | RDKafkaErrorCode::UnsupportedForMessageFormat
            | RDKafkaErrorCode::UnsupportedCompressionType
            | RDKafkaErrorCode::UnsupportedVersion
            | RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
            | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::Authentication
            | RDKafkaErrorCode::InvalidProducerEpoch
            | RDKafkaErrorCode::Fatal
    )
}

/// Fallos de transporte / unión al grupo que librdkafka reporta mientras el
/// consumidor aún no ha leído nada: no abortan el ciclo vacío.
pub(crate) fn is_transient_join(error: &KafkaError) -> bool {
    let text = error.to_string();
    text.contains("BrokerTransportFailure")
        || text.contains("AllBrokerConnectionsDown")
        || text.contains("AllBrokersDown")
        || text.contains("Timed out")
        || text.contains("Local: Wait timed out")
        || matches!(
            error.rdkafka_error_code(),
            Some(
                RDKafkaErrorCode::BrokerTransportFailure
                    | RDKafkaErrorCode::AllBrokersDown
                    | RDKafkaErrorCode::OperationTimedOut
                    | RDKafkaErrorCode::RequestTimedOut
                    | RDKafkaErrorCode::MessageTimedOut
            )
        )
}

/// Clasifica un error de librdkafka: red, timeout, líder ausente y cola llena
/// son reintentables (`MessageConnector`); tamaño, autorización, formato y
/// política son `MessageRejected`.
pub(crate) fn kafka_error(error: KafkaError) -> FlowError {
    let message = error.to_string();
    match &error {
        KafkaError::MessageConsumptionFatal(_) | KafkaError::Canceled => {
            FlowError::MessageRejected(message)
        }
        KafkaError::ClientConfig(..)
        | KafkaError::ClientCreation(_)
        | KafkaError::Subscription(_) => FlowError::Configuration(message),
        other => match other.rdkafka_error_code() {
            Some(code) if is_rejected_code(code) => FlowError::MessageRejected(message),
            _ => FlowError::MessageConnector(message),
        },
    }
}

/// ¿El circuit breaker debe abrirse ante este error? Los rechazos de datos no
/// cuentan: reintentarlos no ayuda y un poison message no debe tumbar el
/// conector.
pub(crate) fn opens_circuit(error: &FlowError) -> bool {
    !matches!(
        error,
        FlowError::MessageRejected(_)
            | FlowError::Configuration(_)
            | FlowError::PacketTooLarge { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_too_large_is_rejected() {
        let error = kafka_error(KafkaError::MessageProduction(
            RDKafkaErrorCode::MessageSizeTooLarge,
        ));
        assert!(matches!(error, FlowError::MessageRejected(_)));
        assert!(!error.is_retryable());
        assert!(!opens_circuit(&error));
    }

    #[test]
    fn authorization_is_rejected() {
        let error = kafka_error(KafkaError::MessageConsumption(
            RDKafkaErrorCode::TopicAuthorizationFailed,
        ));
        assert!(matches!(error, FlowError::MessageRejected(_)));
        assert!(!error.is_retryable());
    }

    #[test]
    fn timeout_is_retryable() {
        let error = kafka_error(KafkaError::MessageProduction(
            RDKafkaErrorCode::RequestTimedOut,
        ));
        assert!(matches!(error, FlowError::MessageConnector(_)));
        assert!(error.is_retryable());
        assert!(opens_circuit(&error));
    }

    #[test]
    fn transport_failure_is_transient_join() {
        assert!(is_transient_join(&KafkaError::MessageConsumption(
            RDKafkaErrorCode::BrokerTransportFailure
        )));
    }
}
