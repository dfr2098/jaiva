use mongodb::{
    Client, Database,
    bson::{Bson, Document},
};
use serde_json::Value;

use crate::error::FlowError;

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

/// Convierte BSON a Extended JSON relajado. ObjectId, fechas, binarios y otros
/// tipos no representables en JSON conservan su envoltura `$...`.
pub(super) fn document_json(document: Document) -> Value {
    Bson::Document(document).into_relaxed_extjson()
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
        assert_eq!(document_json(document), value);
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
