use serde_json::Value;

use crate::error::MemoryError;

/// Hook para reconstruir un valor de clase `cache` tras miss Hot+Warm.
///
/// El `rebuild_ref` es el token declarado en YAML (`rebuild: query:carrier_by_id`).
/// El runtime real (query/conexión) se enchufa aquí; Paso 5 solo define el contrato.
pub trait RebuildHook: Send {
    fn rebuild(
        &mut self,
        class: &str,
        key: &str,
        rebuild_ref: &str,
    ) -> Result<Option<Value>, MemoryError>;
}

/// Hook de prueba: mapa clave → valor.
#[derive(Debug, Default)]
pub struct MapRebuildHook {
    pub values: std::collections::HashMap<String, Value>,
    pub calls: Vec<(String, String, String)>,
}

impl RebuildHook for MapRebuildHook {
    fn rebuild(
        &mut self,
        class: &str,
        key: &str,
        rebuild_ref: &str,
    ) -> Result<Option<Value>, MemoryError> {
        self.calls
            .push((class.to_owned(), key.to_owned(), rebuild_ref.to_owned()));
        Ok(self.values.get(key).cloned())
    }
}
