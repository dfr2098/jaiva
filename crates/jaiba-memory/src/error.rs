use thiserror::Error;

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("configuración de memoria: {0}")]
    Configuration(String),
    #[error("clase de memoria desconocida: {0}")]
    UnknownClass(String),
    #[error(
        "política '{0}' aún no soportada (Paso 6: volatile|cache|immediate|persistent|deferred)"
    )]
    UnsupportedPolicy(String),
    #[error("rebuild: {0}")]
    Rebuild(String),
    #[error("persistencia: {0}")]
    Persistence(String),
    #[error("warm store: {0}")]
    Warm(String),
    #[error("frozen store: {0}")]
    Frozen(String),
    #[error("hot store lleno ({max_entries}); todas las entradas son critical")]
    CriticalCapacity { max_entries: usize },
    #[error("la política requiere sink de persistencia (immediate/persistent/deferred)")]
    MissingImmediateSink,
    #[error("warm.backend redis requiere compilar jaiba-memory con feature 'redis'")]
    RedisFeatureDisabled,
}
