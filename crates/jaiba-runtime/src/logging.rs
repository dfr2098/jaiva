//! Console and rotating-file execution logs with configurable retention.

use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime},
};

use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    config::{LogRotation, LoggingConfig},
    error::FlowError,
};

const LOG_PREFIX: &str = "jaiva.log";

/// Installs console logging and, when enabled, a non-blocking rotating file.
///
/// The returned guard must live until process shutdown so buffered lines flush.
pub fn initialize(config: &LoggingConfig) -> Result<Option<WorkerGuard>, FlowError> {
    // Sin RUST_LOG: mostrar arranque/API del control plane. Los crates se
    // renombraron desde `jaiva_flow`; un filtro solo a ese target deja la
    // consola en silencio y parece que `serve` se quedó colgado.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "jaiba_cli=info,jaiba_server=info,jaiba_runtime=info,jaiba_connection_manager=info,jaiba=info",
        )
    });
    let console = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    if !config.enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(console)
            .init();
        return Ok(None);
    }
    validate(config)?;
    fs::create_dir_all(&config.directory)?;
    cleanup_expired(config)?;
    let rotation = match config.rotation {
        LogRotation::Hourly => Rotation::HOURLY,
        LogRotation::Daily => Rotation::DAILY,
        LogRotation::Never => Rotation::NEVER,
    };
    let appender = RollingFileAppender::new(rotation, &config.directory, LOG_PREFIX);
    let (writer, guard) = tracing_appender::non_blocking(appender);
    let file = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(writer);
    tracing_subscriber::registry()
        .with(filter)
        .with(console)
        .with(file)
        .init();
    Ok(Some(guard))
}

/// Starts periodic retention cleanup on the current Tokio runtime.
pub fn start_cleanup(config: LoggingConfig) {
    if !config.enabled {
        return;
    }
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_secs(config.cleanup_interval_seconds));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(error) = cleanup_expired(&config) {
                tracing::warn!(%error, "execution log cleanup failed");
            }
        }
    });
}

/// Removes only Jaiva-owned log files older than the configured retention.
pub fn cleanup_expired(config: &LoggingConfig) -> Result<u64, FlowError> {
    if !config.enabled {
        return Ok(0);
    }
    validate(config)?;
    if !config.directory.exists() {
        return Ok(0);
    }
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(
            config.retention_hours.saturating_mul(3600),
        ))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    for entry in fs::read_dir(&config.directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !is_jaiva_log(&name) || !entry.file_type()?.is_file() {
            continue;
        }
        if entry
            .metadata()?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            < cutoff
        {
            fs::remove_file(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn is_jaiva_log(name: &str) -> bool {
    name == LOG_PREFIX || name.starts_with(&format!("{LOG_PREFIX}."))
}

fn validate(config: &LoggingConfig) -> Result<(), FlowError> {
    if config.retention_hours == 0 || config.cleanup_interval_seconds == 0 {
        return Err(FlowError::Configuration(
            "logging retention_hours and cleanup_interval_seconds must be greater than zero"
                .to_owned(),
        ));
    }
    if config.directory == Path::new("") {
        return Err(FlowError::Configuration(
            "logging directory cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn cleanup_never_removes_foreign_files() {
        let directory = std::env::temp_dir().join(format!("jaiva-log-cleanup-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("application.log"), b"keep").unwrap();
        let config = LoggingConfig {
            directory: directory.clone(),
            retention_hours: 1,
            ..LoggingConfig::default()
        };
        assert_eq!(cleanup_expired(&config).unwrap(), 0);
        assert!(directory.join("application.log").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_zero_cleanup_policy() {
        let config = LoggingConfig {
            retention_hours: 0,
            ..LoggingConfig::default()
        };
        assert!(cleanup_expired(&config).is_err());
    }
}
