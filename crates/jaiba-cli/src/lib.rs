use std::{env, fs, net::SocketAddr, process::ExitCode, sync::Arc};

use jaiba_core::config::FlowConfig;
use jaiba_runtime::{
    engine::{
        ConnectionResolver, FlowMetrics, FlowSupervisor, LocalPacketRepository, PacketRepository,
        ProfileConnectionResolver,
    },
    error::FlowError,
    logging,
};
use jaiba_server::ObservabilityServer;
use tracing::info;

/// Construye un resolvedor de conexiones por alias desde el entorno
/// (`JAIBA_MASTER_KEY` + `JAIBA_DATA_DIR`). Devuelve `None` en modo desarrollo.
async fn connection_resolver() -> Result<Option<Arc<dyn ConnectionResolver>>, FlowError> {
    Ok(ProfileConnectionResolver::from_env()
        .await?
        .map(|resolver| Arc::new(resolver) as Arc<dyn ConnectionResolver>))
}

/// Entrada de los binarios: ejecuta [`run`] e imprime el error legible (no `Debug`).
pub async fn run_and_report() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn help_text() -> String {
    format!(
        "Jaiba {version}: motor de flujos de datos definidos en YAML

Uso:
  jaiba <flujo.yaml>                         Ejecuta un flujo hasta que termina
  jaiba serve [flujo.yaml]                   API, WebSocket y métricas (127.0.0.1:9090)
  jaiba validate <flujo.yaml>                Revisa un flujo sin ejecutarlo
  jaiba dead-letter list <flujo.yaml> [LÍMITE]
  jaiba dead-letter replay <flujo.yaml> <QUEUE_ID>
  jaiba provenance recent <flujo.yaml> [LÍMITE]
  jaiba provenance packet <flujo.yaml> <PACKET_ID> [LÍMITE]
  jaiba connections rotate-key <NUEVA_CLAVE>
  jaiba --help | --version

Variables de entorno:
  JAIBA_SERVER_ADDR   dirección de `serve` (p. ej. 127.0.0.1:19090)
  JAIBA_MASTER_KEY    guarda las conexiones cifradas en disco (sin ella, en memoria)
  JAIBA_DATA_DIR      carpeta de datos (por defecto ./data)

Guía: https://github.com/dfr2098/jaiva/blob/main/docs/guia-para-nuevos.md
",
        version = env!("CARGO_PKG_VERSION")
    )
}

/// Executes the Jaiba command line using the process arguments.
pub async fn run() -> Result<(), FlowError> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.first().map(String::as_str) {
        None | Some("-h" | "--help" | "help") => {
            print!("{}", help_text());
            return Ok(());
        }
        Some("-V" | "--version" | "version") => {
            println!("jaiba {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some("validate") => return validate_command(arguments.get(1).map(String::as_str)),
        Some("connections") => return connections_command(&arguments).await,
        Some(option) if option.starts_with('-') => {
            return Err(FlowError::Configuration(format!(
                "opción desconocida '{option}'. Consulta: jaiba --help"
            )));
        }
        _ => {}
    }
    let serving = arguments.first().map(String::as_str) == Some("serve");
    let dead_letter = arguments.first().map(String::as_str) == Some("dead-letter");
    let provenance = arguments.first().map(String::as_str) == Some("provenance");
    let path = if dead_letter || provenance {
        arguments.get(2).map(String::as_str)
    } else if serving {
        arguments.get(1).map(String::as_str)
    } else {
        arguments.first().map(String::as_str)
    };
    let config = path.map(load_config).transpose()?;
    let logging_config = config
        .as_ref()
        .map(|flow| flow.engine.logging.clone())
        .unwrap_or_default();
    let _log_guard = logging::initialize(&logging_config)?;
    logging::start_cleanup(logging_config);

    if dead_letter {
        let config = config.ok_or_else(|| {
            FlowError::Configuration(
                "dead-letter command requires the path to a flow YAML".to_owned(),
            )
        })?;
        dead_letter_command(&arguments, config).await
    } else if provenance {
        let config = config.ok_or_else(|| {
            FlowError::Configuration(
                "provenance command requires the path to a flow YAML".to_owned(),
            )
        })?;
        provenance_command(&arguments, config).await
    } else if serving {
        serve(path, config).await
    } else {
        let config = config.expect("non-server execution always loads a flow");
        info!(flow_id = %config.id, config = %path.unwrap(), "starting flow");
        let resolver = connection_resolver().await?;
        let supervisor =
            FlowSupervisor::new(config, FlowMetrics::default()).with_connection_resolver(resolver);
        supervisor.start().await?;
        let summary = tokio::select! {
            result = supervisor.wait_for_terminal() => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(FlowError::Io)?;
                supervisor.stop_gracefully().await?;
                supervisor.snapshot().metrics
            }
        };
        log_summary(summary);
        Ok(())
    }
}

async fn connections_command(arguments: &[String]) -> Result<(), FlowError> {
    match arguments.get(1).map(String::as_str) {
        Some("rotate-key") => {
            let new_key = arguments
                .get(2)
                .cloned()
                .or_else(|| env::var("JAIBA_NEW_MASTER_KEY").ok())
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    FlowError::Configuration(
                        "uso: jaiba connections rotate-key <NUEVA_CLAVE>  (o define JAIBA_NEW_MASTER_KEY)"
                            .to_owned(),
                    )
                })?;
            let new_id = jaiba_server::rotate_connection_master_key(&new_key).await?;
            println!("Clave maestra rotada. Nueva huella (key_id): {new_id}");
            println!(
                "Actualiza JAIBA_MASTER_KEY con la nueva clave antes de reiniciar el servidor."
            );
            Ok(())
        }
        _ => Err(FlowError::Configuration(
            "uso: jaiba connections rotate-key <NUEVA_CLAVE>".to_owned(),
        )),
    }
}

async fn provenance_command(arguments: &[String], config: FlowConfig) -> Result<(), FlowError> {
    if !config.engine.repository.enabled {
        return Err(FlowError::Configuration(
            "provenance commands require engine.repository.enabled: true".to_owned(),
        ));
    }
    let repository = LocalPacketRepository::open(&config.engine.repository).await?;
    let records = match arguments.get(1).map(String::as_str) {
        Some("packet") => {
            let packet_id = arguments.get(3).ok_or_else(|| {
                FlowError::Configuration(
                    "usage: jaiba provenance packet FLOW.yaml PACKET_ID [LIMIT]".to_owned(),
                )
            })?;
            let limit = parse_limit(arguments.get(4), 1000)?;
            repository
                .provenance_for_packet(&config.id, packet_id, limit)
                .await?
        }
        Some("recent") => {
            let limit = parse_limit(arguments.get(3), 100)?;
            repository.recent_provenance(&config.id, limit).await?
        }
        _ => {
            return Err(FlowError::Configuration(
                "usage: jaiba provenance packet FLOW.yaml PACKET_ID [LIMIT] | provenance recent FLOW.yaml [LIMIT]"
                    .to_owned(),
            ));
        }
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&records)
            .map_err(|error| FlowError::Repository(error.to_string()))?
    );
    Ok(())
}

fn parse_limit(value: Option<&String>, default: u32) -> Result<u32, FlowError> {
    value
        .map(|value| value.parse::<u32>())
        .transpose()
        .map_err(|error| FlowError::Configuration(format!("invalid limit: {error}")))
        .map(|value| value.unwrap_or(default))
}

async fn dead_letter_command(arguments: &[String], config: FlowConfig) -> Result<(), FlowError> {
    if !config.engine.repository.enabled {
        return Err(FlowError::Configuration(
            "dead-letter commands require engine.repository.enabled: true".to_owned(),
        ));
    }
    let repository = LocalPacketRepository::open(&config.engine.repository).await?;
    match arguments.get(1).map(String::as_str) {
        Some("list") => {
            let limit = arguments
                .get(3)
                .map(|value| value.parse::<u32>())
                .transpose()
                .map_err(|error| FlowError::Configuration(format!("invalid limit: {error}")))?
                .unwrap_or(100);
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &repository.dead_letters(&config.id, limit).await?
                )
                .map_err(|error| FlowError::Repository(error.to_string()))?
            );
            Ok(())
        }
        Some("replay") => {
            let queue_id = arguments.get(3).ok_or_else(|| {
                FlowError::Configuration(
                    "usage: jaiba dead-letter replay FLOW.yaml QUEUE_ID".to_owned(),
                )
            })?;
            if !repository.requeue_dead_letter(queue_id).await? {
                return Err(FlowError::Repository(format!(
                    "dead-letter queue item '{queue_id}' does not exist"
                )));
            }
            info!(%queue_id, flow_id = %config.id, "dead-letter packet requeued");
            Ok(())
        }
        _ => Err(FlowError::Configuration(
            "usage: jaiba dead-letter list FLOW.yaml [LIMIT] | dead-letter replay FLOW.yaml QUEUE_ID"
                .to_owned(),
        )),
    }
}

async fn serve(flow_path: Option<&str>, config: Option<FlowConfig>) -> Result<(), FlowError> {
    let address: SocketAddr = env::var("JAIBA_SERVER_ADDR")
        .or_else(|_| env::var("JAIVA_OBSERVABILITY_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:9090".to_owned())
        .parse()
        .map_err(|error| FlowError::Configuration(format!("invalid server address: {error}")))?;
    let metrics = FlowMetrics::default();

    let mut server = ObservabilityServer::new(metrics.clone());
    if let (Some(path), Some(config)) = (flow_path, config) {
        info!(flow_id = %config.id, config = %path, "starting flow");
        let source = fs::read_to_string(path)?;
        let resolver = connection_resolver().await?;
        let supervisor = FlowSupervisor::new(config, metrics).with_connection_resolver(resolver);
        supervisor.start().await?;
        server = server.with_supervisor(supervisor, source);
    }

    server.serve(address).await
}

fn read_flow(path: &str) -> Result<String, FlowError> {
    let yaml = fs::read_to_string(path).map_err(|error| {
        FlowError::Configuration(format!("no se pudo leer el flujo '{path}': {error}"))
    })?;
    let looks_like_flow = serde_yaml::from_str::<serde_yaml::Value>(&yaml)
        .map(|value| value.get("processors").is_some())
        .unwrap_or(true);
    if !looks_like_flow {
        return Err(FlowError::Configuration(format!(
            "'{path}' no es un flujo de Jaiba: le falta la lista `processors` \
             (¿es una política JME u otra configuración?)"
        )));
    }
    Ok(yaml)
}

fn invalid_flow(path: &str, error: impl Into<FlowError>) -> FlowError {
    let detail = match error.into() {
        FlowError::Configuration(message) => message,
        other => other.to_string(),
    };
    FlowError::Configuration(format!("flujo '{path}' inválido: {detail}"))
}

fn load_config(path: &str) -> Result<FlowConfig, FlowError> {
    serde_yaml::from_str(&read_flow(path)?).map_err(|error| invalid_flow(path, error))
}

fn validate_command(path: Option<&str>) -> Result<(), FlowError> {
    let path = path
        .ok_or_else(|| FlowError::Configuration("uso: jaiba validate <flujo.yaml>".to_owned()))?;
    let yaml = read_flow(path)?;
    let config =
        jaiba_server::parse_and_validate(&yaml).map_err(|error| invalid_flow(path, error))?;
    println!(
        "OK: el flujo '{}' es válido ({} procesadores, {} enlaces).",
        config.id,
        config.processors.len(),
        config.connections.len()
    );
    for name in missing_env_vars(&yaml, &config) {
        println!(
            "Aviso: la variable de entorno {name} no está definida; defínela antes de ejecutar."
        );
    }
    Ok(())
}

/// Variables que el flujo lee del entorno (`${env:X}`, `url_env`, `brokers_env`)
/// y que no están definidas ahora mismo.
fn missing_env_vars(yaml: &str, config: &FlowConfig) -> Vec<String> {
    let mut names: Vec<String> = yaml
        .split("${env:")
        .skip(1)
        .filter_map(|rest| rest.split_once('}').map(|(name, _)| name.trim().to_owned()))
        .chain(
            config
                .database_connections
                .values()
                .map(|connection| connection.url_env.clone()),
        )
        .chain(
            config
                .kafka_connections
                .values()
                .map(|connection| connection.brokers_env.clone()),
        )
        .filter(|name| !name.is_empty() && env::var_os(name).is_none())
        .collect();
    names.sort();
    names.dedup();
    names
}

fn log_summary(summary: jaiba_runtime::engine::FlowSummary) {
    info!(
        processed = summary.processed,
        failed = summary.failed,
        retried = summary.retried,
        emitted = summary.emitted,
        "flow completed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example(name: &str) -> String {
        format!("{}/../../examples/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    #[test]
    fn validate_accepts_the_canonical_smoke_flow() {
        validate_command(Some(&example("smoke.yaml"))).unwrap();
    }

    #[test]
    fn every_example_flow_validates_or_names_its_missing_feature() {
        let directory = format!("{}/../../examples", env!("CARGO_MANIFEST_DIR"));
        let mut checked = 0;
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
                continue;
            }
            let path = path.to_string_lossy().into_owned();
            match validate_command(Some(&path)) {
                Ok(()) => checked += 1,
                Err(error) => {
                    let message = error.to_string();
                    assert!(
                        message.contains("activa --features") || message.contains("no es un flujo"),
                        "{message}"
                    );
                }
            }
        }
        assert!(checked >= 20, "solo {checked} ejemplos validados");
    }

    #[test]
    fn validate_reports_the_file_and_the_problem() {
        let error = validate_command(Some("no-existe.yaml"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("no-existe.yaml"), "{error}");
        assert!(
            validate_command(None)
                .unwrap_err()
                .to_string()
                .contains("uso:")
        );
    }

    #[test]
    fn missing_env_vars_lists_undefined_references_once() {
        let yaml = std::fs::read_to_string(example("postgres-read.yaml")).unwrap();
        let config: FlowConfig = serde_yaml::from_str(&yaml).unwrap();
        let mut expected: Vec<String> = config
            .database_connections
            .values()
            .map(|connection| connection.url_env.clone())
            .filter(|name| env::var_os(name).is_none())
            .collect();
        expected.sort();
        expected.dedup();
        assert!(!config.database_connections.is_empty());
        assert_eq!(missing_env_vars(&yaml, &config), expected);

        let yaml = "a: ${env:JAIBA_TEST_UNSET_A}\nb: ${env:JAIBA_TEST_UNSET_A}\nc: ${env:PATH}\n";
        let config: FlowConfig = serde_yaml::from_str("id: t\nprocessors: []\n").unwrap();
        assert_eq!(missing_env_vars(yaml, &config), ["JAIBA_TEST_UNSET_A"]);
    }

    #[test]
    fn help_lists_every_command() {
        let help = help_text();
        for command in [
            "serve",
            "validate",
            "dead-letter",
            "provenance",
            "connections",
        ] {
            assert!(help.contains(command), "{command}");
        }
        assert!(help.contains(env!("CARGO_PKG_VERSION")));
    }
}
