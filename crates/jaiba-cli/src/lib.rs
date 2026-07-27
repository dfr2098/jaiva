use std::{env, fs, net::SocketAddr};

use jaiba_core::config::FlowConfig;
use jaiba_runtime::{
    engine::{FlowMetrics, FlowSupervisor, LocalPacketRepository, PacketRepository},
    error::FlowError,
    logging,
};
use jaiba_server::ObservabilityServer;
use tracing::info;

/// Executes the Jaiba command line using the process arguments.
pub async fn run() -> Result<(), FlowError> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let serving = arguments.first().map(String::as_str) == Some("serve");
    let dead_letter = arguments.first().map(String::as_str) == Some("dead-letter");
    let provenance = arguments.first().map(String::as_str) == Some("provenance");
    let path = if dead_letter || provenance {
        arguments.get(2).map(String::as_str)
    } else if serving {
        arguments.get(1).map(String::as_str)
    } else {
        Some(
            arguments
                .first()
                .map(String::as_str)
                .unwrap_or("examples/basic-flow.yaml"),
        )
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
        let supervisor = FlowSupervisor::new(config, FlowMetrics::default());
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
        let supervisor = FlowSupervisor::new(config, metrics);
        supervisor.start().await?;
        server = server.with_supervisor(supervisor);
    }

    server.serve(address).await
}

fn load_config(path: &str) -> Result<FlowConfig, FlowError> {
    let yaml = fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&yaml)?)
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
