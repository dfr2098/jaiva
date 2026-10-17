//! Ciclo de vida de un flujo servido por `jaiba serve`: arrancar, pausar,
//! reanudar, drenar y detener.

use std::{sync::Arc, time::Duration};

use serde::Serialize;
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{config::FlowConfig, error::FlowError};

use super::{
    ConnectionResolver, FlowControl, FlowControlSnapshot, FlowEngine, FlowLifecycle, FlowMetrics,
    FlowSummary, ProcessorRegistry,
};

type FlowTask = JoinHandle<Result<FlowSummary, FlowError>>;

/// Estado de un flujo supervisado para la API.
#[derive(Debug, Clone, Serialize)]
pub struct SupervisedFlowSnapshot {
    pub flow_id: String,
    pub control: FlowControlSnapshot,
    pub metrics: FlowSummary,
    pub ready: bool,
}

/// Owns a configured flow and its current execution task.
#[derive(Clone)]
pub struct FlowSupervisor {
    config: Arc<FlowConfig>,
    metrics: FlowMetrics,
    control: FlowControl,
    task: Arc<Mutex<Option<FlowTask>>>,
    resolver: Option<Arc<dyn ConnectionResolver>>,
    registry: Option<ProcessorRegistry>,
}

impl FlowSupervisor {
    pub fn new(config: FlowConfig, metrics: FlowMetrics) -> Self {
        metrics.set_flow_id(&config.id);
        metrics.set_flow_status(0);
        Self {
            config: Arc::new(config),
            metrics,
            control: FlowControl::default(),
            task: Arc::new(Mutex::new(None)),
            resolver: None,
            registry: None,
        }
    }

    /// Inyecta un resolvedor de conexiones por alias que se usará en cada
    /// arranque del flujo.
    pub fn with_connection_resolver(
        mut self,
        resolver: Option<Arc<dyn ConnectionResolver>>,
    ) -> Self {
        self.resolver = resolver;
        self
    }

    /// Sustituye el registry de processors en cada arranque (pruebas / plugins).
    pub fn with_registry(mut self, registry: ProcessorRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn flow_id(&self) -> &str {
        &self.config.id
    }

    pub fn config(&self) -> &FlowConfig {
        &self.config
    }

    pub fn control(&self) -> &FlowControl {
        &self.control
    }

    pub fn snapshot(&self) -> SupervisedFlowSnapshot {
        let control = self.control.snapshot();
        SupervisedFlowSnapshot {
            flow_id: self.config.id.clone(),
            ready: matches!(
                control.state,
                FlowLifecycle::Running | FlowLifecycle::Paused
            ),
            control,
            metrics: self.metrics.summary(),
        }
    }

    /// Waits for initialization without holding the task lock, so a flow stuck
    /// in STARTING can still be stopped.
    pub async fn start(&self) -> Result<bool, FlowError> {
        let (task_id, ready) = {
            let mut task = self.task.lock().await;
            if let Some(handle) = task.as_ref()
                && !handle.is_finished()
            {
                return Ok(false);
            }
            if let Some(finished) = task.take() {
                let _ = finished.await;
            }
            let mut engine = FlowEngine::new((*self.config).clone())?
                .with_metrics(self.metrics.clone())
                .with_control(self.control.clone())
                .with_connection_resolver(self.resolver.clone());
            if let Some(registry) = self.registry.clone() {
                engine = engine.with_registry(registry);
            }
            self.control.starting();
            self.metrics.set_flow_status(1);
            let (started, ready) = tokio::sync::oneshot::channel();
            let handle = tokio::spawn(async move { engine.run_started(Some(started)).await });
            let task_id = handle.id();
            *task = Some(handle);
            (task_id, ready)
        };
        if ready.await.is_ok() {
            return Ok(true);
        }
        let handle = {
            let mut task = self.task.lock().await;
            if task.as_ref().is_some_and(|handle| handle.id() == task_id) {
                task.take()
            } else {
                None
            }
        };
        let Some(handle) = handle else {
            return Err(FlowError::Server(
                "flow stopped before initialization completed".into(),
            ));
        };
        handle
            .await
            .map_err(|error| FlowError::Server(error.to_string()))??;
        Err(FlowError::Server(
            "flow terminated before initialization completed".into(),
        ))
    }

    /// Deja de arrancar tareas nuevas; `false` si ya estaba en pausa o no aplica.
    pub fn pause(&self) -> bool {
        let changed = self.control.pause();
        if changed {
            self.metrics.set_flow_status(3);
        }
        changed
    }

    /// Vuelve a programar trabajo tras una pausa.
    pub fn resume(&self) -> bool {
        let changed = self.control.resume();
        if changed {
            self.metrics.set_flow_status(2);
        }
        changed
    }

    /// Termina las tareas en curso sin arrancar trabajo pendiente.
    pub fn drain(&self) -> bool {
        let changed = self.control.drain();
        if changed {
            self.metrics.set_flow_status(4);
        }
        changed
    }

    /// Drena y espera hasta `engine.shutdown.drain_timeout_seconds`; al vencer,
    /// aborta si `force_after_timeout` o devuelve error y conserva la tarea.
    pub async fn stop_gracefully(&self) -> Result<(), FlowError> {
        if self.control.drain() {
            self.metrics.set_flow_status(4);
        }
        let Some(mut handle) = self.task.lock().await.take() else {
            self.control.stopped();
            self.metrics.set_flow_status(0);
            return Ok(());
        };
        let shutdown = &self.config.engine.shutdown;
        match tokio::time::timeout(
            Duration::from_secs(shutdown.drain_timeout_seconds),
            &mut handle,
        )
        .await
        {
            Ok(joined) => {
                joined.map_err(|error| FlowError::Server(error.to_string()))??;
            }
            Err(_) if shutdown.force_after_timeout => {
                handle.abort();
                let _ = handle.await;
                self.control.stopped();
                self.metrics.set_flow_status(0);
            }
            Err(_) => {
                *self.task.lock().await = Some(handle);
                return Err(FlowError::Server(
                    "flow drain timed out and force_after_timeout is false".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Espera a que el flujo quede `Stopped` (resumen) o `Failed` (error).
    pub async fn wait_for_terminal(&self) -> Result<FlowSummary, FlowError> {
        loop {
            match self.control.state() {
                FlowLifecycle::Stopped => return Ok(self.metrics.summary()),
                FlowLifecycle::Failed => {
                    return Err(FlowError::Server(
                        self.control
                            .snapshot()
                            .last_error
                            .unwrap_or_else(|| "flow failed".to_owned()),
                    ));
                }
                _ => self.control.changed().await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{DataPacket, OutputSender, Processor, ProcessorContext};
    use async_trait::async_trait;
    use std::sync::Arc;

    struct SlowSource;
    struct Sink;
    struct HangSource;

    #[async_trait]
    impl Processor for SlowSource {
        async fn execute(
            &self,
            packet: DataPacket,
            _: &ProcessorContext,
            output: &OutputSender,
        ) -> Result<(), FlowError> {
            tokio::time::sleep(Duration::from_millis(150)).await;
            output.success(packet).await
        }
    }

    #[async_trait]
    impl Processor for Sink {
        async fn execute(
            &self,
            _: DataPacket,
            _: &ProcessorContext,
            _: &OutputSender,
        ) -> Result<(), FlowError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Processor for HangSource {
        async fn execute(
            &self,
            _: DataPacket,
            _: &ProcessorContext,
            _: &OutputSender,
        ) -> Result<(), FlowError> {
            // Long enough to outlive drain_timeout_seconds=1 in V3/V4.
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(())
        }
    }

    fn lifecycle_registry() -> ProcessorRegistry {
        let mut registry = ProcessorRegistry::default();
        registry.register("slow_source", |_| Ok(Arc::new(SlowSource)));
        registry.register("test_sink", |_| Ok(Arc::new(Sink)));
        registry.register("hang_source", |_| Ok(Arc::new(HangSource)));
        registry
    }

    fn lifecycle_yaml() -> FlowConfig {
        serde_yaml::from_str(
            r#"
id: lifecycle
processors:
  - { id: source, type: slow_source }
  - { id: sink, type: test_sink }
connections:
  - { from: source, relationship: success, to: sink }
"#,
        )
        .unwrap()
    }

    async fn wait_until(supervisor: &FlowSupervisor, state: FlowLifecycle) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while supervisor.control.state() != state {
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "timeout waiting for {state:?}, got {:?}",
                    supervisor.control.state()
                );
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn completed_flow_can_be_started_again() {
        let config: FlowConfig = serde_yaml::from_str(
            r#"
id: restartable
processors:
  - id: source
    type: generate_records
    config:
      records: []
"#,
        )
        .unwrap();
        let supervisor = FlowSupervisor::new(config, FlowMetrics::default());
        assert!(supervisor.start().await.unwrap());
        supervisor.wait_for_terminal().await.unwrap();
        assert!(supervisor.start().await.unwrap());
        supervisor.wait_for_terminal().await.unwrap();
        assert_eq!(supervisor.snapshot().metrics.processed, 2);
    }

    #[tokio::test]
    async fn flow_stuck_initializing_can_be_stopped() {
        let url_env = format!(
            "JAIBA_TEST_UNREACHABLE_PG_{}",
            uuid::Uuid::new_v4().simple()
        );
        // SAFETY: the variable name is unique to this test.
        unsafe { std::env::set_var(&url_env, "postgres://jaiba:jaiba@127.0.0.1:1/jaiba") };
        let config: FlowConfig = serde_yaml::from_str(&format!(
            r#"
id: stuck-starting
engine:
  shutdown:
    drain_timeout_seconds: 1
database_connections:
  unreachable:
    type: postgres
    url_env: {url_env}
processors:
  - id: source
    type: generate_records
    config:
      records: []
"#
        ))
        .unwrap();
        let supervisor = FlowSupervisor::new(config, FlowMetrics::default());
        let starting = tokio::spawn({
            let supervisor = supervisor.clone();
            async move { supervisor.start().await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(supervisor.control.state(), FlowLifecycle::Starting);

        tokio::time::timeout(Duration::from_secs(5), supervisor.stop_gracefully())
            .await
            .expect("stop must not wait for initialization")
            .unwrap();
        let started = tokio::time::timeout(Duration::from_secs(5), starting)
            .await
            .expect("start returns once the task is stopped")
            .unwrap();
        assert!(started.is_err());
        assert_eq!(supervisor.control.state(), FlowLifecycle::Stopped);
        unsafe { std::env::remove_var(&url_env) };
    }

    /// V1: pause vía Supervisor no tumba el flujo; resume completa el trabajo.
    #[tokio::test]
    async fn supervisor_pause_resume_preserves_work() {
        let supervisor = FlowSupervisor::new(lifecycle_yaml(), FlowMetrics::default())
            .with_registry(lifecycle_registry());
        assert!(supervisor.start().await.unwrap());
        wait_until(&supervisor, FlowLifecycle::Running).await;
        assert!(supervisor.snapshot().ready);
        assert!(supervisor.pause());
        assert_eq!(supervisor.control.state(), FlowLifecycle::Paused);
        assert!(supervisor.snapshot().ready);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(supervisor.control.state(), FlowLifecycle::Paused);
        assert!(supervisor.resume());
        let summary = supervisor.wait_for_terminal().await.unwrap();
        assert_eq!(summary.processed, 2);
        assert!(!supervisor.snapshot().ready);
    }

    /// V2: drain vía Supervisor termina sin programar el trabajo pendiente.
    #[tokio::test]
    async fn supervisor_drain_finishes_active_without_pending() {
        let supervisor = FlowSupervisor::new(lifecycle_yaml(), FlowMetrics::default())
            .with_registry(lifecycle_registry());
        assert!(supervisor.start().await.unwrap());
        wait_until(&supervisor, FlowLifecycle::Running).await;
        assert!(supervisor.drain());
        let summary = supervisor.wait_for_terminal().await.unwrap();
        assert_eq!(summary.processed, 1);
        assert_eq!(supervisor.control.state(), FlowLifecycle::Stopped);
    }

    /// V3: timeout sin force conserva la tarea y deja DRAINING.
    #[tokio::test]
    async fn stop_gracefully_timeout_without_force_keeps_task() {
        let config: FlowConfig = serde_yaml::from_str(
            r#"
id: hang-noforce
engine:
  shutdown:
    drain_timeout_seconds: 1
    force_after_timeout: false
processors:
  - { id: source, type: hang_source }
"#,
        )
        .unwrap();
        let supervisor =
            FlowSupervisor::new(config, FlowMetrics::default()).with_registry(lifecycle_registry());
        assert!(supervisor.start().await.unwrap());
        wait_until(&supervisor, FlowLifecycle::Running).await;
        let error = supervisor.stop_gracefully().await.unwrap_err();
        assert!(
            error.to_string().contains("force_after_timeout"),
            "unexpected error: {error}"
        );
        assert_eq!(supervisor.control.state(), FlowLifecycle::Draining);
        assert!(
            supervisor.task.lock().await.is_some(),
            "task must be retained when force is false"
        );
        // Cleanup: hang ends → engine reaches Stopped.
        tokio::time::timeout(Duration::from_secs(10), supervisor.wait_for_terminal())
            .await
            .expect("hang source should finish")
            .unwrap();
    }

    /// V4: timeout con force aborta y deja STOPPED.
    #[tokio::test]
    async fn stop_gracefully_force_aborts_after_timeout() {
        let config: FlowConfig = serde_yaml::from_str(
            r#"
id: hang-force
engine:
  shutdown:
    drain_timeout_seconds: 1
    force_after_timeout: true
processors:
  - { id: source, type: hang_source }
"#,
        )
        .unwrap();
        let supervisor =
            FlowSupervisor::new(config, FlowMetrics::default()).with_registry(lifecycle_registry());
        assert!(supervisor.start().await.unwrap());
        wait_until(&supervisor, FlowLifecycle::Running).await;
        tokio::time::timeout(Duration::from_secs(5), supervisor.stop_gracefully())
            .await
            .expect("force stop must return within timeout+margin")
            .unwrap();
        assert_eq!(supervisor.control.state(), FlowLifecycle::Stopped);
        assert!(supervisor.task.lock().await.is_none());
    }

    /// V5: segundo start con tarea viva devuelve false.
    #[tokio::test]
    async fn start_returns_false_while_already_running() {
        let supervisor = FlowSupervisor::new(lifecycle_yaml(), FlowMetrics::default())
            .with_registry(lifecycle_registry());
        assert!(supervisor.start().await.unwrap());
        wait_until(&supervisor, FlowLifecycle::Running).await;
        assert!(!supervisor.start().await.unwrap());
        assert!(supervisor.drain());
        supervisor.wait_for_terminal().await.unwrap();
    }
}
