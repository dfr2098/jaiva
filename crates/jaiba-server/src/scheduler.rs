//! Calendario de ejecución continua: intervalo, cron y disparo manual/webhook.
//!
//! Se arma al desplegar/iniciar un flujo con `schedule.enabled` y se desarma al
//! detenerlo. Un disparo llama a `FlowSupervisor::start` (o drena/reemplaza
//! según `overlap`). Estado persistente en `ScheduleStore` para catch-up.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use jaiba_core::config::{CatchUpPolicy, OverlapPolicy, ScheduleConfig, ScheduleTrigger};
use jaiba_runtime::{
    engine::{FlowLifecycle, FlowSupervisor},
    error::FlowError,
};
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};

use crate::{
    flow_registry::FlowRegistry,
    schedule_store::{ScheduleState, ScheduleStore},
};

/// Orquesta disparos repetidos sobre flujos ya cargados en el registro.
pub struct FlowScheduler {
    registry: Arc<FlowRegistry>,
    store: ScheduleStore,
    armed: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl FlowScheduler {
    pub fn new(registry: Arc<FlowRegistry>, store: ScheduleStore) -> Arc<Self> {
        Arc::new(Self {
            registry,
            store,
            armed: Mutex::new(HashMap::new()),
        })
    }

    /// Activa la agenda del flujo si su YAML la declara habilitada.
    pub async fn arm(self: &Arc<Self>, flow_id: &str) {
        let Some(supervisor) = self.registry.supervisor(flow_id).await else {
            return;
        };
        let config = supervisor.config().clone();
        let Some(schedule) = config.schedule.clone() else {
            self.disarm(flow_id).await;
            return;
        };
        if !schedule.enabled {
            self.disarm(flow_id).await;
            return;
        }
        if let Err(error) = schedule.validate() {
            tracing::error!(
                target: "jaiba.schedule",
                flow_id,
                %error,
                "agenda inválida; no se arma"
            );
            return;
        }

        self.disarm(flow_id).await;

        // Catch-up: un disparo inmediato si se perdió la ventana.
        if matches!(schedule.catch_up, CatchUpPolicy::One)
            && should_catch_up(&schedule, &self.store.get(flow_id))
        {
            tracing::info!(target: "jaiba.schedule", flow_id, "catch-up: disparo inmediato");
            let _ = self.fire(flow_id, &schedule.overlap, "catch_up").await;
        }

        match &schedule.trigger {
            ScheduleTrigger::Webhook {} => {
                tracing::info!(
                    target: "jaiba.schedule",
                    flow_id,
                    "agenda webhook armada (POST /api/v1/flows/{flow_id}/trigger)"
                );
            }
            ScheduleTrigger::Interval { every_seconds } => {
                let seconds = *every_seconds;
                let overlap = schedule.overlap;
                let scheduler = Arc::clone(self);
                let id = flow_id.to_owned();
                let handle = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_secs(seconds));
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    // Evita un tick inmediato duplicado tras catch-up/start.
                    ticker.tick().await;
                    loop {
                        ticker.tick().await;
                        if scheduler.registry.supervisor(&id).await.is_none() {
                            break;
                        }
                        let _ = scheduler.fire(&id, &overlap, "interval").await;
                    }
                });
                self.armed.lock().await.insert(flow_id.to_owned(), handle);
                tracing::info!(
                    target: "jaiba.schedule",
                    flow_id,
                    every_seconds = seconds,
                    "agenda por intervalo armada"
                );
            }
            ScheduleTrigger::Cron { expression } => {
                let expression = expression.clone();
                let timezone = schedule
                    .timezone
                    .clone()
                    .unwrap_or_else(|| "UTC".to_owned());
                tracing::info!(
                    target: "jaiba.schedule",
                    flow_id,
                    expression = %expression,
                    timezone = %timezone,
                    "agenda cron armada"
                );
                let overlap = schedule.overlap;
                let scheduler = Arc::clone(self);
                let id = flow_id.to_owned();
                let handle = tokio::spawn(async move {
                    loop {
                        let Some(delay) = delay_until_next_cron(&expression, &timezone) else {
                            tracing::error!(
                                target: "jaiba.schedule",
                                flow_id = %id,
                                "no se pudo calcular el próximo tick cron"
                            );
                            break;
                        };
                        let previous = scheduler.store.get(&id);
                        let next_at = now_secs().saturating_add(delay.as_secs());
                        let _ = scheduler.store.put(
                            &id,
                            ScheduleState {
                                last_fire_at: previous.last_fire_at,
                                next_fire_at: Some(next_at),
                                last_status: previous.last_status,
                                updated_at: now_secs(),
                            },
                        );
                        sleep(delay).await;
                        if scheduler.registry.supervisor(&id).await.is_none() {
                            break;
                        }
                        let _ = scheduler.fire(&id, &overlap, "cron").await;
                    }
                });
                self.armed.lock().await.insert(flow_id.to_owned(), handle);
            }
        }
    }

    pub async fn disarm(&self, flow_id: &str) {
        if let Some(handle) = self.armed.lock().await.remove(flow_id) {
            handle.abort();
            tracing::info!(target: "jaiba.schedule", flow_id, "agenda desarmada");
        }
    }

    /// Disparo manual / webhook respetando la política de solapamiento del YAML
    /// (o `skip` si no hay agenda).
    pub async fn trigger(&self, flow_id: &str) -> Result<bool, FlowError> {
        let Some(supervisor) = self.registry.supervisor(flow_id).await else {
            return Err(FlowError::Configuration(format!(
                "flujo '{flow_id}' no está cargado en runtime"
            )));
        };
        let overlap = supervisor
            .config()
            .schedule
            .as_ref()
            .map(|schedule| schedule.overlap)
            .unwrap_or(OverlapPolicy::Skip);
        self.fire(flow_id, &overlap, "trigger").await
    }

    async fn fire(
        &self,
        flow_id: &str,
        overlap: &OverlapPolicy,
        reason: &str,
    ) -> Result<bool, FlowError> {
        let Some(supervisor) = self.registry.supervisor(flow_id).await else {
            return Err(FlowError::Configuration(format!(
                "flujo '{flow_id}' no está cargado"
            )));
        };

        let started = match overlap {
            OverlapPolicy::Skip => match supervisor.start().await? {
                true => true,
                false => {
                    tracing::warn!(
                        target: "jaiba.schedule",
                        flow_id,
                        reason,
                        "disparo omitido (flujo aún en ejecución)"
                    );
                    self.record(flow_id, "skipped");
                    return Ok(false);
                }
            },
            OverlapPolicy::Queue => {
                wait_until_idle(&supervisor).await?;
                supervisor.start().await?
            }
            OverlapPolicy::Replace => {
                if !is_idle(&supervisor) {
                    supervisor.stop_gracefully().await?;
                }
                supervisor.start().await?
            }
        };

        if started {
            tracing::info!(
                target: "jaiba.schedule",
                flow_id,
                reason,
                "disparo iniciado"
            );
            self.record(flow_id, "started");
        } else {
            self.record(flow_id, "skipped");
        }
        Ok(started)
    }

    fn record(&self, flow_id: &str, status: &str) {
        let previous = self.store.get(flow_id);
        let state = ScheduleState {
            last_fire_at: Some(now_secs()),
            next_fire_at: previous.next_fire_at,
            last_status: Some(status.to_owned()),
            updated_at: now_secs(),
        };
        if let Err(error) = self.store.put(flow_id, state) {
            tracing::warn!(target: "jaiba.schedule", flow_id, %error, "no se pudo persistir estado de agenda");
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_idle(supervisor: &FlowSupervisor) -> bool {
    matches!(
        supervisor.control().state(),
        FlowLifecycle::Stopped | FlowLifecycle::Failed
    )
}

async fn wait_until_idle(supervisor: &FlowSupervisor) -> Result<(), FlowError> {
    if is_idle(supervisor) {
        return Ok(());
    }
    // Espera terminal con tope para no bloquear el scheduler indefinidamente.
    match tokio::time::timeout(Duration::from_secs(3600), supervisor.wait_for_terminal()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(FlowError::Server(
            "timeout esperando fin de ejecución (overlap=queue)".to_owned(),
        )),
    }
}

fn should_catch_up(schedule: &ScheduleConfig, state: &ScheduleState) -> bool {
    let Some(last) = state.last_fire_at else {
        return false;
    };
    let elapsed = now_secs().saturating_sub(last);
    match &schedule.trigger {
        ScheduleTrigger::Interval { every_seconds } => elapsed >= *every_seconds,
        ScheduleTrigger::Cron { expression } => {
            let timezone = schedule.timezone.as_deref().unwrap_or("UTC");
            delay_until_next_cron(expression, timezone)
                .map(|delay| delay < Duration::from_secs(2))
                .unwrap_or(false)
                || elapsed >= 60
        }
        ScheduleTrigger::Webhook {} => false,
    }
}

fn delay_until_next_cron(expression: &str, timezone: &str) -> Option<Duration> {
    let schedule: Schedule = expression.parse().ok()?;
    let tz: Tz = timezone.parse().ok()?;
    let now: DateTime<Tz> = Utc::now().with_timezone(&tz);
    let next = schedule.upcoming(tz).next()?;
    let wait = next.signed_duration_since(now).to_std().ok()?;
    Some(wait.max(Duration::from_millis(50)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_interval_and_cron() {
        let ok = ScheduleConfig {
            enabled: true,
            trigger: ScheduleTrigger::Interval { every_seconds: 5 },
            timezone: None,
            overlap: OverlapPolicy::Skip,
            catch_up: CatchUpPolicy::None,
        };
        assert!(ok.validate().is_ok());

        let bad = ScheduleConfig {
            enabled: true,
            trigger: ScheduleTrigger::Interval { every_seconds: 0 },
            timezone: None,
            overlap: OverlapPolicy::Skip,
            catch_up: CatchUpPolicy::None,
        };
        assert!(bad.validate().is_err());

        let cron = ScheduleConfig {
            enabled: true,
            trigger: ScheduleTrigger::Cron {
                expression: "0 0 2 * * *".to_owned(),
            },
            timezone: Some("America/Mexico_City".to_owned()),
            overlap: OverlapPolicy::Queue,
            catch_up: CatchUpPolicy::One,
        };
        assert!(cron.validate().is_ok());
    }

    #[test]
    fn next_cron_delay_is_positive() {
        let delay = delay_until_next_cron("0 */5 * * * *", "UTC").unwrap();
        assert!(delay > Duration::ZERO);
        assert!(delay <= Duration::from_secs(5 * 60));
    }
}
