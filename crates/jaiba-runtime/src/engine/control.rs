use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::sync::watch;

/// Observable lifecycle of one flow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FlowLifecycle {
    Stopped,
    Starting,
    Running,
    Paused,
    Draining,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowControlSnapshot {
    pub state: FlowLifecycle,
    pub last_error: Option<String>,
    pub changed_at: u64,
}

#[derive(Debug)]
struct ControlDetails {
    last_error: Option<String>,
    changed_at: u64,
}

/// Shared control signal used by the runtime and administrative API.
#[derive(Debug, Clone)]
pub struct FlowControl {
    sender: watch::Sender<FlowLifecycle>,
    details: Arc<Mutex<ControlDetails>>,
}

impl Default for FlowControl {
    fn default() -> Self {
        let (sender, _) = watch::channel(FlowLifecycle::Stopped);
        Self {
            sender,
            details: Arc::new(Mutex::new(ControlDetails {
                last_error: None,
                changed_at: now_epoch(),
            })),
        }
    }
}

impl FlowControl {
    pub fn state(&self) -> FlowLifecycle {
        *self.sender.borrow()
    }

    pub fn snapshot(&self) -> FlowControlSnapshot {
        let details = self.details.lock().expect("flow control poisoned");
        FlowControlSnapshot {
            state: self.state(),
            last_error: details.last_error.clone(),
            changed_at: details.changed_at,
        }
    }

    pub fn starting(&self) {
        self.transition(FlowLifecycle::Starting, None);
    }

    pub fn running(&self) {
        self.transition(FlowLifecycle::Running, None);
    }

    pub fn pause(&self) -> bool {
        if self.state() == FlowLifecycle::Running {
            self.transition(FlowLifecycle::Paused, None);
            true
        } else {
            false
        }
    }

    pub fn resume(&self) -> bool {
        if self.state() == FlowLifecycle::Paused {
            self.transition(FlowLifecycle::Running, None);
            true
        } else {
            false
        }
    }

    pub fn drain(&self) -> bool {
        if matches!(
            self.state(),
            FlowLifecycle::Running | FlowLifecycle::Paused | FlowLifecycle::Starting
        ) {
            self.transition(FlowLifecycle::Draining, None);
            true
        } else {
            false
        }
    }

    pub fn stopped(&self) {
        self.transition(FlowLifecycle::Stopped, None);
    }

    pub fn failed(&self, error: impl Into<String>) {
        self.transition(FlowLifecycle::Failed, Some(error.into()));
    }

    pub async fn changed(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.changed().await;
    }

    fn transition(&self, state: FlowLifecycle, error: Option<String>) {
        {
            let mut details = self.details.lock().expect("flow control poisoned");
            if error.is_some() {
                details.last_error = error;
            } else if matches!(state, FlowLifecycle::Starting | FlowLifecycle::Running) {
                details.last_error = None;
            }
            details.changed_at = now_epoch();
        }
        self.sender.send_replace(state);
    }
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_pause_resume_and_drain_transitions() {
        let control = FlowControl::default();
        assert!(!control.pause());
        control.starting();
        control.running();
        assert!(control.pause());
        assert_eq!(control.state(), FlowLifecycle::Paused);
        assert!(control.resume());
        assert!(control.drain());
        assert!(!control.resume());
        control.stopped();
        assert_eq!(control.state(), FlowLifecycle::Stopped);
    }
}
