use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{config::CircuitBreakerConfig, error::FlowError};

#[derive(Debug)]
struct CircuitState {
    failures: u32,
    opened_at: Option<Instant>,
    half_open_in_flight: u32,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakers {
    config: CircuitBreakerConfig,
    states: Arc<Mutex<HashMap<String, CircuitState>>>,
}

impl CircuitBreakers {
    pub fn new(config: CircuitBreakerConfig) -> Result<Self, FlowError> {
        if config.failure_threshold == 0
            || config.open_seconds == 0
            || config.half_open_requests == 0
        {
            return Err(FlowError::Configuration(
                "circuit breaker thresholds and durations must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            config,
            states: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn permit(&self, connection: &str) -> Result<(), FlowError> {
        if !self.config.enabled {
            return Ok(());
        }
        let mut states = self.states.lock().expect("circuit breaker poisoned");
        let state = states.entry(connection.to_owned()).or_insert(CircuitState {
            failures: 0,
            opened_at: None,
            half_open_in_flight: 0,
        });
        if let Some(opened_at) = state.opened_at {
            if opened_at.elapsed() < Duration::from_secs(self.config.open_seconds) {
                return Err(FlowError::CircuitOpen {
                    connection: connection.to_owned(),
                });
            }
            if state.half_open_in_flight >= self.config.half_open_requests {
                return Err(FlowError::CircuitOpen {
                    connection: connection.to_owned(),
                });
            }
            state.half_open_in_flight += 1;
        }
        Ok(())
    }

    pub fn success(&self, connection: &str) {
        if let Some(state) = self
            .states
            .lock()
            .expect("circuit breaker poisoned")
            .get_mut(connection)
        {
            state.failures = 0;
            state.opened_at = None;
            state.half_open_in_flight = 0;
        }
    }

    pub fn failure(&self, connection: &str) {
        if !self.config.enabled {
            return;
        }
        let mut states = self.states.lock().expect("circuit breaker poisoned");
        let state = states.entry(connection.to_owned()).or_insert(CircuitState {
            failures: 0,
            opened_at: None,
            half_open_in_flight: 0,
        });
        state.half_open_in_flight = 0;
        state.failures = state.failures.saturating_add(1);
        if state.failures >= self.config.failure_threshold {
            state.opened_at = Some(Instant::now());
        }
    }

    pub fn open_count(&self) -> usize {
        self.states
            .lock()
            .expect("circuit breaker poisoned")
            .values()
            .filter(|state| state.opened_at.is_some())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_threshold_and_closes_after_success() {
        let breakers = CircuitBreakers::new(CircuitBreakerConfig {
            failure_threshold: 2,
            open_seconds: 60,
            ..CircuitBreakerConfig::default()
        })
        .unwrap();
        breakers.failure("db");
        assert!(breakers.permit("db").is_ok());
        breakers.failure("db");
        assert!(breakers.permit("db").is_err());
        breakers.success("db");
        assert!(breakers.permit("db").is_ok());
    }

    #[test]
    fn rejects_concurrent_pressure_while_the_circuit_is_open() {
        let breakers = CircuitBreakers::new(CircuitBreakerConfig {
            failure_threshold: 1,
            open_seconds: 60,
            ..CircuitBreakerConfig::default()
        })
        .unwrap();
        breakers.failure("database:main");

        let attempts: Vec<_> = (0..64)
            .map(|_| {
                let breakers = breakers.clone();
                std::thread::spawn(move || breakers.permit("database:main").is_err())
            })
            .collect();

        assert!(attempts.into_iter().all(|attempt| attempt.join().unwrap()));
        assert_eq!(breakers.open_count(), 1);
    }
}
