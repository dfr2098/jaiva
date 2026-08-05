use std::time::Duration;

use crate::error::MemoryError;

/// Parsea duraciones estilo `30s`, `5m`, `2h`, `1d`.
pub fn parse_duration(value: &str) -> Result<Duration, MemoryError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(MemoryError::Configuration(
            "duración vacía; usa p. ej. 30s, 5m, 2h".to_owned(),
        ));
    }
    let (number, unit) = value.split_at(value.len().saturating_sub(1));
    let amount: u64 = number.parse().map_err(|_| {
        MemoryError::Configuration(format!(
            "duración inválida '{value}'; usa p. ej. 30s, 5m, 2h"
        ))
    })?;
    let secs = match unit {
        "s" => amount,
        "m" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(3_600),
        "d" => amount.saturating_mul(86_400),
        _ => {
            return Err(MemoryError::Configuration(format!(
                "unidad de duración desconocida en '{value}' (usa s|m|h|d)"
            )));
        }
    };
    if secs == 0 {
        return Err(MemoryError::Configuration(
            "la duración debe ser mayor que cero".to_owned(),
        ));
    }
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_units() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7_200));
    }

    #[test]
    fn rejects_bad_input() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("5").is_err());
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("10x").is_err());
    }
}
