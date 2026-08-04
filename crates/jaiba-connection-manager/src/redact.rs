//! Redacción de credenciales y URIs en mensajes expuestos al cliente.

use std::sync::OnceLock;

use regex::Regex;

/// Elimina o enmascara fragmentos que suelen filtrar secretos (URI con userinfo,
/// `password=`, tokens largos). El mensaje original debe quedar solo en logs
/// del servidor.
pub fn redact_sensitive(input: &str) -> String {
    let mut text = input.to_owned();
    for (pattern, replacement) in redaction_patterns() {
        text = pattern.replace_all(&text, *replacement).into_owned();
    }
    text
}

fn redaction_patterns() -> &'static [(Regex, &'static str)] {
    static PATTERNS: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            // esquemas://usuario:secreto@host
            (r"(?i)([a-z][a-z0-9+.-]*://[^:/?#\s]+:)[^@/\s]+@", "$1***@"),
            // password=... / pwd=... / pass=...
            (
                r"(?i)((?:password|passwd|pwd|pass|secret|token|api[_-]?key)\s*[=:]\s*)([^\s,&;]+)",
                "$1***",
            ),
            // user:pass@host sin esquema
            (r"(?i)(\b[\w.%+-]+:)[^@\s/]+@", "$1***@"),
        ]
        .into_iter()
        .map(|(pattern, replacement)| (Regex::new(pattern).expect("redaction regex"), replacement))
        .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_uri_userinfo() {
        let raw = "error connecting to postgres://dma:S3cret!@127.0.0.1:5432/db";
        let clean = redact_sensitive(raw);
        assert!(!clean.contains("S3cret"));
        assert!(clean.contains("postgres://dma:***@"));
    }

    #[test]
    fn redacts_password_assignment() {
        let raw = "login failed password=hunter2 for user";
        let clean = redact_sensitive(raw);
        assert!(!clean.contains("hunter2"));
        assert!(clean.contains("password="));
    }
}
