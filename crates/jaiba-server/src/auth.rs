//! Autenticación y autorización del control plane (fase 10B).
//!
//! - Compat: un solo `JAIBA_ADMIN_TOKEN` ⇒ actor `bearer`, rol `admin`.
//! - Multi-usuario: `JAIBA_ADMIN_USERS_FILE` (JSON) con rol + proyectos.
//! - Sin SSO/OAuth (fuera de alcance).

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use jaiba_runtime::error::FlowError;

/// Rol administrativo (orden: viewer < operator < admin).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Operator => "operator",
            Self::Admin => "admin",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "viewer" | "read" => Some(Self::Viewer),
            "operator" | "ops" => Some(Self::Operator),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }
}

/// Capacidad requerida por una ruta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Lectura de flujos, provenance, metadatos, runtime.
    Read,
    /// Ciclo de vida / deploy / validate / DLQ replay.
    Operate,
    /// Mutaciones de conexiones y secretos.
    Admin,
}

impl Permission {
    fn min_role(self) -> Role {
        match self {
            Self::Read => Role::Viewer,
            Self::Operate => Role::Operator,
            Self::Admin => Role::Admin,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub actor: String,
    pub role: Role,
    /// `*` o lista de flow_id permitidos.
    pub projects: Vec<String>,
}

impl AuthContext {
    pub fn allows_project(&self, flow_id: &str) -> bool {
        self.projects
            .iter()
            .any(|entry| entry == "*" || entry == flow_id)
    }

    pub fn has_permission(&self, permission: Permission) -> bool {
        self.role >= permission.min_role()
    }
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub id: String,
    pub role: Role,
    pub projects: Vec<String>,
    /// Secreto en claro o `sha256:<hex>` del token presentado.
    pub token_secret: String,
}

#[derive(Debug, Deserialize)]
struct UsersFile {
    users: Vec<UserRecord>,
}

#[derive(Debug, Deserialize)]
struct UserRecord {
    id: String,
    role: String,
    token: String,
    #[serde(default = "default_all_projects")]
    projects: Vec<String>,
}

fn default_all_projects() -> Vec<String> {
    vec!["*".to_owned()]
}

/// Carga usuarios desde JSON. Tokens nunca se loguean.
pub fn load_users_file(path: &Path) -> Result<Vec<Principal>, FlowError> {
    let raw = fs::read_to_string(path).map_err(|error| {
        FlowError::Configuration(format!(
            "no se pudo leer JAIBA_ADMIN_USERS_FILE '{}': {error}",
            path.display()
        ))
    })?;
    let file: UsersFile = serde_json::from_str(&raw).map_err(|error| {
        FlowError::Configuration(format!(
            "JAIBA_ADMIN_USERS_FILE inválido ({}): {error}",
            path.display()
        ))
    })?;
    if file.users.is_empty() {
        return Err(FlowError::Configuration(
            "JAIBA_ADMIN_USERS_FILE no contiene usuarios".to_owned(),
        ));
    }
    let mut principals = Vec::with_capacity(file.users.len());
    for user in file.users {
        let id = user.id.trim().to_owned();
        if id.is_empty() {
            return Err(FlowError::Configuration(
                "usuario sin id en JAIBA_ADMIN_USERS_FILE".to_owned(),
            ));
        }
        let role = Role::parse(&user.role).ok_or_else(|| {
            FlowError::Configuration(format!(
                "rol inválido '{}' para usuario '{id}' (viewer|operator|admin)",
                user.role
            ))
        })?;
        let token = user.token.trim().to_owned();
        if token.is_empty() {
            return Err(FlowError::Configuration(format!(
                "usuario '{id}' sin token"
            )));
        }
        let projects = if user.projects.is_empty() {
            default_all_projects()
        } else {
            user.projects
        };
        principals.push(Principal {
            id,
            role,
            projects,
            token_secret: token,
        });
    }
    Ok(principals)
}

pub fn match_principal<'a>(principals: &'a [Principal], presented: &str) -> Option<&'a Principal> {
    principals
        .iter()
        .find(|principal| token_matches(&principal.token_secret, presented))
}

fn token_matches(secret: &str, presented: &str) -> bool {
    if let Some(expected_hex) = secret.strip_prefix("sha256:") {
        let digest = Sha256::digest(presented.as_bytes());
        let actual = to_hex(&digest);
        let expected = expected_hex.trim().to_ascii_lowercase();
        return constant_time_eq(actual.as_bytes(), expected.as_bytes());
    }
    constant_time_eq(secret.as_bytes(), presented.as_bytes())
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Respuesta de `GET /api/v1/whoami`.
#[derive(Debug, Serialize)]
pub struct WhoAmI {
    pub actor: String,
    pub role: &'static str,
    pub projects: Vec<String>,
    pub authentication: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_ordering() {
        assert!(Role::Viewer < Role::Operator);
        assert!(Role::Operator < Role::Admin);
        assert!(
            AuthContext {
                actor: "x".into(),
                role: Role::Operator,
                projects: vec!["*".into()],
            }
            .has_permission(Permission::Operate)
        );
        assert!(
            !AuthContext {
                actor: "x".into(),
                role: Role::Viewer,
                projects: vec!["*".into()],
            }
            .has_permission(Permission::Operate)
        );
    }

    #[test]
    fn project_allowlist() {
        let ctx = AuthContext {
            actor: "bob".into(),
            role: Role::Operator,
            projects: vec!["alpha".into(), "beta".into()],
        };
        assert!(ctx.allows_project("alpha"));
        assert!(!ctx.allows_project("gamma"));
    }

    #[test]
    fn sha256_token_match() {
        let digest = to_hex(&Sha256::digest(b"secret-token"));
        let secret = format!("sha256:{digest}");
        assert!(token_matches(&secret, "secret-token"));
        assert!(!token_matches(&secret, "wrong"));
    }
}
