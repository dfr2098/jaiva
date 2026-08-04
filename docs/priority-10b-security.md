# Fase 10B — Seguridad restante (TLS, roles, proyectos)

Complementa la [fase 9A](priority-9a-admin-hardening.md) sin SSO/OAuth.
Objetivo: HTTPS opcional, varios actores Bearer con roles y allowlist de flujos
(“proyectos”).

## Alcance

| Incluido | Fuera |
|---|---|
| HTTPS con PEM (`JAIBA_TLS_CERT_FILE` + `JAIBA_TLS_KEY_FILE`) | mTLS / ACME automático |
| Fichero de usuarios + roles `viewer` / `operator` / `admin` | SSO, OIDC, LDAP |
| Allowlist de proyectos (`flow_id` o `*`) | Multi-tenant con aislamiento de datos |
| `GET /api/v1/whoami` | UI completa de gestión de usuarios |
| Token único `JAIBA_ADMIN_TOKEN` (compat = admin global) | |

## Roles

| Rol | Puede |
|---|---|
| `viewer` | Leer flujos, versiones, provenance, DLQ, conexiones, metadatos, whoami |
| `operator` | + deploy / start / stop / validate / trigger / replay DLQ |
| `admin` | + crear/editar/borrar/probar conexiones (secretos) |

`authentication: none` (solo loopback) sigue equivaliendo a admin completo.

## Usuarios

```bash
export JAIBA_ADMIN_USERS_FILE=examples/admin-users.json
# Opcional: sigue siendo admin global adicional
export JAIBA_ADMIN_TOKEN=bootstrap-token
JAIBA_ADMIN_AUTH=bearer cargo run -- serve examples/visualisa-flow.yaml
```

Formato (`examples/admin-users.json`):

```json
{
  "users": [
    {
      "id": "ops",
      "role": "operator",
      "token": "dev-ops-token",
      "projects": ["*"]
    },
    {
      "id": "viewer",
      "role": "viewer",
      "token": "sha256:<hex del token en claro>",
      "projects": ["visualisa-example"]
    }
  ]
}
```

El cliente sigue enviando `Authorization: Bearer <token en claro>`. Si el
campo `token` del fichero empieza por `sha256:`, se compara el hash SHA-256 del
Bearer presentado.

## TLS

```bash
# Certificado y clave PEM
export JAIBA_TLS_CERT_FILE=/etc/jaiba/tls/cert.pem
export JAIBA_TLS_KEY_FILE=/etc/jaiba/tls/key.pem
export JAIBA_ADMIN_TOKEN=...
cargo run -- serve examples/basic-flow.yaml
# Escucha HTTPS en el mismo bind (p. ej. 127.0.0.1:9090)
```

Sin esas variables el servidor sigue en HTTP (comportamiento 9A).

Autofirmado de desarrollo:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout /tmp/jaiba-key.pem -out /tmp/jaiba-cert.pem \
  -subj "/CN=localhost"
```

## whoami

```bash
curl -s -H "Authorization: Bearer dev-ops-token" \
  http://127.0.0.1:9090/api/v1/whoami
# {"actor":"ops","role":"operator","projects":["*"],"authentication":"users"}
```

## Checklist ops

1. Producción: HTTPS (TLS en Jaiba **o** reverse proxy) + Bearer fuerte.
2. Preferir `JAIBA_ADMIN_USERS_FILE` con tokens `sha256:` (no secretos en claro en disco si puedes evitarlo).
3. Restringir `projects` por operador; `*` solo para admins de confianza.
4. No uses `authentication: none` fuera de loopback.
5. UI: el token en `sessionStorage` sigue siendo el Bearer; whoami refleja rol.

## Relación

- 9A: defaults, redacción, auditoría base.
- 10A: desktop sidecar (mismo contrato API; CSP loopback; HTTPS remoto vía proxy).
- 10B: este documento.
