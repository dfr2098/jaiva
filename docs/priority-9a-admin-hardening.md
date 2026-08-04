# Fase 9A — Endurecimiento del control plane

Complementa la [fase 7](priority-7-control-plane.md) sin introducir SSO/OAuth.
El objetivo es cerrar bypasses de desarrollo, completar auditoría y evitar
fugas de secretos en respuestas HTTP.

## Cambios

### Autenticación

| Antes (riesgo) | Ahora |
|---|---|
| Bearer sin `JAIBA_ADMIN_TOKEN` en loopback degradaba a `none` | **Falla al arrancar**. Dev local: `authentication: none` o `JAIBA_ADMIN_AUTH=none` |
| Comparación Bearer con `==` | Comparación en tiempo constante |
| `/runtime` y `/ws*` siempre abiertos | En bind **no loopback** exigen Bearer (header o `?access_token=`) |

`authentication: none` sigue permitido **solo** en bind loopback.

### Auditoría

- `audit_action` en `POST /api/v1/flows/validate` (cuerpo).
- Mutaciones de conexiones: `connection_create|update|delete|duplicate|test`.
- Campo `actor`: `bearer` / `none`.
- File audit (`data/audit.log` con `JAIBA_MASTER_KEY`) incluye actor `api` y acción `tested`.

### Secretos

- Errores de plugin/driver se **redactan** en JSON al cliente; el detalle queda en logs.
- Status de `test` no reenvía URIs con password.
- `duplicate` clona el secreto a una nueva `secret_ref` (borrar uno no rompe el otro).

## Checklist operativo

1. Producción: bind `127.0.0.1` detrás de reverse proxy **o** `0.0.0.0` con Bearer + token fuerte.
2. `export JAIBA_ADMIN_TOKEN='…'` (mín. 32 bytes aleatorios).
3. `export JAIBA_MASTER_KEY='…'` para secretos/perfiles persistentes + `audit.log`.
4. No uses `authentication: none` fuera de loopback (el arranque lo rechaza).
5. UI: token en `sessionStorage`; WebSocket añade `access_token` cuando hay token.
6. Revisar logs: no deben aparecer passwords en respuestas API; sí pueden aparecer en logs de servidor tras fallo de driver (antes de redactar al cliente).

## Desarrollo local

Los ejemplos `visualisa-flow.yaml` / `continuous-interval-flow.yaml` ya traen
`authentication: none` para loopback:

```bash
cargo run -- serve examples/visualisa-flow.yaml
# API admin abierta solo en 127.0.0.1
```

Con Bearer explícito:

```bash
export JAIBA_ADMIN_TOKEN=dev-token
# YAML con authentication: bearer (default del motor)
cargo run -- serve examples/basic-flow.yaml
```

## Relación con 9B / 10B

El desktop Tauri sigue el mismo contrato API. Ver
[priority-9b-tauri-desktop.md](priority-9b-tauri-desktop.md).

TLS nativo, roles y proyectos: [priority-10b-security.md](priority-10b-security.md).
