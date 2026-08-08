# Fase 7: control y endurecimiento operativo

La fase 7 convierte el ejecutor en un servicio controlable y preparado para
operación continua. Su alcance es independiente del motor de base de datos: se
aplica a PostgreSQL, MySQL/MariaDB, Oracle, SQL Server y Kafka.

## 7.1 Ciclo de vida

Cada flujo publica uno de estos estados:

```text
STOPPED → STARTING → RUNNING ⇄ PAUSED
                         │         │
                         └──→ DRAINING → STOPPED

STARTING/RUNNING/DRAINING → FAILED
```

- `PAUSED` conserva las colas y deja terminar las tareas activas, pero no
  programa trabajo nuevo.
- `DRAINING` deja terminar las tareas activas y conserva en el repositorio los
  paquetes pendientes para el siguiente arranque.
- Un flujo `STOPPED` o `FAILED` puede iniciarse otra vez.

## 7.2 Apagado coordinado

`Ctrl+C` y `POST /api/v1/flows/{id}/stop` solicitan primero `DRAINING`. Jaiva
espera hasta `drain_timeout_seconds`; al agotarse, cancela la tarea únicamente
si `force_after_timeout` es `true`. Los paquetes persistidos quedan disponibles
para recuperación.

## 7.3 Circuit breaker

Cada conexión de salida tiene un circuito independiente:

- `database:{nombre}` para escrituras de base de datos.
- `kafka:{nombre}` para publicaciones Kafka.

Al alcanzar `failure_threshold`, el circuito se abre y rechaza temporalmente
las operaciones para no saturar un destino caído. Después de `open_seconds`
permite un número limitado de pruebas en estado semiabierto. Una operación
correcta cierra el circuito.

Métricas:

```text
jaiva_circuit_breaker_rejections_total
jaiva_circuit_breakers_open
```

## 7.4 Salud y disponibilidad

- `GET /health` indica que el proceso HTTP está vivo.
- `GET /ready` devuelve `200` cuando el flujo está `RUNNING` o `PAUSED`.
- `GET /ready` devuelve `503` en `STOPPED`, `STARTING`, `DRAINING` o `FAILED`.

La construcción del flujo valida la configuración y abre los pools de
PostgreSQL/MySQL utilizados. Los fallos posteriores de destinos se reflejan en
reintentos, circuit breaker, estado y métricas.

## 7.5 API administrativa

Todas las rutas `/api/v1/*` requieren:

```http
Authorization: Bearer <token>
```

El token se lee de `engine.admin.token_env`; no se guarda en YAML ni se
imprime en los logs.

Durante desarrollo local puede configurarse `authentication: none`. Este modo
solo arranca si el servidor escucha en loopback; el valor predeterminado y
recomendado para operación continúa siendo `bearer`.

Si eliges `bearer` y falta `JAIBA_ADMIN_TOKEN`, el arranque **falla** (incluso
en loopback). No hay degradación silenciosa a `none`. Ver
[priority-9a-admin-hardening.md](priority-9a-admin-hardening.md).

| Método | Ruta | Acción |
|---|---|---|
| `GET` | `/api/v1/flows` | Lista el flujo configurado |
| `GET` | `/api/v1/flows/{id}` | Estado y métricas |
| `POST` | `/api/v1/flows/validate` | Valida un YAML sin reemplazar el flujo |
| `PUT` | `/api/v1/flows/{id}?start=true|false` | Publica un YAML validado |
| `POST` | `/api/v1/flows/{id}/start` | Inicia o reinicia |
| `POST` | `/api/v1/flows/{id}/pause` | Pausa programación nueva |
| `POST` | `/api/v1/flows/{id}/resume` | Reanuda |
| `POST` | `/api/v1/flows/{id}/drain` | Drena |
| `POST` | `/api/v1/flows/{id}/stop` | Drena y detiene |
| `GET` | `/api/v1/provenance?limit=100` | Procedencia reciente |
| `GET` | `/api/v1/provenance?packet_id=...` | Historial de paquete |
| `GET` | `/api/v1/dead-letter?limit=100` | Paquetes agotados |
| `POST` | `/api/v1/dead-letter/{queue_id}/replay` | Reencola un paquete |

Una transición inválida devuelve `409`, un flujo inexistente `404`, un token
inválido `401` y una API o repositorio no disponible `503`.

## 7.6 Seguridad y auditoría

- Bearer token obligatorio para control, provenance y dead-letter.
- Límite configurable del cuerpo HTTP.
- Credenciales de bases y Kafka únicamente mediante variables de entorno.
- Cada mutación administrativa genera un evento estructurado
  `audit_action` en los logs persistentes.
- `/health`, `/ready`, `/metrics` y `/ws` se mantienen sin autenticación para
  sondas y recopiladores. En una red no confiable deben publicarse detrás de
  TLS, firewall o reverse proxy.

## 7.7 Pruebas de resistencia y recuperación

La suite automatizada cubre:

- pausa y reanudación sin perder el paquete;
- drain de una tarea activa sin programar el trabajo pendiente;
- reinicio de un flujo terminado;
- presión concurrente contra un circuito abierto;
- límites de memoria y backpressure;
- recuperación del repositorio y dead-letter/provenance;
- compilación de PostgreSQL, MySQL, Oracle, SQL Server y Kafka.

Comandos de aceptación:

```bash
cargo fmt --check
cargo test
cargo test --all-features
cargo doc --all-features --no-deps
```
