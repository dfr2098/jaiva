# Pruebas del stack Estable (release-core)

Guía de **stress**, **soak** y **chaos** contra el stack Docker
Postgres + Jaiba serve + UI + Prometheus/Grafana.

Prerrequisito siempre:

```bash
./scripts/release-core-up.sh
# API: http://127.0.0.1:19090
# Grafana: http://127.0.0.1:13000/d/jaiba-runtime-jme
```

Flow de carga: [`examples/stable-runtime-stress.yaml`](../examples/stable-runtime-stress.yaml)
(`query_postgres` → `encode_json`, con `retry` y `scheduling.timeout_ms`).
Una segunda rama `memory_upsert` escribe cada lote en JME (política embebida,
Cold bajo `JAIBA_DATA_DIR/jme/cold/stable-runtime-stress`). El stress falla si
JME no registra objetos; el soak y el chaos fallan con cualquier `failed > 0`,
incluidos los de esa rama.

`DATABASE_URL` del contenedor Jaiba usa el hostname
`jaiba_stable_postgres` (no solo el alias Compose `postgres`), para que un
`docker network disconnect/connect` no deje el DNS roto.

---

## Escalera recomendada

| Orden | Prueba | Script | Objetivo | Duración típica |
| --- | --- | --- | --- | --- |
| 1 | Stress | `stress-stable-stack.sh` | Un ciclo limpio + métricas Prometheus | &lt; 5 min |
| 2 | Soak | `soak-stable-stack.sh` | Carga sostenida sin caos | 1 h (default) |
| 3 | Chaos v1 | `chaos-soak-stable-stack.sh` | Fallos suaves + watchdog stall | 1 h |
| 4 | Chaos v2 | `CHAOS_PROFILE=v2 …` | Outage DB, kill, net_flap | 1 h |
| 5 | net_flap only | `CHAOS_ACTIONS=net_flap` | Validar DNS/reconnect aislado | 10 min |

No saltes a chaos v2 si stress/soak no pasan.

---

## 1. Stress (un ciclo)

```bash
STRESS_ROWS=250000 ./scripts/stress-stable-stack.sh
```

| Variable | Default | Rango / notas |
| --- | --- | --- |
| `STRESS_ROWS` | `250000` | 1 … 10 000 000 |

**PASS**

- Estado final `STOPPED` (no `FAILED`)
- `failed == 0`
- Termina antes de 300 s

**FAIL**

- `FAILED` o `failed != 0`
- Timeout 300 s sin terminar

Al final consulta Prometheus
`jaiva_processor_records_total{flow="stable-runtime-stress"}`.

---

## 2. Soak (carga sostenida)

```bash
SOAK_DURATION_SECONDS=3600 SOAK_ROWS_PER_CYCLE=250000 \
  ./scripts/soak-stable-stack.sh
```

| Variable | Default | Rango / notas |
| --- | --- | --- |
| `SOAK_DURATION_SECONDS` | `3600` | 60 … 86400 |
| `SOAK_ROWS_PER_CYCLE` | `250000` | 1 … 10 000 000 |

Comportamiento: despliega el flow, en cada `STOPPED` hace `trigger` del
siguiente ciclo, detiene con stop externo al acabar (o `Ctrl+C`).

**PASS**

- Completa la duración sin abortar
- Nunca `FAILED` ni `failed > 0` durante el bucle

**FAIL**

- Cualquier `FAILED` o `failed > 0` → aborta de inmediato

No hay watchdog de stall: un hang silencioso con `failed=0` **no** falla el
soak. Para eso usar chaos.

---

## 3. Chaos soak (Fase B / v1 y Fase C / v2)

```bash
# Fase B — default v1, 1 h
CHAOS_SEED=42 ./scripts/chaos-soak-stable-stack.sh

# Fase C — v2, 1 h
CHAOS_PROFILE=v2 CHAOS_SEED=7 ./scripts/chaos-soak-stable-stack.sh

# Corto 10 min (validación de script)
CHAOS_SEED=42 \
SOAK_DURATION_SECONDS=600 \
CHAOS_INTERVAL_SECONDS=60 \
CHAOS_RECOVERY_SECONDS=60 \
STALL_SECONDS=120 \
  ./scripts/chaos-soak-stable-stack.sh

# Solo net_flap (Fase C aislada)
CHAOS_PROFILE=v2 CHAOS_ACTIONS=net_flap \
SOAK_DURATION_SECONDS=600 CHAOS_INTERVAL_SECONDS=90 \
CHAOS_RECOVERY_SECONDS=60 STALL_SECONDS=300 \
  ./scripts/chaos-soak-stable-stack.sh

# Auditoría full — todas las acciones v2 (round-robin) + reporte JSON
CHAOS_PROFILE=v2 CHAOS_MODE=coverage CHAOS_SEED=20260809 \
CHAOS_REPORT_DIR=./artifacts/chaos-audit \
  ./scripts/chaos-soak-stable-stack.sh
```

`CHAOS_MODE=coverage` recorre **todas** las acciones de la lista (no RNG).
Con `CHAOS_REQUIRE_FULL_COVERAGE=1` (default en coverage) el run **FAIL**
si alguna acción quedó en 0 hits.

Artefactos en `CHAOS_REPORT_DIR/<run_id>/`:

| Archivo | Contenido |
| --- | --- |
| `manifest.env` | parámetros del run + git head |
| `flow.yaml` | flow desplegado |
| `events.log` | log humano de caos |
| `events.jsonl` | un JSON por evento (inject/skip/defer) |
| `report.json` | veredicto, cobertura, métricas |
| `SUMMARY.md` | resumen legible |
### Variables

| Variable | Default v1 | Default v2 | Notas |
| --- | --- | --- | --- |
| `CHAOS_PROFILE` | `v1` | — | `v1` \| `v2` |
| `CHAOS_MODE` | `random` | `random` | `random` \| `coverage` (round-robin todas) |
| `CHAOS_SEED` | `1` | `1` | RNG o offset de orden en coverage |
| `CHAOS_REPORT_DIR` | (tmp) | (tmp) | Si se setea → `report.json` + `events.jsonl` + `SUMMARY.md` |
| `CHAOS_REQUIRE_FULL_COVERAGE` | `0` | `0` | En coverage default `1`: FAIL si falta alguna acción |
| `SOAK_DURATION_SECONDS` | `3600` | `3600` | 120 … 86400 |
| `SOAK_ROWS_PER_CYCLE` | `250000` | `250000` | 1 … 10 000 000 |
| `CHAOS_INTERVAL_SECONDS` | `120` | `150` | Tiempo entre eventos |
| `CHAOS_RECOVERY_SECONDS` | `300` | `300` | Calma final **sin** caos; debe ser &lt; duración |
| `CHAOS_DB_PAUSE_SECONDS` | `20` | `20` | Duración `db_blip` |
| `CHAOS_DB_OUTAGE_SECONDS` | `120` | `120` | Duración `db_outage` |
| `CHAOS_NET_FLAP_SECONDS` | `30` | `30` | Tiempo desconectado de red |
| `STALL_SECONDS` | `180` | `300` | Sin progreso → FAIL; mínimo 30; en v2 debe ser **&gt;** outage DB |
| `CHAOS_ACTIONS` | (lista v1) | (lista v2) | Override CSV; vacío = lista del profile |
| `POSTGRES_CONTAINER` | `jaiba_stable_postgres` | igual | |
| `JAIBA_CONTAINER` | `jaiba_stable_server` | igual | |

### Acciones por profile

| Acción | v1 | v2 | Qué hace |
| --- | --- | --- | --- |
| `db_blip` | sí | sí | `docker pause` Postgres corto → `unpause` |
| `db_outage` | no | sí | pause Postgres largo |
| `flow_bounce` | sí | sí | `stop` → `start`/`trigger` |
| `runtime_restart` | sí | sí | reinicia contenedor Jaiba + redespliega flow |
| `runtime_kill` | no | sí | `docker kill` + start + redespliegue |
| `net_flap` | no | sí | disconnect/connect red Docker de Postgres **con alias DNS** |
| `double_stop` | sí | sí | dos `stop` y reactiva |
| `noop` | sí | sí | control (sin fallo) |

Lista default v1:
`db_blip,flow_bounce,runtime_restart,double_stop,noop`

Lista default v2:
`db_blip,db_outage,flow_bounce,runtime_restart,runtime_kill,net_flap,double_stop,noop`

### Condiciones operativas del script

Antes de inyectar caos:

1. Espera API (`/api/v1/flows`).
2. **Heal DNS**: si Postgres no resuelve desde Jaiba, reattach con
   `--alias postgres` (recupera stacks rotos por un flap anterior).
3. Despliega el flow y espera **`RUNNING` o `STOPPED`** (máx 180 s).
   Si queda en `STARTING` → **FAIL** inmediato (no empieza el caos).

Durante el caos:

- Solo dispara acciones si el estado es `RUNNING` o `STOPPED`.
- Si está en `STARTING`: **SKIP** / aplaza el evento (no redespliega; eso
  abortaría los retries de connect).
- Tras caos: `watchdog_grace` (no pone a cero el baseline de progreso).
- `ensure_progressing` solo reactiva en `STOPPED` / `FAILED` / `UNKNOWN`;
  **nunca** redespliega en `STARTING`.

`net_flap` específico:

1. `docker network disconnect` de Postgres.
2. Espera `CHAOS_NET_FLAP_SECONDS`.
3. `docker network connect --alias postgres` (y `jaiba_stable_postgres`).
4. Espera DNS + `pg_isready` reachable desde el stack.
5. Si falla, intenta `heal_postgres_dns` otra vez.

**Por qué importa el alias:** un `connect` sin `--alias` pierde el nombre
Compose `postgres`. Con `DATABASE_URL=@postgres` el flow se queda en
`STARTING` con `Name or service not known`. El compose estable ya apunta a
`jaiba_stable_postgres`; el script igual restaura el alias por compatibilidad.

### PASS / FAIL (chaos)

**PASS** (`exit 0`) cuando se cumplen todas:

| Condición | Detalle |
| --- | --- |
| Sin stall de progreso | En `STALL_SECONDS` debe subir `ciclos` **o** `registros` |
| Sin stall en `STARTING` | No permanecer `STARTING` ≥ `STALL_SECONDS` |
| Trabajo útil | `ciclos >= 1` al final |
| Stop limpio | Tras stop externo, runtime limpio (si `runtime` sigue presente, reintenta stop; si no limpia → FAIL) |
| No `FAILED` final | `estado_final != FAILED` |

Mensaje típico: `PASS: sin stall detectado; stop externo aplicado.`

**FAIL** (`exit 1`) si:

| Condición | Mensaje / causa típica |
| --- | --- |
| No llega a RUNNING/STOPPED en 180 s al inicio | DNS roto / Postgres inalcanzable / pool sin conectar |
| Stall de ciclos/registros | Hang silencioso (`failed=0` pero sin progreso) |
| Stall en `STARTING` | Connect retries agotados o DNS aún roto |
| `ciclos=0` | Caos demasiado pronto o flow nunca ejecutó |
| `estado_final=FAILED` | Recuperación insuficiente |
| Runtime no limpio tras stop | Supervisor dejó runtime presente / estado raro |

Notas:

- Tras `runtime_restart` / `runtime_kill` las métricas se reinician; el
  script detecta el reset y **no** lo cuenta como hang.
- `estado_final=UNKNOWN` con `runtime=null` tras el stop es **normal**
  (cleanup); no es FAIL por sí solo.
- El log de eventos queda en `/tmp/jaiba-chaos-events.*.log`.

---

## Matriz de validación (resultados de referencia)

Ejecutada contra release-core local (stack `jaiba_stable_*`). Valores
orientativos; lo importante es **PASS/FAIL** y `fallos=0`.

| Prueba | Condiciones | Resultado esperado |
| --- | --- | --- |
| Soak 1 h | default | PASS; miles de ciclos; `failed=0` |
| Chaos v1 1 h | `CHAOS_SEED` fijo | PASS; ~decenas de eventos; sin stall |
| Chaos v2 1 h | profile v2 | PASS; incluye `db_outage` + `runtime_kill` |
| net_flap only 10 min | `CHAOS_ACTIONS=net_flap`, recovery 60 s | PASS; varios flaps; ciclos &gt; 0; progreso tras cada reconnect |

Ejemplo real (net_flap only, 600 s, 5 flaps): ~414 ciclos, ~103 M registros,
`fallos=0`, `exit=0`.

---

## Smoke relacionados (CI / producto)

No son chaos, pero cierran el camino Estable:

| Script | Qué valida |
| --- | --- |
| `scripts/smoke-release-core.sh` | Binario/features release-core |
| `scripts/smoke-stable-path.sh` | Postgres → CSV en el stack Compose |
| `scripts/smoke-regression.sh` | Suite e2e de regresión (~14) |

Freeze de roadmap: ver [release-core.md](release-core.md#congelar-roadmap).

---

## Observabilidad

- Grafana: `http://127.0.0.1:13000/d/jaiba-runtime-jme`
- Prometheus: `http://127.0.0.1:19091` (`up{job="jaiba"}` = 1)
- Métricas live: `http://127.0.0.1:19090/metrics`

Durante chaos conviene mirar:

- Estado del flow (`RUNNING` / `STOPPED` / `STARTING`)
- Contadores de ciclos y registros (no solo `failed`)
- Logs del contenedor: `transient PostgreSQL connect failure; retrying`

---

## Checklist rápido antes de reportar PASS

1. `./scripts/release-core-up.sh` saludable (API 19090).
2. Stress PASS.
3. Soak (al menos 10–60 min) sin `FAILED`.
4. Chaos v1 PASS.
5. Chaos v2 PASS **o** al menos `CHAOS_ACTIONS=net_flap` PASS con ciclos &gt; 0.
6. Tras flaps: `getent hosts jaiba_stable_postgres` (y/o `postgres`) OK
   desde `jaiba_stable_server`.
