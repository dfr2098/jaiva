#!/usr/bin/env bash
# Chaos soak Jaiba — Fase B (v1) y Fase C / v2 (fallos más duros).
#
# Requiere el stack release-core levantado (mismo que soak-stable-stack.sh).
#
# Fase B (default, 1 h):
#   CHAOS_SEED=42 ./scripts/chaos-soak-stable-stack.sh
#
# Fase C / v2 (1 h, corte DB largo + kill + flap de red):
#   CHAOS_PROFILE=v2 CHAOS_SEED=7 ./scripts/chaos-soak-stable-stack.sh
#
# Auditoría full (todas las acciones v2, round-robin, reporte JSON):
#   CHAOS_PROFILE=v2 CHAOS_MODE=coverage CHAOS_SEED=20260809 \
#   CHAOS_REPORT_DIR=./artifacts/chaos-audit \
#     ./scripts/chaos-soak-stable-stack.sh
#
# Ejemplo corto (10 min):
#   CHAOS_SEED=42 \
#   SOAK_DURATION_SECONDS=600 \
#   CHAOS_INTERVAL_SECONDS=60 \
#   CHAOS_RECOVERY_SECONDS=60 \
#   STALL_SECONDS=120 \
#     ./scripts/chaos-soak-stable-stack.sh
#
# Vars:
#   CHAOS_PROFILE           v1 (default) | v2
#   CHAOS_MODE              random (default) | coverage (round-robin todas)
#   CHAOS_SEED              semilla RNG (default: 1); en coverage fija el orden base
#   CHAOS_REPORT_DIR        si se setea, escribe report.json + events.jsonl + events.log
#   CHAOS_REQUIRE_FULL_COVERAGE  1 (default en coverage) exige cada acción ≥1 vez
#   CHAOS_INTERVAL_SECONDS  segundos entre eventos (v1:120, v2:150)
#   CHAOS_RECOVERY_SECONDS  calma final (default: 300)
#   CHAOS_DB_PAUSE_SECONDS  pause corto Postgres / db_blip (default: 20)
#   CHAOS_DB_OUTAGE_SECONDS pause largo / db_outage (default: 120)
#   CHAOS_NET_FLAP_SECONDS  disconnect red Postgres (default: 30)
#   CHAOS_ACTIONS           override lista (coma). Si vacío, según profile.
#   STALL_SECONDS           sin progreso → FAIL (v1:180, v2:300)
#   SOAK_DURATION_SECONDS   default: 3600
#   SOAK_ROWS_PER_CYCLE     default: 250000
#   POSTGRES_CONTAINER      default: jaiba_stable_postgres
#   JAIBA_CONTAINER         default: jaiba_stable_server
#
# Acciones v1: db_blip,flow_bounce,runtime_restart,double_stop,noop
# Acciones v2 (+): db_outage,runtime_kill,net_flap
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$ROOT/deploy/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE="$ROOT/deploy/.env.example"
fi

# shellcheck disable=SC1090
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

PROFILE="${CHAOS_PROFILE:-v1}"
case "$PROFILE" in
  v1|v2) ;;
  *) echo "CHAOS_PROFILE debe ser v1 o v2" >&2; exit 2 ;;
esac

CHAOS_MODE="${CHAOS_MODE:-random}"
case "$CHAOS_MODE" in
  random|coverage) ;;
  *) echo "CHAOS_MODE debe ser random o coverage" >&2; exit 2 ;;
esac

DURATION="${SOAK_DURATION_SECONDS:-3600}"
ROWS="${SOAK_ROWS_PER_CYCLE:-250000}"
SEED="${CHAOS_SEED:-1}"
RECOVERY_SECONDS="${CHAOS_RECOVERY_SECONDS:-300}"
DB_PAUSE_SECONDS="${CHAOS_DB_PAUSE_SECONDS:-20}"
DB_OUTAGE_SECONDS="${CHAOS_DB_OUTAGE_SECONDS:-120}"
NET_FLAP_SECONDS="${CHAOS_NET_FLAP_SECONDS:-30}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-jaiba_stable_postgres}"
JAIBA_CONTAINER="${JAIBA_CONTAINER:-jaiba_stable_server}"

if [[ "$PROFILE" == "v2" ]]; then
  CHAOS_INTERVAL="${CHAOS_INTERVAL_SECONDS:-150}"
  STALL_SECONDS="${STALL_SECONDS:-300}"
  DEFAULT_ACTIONS="db_blip,db_outage,flow_bounce,runtime_restart,runtime_kill,net_flap,double_stop,noop"
else
  CHAOS_INTERVAL="${CHAOS_INTERVAL_SECONDS:-120}"
  STALL_SECONDS="${STALL_SECONDS:-180}"
  DEFAULT_ACTIONS="db_blip,flow_bounce,runtime_restart,double_stop,noop"
fi
ACTIONS_CSV="${CHAOS_ACTIONS:-$DEFAULT_ACTIONS}"

if [[ "$CHAOS_MODE" == "coverage" ]]; then
  REQUIRE_FULL_COVERAGE="${CHAOS_REQUIRE_FULL_COVERAGE:-1}"
else
  REQUIRE_FULL_COVERAGE="${CHAOS_REQUIRE_FULL_COVERAGE:-0}"
fi

for VALUE in "$DURATION" "$ROWS" "$SEED" "$CHAOS_INTERVAL" "$RECOVERY_SECONDS" \
  "$DB_PAUSE_SECONDS" "$DB_OUTAGE_SECONDS" "$NET_FLAP_SECONDS" "$STALL_SECONDS"; do
  case "$VALUE" in
    ''|*[!0-9]*) echo "parámetros numéricos inválidos: $VALUE" >&2; exit 2 ;;
  esac
done
if (( DURATION < 120 || DURATION > 86400 )); then
  echo "SOAK_DURATION_SECONDS debe estar entre 120 y 86400" >&2
  exit 2
fi
if (( ROWS < 1 || ROWS > 10000000 )); then
  echo "SOAK_ROWS_PER_CYCLE debe estar entre 1 y 10000000" >&2
  exit 2
fi
if (( RECOVERY_SECONDS >= DURATION )); then
  echo "CHAOS_RECOVERY_SECONDS debe ser < SOAK_DURATION_SECONDS" >&2
  exit 2
fi
if (( STALL_SECONDS < 30 )); then
  echo "STALL_SECONDS mínimo 30" >&2
  exit 2
fi
if (( STALL_SECONDS <= DB_OUTAGE_SECONDS )); then
  echo "STALL_SECONDS ($STALL_SECONDS) debe ser > CHAOS_DB_OUTAGE_SECONDS ($DB_OUTAGE_SECONDS)" >&2
  exit 2
fi

command -v curl >/dev/null || { echo "se requiere curl" >&2; exit 2; }
command -v jq >/dev/null || { echo "se requiere jq" >&2; exit 2; }
command -v docker >/dev/null || { echo "se requiere docker" >&2; exit 2; }
command -v python3 >/dev/null || { echo "se requiere python3" >&2; exit 2; }

API="http://127.0.0.1:${JAIBA_API_PORT:-19090}"
TOKEN="${JAIBA_ADMIN_TOKEN:-jaiba-stable-admin-token}"
FLOW_ID="stable-runtime-stress"
FLOW_FILE="$(mktemp "${TMPDIR:-/tmp}/jaiba-chaos-soak.XXXXXX.yaml")"
RUNNING=0
DB_PAUSED=0
NET_DISCONNECTED=0
POSTGRES_NETWORK=""
CHAOS_EVENTS=0
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-pid$$"
if [[ -n "${CHAOS_REPORT_DIR:-}" ]]; then
  REPORT_DIR="${CHAOS_REPORT_DIR%/}/$RUN_ID"
  mkdir -p "$REPORT_DIR"
  CHAOS_LOG="$REPORT_DIR/events.log"
  CHAOS_JSONL="$REPORT_DIR/events.jsonl"
  : >"$CHAOS_LOG"
  : >"$CHAOS_JSONL"
else
  REPORT_DIR=""
  CHAOS_LOG="$(mktemp "${TMPDIR:-/tmp}/jaiba-chaos-events.XXXXXX.log")"
  CHAOS_JSONL="$(mktemp "${TMPDIR:-/tmp}/jaiba-chaos-events.XXXXXX.jsonl")"
fi

IFS=',' read -r -a ACTIONS <<<"$ACTIONS_CSV"
if [[ ${#ACTIONS[@]} -eq 0 ]]; then
  echo "CHAOS_ACTIONS vacío" >&2
  exit 2
fi

# Cobertura: conteo por acción ejecutada (no SKIP).
declare -A ACTION_HITS=()
for a in "${ACTIONS[@]}"; do
  ACTION_HITS["$a"]=0
done

auth_hdr=(-H "Authorization: Bearer $TOKEN")

log() { printf '\n[%s] %s\n' "$(date -Is)" "$*"; }
chaos_log() {
  local line="$*"
  printf '%s\n' "$line" >>"$CHAOS_LOG"
  log "CHAOS $line"
}

audit_event() {
  # audit_event <kind> <action> <status> [detail]
  local kind="$1" action="$2" status="$3" detail="${4:-}"
  local ts state cycles records failed
  ts="$(date -Is)"
  state="$(flow_state 2>/dev/null || echo UNKNOWN)"
  cycles="${CYCLES:-0}"
  records="${LAST_RECORDS:-0}"
  failed="${FAILED:-0}"
  python3 - "$CHAOS_JSONL" "$ts" "$kind" "$action" "$status" "$detail" \
    "$state" "$cycles" "$records" "$failed" "$CHAOS_EVENTS" "$SEED" "$PROFILE" "$CHAOS_MODE" <<'PY'
import json, sys
path, ts, kind, action, status, detail, state, cycles, records, failed, events, seed, profile, mode = sys.argv[1:]
row = {
  "ts": ts,
  "kind": kind,
  "action": action,
  "status": status,
  "detail": detail,
  "state": state,
  "cycles": int(cycles),
  "records": int(records),
  "failed": int(failed),
  "event_index": int(events),
  "seed": int(seed),
  "profile": profile,
  "mode": mode,
}
with open(path, "a", encoding="utf-8") as f:
    f.write(json.dumps(row, ensure_ascii=False) + "\n")
PY
}

cleanup() {
  if (( NET_DISCONNECTED == 1 )) && [[ -n "$POSTGRES_NETWORK" ]]; then
    docker network connect "$POSTGRES_NETWORK" "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
    NET_DISCONNECTED=0
  fi
  if (( DB_PAUSED == 1 )); then
    docker unpause "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
    DB_PAUSED=0
  fi
  if (( RUNNING == 1 )); then
    curl -fsS "${auth_hdr[@]}" -X POST \
      "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
  fi
  rm -f "$FLOW_FILE"
}
trap cleanup EXIT INT TERM

pick_action() {
  if [[ "$CHAOS_MODE" == "coverage" ]]; then
    # Round-robin determinista: orden = ACTIONS rotado por seed.
    local idx n
    n="${#ACTIONS[@]}"
    idx=$(( (SEED + CHAOS_EVENTS) % n ))
    printf '%s\n' "${ACTIONS[$idx]}"
    return 0
  fi
  python3 - "$SEED" "$CHAOS_EVENTS" "${ACTIONS[@]}" <<'PY'
import sys
seed = int(sys.argv[1])
event = int(sys.argv[2])
actions = sys.argv[3:]
x = (seed & 0xFFFFFFFF) ^ ((event + 1) * 0x9E3779B9)
x = (x ^ (x >> 16)) * 0x7FEB352D
x &= 0xFFFFFFFF
x = (x ^ (x >> 15)) * 0x846CA68B
x &= 0xFFFFFFFF
x ^= x >> 16
print(actions[x % len(actions)])
PY
}

api_flow() {
  curl -fsS "${auth_hdr[@]}" "$API/api/v1/flows/$FLOW_ID"
}

wait_api() {
  local attempts="${1:-60}" i
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${auth_hdr[@]}" "$API/api/v1/flows" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

deploy_flow() {
  curl -fsS \
    "${auth_hdr[@]}" \
    -H "Content-Type: application/yaml" \
    --data-binary "@$FLOW_FILE" \
    -X PUT "$API/api/v1/flows/$FLOW_ID?start=true" >/dev/null
  RUNNING=1
}

ensure_progressing() {
  # Solo reactivar si está parado/fallido. NO redesplegar en STARTING:
  # eso aborta el retry de connect y empeora net_flap.
  local body state
  body="$(api_flow 2>/dev/null || true)"
  [[ -n "$body" ]] || return 0
  state="$(jq -r '.runtime.control.state // "UNKNOWN"' <<<"$body")"
  case "$state" in
    STOPPED|FAILED|UNKNOWN)
      curl -fsS "${auth_hdr[@]}" -X POST \
        "$API/api/v1/flows/$FLOW_ID/start" >/dev/null 2>&1 \
        || curl -fsS "${auth_hdr[@]}" -X POST \
          "$API/api/v1/flows/$FLOW_ID/trigger" >/dev/null 2>&1 \
        || deploy_flow || true
      ;;
  esac
}

flow_state() {
  local body
  body="$(api_flow 2>/dev/null || echo '{}')"
  jq -r '.runtime.control.state // "UNKNOWN"' <<<"$body"
}

wait_until_running() {
  local timeout_s="${1:-180}" start_wait now state
  start_wait="$(date +%s)"
  log "Esperando Estado=RUNNING antes de inyectar caos (máx ${timeout_s}s)..."
  while :; do
    state="$(flow_state)"
    if [[ "$state" == "RUNNING" || "$state" == "STOPPED" ]]; then
      # STOPPED tras un ciclo completo también es progreso usable.
      ok "Flow listo para caos (estado=$state)"
      return 0
    fi
    now="$(date +%s)"
    if (( now - start_wait >= timeout_s )); then
      warn "Timeout esperando RUNNING (último estado=$state)"
      return 1
    fi
    printf '\r  waiting state=%-10s elapsed=%ss' "$state" "$((now - start_wait))"
    sleep 1
  done
}

ok() { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[aviso]\033[0m %s\n' "$*"; }


container_network() {
  docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$1" 2>/dev/null | awk '{print $1}'
}

chaos_db_blip() {
  if ! docker inspect "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
    chaos_log "db_blip SKIP (no container $POSTGRES_CONTAINER)"
    return 0
  fi
  chaos_log "db_blip pause ${DB_PAUSE_SECONDS}s ($POSTGRES_CONTAINER)"
  docker pause "$POSTGRES_CONTAINER"
  DB_PAUSED=1
  sleep "$DB_PAUSE_SECONDS"
  docker unpause "$POSTGRES_CONTAINER"
  DB_PAUSED=0
  chaos_log "db_blip unpause OK"
  sleep 3
  ensure_progressing
}

chaos_db_outage() {
  if ! docker inspect "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
    chaos_log "db_outage SKIP (no container $POSTGRES_CONTAINER)"
    return 0
  fi
  chaos_log "db_outage pause ${DB_OUTAGE_SECONDS}s ($POSTGRES_CONTAINER)"
  docker pause "$POSTGRES_CONTAINER"
  DB_PAUSED=1
  sleep "$DB_OUTAGE_SECONDS"
  docker unpause "$POSTGRES_CONTAINER"
  DB_PAUSED=0
  chaos_log "db_outage unpause OK"
  sleep 5
  ensure_progressing
}

chaos_flow_bounce() {
  chaos_log "flow_bounce stop→start"
  curl -fsS "${auth_hdr[@]}" -X POST \
    "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
  sleep 2
  curl -fsS "${auth_hdr[@]}" -X POST \
    "$API/api/v1/flows/$FLOW_ID/start" >/dev/null 2>&1 \
    || curl -fsS "${auth_hdr[@]}" -X POST \
      "$API/api/v1/flows/$FLOW_ID/trigger" >/dev/null 2>&1 \
    || deploy_flow
  chaos_log "flow_bounce OK"
}

chaos_runtime_restart() {
  if ! docker inspect "$JAIBA_CONTAINER" >/dev/null 2>&1; then
    chaos_log "runtime_restart SKIP (no container $JAIBA_CONTAINER)"
    return 0
  fi
  chaos_log "runtime_restart docker restart $JAIBA_CONTAINER"
  RUNNING=0
  docker restart "$JAIBA_CONTAINER" >/dev/null
  if ! wait_api 60; then
    echo "API no volvió tras restart de $JAIBA_CONTAINER" >&2
    exit 1
  fi
  deploy_flow
  chaos_log "runtime_restart OK (flow redeployed)"
}

chaos_runtime_kill() {
  if ! docker inspect "$JAIBA_CONTAINER" >/dev/null 2>&1; then
    chaos_log "runtime_kill SKIP (no container $JAIBA_CONTAINER)"
    return 0
  fi
  chaos_log "runtime_kill docker kill+start $JAIBA_CONTAINER"
  RUNNING=0
  docker kill "$JAIBA_CONTAINER" >/dev/null 2>&1 || true
  sleep 2
  docker start "$JAIBA_CONTAINER" >/dev/null
  if ! wait_api 90; then
    echo "API no volvió tras kill de $JAIBA_CONTAINER" >&2
    exit 1
  fi
  deploy_flow
  chaos_log "runtime_kill OK (flow redeployed)"
}

# DNS usable desde jaiba tras reconnect de red.
# Preferimos container_name; también aceptamos alias Compose `postgres`.
postgres_dns_ok() {
  docker exec "$JAIBA_CONTAINER" sh -c \
    'getent hosts jaiba_stable_postgres >/dev/null 2>&1 \
     || getent hosts postgres >/dev/null 2>&1' \
    2>/dev/null
}

wait_postgres_reachable() {
  local timeout_s="${1:-60}" start_wait now
  start_wait="$(date +%s)"
  while :; do
    if postgres_dns_ok; then
      if docker exec "$POSTGRES_CONTAINER" pg_isready -U jaiba >/dev/null 2>&1; then
        return 0
      fi
    fi
    now="$(date +%s)"
    if (( now - start_wait >= timeout_s )); then
      return 1
    fi
    sleep 2
  done
}

# Tras disconnect/connect, Docker pierde aliases Compose (`postgres`).
# Reconectamos con --alias postgres y, si hace falta, forzamos reattach.
reconnect_postgres_network() {
  local net="$1"
  docker network connect \
    --alias postgres \
    --alias jaiba_stable_postgres \
    "$net" "$POSTGRES_CONTAINER" >/dev/null 2>&1 \
    || docker network connect --alias postgres "$net" "$POSTGRES_CONTAINER" >/dev/null
}

# Recupera alias DNS si un net_flap previo dejó el stack sin `postgres`.
heal_postgres_dns() {
  local net
  net="$(container_network "$POSTGRES_CONTAINER")"
  [[ -n "$net" ]] || return 1
  if postgres_dns_ok && docker exec "$POSTGRES_CONTAINER" pg_isready -U jaiba >/dev/null 2>&1; then
    return 0
  fi
  chaos_log "heal_postgres_dns: reattach con alias postgres (net=$net)"
  docker network disconnect "$net" "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  reconnect_postgres_network "$net"
  wait_postgres_reachable 60
}

chaos_net_flap() {
  local state
  state="$(flow_state)"
  if [[ "$state" != "RUNNING" && "$state" != "STOPPED" ]]; then
    chaos_log "net_flap SKIP (estado=$state; requiere RUNNING/STOPPED)"
    return 0
  fi
  if ! docker inspect "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
    chaos_log "net_flap SKIP (no container $POSTGRES_CONTAINER)"
    return 0
  fi
  POSTGRES_NETWORK="$(container_network "$POSTGRES_CONTAINER")"
  if [[ -z "$POSTGRES_NETWORK" ]]; then
    chaos_log "net_flap SKIP (sin red en $POSTGRES_CONTAINER)"
    return 0
  fi
  chaos_log "net_flap disconnect ${NET_FLAP_SECONDS}s net=$POSTGRES_NETWORK"
  if ! docker network disconnect "$POSTGRES_NETWORK" "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
    chaos_log "net_flap SKIP (disconnect falló; ¿único contenedor en la red?)"
    return 0
  fi
  NET_DISCONNECTED=1
  sleep "$NET_FLAP_SECONDS"
  # Importante: restaurar alias Compose `postgres` (DATABASE_URL legado).
  reconnect_postgres_network "$POSTGRES_NETWORK"
  NET_DISCONNECTED=0
  chaos_log "net_flap reconnect OK (alias postgres)"
  if wait_postgres_reachable 90; then
    chaos_log "net_flap postgres reachable"
  else
    chaos_log "net_flap WARN postgres aún no reachable tras reconnect"
    heal_postgres_dns || true
  fi
  sleep 2
  ensure_progressing
}

chaos_double_stop() {
  chaos_log "double_stop"
  curl -fsS "${auth_hdr[@]}" -X POST \
    "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
  curl -fsS "${auth_hdr[@]}" -X POST \
    "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
  sleep 1
  curl -fsS "${auth_hdr[@]}" -X POST \
    "$API/api/v1/flows/$FLOW_ID/start" >/dev/null 2>&1 \
    || curl -fsS "${auth_hdr[@]}" -X POST \
      "$API/api/v1/flows/$FLOW_ID/trigger" >/dev/null 2>&1 \
    || deploy_flow
  chaos_log "double_stop OK (restarted)"
}

chaos_noop() {
  chaos_log "noop"
}

run_chaos_action() {
  local action state before_cycles before_records
  state="$(flow_state)"
  # No inyectar caos de red/DB mientras el flow aún no arrancó.
  if [[ "$state" == "STARTING" ]]; then
    chaos_log "SKIP chaos (estado=STARTING; esperando RUNNING)"
    audit_event "skip" "pending" "skip" "estado=STARTING"
    return 0
  fi
  action="$(pick_action)"
  before_cycles="$CYCLES"
  before_records="$LAST_RECORDS"
  audit_event "inject" "$action" "ok" "begin cycles=$before_cycles records=$before_records"
  case "$action" in
    db_blip) chaos_db_blip ;;
    db_outage) chaos_db_outage ;;
    flow_bounce) chaos_flow_bounce ;;
    runtime_restart) chaos_runtime_restart ;;
    runtime_kill) chaos_runtime_kill ;;
    net_flap) chaos_net_flap ;;
    double_stop) chaos_double_stop ;;
    noop) chaos_noop ;;
    *)
      chaos_log "UNKNOWN_ACTION=$action (tratado como noop)"
      action="noop"
      ;;
  esac
  ACTION_HITS["$action"]=$(( ACTION_HITS["$action"] + 1 ))
  CHAOS_EVENTS=$((CHAOS_EVENTS + 1))
  # Gracia post-caos: no reiniciar baseline a 0/0 (eso oculta stall real).
  WATCH_SINCE="$(date +%s)"
  chaos_log "watchdog_grace (post-chaos:$action) cycles=$CYCLES records=$LAST_RECORDS"
  audit_event "inject" "$action" "ok" "end cycles=$CYCLES records=$LAST_RECORDS state=$(flow_state)"
}

reset_watchdog() {
  local why="${1:-manual}"
  WATCH_CYCLES="$CYCLES"
  WATCH_RECORDS="$LAST_RECORDS"
  WATCH_SINCE="$(date +%s)"
  chaos_log "watchdog_reset ($why) cycles=$WATCH_CYCLES records=$WATCH_RECORDS"
}

# --- main ---
sed "s/__ROWS__/$ROWS/g" "$ROOT/examples/stable-runtime-stress.yaml" >"$FLOW_FILE"

if ! wait_api 15; then
  echo "API no responde en $API — levanta release-core primero." >&2
  exit 1
fi

log "Chaos soak Jaiba profile=$PROFILE mode=$CHAOS_MODE"
echo "  duration=${DURATION}s rows/ciclo=$ROWS seed=$SEED"
echo "  chaos_interval=${CHAOS_INTERVAL}s recovery=${RECOVERY_SECONDS}s stall=${STALL_SECONDS}s"
echo "  db_blip=${DB_PAUSE_SECONDS}s db_outage=${DB_OUTAGE_SECONDS}s net_flap=${NET_FLAP_SECONDS}s"
echo "  actions=${ACTIONS_CSV}"
echo "  require_full_coverage=$REQUIRE_FULL_COVERAGE"
echo "  postgres=$POSTGRES_CONTAINER jaiba=$JAIBA_CONTAINER"
echo "  grafana=http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"
[[ -n "$REPORT_DIR" ]] && echo "  report_dir=$REPORT_DIR"

# Si un net_flap previo dejó Postgres sin alias DNS, sanar antes de desplegar.
if ! heal_postgres_dns; then
  echo "FAIL: Postgres no reachable desde $JAIBA_CONTAINER (DNS/TCP)." >&2
  exit 1
fi

if [[ -n "$REPORT_DIR" ]]; then
  cp "$FLOW_FILE" "$REPORT_DIR/flow.yaml"
  {
    echo "run_id=$RUN_ID"
    echo "profile=$PROFILE"
    echo "mode=$CHAOS_MODE"
    echo "seed=$SEED"
    echo "actions=$ACTIONS_CSV"
    echo "duration=$DURATION"
    echo "rows=$ROWS"
    echo "interval=$CHAOS_INTERVAL"
    echo "recovery=$RECOVERY_SECONDS"
    echo "stall=$STALL_SECONDS"
    echo "db_blip=$DB_PAUSE_SECONDS"
    echo "db_outage=$DB_OUTAGE_SECONDS"
    echo "net_flap=$NET_FLAP_SECONDS"
    echo "require_full_coverage=$REQUIRE_FULL_COVERAGE"
    echo "api=$API"
    echo "started_at=$(date -Is)"
    echo "git_head=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  } >"$REPORT_DIR/manifest.env"
  log "Auditoría → $REPORT_DIR"
fi

deploy_flow

if ! wait_until_running 180; then
  echo "FAIL: el flow no llegó a RUNNING/STOPPED antes del caos." >&2
  exit 1
fi

START="$(date +%s)"
DEADLINE="$((START + DURATION))"
CHAOS_UNTIL="$((DEADLINE - RECOVERY_SECONDS))"
NEXT_CHAOS="$((START + CHAOS_INTERVAL))"
CYCLES=0
LAST_PROCESSED=0
LAST_RECORDS=0
FAILED=0
WATCH_CYCLES=0
WATCH_RECORDS=0
WATCH_SINCE="$START"
STARTING_SINCE=0
EXIT_CODE=0
FAIL_REASON=""

while (( $(date +%s) < DEADLINE )); do
  NOW="$(date +%s)"
  ELAPSED="$((NOW - START))"
  REMAINING="$((DEADLINE - NOW))"

  BODY="$(api_flow 2>/dev/null || echo '{}')"
  STATE="$(jq -r '.runtime.control.state // "UNKNOWN"' <<<"$BODY")"
  FAILED="$(jq -r '.runtime.metrics.failed // 0' <<<"$BODY")"
  LAST_PROCESSED="$(jq -r '.runtime.metrics.processed // 0' <<<"$BODY")"
  LAST_RECORDS="$(jq -r '.runtime.metrics.processors.generate.records // 0' <<<"$BODY")"
  [[ "$LAST_RECORDS" =~ ^[0-9]+$ ]] || LAST_RECORDS=0
  [[ "$LAST_PROCESSED" =~ ^[0-9]+$ ]] || LAST_PROCESSED=0
  [[ "$FAILED" =~ ^[0-9]+$ ]] || FAILED=0

  if (( NOW < CHAOS_UNTIL && NOW >= NEXT_CHAOS )); then
    if [[ "$STATE" == "RUNNING" || "$STATE" == "STOPPED" ]]; then
      run_chaos_action
      NEXT_CHAOS="$((NOW + CHAOS_INTERVAL))"
      NOW="$(date +%s)"
    else
      # Aplazar caos hasta que el flow esté operativo.
      NEXT_CHAOS="$((NOW + 15))"
      chaos_log "defer chaos (estado=$STATE)"
      audit_event "defer" "pending" "skip" "estado=$STATE"
    fi
  fi

  PHASE="chaos"
  (( NOW >= CHAOS_UNTIL )) && PHASE="recovery"

  printf '\rFase=%-8s Estado=%-9s ciclos=%-5s registros=%-12s fallos=%-5s caos=%-3s t=%-5ss rest=%-5ss' \
    "$PHASE" "$STATE" "$CYCLES" "$LAST_RECORDS" "$FAILED" "$CHAOS_EVENTS" "$ELAPSED" "$REMAINING"

  if [[ "$STATE" == "STOPPED" ]]; then
    CYCLES="$((CYCLES + 1))"
    STARTING_SINCE=0
    curl -fsS "${auth_hdr[@]}" -X POST \
      "$API/api/v1/flows/$FLOW_ID/trigger" >/dev/null 2>&1 || true
  elif [[ "$STATE" == "FAILED" ]]; then
    STARTING_SINCE=0
    ensure_progressing
  elif [[ "$STATE" == "STARTING" ]]; then
    if (( STARTING_SINCE == 0 )); then
      STARTING_SINCE="$NOW"
    elif (( NOW - STARTING_SINCE >= STALL_SECONDS )); then
      echo
      log "STALL: STARTING > ${STALL_SECONDS}s sin llegar a RUNNING"
      FAIL_REASON="stall_starting"
      EXIT_CODE=1
      break
    fi
  else
    STARTING_SINCE=0
  fi

  if (( CYCLES > WATCH_CYCLES )) || (( LAST_RECORDS > WATCH_RECORDS )); then
    WATCH_CYCLES="$CYCLES"
    WATCH_RECORDS="$LAST_RECORDS"
    WATCH_SINCE="$NOW"
  elif (( LAST_RECORDS < WATCH_RECORDS )); then
    WATCH_RECORDS="$LAST_RECORDS"
    WATCH_SINCE="$NOW"
  elif (( NOW - WATCH_SINCE >= STALL_SECONDS )); then
    echo
    log "STALL: sin progreso en ${STALL_SECONDS}s (ciclos=$CYCLES records=$LAST_RECORDS estado=$STATE caos=$CHAOS_EVENTS)"
    FAIL_REASON="stall_progress"
    EXIT_CODE=1
    break
  fi

  sleep 1
done

echo
curl -fsS "${auth_hdr[@]}" -X POST \
  "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
RUNNING=0

sleep 2
FINAL_BODY="$(api_flow 2>/dev/null || echo '{}')"
FINAL_STATE="$(jq -r '.runtime.control.state // "UNKNOWN"' <<<"$FINAL_BODY")"
RUNTIME_NULL="$(jq -r 'if .runtime == null then "null" else "present" end' <<<"$FINAL_BODY")"

TOTAL_RECORDS="$((CYCLES * ROWS))"
echo "Chaos soak terminado: profile=$PROFILE mode=$CHAOS_MODE exit=$EXIT_CODE"
echo "  ciclos=$CYCLES registros_aprox=$TOTAL_RECORDS paquetes_ultimo_ciclo=$LAST_PROCESSED"
echo "  eventos_caos=$CHAOS_EVENTS estado_final=$FINAL_STATE runtime=$RUNTIME_NULL"
echo "  log_eventos=$CHAOS_LOG"
echo "  jsonl_eventos=$CHAOS_JSONL"
[[ -n "$REPORT_DIR" ]] && echo "  report_dir=$REPORT_DIR"
echo "  Grafana: http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"

echo "  cobertura_acciones:"
MISSING_ACTIONS=()
for a in "${ACTIONS[@]}"; do
  hits="${ACTION_HITS[$a]:-0}"
  printf '    - %s: %s\n' "$a" "$hits"
  if (( hits < 1 )); then
    MISSING_ACTIONS+=("$a")
  fi
done

warn_final=""
if (( CYCLES < 1 )); then
  warn_final="ciclos=0 (sin trabajo útil)"
  FAIL_REASON="${FAIL_REASON:-cycles_zero}"
  EXIT_CODE=1
fi
if [[ "$FINAL_STATE" == "FAILED" ]]; then
  warn_final="${warn_final} estado_final=FAILED"
  FAIL_REASON="${FAIL_REASON:-final_failed}"
  EXIT_CODE=1
fi
if [[ "$RUNTIME_NULL" != "null" ]]; then
  curl -fsS "${auth_hdr[@]}" -X POST \
    "$API/api/v1/flows/$FLOW_ID/stop" >/dev/null 2>&1 || true
  sleep 2
  FINAL_BODY="$(api_flow 2>/dev/null || echo '{}')"
  RUNTIME_NULL="$(jq -r 'if .runtime == null then "null" else "present" end' <<<"$FINAL_BODY")"
  FINAL_STATE="$(jq -r '.runtime.control.state // "UNKNOWN"' <<<"$FINAL_BODY")"
  if [[ "$RUNTIME_NULL" != "null" || "$FINAL_STATE" == "FAILED" ]]; then
    EXIT_CODE=1
    FAIL_REASON="${FAIL_REASON:-runtime_unclean}"
    warn_final="${warn_final} runtime/estado no limpio tras stop ($FINAL_STATE/$RUNTIME_NULL)"
  fi
fi

if [[ "$REQUIRE_FULL_COVERAGE" == "1" && ${#MISSING_ACTIONS[@]} -gt 0 ]]; then
  EXIT_CODE=1
  FAIL_REASON="${FAIL_REASON:-incomplete_coverage}"
  warn_final="${warn_final} cobertura incompleta: ${MISSING_ACTIONS[*]}"
fi

# Reporte JSON auditable.
write_audit_report() {
  local verdict="PASS" ended cov_file
  (( EXIT_CODE != 0 )) && verdict="FAIL"
  ended="$(date -Is)"
  cov_file="$(mktemp "${TMPDIR:-/tmp}/jaiba-chaos-cov.XXXXXX.txt")"
  for a in "${ACTIONS[@]}"; do
    printf '%s\t%s\n' "$a" "${ACTION_HITS[$a]:-0}" >>"$cov_file"
  done
  REPORT_PATH="${REPORT_DIR:-}/report.json"
  [[ -n "${REPORT_DIR:-}" ]] || REPORT_PATH=""
  python3 - "$cov_file" "$verdict" "$ended" "$REPORT_PATH" <<'PY'
import json, sys
from pathlib import Path

cov_path, verdict, ended, report_path = sys.argv[1:5]
coverage = {}
with open(cov_path, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        k, v = line.split("\t", 1)
        coverage[k] = int(v)

# Env-like values passed via os.environ below
import os
env = os.environ
missing = [a for a, n in coverage.items() if n < 1]
planned = list(coverage.keys())
report = {
    "run_id": env["JAIBA_AUDIT_RUN_ID"],
    "verdict": verdict,
    "exit_code": int(env["JAIBA_AUDIT_EXIT"]),
    "fail_reason": env.get("JAIBA_AUDIT_FAIL_REASON", ""),
    "warn_final": env.get("JAIBA_AUDIT_WARN", "").strip(),
    "profile": env["JAIBA_AUDIT_PROFILE"],
    "mode": env["JAIBA_AUDIT_MODE"],
    "seed": int(env["JAIBA_AUDIT_SEED"]),
    "actions_planned": planned,
    "coverage": coverage,
    "missing_actions": missing,
    "require_full_coverage": env.get("JAIBA_AUDIT_REQUIRE_COV", "0") == "1",
    "duration_seconds": int(env["JAIBA_AUDIT_DURATION"]),
    "rows_per_cycle": int(env["JAIBA_AUDIT_ROWS"]),
    "chaos_interval_seconds": int(env["JAIBA_AUDIT_INTERVAL"]),
    "recovery_seconds": int(env["JAIBA_AUDIT_RECOVERY"]),
    "stall_seconds": int(env["JAIBA_AUDIT_STALL"]),
    "db_blip_seconds": int(env["JAIBA_AUDIT_DB_BLIP"]),
    "db_outage_seconds": int(env["JAIBA_AUDIT_DB_OUTAGE"]),
    "net_flap_seconds": int(env["JAIBA_AUDIT_NET_FLAP"]),
    "events": int(env["JAIBA_AUDIT_EVENTS"]),
    "cycles": int(env["JAIBA_AUDIT_CYCLES"]),
    "records_approx": int(env["JAIBA_AUDIT_RECORDS"]),
    "packets_last_cycle": int(env["JAIBA_AUDIT_PACKETS"]),
    "failed_metric": int(env["JAIBA_AUDIT_FAILED"]),
    "final_state": env["JAIBA_AUDIT_FINAL_STATE"],
    "runtime": env["JAIBA_AUDIT_RUNTIME"],
    "started_unix": int(env["JAIBA_AUDIT_START"]),
    "ended_at": ended,
    "api": env["JAIBA_AUDIT_API"],
    "grafana": env["JAIBA_AUDIT_GRAFANA"],
    "files": {
        "events_log": env["JAIBA_AUDIT_LOG"],
        "events_jsonl": env["JAIBA_AUDIT_JSONL"],
        "report_dir": env.get("JAIBA_AUDIT_REPORT_DIR", ""),
    },
}
text = json.dumps(report, indent=2, ensure_ascii=False)
print(text)
if report_path:
    Path(report_path).write_text(text + "\n", encoding="utf-8")
    md = Path(report_path).with_name("SUMMARY.md")
    cov_lines = "\n".join(f"- `{k}`: {v}" for k, v in sorted(coverage.items()))
    md.write_text(
        f"""# Chaos audit {report['run_id']}

**Verdict:** {report['verdict']} (exit {report['exit_code']})

| Campo | Valor |
| --- | --- |
| profile / mode | {report['profile']} / {report['mode']} |
| seed | {report['seed']} |
| eventos | {report['events']} |
| ciclos | {report['cycles']} |
| registros_aprox | {report['records_approx']} |
| fail_reason | {report['fail_reason'] or '—'} |

## Cobertura

{cov_lines}

## Archivos

- `events.jsonl` — eventos estructurados
- `events.log` — log humano
- `manifest.env` — parámetros del run
- `flow.yaml` — flow desplegado
- `report.json` — este resumen en JSON
""",
        encoding="utf-8",
    )
PY
  rm -f "$cov_file"
}

export JAIBA_AUDIT_RUN_ID="$RUN_ID"
export JAIBA_AUDIT_EXIT="$EXIT_CODE"
export JAIBA_AUDIT_FAIL_REASON="${FAIL_REASON:-}"
export JAIBA_AUDIT_WARN="${warn_final:-}"
export JAIBA_AUDIT_PROFILE="$PROFILE"
export JAIBA_AUDIT_MODE="$CHAOS_MODE"
export JAIBA_AUDIT_SEED="$SEED"
export JAIBA_AUDIT_REQUIRE_COV="$REQUIRE_FULL_COVERAGE"
export JAIBA_AUDIT_DURATION="$DURATION"
export JAIBA_AUDIT_ROWS="$ROWS"
export JAIBA_AUDIT_INTERVAL="$CHAOS_INTERVAL"
export JAIBA_AUDIT_RECOVERY="$RECOVERY_SECONDS"
export JAIBA_AUDIT_STALL="$STALL_SECONDS"
export JAIBA_AUDIT_DB_BLIP="$DB_PAUSE_SECONDS"
export JAIBA_AUDIT_DB_OUTAGE="$DB_OUTAGE_SECONDS"
export JAIBA_AUDIT_NET_FLAP="$NET_FLAP_SECONDS"
export JAIBA_AUDIT_EVENTS="$CHAOS_EVENTS"
export JAIBA_AUDIT_CYCLES="$CYCLES"
export JAIBA_AUDIT_RECORDS="$TOTAL_RECORDS"
export JAIBA_AUDIT_PACKETS="$LAST_PROCESSED"
export JAIBA_AUDIT_FAILED="$FAILED"
export JAIBA_AUDIT_FINAL_STATE="$FINAL_STATE"
export JAIBA_AUDIT_RUNTIME="$RUNTIME_NULL"
export JAIBA_AUDIT_START="$START"
export JAIBA_AUDIT_API="$API"
export JAIBA_AUDIT_GRAFANA="http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"
export JAIBA_AUDIT_LOG="$CHAOS_LOG"
export JAIBA_AUDIT_JSONL="$CHAOS_JSONL"
export JAIBA_AUDIT_REPORT_DIR="${REPORT_DIR:-}"

write_audit_report

if (( EXIT_CODE != 0 )); then
  echo "FAIL: stall o recuperación insuficiente${warn_final:+ ($warn_final)} (ver log de caos)." >&2
  exit "$EXIT_CODE"
fi

echo "PASS: sin stall detectado; stop externo aplicado; cobertura OK."
