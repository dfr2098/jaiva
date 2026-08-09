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
#   CHAOS_SEED              semilla RNG (default: 1)
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
CHAOS_LOG="$(mktemp "${TMPDIR:-/tmp}/jaiba-chaos-events.XXXXXX.log")"

IFS=',' read -r -a ACTIONS <<<"$ACTIONS_CSV"
if [[ ${#ACTIONS[@]} -eq 0 ]]; then
  echo "CHAOS_ACTIONS vacío" >&2
  exit 2
fi

auth_hdr=(-H "Authorization: Bearer $TOKEN")

log() { printf '\n[%s] %s\n' "$(date -Is)" "$*"; }
chaos_log() {
  local line="$*"
  printf '%s\n' "$line" >>"$CHAOS_LOG"
  log "CHAOS $line"
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
  local action state
  state="$(flow_state)"
  # No inyectar caos de red/DB mientras el flow aún no arrancó.
  if [[ "$state" == "STARTING" ]]; then
    chaos_log "SKIP chaos (estado=STARTING; esperando RUNNING)"
    return 0
  fi
  action="$(pick_action)"
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
      ;;
  esac
  CHAOS_EVENTS=$((CHAOS_EVENTS + 1))
  # Gracia post-caos: no reiniciar baseline a 0/0 (eso oculta stall real).
  WATCH_SINCE="$(date +%s)"
  chaos_log "watchdog_grace (post-chaos:$action) cycles=$CYCLES records=$LAST_RECORDS"
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

log "Chaos soak Jaiba profile=$PROFILE"
echo "  duration=${DURATION}s rows/ciclo=$ROWS seed=$SEED"
echo "  chaos_interval=${CHAOS_INTERVAL}s recovery=${RECOVERY_SECONDS}s stall=${STALL_SECONDS}s"
echo "  db_blip=${DB_PAUSE_SECONDS}s db_outage=${DB_OUTAGE_SECONDS}s net_flap=${NET_FLAP_SECONDS}s"
echo "  actions=${ACTIONS_CSV}"
echo "  postgres=$POSTGRES_CONTAINER jaiba=$JAIBA_CONTAINER"
echo "  grafana=http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"

# Si un net_flap previo dejó Postgres sin alias DNS, sanar antes de desplegar.
if ! heal_postgres_dns; then
  echo "FAIL: Postgres no reachable desde $JAIBA_CONTAINER (DNS/TCP)." >&2
  exit 1
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
WATCH_CYCLES=0
WATCH_RECORDS=0
WATCH_SINCE="$START"
STARTING_SINCE=0
EXIT_CODE=0

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
echo "Chaos soak terminado: profile=$PROFILE exit=$EXIT_CODE"
echo "  ciclos=$CYCLES registros_aprox=$TOTAL_RECORDS paquetes_ultimo_ciclo=$LAST_PROCESSED"
echo "  eventos_caos=$CHAOS_EVENTS estado_final=$FINAL_STATE runtime=$RUNTIME_NULL"
echo "  log_eventos=$CHAOS_LOG"
echo "  Grafana: http://127.0.0.1:${GRAFANA_PORT:-13000}/d/jaiba-runtime-jme"

warn_final=""
if (( CYCLES < 1 )); then
  warn_final="ciclos=0 (sin trabajo útil)"
  EXIT_CODE=1
fi
if [[ "$FINAL_STATE" == "FAILED" ]]; then
  warn_final="${warn_final} estado_final=FAILED"
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
    warn_final="${warn_final} runtime/estado no limpio tras stop ($FINAL_STATE/$RUNTIME_NULL)"
  fi
fi

if (( EXIT_CODE != 0 )); then
  echo "FAIL: stall o recuperación insuficiente${warn_final:+ ($warn_final)} (ver log de caos)." >&2
  exit "$EXIT_CODE"
fi

echo "PASS: sin stall detectado; stop externo aplicado."
