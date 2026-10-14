#!/usr/bin/env bash
# Smoke del catálogo de plugins (`jaiba plugins …`):
#   1. instala `mayusculas` (proceso) desde catalog/plugins.json y corre el flujo;
#   2. si hay Docker, publica la imagen del ejemplo en un registro local, arma un
#      catálogo con la imagen fijada por digest, la instala y corre el flujo.
# Uso: bash scripts/smoke-plugin-catalog.sh [binario jaiba]   (por defecto target/debug/jaiba)
set -euo pipefail
cd "$(dirname "$0")/.."

JAIBA="${1:-target/debug/jaiba}"
REGISTRY_PORT="${REGISTRY_PORT:-15000}"
REGISTRY_NAME="jaiba-smoke-registry"
WORK="$(mktemp -d)"
export JAIBA_PLUGIN_DIR="$WORK/plugins"
unset JAIBA_PLUGIN_CATALOG

cleanup() {
  if command -v docker >/dev/null 2>&1; then
    docker rm -f "$REGISTRY_NAME" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

run_flow() {
  "$JAIBA" examples/plugins/mayusculas-flow.yaml >"$WORK/$1.log" 2>&1
  grep -q '"name":"ADA"' "$WORK/$1.log" || { cat "$WORK/$1.log"; exit 1; }
}

echo "== proceso: catalog/plugins.json"
"$JAIBA" plugins search mayus --catalog catalog/plugins.json
"$JAIBA" plugins install mayusculas --catalog catalog/plugins.json
"$JAIBA" plugins list
run_flow process

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "Docker no disponible: se omite el plugin en contenedor"
  echo "=== smoke plugin catalog OK ==="
  exit 0
fi

echo "== contenedor: registro local e imagen fijada por digest"
docker run -d --name "$REGISTRY_NAME" -p "127.0.0.1:$REGISTRY_PORT:5000" registry:2 >/dev/null
docker build -q -t jaiba-plugin-mayusculas:0.1.0 examples/plugins/mayusculas >/dev/null
REMOTE="localhost:$REGISTRY_PORT/jaiba-plugin-mayusculas"
docker tag jaiba-plugin-mayusculas:0.1.0 "$REMOTE:0.1.0"
for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$REGISTRY_PORT/v2/" >/dev/null && break
  sleep 0.5
done
docker push -q "$REMOTE:0.1.0" >/dev/null
DIGEST="$(docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$REMOTE:0.1.0" \
  | grep "^$REMOTE@sha256:" | head -n 1 | cut -d@ -f2)"
test -n "$DIGEST" || { echo "no se obtuvo el digest de $REMOTE"; exit 1; }
docker rmi "$REMOTE:0.1.0" >/dev/null

cat >"$WORK/catalog.json" <<EOF
{
  "catalog_version": 1,
  "plugins": [{
    "id": "mayusculas",
    "description": "Pasa a mayúsculas los campos indicados (contenedor)",
    "versions": [{
      "version": "0.2.0",
      "protocol_version": 1,
      "container": {"image": "$REMOTE@$DIGEST"}
    }]
  }]
}
EOF

"$JAIBA" plugins install mayusculas --force --catalog "$WORK/catalog.json"
grep -q "image: $REMOTE@$DIGEST" "$JAIBA_PLUGIN_DIR/mayusculas/plugin.yaml"
"$JAIBA" plugins list
run_flow container
sleep 2
test -z "$(docker ps -aq --filter label=jaiba.plugin)" || { echo "quedaron contenedores del plugin"; exit 1; }
"$JAIBA" plugins remove mayusculas
docker rmi "$REMOTE@$DIGEST" >/dev/null 2>&1 || true

echo "=== smoke plugin catalog OK ==="
