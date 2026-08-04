#!/usr/bin/env bash
# Copia el binario `jaiba` al layout que espera Tauri (`externalBin`).
# Uso:
#   scripts/prepare-desktop-sidecar.sh              # target/debug/jaiba
#   scripts/prepare-desktop-sidecar.sh release      # target/release/jaiba
#   scripts/prepare-desktop-sidecar.sh /ruta/jaiba
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="$ROOT/apps/jaiba-ui/src-tauri/binaries"
TRIPLE="$(rustc -vV | sed -n 's/^host: //p')"

if [[ "${1:-}" == "release" ]]; then
  SRC="$ROOT/target/release/jaiba"
elif [[ -n "${1:-}" ]]; then
  SRC="$1"
else
  SRC="$ROOT/target/debug/jaiba"
fi

if [[ ! -x "$SRC" && ! -f "$SRC" ]]; then
  echo "No existe $SRC" >&2
  echo "Compila primero: cargo build -p jaiba-cli   (o --release)" >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
DEST="$DEST_DIR/jaiba-${TRIPLE}"
cp -f "$SRC" "$DEST"
chmod +x "$DEST"
echo "Sidecar listo: $DEST"
