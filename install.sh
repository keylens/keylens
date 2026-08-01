#!/usr/bin/env bash
# keylens installer
# Usage:
#   curl -fsSL https://github.com/keylens/keylens/releases/latest/download/install.sh | bash
#
# Options (env vars):
#   VERSION     — pin a specific release tag, e.g. VERSION=v0.1.0
#   INSTALL_DIR — override install location (default: /usr/local/bin)

set -euo pipefail

REPO="keylens/keylens"
BIN="keylens"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
VERSION="${VERSION:-latest}"

# ── Detect platform ──────────────────────────────────────────────────────────

OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Darwin)
    case "$ARCH" in
      x86_64)        TARGET="x86_64-apple-darwin" ;;
      arm64|aarch64) TARGET="aarch64-apple-darwin" ;;
      *) echo "error: unsupported macOS architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  Linux)
    case "$ARCH" in
      x86_64)  TARGET="x86_64-unknown-linux-gnu" ;;
      aarch64) TARGET="aarch64-unknown-linux-gnu" ;;
      *) echo "error: unsupported Linux architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  *)
    echo "error: unsupported OS '$OS'." >&2
    echo "       For Windows, download the .zip from:" >&2
    echo "       https://github.com/$REPO/releases/latest" >&2
    exit 1
    ;;
esac

# ── Resolve download URL ─────────────────────────────────────────────────────

if [ "$VERSION" = "latest" ]; then
  BASE_URL="https://github.com/$REPO/releases/latest/download"
else
  BASE_URL="https://github.com/$REPO/releases/download/$VERSION"
fi

ARCHIVE="${BIN}-${TARGET}.tar.gz"
URL="${BASE_URL}/${ARCHIVE}"

# ── Download & extract ───────────────────────────────────────────────────────

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "  Downloading ${BIN} (${TARGET})…"
if ! curl -fsSL "$URL" -o "$TMP/$ARCHIVE"; then
  echo "error: download failed: $URL" >&2
  echo "       check that a release exists at https://github.com/$REPO/releases" >&2
  exit 1
fi

tar -xzf "$TMP/$ARCHIVE" -C "$TMP"

if [ ! -f "$TMP/$BIN" ]; then
  echo "error: binary '$BIN' not found in archive" >&2
  exit 1
fi

chmod +x "$TMP/$BIN"

# ── Install ──────────────────────────────────────────────────────────────────

if [ -w "$INSTALL_DIR" ]; then
  mv "$TMP/$BIN" "$INSTALL_DIR/$BIN"
else
  echo "  Installing to $INSTALL_DIR  (sudo required)"
  sudo mv "$TMP/$BIN" "$INSTALL_DIR/$BIN"
fi

# ── Done ─────────────────────────────────────────────────────────────────────

echo ""
echo "  ✓  keylens installed → $INSTALL_DIR/$BIN"
echo ""
"$INSTALL_DIR/$BIN" --version
echo ""
echo "  Next steps:"
echo "    keylens                          # browse redis://127.0.0.1:6379"
echo "    keylens --url redis://host:6379  # browse a specific server"
echo "    keylens probe --queues           # non-interactive report"
echo ""
echo "  keylens is read-only — safe to point at production."
echo ""
