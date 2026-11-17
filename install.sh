#!/usr/bin/env bash
# keylens installer
# Usage:
#   curl -fsSL https://github.com/keylens/keylens/releases/latest/download/install.sh | bash
#
# Options (env vars):
#   VERSION     — pin a specific release tag, e.g. VERSION=v0.1.0
#   INSTALL_DIR — override install location (default: /usr/local/bin)
#
# The archive is verified against the release's checksums.txt before anything is
# extracted or installed, and the install fails closed: a mismatch, a missing entry or
# no SHA-256 tool all stop here. That protects against a corrupted or swapped download;
# it does not protect against a compromised release, since both files come from it. For
# provenance, verify the build attestation instead (see the README):
#   gh attestation verify keylens-<target>.tar.gz --repo keylens/keylens

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
# For the installer's own CI test, which serves a fake release locally. Never set this
# to a host you do not control: it is where the binary *and* its checksum come from.
BASE_URL="${KEYLENS_INSTALL_BASE_URL:-$BASE_URL}"

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

# ── Verify ───────────────────────────────────────────────────────────────────

if ! curl -fsSL "$BASE_URL/checksums.txt" -o "$TMP/checksums.txt"; then
  echo "error: could not download checksums.txt from $BASE_URL" >&2
  echo "       refusing to install an unverified binary" >&2
  exit 1
fi

# Exact filename match on the second field, so `keylens-x.tar.gz.sig` or a crafted
# line cannot satisfy the lookup for `keylens-x.tar.gz`.
EXPECTED=$(awk -v f="$ARCHIVE" '$2 == f || $2 == "*"f { print $1; exit }' "$TMP/checksums.txt")
if [ -z "$EXPECTED" ]; then
  echo "error: checksums.txt has no entry for $ARCHIVE" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL=$(sha256sum "$TMP/$ARCHIVE" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
  ACTUAL=$(shasum -a 256 "$TMP/$ARCHIVE" | awk '{ print $1 }')
else
  echo "error: neither sha256sum nor shasum is available to verify the download" >&2
  exit 1
fi

if [ "$ACTUAL" != "$EXPECTED" ]; then
  echo "error: checksum mismatch for $ARCHIVE" >&2
  echo "       expected $EXPECTED" >&2
  echo "       got      $ACTUAL" >&2
  exit 1
fi
echo "  Checksum OK (sha256 $ACTUAL)"

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
