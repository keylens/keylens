#!/usr/bin/env bash
# Exercise install.sh against a fake release served locally: a good archive must
# install; a tampered archive and a missing checksum entry must both be refused, with
# nothing written to the install directory.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
WORK=$(mktemp -d)
trap 'kill "${SERVER_PID:-}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) TARGET=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) TARGET=aarch64-unknown-linux-gnu ;;
  Darwin-x86_64) TARGET=x86_64-apple-darwin ;;
  Darwin-arm64) TARGET=aarch64-apple-darwin ;;
  *) echo "unsupported test platform"; exit 1 ;;
esac
ARCHIVE="keylens-${TARGET}.tar.gz"

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'; }

# A stand-in binary: install.sh runs `keylens --version` at the end.
mkdir -p "$WORK/pkg"
printf '#!/bin/sh\necho "keylens 0.0.0-test"\n' > "$WORK/pkg/keylens"
chmod +x "$WORK/pkg/keylens"

# Serve directory $1 on a free port; sets BASE and SERVER_PID. Not used inside `$(...)`:
# a background server there holds the substitution's pipe open forever.
serve() {
  local port
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  python3 -m http.server "$port" --bind 127.0.0.1 --directory "$WORK/$1" >/dev/null 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 100); do
    curl -fs "http://127.0.0.1:$port/" >/dev/null 2>&1 && break
    sleep 0.1
  done
  BASE="http://127.0.0.1:$port"
}

run_install() {
  local base=$1 dest=$2
  mkdir -p "$dest"
  KEYLENS_INSTALL_BASE_URL="$base" INSTALL_DIR="$dest" bash "$ROOT/install.sh"
}

# 1. Good release installs.
mkdir -p "$WORK/good"
tar -czf "$WORK/good/$ARCHIVE" -C "$WORK/pkg" keylens
echo "$(sha "$WORK/good/$ARCHIVE")  $ARCHIVE" > "$WORK/good/checksums.txt"
serve good
run_install "$BASE" "$WORK/bin-good"
test -x "$WORK/bin-good/keylens" || { echo "FAIL: good release did not install"; exit 1; }
kill "$SERVER_PID"

# 2. Tampered archive is refused.
mkdir -p "$WORK/bad"
cp "$WORK/good/checksums.txt" "$WORK/bad/"
printf '#!/bin/sh\necho pwned\n' > "$WORK/pkg/keylens"
tar -czf "$WORK/bad/$ARCHIVE" -C "$WORK/pkg" keylens
serve bad
if run_install "$BASE" "$WORK/bin-bad" 2>"$WORK/bad.err"; then
  echo "FAIL: tampered archive was installed"; exit 1
fi
grep -q "checksum mismatch" "$WORK/bad.err" || { cat "$WORK/bad.err"; exit 1; }
test ! -e "$WORK/bin-bad/keylens" || { echo "FAIL: tampered binary written"; exit 1; }
kill "$SERVER_PID"

# 3. A checksums.txt without our entry (only a look-alike) is refused.
mkdir -p "$WORK/missing"
cp "$WORK/bad/$ARCHIVE" "$WORK/missing/"
echo "$(sha "$WORK/missing/$ARCHIVE")  ${ARCHIVE}.sig" > "$WORK/missing/checksums.txt"
serve missing
if run_install "$BASE" "$WORK/bin-missing" 2>"$WORK/missing.err"; then
  echo "FAIL: installed without a checksum entry"; exit 1
fi
grep -q "no entry for" "$WORK/missing.err" || { cat "$WORK/missing.err"; exit 1; }

echo "install.sh: good release installed; tampered and unlisted archives refused"
