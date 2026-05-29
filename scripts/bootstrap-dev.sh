#!/usr/bin/env bash
#
# bootstrap-dev.sh — install the gate tools needed by `just ci`. Idempotent.
#
# Needed because this machine's CARGO_HOME is on a ramdisk (/mnt/ramdisk/cargo), so installed
# cargo subcommands can disappear on reboot. Re-run this (or `just bootstrap-tools`) to restore
# them. CI installs its own tools and does not use this script.
set -euo pipefail

CARGO_BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
export PATH="$CARGO_BIN:$PATH"

have() { command -v "$1" >/dev/null 2>&1; }

echo "== rust components (rustfmt, clippy, llvm-tools)"
rustup component add rustfmt clippy llvm-tools >/dev/null 2>&1 || true

if ! have cargo-binstall; then
  echo "== installing cargo-binstall (prebuilt)"
  tmp="$(mktemp -d)"
  curl -L --proto '=https' --tlsv1.2 -sSf \
    https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-x86_64-unknown-linux-gnu.tgz \
    -o "$tmp/cb.tgz"
  mkdir -p "$CARGO_BIN"
  tar -xzf "$tmp/cb.tgz" -C "$CARGO_BIN"
  rm -rf "$tmp"
fi

echo "== installing gate tools (just, cargo-deny, cargo-audit, cargo-llvm-cov)"
need=()
have just            || need+=("just")
have cargo-deny      || need+=("cargo-deny")
have cargo-audit     || need+=("cargo-audit")
have cargo-llvm-cov  || need+=("cargo-llvm-cov")
if [[ ${#need[@]} -gt 0 ]]; then
  cargo binstall -y "${need[@]}"
else
  echo "   all present"
fi

echo
echo "Tools ready. Ensure '$CARGO_BIN' is on your PATH (needed to invoke 'just' directly)."
just --version >/dev/null 2>&1 && echo "✓ just $(just --version)" || echo "⚠ 'just' not on PATH yet — add $CARGO_BIN to PATH."
