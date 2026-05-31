#!/usr/bin/env bash
# Thin wrapper so `scripts/cov.sh` and `just cov` enforce the SAME line-coverage floor
# (the floor lives in the justfile as `cov_min`; do not duplicate it here — a second,
# drifting floor is exactly the masking P6-T0 re-armed against).
set -euo pipefail
cd "$(dirname "$0")/.."
exec just cov
