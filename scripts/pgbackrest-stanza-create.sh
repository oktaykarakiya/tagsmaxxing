#!/usr/bin/env bash
# Create the pgBackRest stanza for the kb cluster (plan §21).
#
# Idempotent: if the stanza already exists this is a no-op. Run this ONCE after
# installing pgBackRest and placing pgbackrest.conf in /etc/pgbackrest/.
#
# Before running:
#   1. Install pgBackRest (apt install pgbackrest / yum install pgbackrest).
#   2. Copy ops/pgbackrest.conf → /etc/pgbackrest/pgbackrest.conf (or pass
#      --config=ops/pgbackrest.conf explicitly).
#   3. Set the required environment variables (§24):
#        export B2_BACKUP_KEY_ID="…"
#        export B2_BACKUP_KEY="…"
#        export PGBACKREST_REPO1_CIPHER_PASS="…"
#
# Usage:
#   bash scripts/pgbackrest-stanza-create.sh [--stanza <name>] [--config <path>]
#
# Defaults:
#   --stanza kb
#   --config ops/pgbackrest.conf

set -euo pipefail

STANZA="kb"
CONFIG="${PGBACKREST_CONFIG:-ops/pgbackrest.conf}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stanza)
            STANZA="$2"
            shift 2
            ;;
        --config)
            CONFIG="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Usage: $0 [--stanza <name>] [--config <path>]" >&2
            exit 1
            ;;
    esac
done

echo "=== Creating pgBackRest stanza '${STANZA}' ==="
echo "Config: ${CONFIG}"

# Check that the config file exists.
if [[ ! -f "${CONFIG}" ]]; then
    echo "ERROR: config file not found: ${CONFIG}" >&2
    exit 1
fi

# Check required environment variables for B2 access.
if [[ -z "${B2_BACKUP_KEY_ID:-}" ]]; then
    echo "ERROR: B2_BACKUP_KEY_ID is not set" >&2
    exit 1
fi
if [[ -z "${B2_BACKUP_KEY:-}" ]]; then
    echo "ERROR: B2_BACKUP_KEY is not set" >&2
    exit 1
fi

# Create the stanza (no-op if it already exists — idempotent).
if pgbackrest --config="${CONFIG}" --stanza="${STANZA}" --log-level-console=info stanza-create; then
    echo "Stanza '${STANZA}' is ready."
else
    # stanza-create exits non-zero if the stanza already exists but is otherwise
    # healthy. Check whether it's actually there.
    if pgbackrest --config="${CONFIG}" --stanza="${STANZA}" info &>/dev/null; then
        echo "Stanza '${STANZA}' already exists — continuing (idempotent)."
    else
        echo "ERROR: stanza creation failed and '${STANZA}' is not present." >&2
        exit 1
    fi
fi

# Verify the stanza is healthy.
echo ""
echo "=== Stanza info ==="
pgbackrest --config="${CONFIG}" --stanza="${STANZA}" info

echo ""
echo "=== Stanza created successfully ==="
echo "Next: schedule backups with scripts/pgbackrest-backup.sh"
echo "      configure WAL archiving on the Postgres host (see docs/DR_RUNBOOK.md)"
