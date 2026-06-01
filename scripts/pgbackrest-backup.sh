#!/usr/bin/env bash
# Run a pgBackRest backup: full, differential, or incremental (plan §21).
#
# Schedule (cron or systemd timer):
#   Daily full:     0 3 * * *  bash scripts/pgbackrest-backup.sh --type full
#   Hourly incr:   42 * * * *  bash scripts/pgbackrest-backup.sh --type incr
# (The shifted minutes avoid crowding the top of the hour — §31 systemd/cron
# guidance. The daily full runs at 03:00 local during low-usage hours.)
#
# Idempotent: running a backup while another is in progress is blocked by
# pgBackRest's internal lock file. The script exits with a clear message.
#
# Secrets are read from the environment:
#   B2_BACKUP_KEY_ID             — B2 application key ID
#   B2_BACKUP_KEY                — B2 application key
#   PGBACKREST_REPO1_CIPHER_PASS — repo encryption passphrase
#
# Usage:
#   bash scripts/pgbackrest-backup.sh --type full|diff|incr [--stanza <name>] [--config <path>]

set -euo pipefail

STANZA="kb"
CONFIG="${PGBACKREST_CONFIG:-ops/pgbackrest.conf}"
BACKUP_TYPE=""

usage() {
    echo "Usage: $0 --type full|diff|incr [--stanza <name>] [--config <path>]" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --type)
            BACKUP_TYPE="$2"
            shift 2
            ;;
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
            usage
            ;;
    esac
done

if [[ -z "${BACKUP_TYPE}" ]]; then
    echo "ERROR: --type is required (full, diff, or incr)" >&2
    usage
fi

case "${BACKUP_TYPE}" in
    full|diff|incr) ;;
    *)
        echo "ERROR: --type must be 'full', 'diff', or 'incr' (got '${BACKUP_TYPE}')" >&2
        usage
        ;;
esac

# Validate config is present.
if [[ ! -f "${CONFIG}" ]]; then
    echo "ERROR: config file not found: ${CONFIG}" >&2
    exit 1
fi

# Validate required environment variables.
if [[ -z "${B2_BACKUP_KEY_ID:-}" ]]; then
    echo "ERROR: B2_BACKUP_KEY_ID is not set" >&2
    exit 1
fi
if [[ -z "${B2_BACKUP_KEY:-}" ]]; then
    echo "ERROR: B2_BACKUP_KEY is not set" >&2
    exit 1
fi

echo "=== pgBackRest ${BACKUP_TYPE} backup — stanza '${STANZA}' ==="
echo "Started at $(date -Iseconds)"

# Run the backup. pgBackRest's internal lock prevents concurrent runs; if a
# backup is already in flight this exits non-zero with a clear message.
pgbackrest --config="${CONFIG}" --stanza="${STANZA}" \
    --type="${BACKUP_TYPE}" \
    --log-level-console=info \
    backup

echo "=== Backup complete at $(date -Iseconds) ==="

# Print retention summary so operators can see what's on disk.
echo ""
echo "=== Retention summary ==="
pgbackrest --config="${CONFIG}" --stanza="${STANZA}" info
