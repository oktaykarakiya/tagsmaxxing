#!/usr/bin/env bash
# PITR restore: restore the Postgres cluster from a pgBackRest backup (plan §21).
#
# This script is part of the DR runbook (docs/DR_RUNBOOK.md). It assumes:
#   - A fresh node has been provisioned (see DR_RUNBOOK.md §"Provision node").
#   - Postgres binaries (same major version) and pgBackRest are installed.
#   - ops/pgbackrest.conf (or /etc/pgbackrest/pgbackrest.conf) is in place.
#   - Required environment variables are set (B2_BACKUP_KEY_ID,
#     B2_BACKUP_KEY, PGBACKREST_REPO1_CIPHER_PASS).
#
# The script STOPS Postgres, restores the data directory, and restarts Postgres.
# On success it prints the recovery target time and the last WAL segment applied.
#
# Usage:
#   # Restore to the latest available point (end of WAL archive):
#   bash scripts/pgbackrest-restore.sh
#
#   # Point-in-time restore (e.g. undo a bad migration):
#   bash scripts/pgbackrest-restore.sh --target "2026-06-01 12:00:00+00"
#
# Options:
#   --target <time>   Point-in-time to recover to (ISO-8601 timestamp).
#                     If omitted, restores to the latest available point.
#   --stanza <name>   Stanza name (default: kb).
#   --config <path>   Path to pgbackrest.conf (default: ops/pgbackrest.conf).
#   --pgdata <path>   Postgres data directory (default: /var/lib/postgresql/data).
#   --force           Skip the confirmation prompt.

set -euo pipefail

STANZA="kb"
CONFIG="${PGBACKREST_CONFIG:-ops/pgbackrest.conf}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
TARGET=""
FORCE=false

usage() {
    echo "Usage: $0 [--target <time>] [--stanza <name>] [--config <path>] [--pgdata <path>] [--force]" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)
            TARGET="$2"
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
        --pgdata)
            PGDATA="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            ;;
    esac
done

# ── Pre-flight checks ────────────────────────────────────────────────────

if [[ ! -f "${CONFIG}" ]]; then
    echo "ERROR: config file not found: ${CONFIG}" >&2
    exit 1
fi

if [[ -z "${B2_BACKUP_KEY_ID:-}" ]]; then
    echo "ERROR: B2_BACKUP_KEY_ID is not set" >&2
    exit 1
fi
if [[ -z "${B2_BACKUP_KEY:-}" ]]; then
    echo "ERROR: B2_BACKUP_KEY is not set" >&2
    exit 1
fi

# ── Confirmation ─────────────────────────────────────────────────────────

echo "=== pgBackRest PITR RESTORE — stanza '${STANZA}' ==="
echo ""
echo "  Config:   ${CONFIG}"
echo "  PGDATA:   ${PGDATA}"
if [[ -n "${TARGET}" ]]; then
    echo "  Target:   ${TARGET} (point-in-time)"
else
    echo "  Target:   latest (end of WAL archive)"
fi
echo ""

if [[ "${FORCE}" != "true" ]]; then
    echo "WARNING: This will REPLACE the contents of '${PGDATA}'."
    echo "The existing Postgres data directory will be moved to '${PGDATA}.bak'."
    echo ""
    read -r -p "Proceed? [y/N] " confirm
    if [[ "${confirm}" != "y" && "${confirm}" != "Y" ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# ── Stop Postgres ────────────────────────────────────────────────────────

echo "=== Stopping Postgres ==="
if pg_isready -h 127.0.0.1 -p 5432 &>/dev/null; then
    echo "Postgres is running. Stopping…"
    pg_ctlcluster "$(pg_lsclusters -h | head -1 | awk '{print $1}')" "$(pg_lsclusters -h | head -1 | awk '{print $2}')" stop 2>/dev/null || \
    pg_ctl -D "${PGDATA}" stop -m fast 2>/dev/null || \
    echo "NOTE: could not stop Postgres via pg_ctlcluster or pg_ctl; proceeding anyway."
else
    echo "Postgres is not running — proceeding with restore."
fi

# ── Save existing data (safety net) ──────────────────────────────────────

if [[ -d "${PGDATA}" ]]; then
    BACKUP_DIR="${PGDATA}.bak.$(date +%Y%m%d-%H%M%S)"
    echo "=== Moving existing PGDATA to ${BACKUP_DIR} ==="
    mv "${PGDATA}" "${BACKUP_DIR}"
    echo "Saved previous data directory to ${BACKUP_DIR}"
fi

# ── Restore ──────────────────────────────────────────────────────────────

echo "=== Restoring from backup ==="
echo "Started at $(date -Iseconds)"

RESTORE_ARGS=(
    --config="${CONFIG}"
    --stanza="${STANZA}"
    --pg1-path="${PGDATA}"
    --log-level-console=info
)

if [[ -n "${TARGET}" ]]; then
    RESTORE_ARGS+=(--type=time "--target=${TARGET}")
fi

# The restore command fetches the base backup + replays WAL up to the target.
# If no target is given, it replays ALL available WAL (latest point).
pgbackrest "${RESTORE_ARGS[@]}" restore

echo "=== Restore complete at $(date -Iseconds) ==="

# ── Post-restore configuration ───────────────────────────────────────────

# pgBackRest writes recovery.signal (PG ≥ 12) or recovery.conf into PGDATA;
# Postgres will start recovery on next boot. The restore already handled
# WAL replay, so on start Postgres just cleans up and becomes consistent.
echo "=== PGDATA restored: $(du -sh "${PGDATA}" | cut -f1) ==="

# ── Start Postgres ───────────────────────────────────────────────────────

echo "=== Starting Postgres ==="
if command -v pg_ctlcluster &>/dev/null; then
    pg_ctlcluster "$(pg_lsclusters -h | head -1 | awk '{print $1}')" "$(pg_lsclusters -h | head -1 | awk '{print $2}')" start
elif command -v pg_ctl &>/dev/null; then
    pg_ctl -D "${PGDATA}" -l "${PGDATA}/log" start
else
    echo "ERROR: cannot find pg_ctlcluster or pg_ctl to start Postgres." >&2
    echo "Start Postgres manually, then verify with pg_isready." >&2
    exit 1
fi

# ── Verify ───────────────────────────────────────────────────────────────

echo ""
echo "=== Verifying Postgres ==="
sleep 2
if pg_isready -h 127.0.0.1 -p 5432 -t 10 &>/dev/null; then
    echo "Postgres is accepting connections."
else
    echo "WARNING: Postgres is not ready yet. Check logs at ${PGDATA}/log." >&2
fi

echo ""
echo "=== Restore finished ==="
echo "Stanza info:"
pgbackrest --config="${CONFIG}" --stanza="${STANZA}" info

echo ""
echo "Next steps (see docs/DR_RUNBOOK.md):"
echo "  1. Run integrity checks: just ci-integration"
echo "  2. Verify tenant data: psql -c \"SELECT count(*) FROM documents;\""
echo "  3. Point the application at this node."
echo "  4. Re-enable WAL archiving (archive_command in postgresql.conf)."
echo "  5. Schedule backups on the new node."
