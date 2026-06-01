# Disaster Recovery Runbook — Local File Knowledge Base

**Plan reference:** §21 (Backups, PITR & disaster recovery)
**Last updated:** 2026-06-01

## Recovery Objectives

| Metric | Target | Notes |
|--------|--------|-------|
| **RTO** (Recovery Time Objective) | ≤ 30 minutes | Provision node + restore backup + start Postgres + start app |
| **RPO** (Recovery Point Objective) | ≤ 1 hour | Hourly incremental WAL archiving; continuous archive-push reduces window to seconds |

The app is stateless (§1) — the only durable state is Postgres. Recovery ≈ restore time.

**Idempotency:** All backup and restore operations are idempotent and safe to re-run.
Stanza creation is a no-op if the stanza already exists. Backup is blocked by
pgBackRest's internal lock if another backup is in progress. Restore overwrites the
data directory each time (the previous contents are preserved in a `.bak` directory).
The entire recovery procedure can be repeated without side effects.

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Postgres    │────▶│  pgBackRest      │────▶│  B2 Backup Bucket   │
│  (pgvector)  │     │  WAL archiving   │     │  (Object Lock ON)   │
│  checksums   │     │  + compression   │     │  separate from app  │
└──────────────┘     └──────────────────┘     └─────────────────────┘
```

- **pgBackRest** pushes WAL segments continuously + scheduled full/incremental backups.
- **Backup bucket** has Object Lock enabled — ransomware cannot delete backups (§21, §23).
- **Repo encryption** (AES-256-CBC) — backup data is encrypted at rest in B2 (§24).
- **App blobs** (B2Blob, P8-T1) are protected by B2 versioning + Object Lock; they are not
  duplicated into the backup bucket.

## Prerequisites

Before a restore, you need:

1. **pgBackRest** installed on the target node (`apt install pgbackrest` / `yum install pgbackrest`).
2. **Postgres** installed, same major version (17) as the backup source.
3. **ops/pgbackrest.conf** (or `/etc/pgbackrest/pgbackrest.conf`) on the target node.
4. **Secrets** set in the environment (§24):
   ```bash
   export B2_BACKUP_KEY_ID="<backup-bucket-key-id>"
   export B2_BACKUP_KEY="<backup-bucket-key>"
   export PGBACKREST_REPO1_CIPHER_PASS="<repo-encryption-passphrase>"
   ```
5. **Network access** from the target node to the B2 S3 endpoint (`s3.us-west-004.backblazeb2.com`).
6. **Sufficient disk** for the restored data directory (check backup size with `pgbackrest info`).

## Initial Setup (run once per cluster)

### 1. Configure pgBackRest

Copy the configuration and set secrets:

```bash
sudo mkdir -p /etc/pgbackrest
sudo cp ops/pgbackrest.conf /etc/pgbackrest/pgbackrest.conf

# Set B2 credentials + encryption passphrase (use a secrets manager in production).
export B2_BACKUP_KEY_ID="004..."
export B2_BACKUP_KEY="K004..."
export PGBACKREST_REPO1_CIPHER_PASS="$(openssl rand -base64 48)"
```

### 2. Create the stanza

```bash
bash scripts/pgbackrest-stanza-create.sh
```

Expected output:
```
=== Creating pgBackRest stanza 'kb' ===
Config: ops/pgbackrest.conf
...
stanza: kb
    status: ok
    cipher: aes-256-cbc

=== Stanza created successfully ===
```

### 3. Enable WAL archiving on Postgres

Edit `postgresql.conf` (or `ALTER SYSTEM`):

```ini
# WAL archiving for pgBackRest (§21)
archive_mode = on
archive_command = 'pgbackrest --stanza=kb archive-push %p'
wal_level = replica
```

In the compose deployment (`compose.yaml`), add these as `POSTGRES_INITDB_ARGS` overrides
or configure them via the Postgres config volume. After changing, restart Postgres:

```bash
# On the host:
sudo systemctl restart postgresql
# In compose:
podman compose restart postgres
```

Verify archiving is working:

```bash
# Check that WAL segments are being archived.
pgbackrest --config=ops/pgbackrest.conf --stanza=kb check
```

Expected output:
```
stanza: kb
    status: ok
    ...
    archive: ok (0 pending)
```

### 4. Schedule backups

Add to the crontab (or use systemd timers):

```cron
# Daily full backup at 03:00 local
0 3 * * * bash /opt/kb/scripts/pgbackrest-backup.sh --type full

# Hourly incremental backup at 42 minutes past the hour
42 * * * * bash /opt/kb/scripts/pgbackrest-backup.sh --type incr
```

Or, with systemd timers — create `pgbackrest-backup-full.service`:

```ini
[Unit]
Description=pgBackRest full backup for kb

[Service]
Type=oneshot
Environment=B2_BACKUP_KEY_ID=%N_backup_key_id
Environment=B2_BACKUP_KEY=%N_backup_key
Environment=PGBACKREST_REPO1_CIPHER_PASS=%N_cipher_pass
ExecStart=bash /opt/kb/scripts/pgbackrest-backup.sh --type full
```

And matching `pgbackrest-backup-full.timer`:

```ini
[Unit]
Description=Daily pgBackRest full backup

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Repeat for incremental (`pgbackrest-backup-incr.{service,timer}` with `--type incr`).

## Restore Procedure

### Step 1 — Provision a replacement node

```bash
# Fresh VM or bare-metal host. Install dependencies:
sudo apt update && sudo apt install -y postgresql-17 postgresql-client-17 pgbackrest podman

# Install the application (from Containerfile or pre-built image):
podman pull <kb-image-registry>/kb:latest

# Place the configuration and scripts:
scp -r ops/pgbackrest.conf user@new-node:/opt/kb/ops/
scp -r scripts/pgbackrest-restore.sh user@new-node:/opt/kb/scripts/
```

### Step 2 — Set secrets and restore

```bash
export B2_BACKUP_KEY_ID="<backup-bucket-key-id>"
export B2_BACKUP_KEY="<backup-bucket-key>"
export PGBACKREST_REPO1_CIPHER_PASS="<repo-encryption-passphrase>"

# Restore to the latest point:
bash scripts/pgbackrest-restore.sh --force

# Or, point-in-time (e.g. undo a bad migration at 11:45 UTC):
bash scripts/pgbackrest-restore.sh --target "2026-06-01 11:45:00+00" --force
```

Expected output:
```
=== pgBackRest PITR RESTORE — stanza 'kb' ===
...
=== Stopping Postgres ===
Postgres is not running — proceeding with restore.
=== Moving existing PGDATA to /var/lib/postgresql/data.bak.20260601-120000 ===
=== Restoring from backup ===
Started at 2026-06-01T12:00:00+00:00
restore: size = 2.3GiB
=== Restore complete at 2026-06-01T12:05:00+00:00 ===
=== PGDATA restored: 2.3G ===
=== Starting Postgres ===
=== Verifying Postgres ===
Postgres is accepting connections.
=== Restore finished ===
```

### Step 3 — Verify integrity

```bash
# Basic connectivity:
psql -h 127.0.0.1 -U kb -d kb -c "SELECT count(*) FROM documents;"

# Run the integration test suite against the restored DB:
cd /opt/kb
just ci-integration
```

Expected: document count matches pre-failure state; `just ci-integration` is green.

### Step 4 — Start the application

```bash
podman compose up -d

# Wait for healthy:
podman compose ps
# All services should show (healthy).

# Verify the API responds:
curl -sf http://localhost:9999/health
# Expected: {"status":"ok"}
```

### Step 5 — Re-enable WAL archiving and schedule backups

Repeat the "Initial Setup" steps 3 and 4 on the new node so WAL archiving resumes and
scheduled backups continue.

### Step 6 — Repoint DNS / clients

Update DNS or load-balancer configuration to route traffic to the new node. The app is
stateless; no session or cache migration is needed.

## Backup Verification (Scheduled)

An untested backup is not a backup (§21). A scheduled job restores the latest backup to a
scratch instance and runs integrity checks:

```bash
# Run on a non-production host with sufficient disk:
bash scripts/pgbackrest-restore.sh --pgdata /tmp/kb-restore-test --force

# Verify:
psql -h /tmp -p 5433 -d kb -c "SELECT count(*) FROM documents;"
psql -h /tmp -p 5433 -d kb -c "SELECT count(*) FROM chunks;"

# Clean up:
rm -rf /tmp/kb-restore-test
```

On failure: alert on `pgbackrest check` returning non-zero or the restore test failing.
Monitor: backup freshness (last backup timestamp) and WAL archive lag (pending segments).

## Common Failure Scenarios

### Scenario A: Accidental DROP TABLE / bad migration

Use PITR to restore to just before the bad statement was executed:

```bash
bash scripts/pgbackrest-restore.sh --target "2026-06-01 14:25:00+00" --force
```

The `--target` timestamp should be the moment RIGHT BEFORE the bad statement. WAL replay
stops at this point, so the destructive action never happened.

### Scenario B: Full node loss (hardware failure, disk corruption)

Follow the full restore procedure (Steps 1–6). Data-checksums (§23) detect on-disk corruption;
pgBackRest backups are block-level consistent and skip corrupt pages.

### Scenario C: B2 outage

- pgBackRest backups pause until B2 is reachable (archive-async queues WAL locally).
- WAL segments are written to `pg_wal/` on the Postgres host during the outage.
- The application's B2 blob operations (ingest/retrieve) handle B2 unavailability via
  the read-through cache (P8-T2) — reads from cache, writes queue/retry.
- No data loss: WAL accumulates locally until B2 recovers.

### Scenario D: Encryption key compromise

If the repo cipher passphrase or B2 backup key is compromised:

1. Rotate the B2 application key in the B2 console (new key for the backup bucket).
2. Generate a new cipher passphrase: `openssl rand -base64 48`
3. Start a new stanza with the new keys (archive the old repository as a fallback).
4. Old backups remain accessible with the old key (Object Lock prevents deletion).

## Appendix: Systemd Timer Units

`/etc/systemd/system/pgbackrest-backup-full.service`:
```ini
[Unit]
Description=pgBackRest full backup for kb stanza
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
Environment=PGBACKREST_CONFIG=/etc/pgbackrest/pgbackrest.conf
EnvironmentFile=-/etc/kb/backup.env
ExecStart=bash /opt/kb/scripts/pgbackrest-backup.sh --type full
User=postgres
```

`/etc/systemd/system/pgbackrest-backup-full.timer`:
```ini
[Unit]
Description=Daily pgBackRest full backup

[Timer]
OnCalendar=*-*-* 03:00:00
RandomizedDelaySec=300
Persistent=true

[Install]
WantedBy=timers.target
```

`/etc/systemd/system/pgbackrest-backup-incr.timer`:
```ini
[Unit]
Description=Hourly pgBackRest incremental backup

[Timer]
OnCalendar=*-*-* *:42:00
RandomizedDelaySec=60
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl enable --now pgbackrest-backup-full.timer
sudo systemctl enable --now pgbackrest-backup-incr.timer
```

`/etc/kb/backup.env` (mode 600, owned by postgres):
```ini
B2_BACKUP_KEY_ID=...
B2_BACKUP_KEY=...
PGBACKREST_REPO1_CIPHER_PASS=...
```
