# Local File Knowledge Base

A self-hosted, modular tool that ingests files of **any** type, enriches them with an
LLM-generated title/summary/tags plus your notes and extracted metadata, stores everything in
Postgres, and retrieves it via **hybrid (vector + keyword) search with reranking** — all
inference on **local llama.cpp servers** behind a slot-aware, multi-host scheduler.

Full design: [`local-kb-plan.md`](./local-kb-plan.md). Build protocol: [`CLAUDE.md`](./CLAUDE.md).

> **Status: foundation (bootstrap).** The Cargo workspace, quality gates, build ledger, and
> the `kb-core` contract (domain types + capability traits) are in place and green. Feature
> crates are compiling skeletons, implemented incrementally per
> [`BUILD_LEDGER.toml`](./BUILD_LEDGER.toml). The HTTP API will serve on **port 9999**.

## Workspace

```
crates/
  core/        # domain types + capability traits (the stable, I/O-free contract)  ← implemented
  config/      # TOML config + hot-reload                                          ← skeleton
  scheduler/   # multi-host, slot-aware model pool (the centerpiece)               ← skeleton
  llm/         # llama.cpp OpenAI-compatible client; Tagger/Embedder/Reranker      ← skeleton
  extract/     # Extractor impls (native docs, Tika, ffmpeg/whisper, vision)       ← skeleton
  store/       # Postgres + pgvector store; hybrid search                          ← skeleton
  pipeline/    # ingest + query orchestration                                      ← skeleton
  api/         # axum HTTP + clap CLI + web UI (serves on :9999)                   ← skeleton
```

## Develop

Prerequisites: Rust (pinned to 1.92.0 via `rust-toolchain.toml`) and the gate tools. To
install/restore the gate tools (`just`, `cargo-deny`, `cargo-audit`, `cargo-llvm-cov`):

```bash
just bootstrap-tools        # or: bash scripts/bootstrap-dev.sh
```

Run the full Definition-of-Done gate suite (identical to CI):

```bash
just ci                     # fmt-check, build, clippy -D warnings, test, deny, audit, coverage
```

Individual gates: `just fmt`, `just clippy`, `just test`, `just deny`, `just audit`,
`just cov`. List everything with `just`.

## Run the sidecar stack

The stateful sidecars — Postgres (with **pgvector**, `data-checksums` on) and **Apache Tika** —
come up from a single [`compose.yaml`](./compose.yaml) via **Podman** (this project targets
Podman exclusively — no Docker):

```bash
cp .env.example .env        # optional: override ports / image tags / credentials
podman compose up -d
```

Both services declare healthchecks, so `podman compose ps` reports `healthy` once Postgres is
accepting connections and Tika is serving. Postgres is published on `127.0.0.1:5432` and Tika on
`127.0.0.1:9998` by default (change `POSTGRES_PORT` / `TIKA_PORT` if a port is taken). The app
(once containerised) and GPU inference join via later profiles; for now the app runs on the host
against these sidecars. Tear down with `podman compose down` (add `-v` to also drop the data
volume).

## Deploy with Quadlet (systemd)

For single-node Podman users, **Quadlet `.container` files** are the recommended production
method (plan §14). Quadlet translates `.container` / `.pod` / `.volume` files into native
systemd units, so your stack starts on boot, restarts on failure, and logs via `journalctl`.

All files are in [`quadlet/`](./quadlet). Copy them into the systemd user unit directory:

```bash
mkdir -p ~/.config/containers/systemd
cp quadlet/* ~/.config/containers/systemd/
```

### Units

| File | What it runs |
|------|-------------|
| `kb.pod` | Shared pod — all containers share a network namespace (services reach each other via `localhost`) |
| `kb-postgres.container` | Postgres 17 + pgvector (data-checksums on, §23), persistent volume |
| `kb-tika.container` | Apache Tika 3.3 (document text/metadata extraction, §2) |
| `kb-app.container` | The `kb` binary — API on `:9999` (`/health`, search, ingest, web UI) |
| `kb-llama.container` | llama.cpp server — optional GPU inference (chat + embed + rerank) |
| `kb-whisper.container` | whisper.cpp server — optional GPU transcription |
| `kb-pgdata.volume` | Persistent named volume for Postgres data |
| `kb-app-data.volume` | Persistent named volume for the app (blob store + logs) |

### Quick start

```bash
# 1. Build the app image
podman build -t kb:latest .

# 2. Place your GGUF models (optional — only for GPU inference)
mkdir -p ~/.config/containers/systemd/models
cp /path/to/model.gguf ~/.config/containers/systemd/models/model.gguf
cp /path/to/whisper-model.gguf ~/.config/containers/systemd/models/whisper-model.gguf

# 3. Edit the container files to set passwords / secrets:
#    - kb-postgres.container: POSTGRES_PASSWORD
#    - kb-app.container: SESSION_SECRET (≥32 bytes), POSTGRES_URL passwords

# 4. Reload systemd and start the core stack
systemctl --user daemon-reload
systemctl --user start kb-postgres.service kb-tika.service kb-app.service

# 5. (Optional) Enable GPU inference — only if you have an NVIDIA GPU
#    + nvidia-container-toolkit installed
systemctl --user start kb-llama.service kb-whisper.service

# 6. Verify
curl http://localhost:9999/health
journalctl --user -u kb-app.service -f

# 7. Enable on boot
systemctl --user enable kb-postgres.service kb-tika.service kb-app.service
```

> **Inside the pod** all containers share `localhost`, so the app connects to Postgres at
> `localhost:5432` and Tika at `localhost:9998`. The pod publishes ports to the host
> (loopback by default — remove `127.0.0.1:` in `kb.pod` for LAN access).

> **GPU services** have `AddDevice=nvidia.com/gpu=all`. Remove that line to run CPU-only,
> or just don't enable those two units. See `compose.cpu.yaml` for the equivalent compose
> override.

To validate your files before starting:

```bash
/usr/libexec/podman/quadlet -dryrun -user
```

To tear down:

```bash
systemctl --user stop kb-llama.service kb-whisper.service kb-app.service \
                    kb-tika.service kb-postgres.service
podman pod rm -f kb
```

## Autonomous build loop

Development is **ledger-driven**: one small task at a time, each gated by `just ci`
(see [`CLAUDE.md`](./CLAUDE.md) / plan §31). Inspect and drive it:

```bash
just status                 # ledger summary + next eligible task
just next                   # just the next task id
just loop                   # run the loop until a checkpoint / blocked / phase complete
just loop --approve P1-T1   # clear a human-review checkpoint and continue
just loop --max 1           # do exactly one task
just loop --dry-run         # show the next task without invoking anything
```

The loop invokes Claude Code per task, then **independently re-runs the gates** and confirms
the task committed before advancing. It stops immediately on a red gate, a blocked task, or an
unapproved checkpoint — so progress is always safe and resumable. For unattended runs,
`just loop --yolo` bypasses permission prompts (review the warning it prints).

## License

Dual-licensed under Apache-2.0 or MIT. Model weights are separately licensed.
