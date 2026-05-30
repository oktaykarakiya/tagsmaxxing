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
come up from a single [`compose.yaml`](./compose.yaml) that runs identically under Docker and
Podman:

```bash
cp .env.example .env        # optional: override ports / image tags / credentials
docker compose up -d        # or: podman compose up -d
```

Both services declare healthchecks, so `compose ps` reports `healthy` once Postgres is accepting
connections and Tika is serving. Postgres is published on `127.0.0.1:5432` and Tika on
`127.0.0.1:9998` by default (change `POSTGRES_PORT` / `TIKA_PORT` if a port is taken). The app
(once containerised) and GPU inference join via later profiles; for now the app runs on the host
against these sidecars. Tear down with `docker compose down` (add `-v` to also drop the data
volume).

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
