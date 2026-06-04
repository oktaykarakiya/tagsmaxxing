# ──────────────────────────────────────────────────────────────────────────────
# Multi-stage Containerfile for the Local File Knowledge Base (plan §14, P7-T1).
#
# Stages:
#   1. chef    — cargo-chef for dependency-layer caching
#   2. planner — compute the dependency recipe
#   3. builder — compile the release binary (rustls, no OpenSSL)
#   4. runtime — minimal debian:slim image, non-root user
#
# Build with:
#   podman build -t kb:latest .
#
# Works with both `podman build` and `docker build` (common subset spec).
# ──────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Chef (cargo-chef, for dep-caching) ─────────────────────────────
FROM docker.io/rust:1.92.0-slim-bookworm AS chef
RUN cargo install cargo-chef --version 0.1.73
WORKDIR /app

# ── Stage 2: Planner (compute the dependency recipe) ─────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Builder (compile release binary) ────────────────────────────────
FROM chef AS builder
# Only the essentials for a rustls build — no libssl-dev / OpenSSL.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Pull in the cached deps.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
# Copy the actual source and build.
COPY . .
RUN cargo build --release -p kb-api --bin kb

# ── Stage 4: Runtime (minimal debian:slim, non-root) ─────────────────────────
FROM docker.io/debian:bookworm-slim AS runtime
# curl is needed by the HEALTHCHECK; ca-certificates for TLS; ffmpeg is used by
# the audio/video extractors to transcode media to 16 kHz mono WAV / keyframes
# before whisper transcription.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*
# Non-root user (uid 1000) as required by plan §14.
RUN useradd --uid 1000 --create-home --shell /sbin/nologin kb
# Copy only the compiled binary.
COPY --from=builder /app/target/release/kb /usr/local/bin/kb
# Create data directories for blob storage and logs (owned by the kb user).
RUN mkdir -p /data/kb-blobs /data/logs && chown -R kb:kb /data
USER kb
WORKDIR /data
EXPOSE 9999
# Liveness probe: the binary's /health endpoint returns 200 when alive.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9999/health || exit 1
ENTRYPOINT ["/usr/local/bin/kb"]
CMD ["serve", "--config", "/data/config.toml"]
