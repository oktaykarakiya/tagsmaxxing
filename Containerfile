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
        mold \
        clang \
    && rm -rf /var/lib/apt/lists/*
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
WORKDIR /app
# Pull in the cached deps.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
# Copy the actual source and build.
COPY . .
RUN cargo build --release -p kb-api --bin kb

# ── Stage 4: Runtime (minimal debian:slim, non-root) ─────────────────────────
FROM docker.io/debian:bookworm-slim AS runtime
# OCI image annotations (https://github.com/opencontainers/image-spec/blob/main/annotations.md)
LABEL org.opencontainers.image.title="Local File Knowledge Base (tagsmaxxing)"
LABEL org.opencontainers.image.description="AI-powered local file knowledge base"
LABEL org.opencontainers.image.licenses="AGPL-3.0-or-later"
LABEL org.opencontainers.image.source="https://github.com/oktaykarakiya/tagsmaxxing"
LABEL org.opencontainers.image.documentation="https://tagsmaxxing.com"
# curl is needed by the HEALTHCHECK; ca-certificates for TLS; ffmpeg is used by
# the audio/video extractors to transcode media to 16 kHz mono WAV / keyframes
# before whisper transcription.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*
# Self-hosted Tailwind CSS: download the standalone CLI and scan the templates
# plus the Rust sources (handlers pick badge/status classes in code) to produce
# a minified stylesheet containing only the utility classes actually used.
# Dark mode via the `class` strategy. NOTE: the CLI's --content flag is
# single-valued — repeating it silently keeps only the last flag — so all
# globs MUST go in one comma-separated value.
ARG TAILWIND_VERSION=3.4.17
RUN curl -fsSL -o /usr/local/bin/tailwindcss \
        "https://github.com/tailwindlabs/tailwindcss/releases/download/v${TAILWIND_VERSION}/tailwindcss-linux-x64" \
    && chmod +x /usr/local/bin/tailwindcss
COPY crates/api/templates/ /tmp/kb-templates/
COPY crates/assistant/templates/ /tmp/kb-assistant-templates/
COPY crates/api/src/ /tmp/kb-api-src/
COPY crates/assistant/src/ /tmp/kb-assistant-src/
COPY crates/api/tailwind.config.js /tmp/tailwind.config.js
RUN /usr/local/bin/tailwindcss \
        --config /tmp/tailwind.config.js \
        --content '/tmp/kb-templates/**/*.html,/tmp/kb-assistant-templates/**/*.html,/tmp/kb-api-src/**/*.rs,/tmp/kb-assistant-src/**/*.rs' \
        --minify \
        -o /usr/local/lib/kb-static/tailwind.css
RUN rm -rf /tmp/kb-templates /tmp/kb-assistant-templates /tmp/kb-api-src /tmp/kb-assistant-src /tmp/tailwind.config.js /usr/local/bin/tailwindcss
# Self-hosted frontend assets (HTMX + extensions).
COPY crates/api/static/ /usr/local/lib/kb-static/
# OpenCode CLI for the assistant agent (v1.17.13 from GitHub releases).
ARG OPENCODE_VERSION=1.17.13
RUN curl -fsSL -o /tmp/opencode.tar.gz \
        "https://github.com/anomalyco/opencode/releases/download/v${OPENCODE_VERSION}/opencode-linux-x64.tar.gz" \
    && tar -xzf /tmp/opencode.tar.gz -C /usr/local/bin/ \
    && rm /tmp/opencode.tar.gz \
    && chmod +x /usr/local/bin/opencode
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
