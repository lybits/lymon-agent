# syntax=docker/dockerfile:1.7
#
# Build context expected: parent directory containing both
# lymon-agent/ and lymon-protos/ as siblings.
#
# Built from docker-compose with:
#   build:
#     context: ..
#     dockerfile: lymon-agent/Dockerfile

# =============================================================================
# Build stage
# =============================================================================
FROM rust:1.85-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Protos must be present at the path expected by build.rs (../lymon-protos)
COPY lymon-protos /build/lymon-protos
COPY lymon-agent  /build/lymon-agent

WORKDIR /build/lymon-agent

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/lymon-agent/target \
    cargo build --release && \
    cp target/release/lymon-agent /lymon-agent-bin

# =============================================================================
# Runtime stage (distroless — no shell, minimal attack surface)
# =============================================================================
# We use the root variant (not :nonroot) so the agent can write to the
# /var/lib/lymon-agent volume mounted by docker-compose. Production hardening
# (drop to nonroot with proper volume ownership) is Fase 1.
FROM gcr.io/distroless/cc-debian12:latest

COPY --from=builder /lymon-agent-bin /usr/local/bin/lymon-agent

ENTRYPOINT ["/usr/local/bin/lymon-agent"]
