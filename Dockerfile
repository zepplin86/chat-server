# syntax=docker/dockerfile:1

# One build stage compiles every service; each service image copies out its own binary.
FROM rust:1-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY proto proto
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --workspace \
    && mkdir /out \
    && cp target/release/gateway target/release/chat-service \
          target/release/persist-worker target/release/push-worker /out/

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home app
USER app

FROM runtime AS gateway
COPY --from=builder /out/gateway /usr/local/bin/gateway
EXPOSE 8080
CMD ["gateway"]

FROM runtime AS chat-service
COPY --from=builder /out/chat-service /usr/local/bin/chat-service
EXPOSE 50051 8081
CMD ["chat-service"]

FROM runtime AS persist-worker
COPY --from=builder /out/persist-worker /usr/local/bin/persist-worker
CMD ["persist-worker"]

FROM runtime AS push-worker
COPY --from=builder /out/push-worker /usr/local/bin/push-worker
CMD ["push-worker"]
