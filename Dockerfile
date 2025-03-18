FROM rust:1.81-bullseye AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev libpq-dev ca-certificates \
    clang libclang-dev llvm-dev \
    cmake \
 && rm -rf /var/lib/apt/lists/*

ENV LIBCLANG_PATH=/usr/lib/llvm-11/lib

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl1.1 libpq5 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/penumbra-explorer /app/penumbra-explorer
COPY genesis.json /app/genesis.json

RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 3000

ENTRYPOINT ["/app/penumbra-explorer"]
