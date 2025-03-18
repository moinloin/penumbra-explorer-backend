FROM rust:1.76-slim-bookworm AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a new empty project and build dependencies to cache them
RUN USER=root cargo new --bin penumbra-explorer
WORKDIR /app/penumbra-explorer
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release && \
    rm -rf src/ && \
    rm -rf target/release/.fingerprint/penumbra-explorer-*

# Copy the source code
COPY src/ src/
COPY .cargo/ .cargo/

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libssl-dev \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /app/penumbra-explorer/target/release/penumbra-explorer /app/penumbra-explorer

# Copy the genesis file
COPY genesis.json /app/genesis.json

# Create a non-root user to run the application
RUN useradd -m appuser && \
    chown -R appuser:appuser /app
USER appuser

# Set up the environment
ENV RUST_LOG=info

# Run the application
ENTRYPOINT ["/app/penumbra-explorer"]
CMD ["-s", "${SOURCE_DB_URL}", "-d", "${DEST_DB_URL}", "--genesis-json", "/app/genesis.json"]