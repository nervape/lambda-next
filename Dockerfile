ARG RUST_IMAGE=rust:1.85-slim
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS builder

WORKDIR /usr/src/app

# Install dependencies
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy the Cargo files for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY spore-api-server ./spore-api-server

# Copy the actual source code
COPY src ./src
COPY spore-api-server/src ./spore-api-server/src

# Rebuild with actual source code
RUN cargo build --release

# Runtime stage
FROM ${RUNTIME_IMAGE}

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/app/target/release/lambda_next .

# Copy config directory
COPY config/default.toml ./config/default.toml

# Set the entrypoint
CMD ["./lambda_next"]
