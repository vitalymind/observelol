ARG RUST_BUILDER_IMAGE
ARG DEBIAN_RUNTIME_IMAGE

FROM ${RUST_BUILDER_IMAGE} AS builder

WORKDIR /app

# Copy workspace manifests first to cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml crates/common/Cargo.toml
COPY dispatcher/Cargo.toml dispatcher/Cargo.toml
COPY worker/Cargo.toml worker/Cargo.toml

# Create dummy sources so cargo can build and cache third-party dependencies
RUN mkdir -p crates/common/src dispatcher/src worker/src && \
    echo "" > crates/common/src/lib.rs && \
    echo "fn main() {}" > dispatcher/src/main.rs && \
    echo "fn main() {}" > worker/src/main.rs && \
    cargo build --release --bin dispatcher && \
    rm -rf crates/common/src dispatcher/src worker/src

# Copy actual source code and build the dispatcher binary
COPY crates/common/src crates/common/src
COPY dispatcher/src dispatcher/src
COPY worker/src worker/src

RUN touch crates/common/src/lib.rs dispatcher/src/main.rs && \
    cargo build --release --bin dispatcher

FROM ${DEBIAN_RUNTIME_IMAGE} AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/dispatcher /usr/local/bin/dispatcher

ENTRYPOINT ["/usr/local/bin/dispatcher"]
