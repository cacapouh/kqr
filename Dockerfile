# Multi-stage build for the kqr CLI.
# Build:  docker build -t kqr:dev .
# Run:    docker run --rm kqr:dev --help
#         docker run --rm --network host kqr:dev query -t demo --last 1m "select 1"   # Linux
#         docker run --rm --add-host host.docker.internal:host-gateway \
#             kqr:dev query -t demo --brokers host.docker.internal:9092 ...           # macOS / Windows
#         docker compose -f docker/compose.yaml run --rm \
#             -e KQR_BROKERS=kafka:9094 \
#             kqr query -t demo ...                                                    # against compose Kafka
#
# Forward-compatible: the runtime stage installs librdkafka's transitive
# system deps (libsasl2 / libssl / libzstd) in advance so step 3 onwards
# (which adds the rdkafka crate) does not need to touch this Dockerfile.

# ---- builder ---------------------------------------------------------------
FROM rust:1.91-slim-bookworm AS builder

# Compile-time deps for librdkafka (will be needed once the rdkafka crate
# lands in step 3). Installed up-front so layer caching survives across
# steps.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        pkg-config \
        libsasl2-dev \
        libssl-dev \
        libzstd-dev \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the workspace and build the release binary. Cargo.lock is committed
# so --locked guarantees reproducible builds.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY kqr-core ./kqr-core
COPY kqr-cli ./kqr-cli

RUN cargo build --release --bin kqr --locked \
    && strip target/release/kqr

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsasl2-2 \
        libssl3 \
        libzstd1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 kqr

USER kqr
WORKDIR /home/kqr

COPY --from=builder /build/target/release/kqr /usr/local/bin/kqr

ENTRYPOINT ["kqr"]
CMD ["--help"]
