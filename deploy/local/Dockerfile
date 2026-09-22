# Locally built development/RC image. Base operating-system components retain
# their own licenses; this Dockerfile is not an OCI redistribution approval.
FROM python:3.14-slim-bookworm@sha256:82bc3c539b8813ada9d68c63b40158fa002f7f33de9bf3312a3dfdc0620dff56 AS python-runtime
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
COPY --from=python-runtime /usr/local /usr/local
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY .sqlx ./.sqlx
COPY examples/python ./examples/python
ENV SQLX_OFFLINE=true
RUN cargo build --release --locked --bins --all-features -p ledgence-worker -p ledgence-orchestrator -p ledgence-cli
COPY tools/release ./tools/release
COPY legal ./legal
COPY LICENSE ./LICENSE
RUN python3 tools/release/notices.py --target "$(rustc -vV | sed -n 's/^host: //p')" --output /legal

FROM python-runtime
ARG SOURCE_REVISION=unrecorded
LABEL org.opencontainers.image.title="Ledgence local candidate" \
      org.opencontainers.image.source="https://github.com/Ledgence/ledgence" \
      org.opencontainers.image.revision=$SOURCE_REVISION
WORKDIR /opt/ledgence
COPY --from=builder /src/target/release/ledgence /src/target/release/ledgence-worker /src/target/release/ledgence-orchestrator /usr/local/bin/
COPY --from=builder /legal /opt/ledgence/legal
COPY sdk/python/ledgence /opt/ledgence/runtime/ledgence
COPY deploy/local /opt/ledgence/local
COPY examples/checkpoint-workflow /opt/ledgence/examples/checkpoint-workflow
COPY LICENSE /opt/ledgence/LICENSE
RUN mkdir -p /programs /cache /demo-state && python3 /opt/ledgence/local/image-inventory.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
STOPSIGNAL SIGTERM
CMD ["ledgence-orchestrator", "--help"]
