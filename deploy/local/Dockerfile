# Frontend tooling is confined to this build stage. Only verified static files
# and their retained third-party notices enter the CPython runtime image.
FROM node:24.21.0-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6 AS console-builder
WORKDIR /src/console
RUN corepack enable && corepack prepare pnpm@11.27.1 --activate
COPY console/package.json console/pnpm-lock.yaml console/pnpm-workspace.yaml ./
COPY console/patches ./patches
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY console ./
COPY LICENSE /src/LICENSE
# The full copy can give unchanged patches newer timestamps than the cached
# install. Revalidate the final files without downloading or changing the lockfile.
RUN pnpm install --offline --frozen-lockfile --ignore-scripts
ARG SOURCE_REVISION
ARG SOURCE_DIRTY
RUN LEDGENCE_SOURCE_REVISION="$SOURCE_REVISION" LEDGENCE_SOURCE_DIRTY="$SOURCE_DIRTY" pnpm build

# Base components retain their own licenses. Public image publication requires
# the exact runtime inventory and separately qualified corresponding sources.
FROM python:3.14-slim-bookworm@sha256:82bc3c539b8813ada9d68c63b40158fa002f7f33de9bf3312a3dfdc0620dff56 AS python-runtime
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
COPY --from=python-runtime /usr/local /usr/local
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY .sqlx ./.sqlx
COPY examples/python ./examples/python
ENV SQLX_OFFLINE=true
RUN cargo build --release --locked --all-features -p ledgence-cli --bin ledgence
COPY tools/release ./tools/release
COPY legal ./legal
COPY LICENSE ./LICENSE
RUN python3 tools/release/notices.py --target "$(rustc -vV | sed -n 's/^host: //p')" --output /legal

FROM python-runtime
ARG SOURCE_REVISION=unrecorded
ARG VERSION=unrecorded
LABEL org.opencontainers.image.title="Ledgence" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.source="https://github.com/Ledgence/ledgence" \
      org.opencontainers.image.revision=$SOURCE_REVISION
WORKDIR /opt/ledgence
COPY --from=builder /src/target/release/ledgence /usr/local/bin/
COPY --from=builder /legal /opt/ledgence/legal
COPY deploy/distribution/base-image.json /opt/ledgence/legal/base-image.json
COPY --from=console-builder /src/console/dist /opt/ledgence/console
COPY sdk/python/ledgence /opt/ledgence/runtime/ledgence
COPY deploy/local /opt/ledgence/local
COPY examples/checkpoint-workflow /opt/ledgence/examples/checkpoint-workflow
COPY LICENSE /opt/ledgence/LICENSE
RUN mkdir -p /programs /cache /demo-state && python3 /opt/ledgence/local/image-inventory.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
STOPSIGNAL SIGTERM
CMD ["ledgence", "--help"]
