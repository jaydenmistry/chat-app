# syntax=docker/dockerfile:1
#
# One image for everything: the server binary plus the built web client.
#   docker build -f deploy/Dockerfile -t chat-app .
#
# Behind a TLS-intercepting proxy, pass its CA certificate as a build secret:
#   docker build --secret id=proxy_ca,src=/path/ca.pem ...
# Without the secret, the builds below use the image's normal trust store.

ARG REGISTRY=docker.io

# --- Web client ---------------------------------------------------------------
FROM ${REGISTRY}/library/node:26.13.0-alpine3.24 AS web
WORKDIR /web
RUN --mount=type=secret,id=proxy_ca,required=false \
    if [ -f /run/secrets/proxy_ca ]; then export NODE_EXTRA_CA_CERTS=/run/secrets/proxy_ca; fi; \
    npm install --global pnpm@12.16.1
COPY web/package.json web/pnpm-lock.yaml ./
RUN --mount=type=secret,id=proxy_ca,required=false \
    --mount=type=cache,target=/root/.cache/pnpm \
    if [ -f /run/secrets/proxy_ca ]; then export NODE_EXTRA_CA_CERTS=/run/secrets/proxy_ca; fi; \
    pnpm install --frozen-lockfile --store-dir /root/.cache/pnpm
COPY web/ ./
RUN pnpm build

# --- Server -------------------------------------------------------------------
FROM ${REGISTRY}/library/rust:1.99.1-slim-trixie AS server
WORKDIR /src
# Use the image's toolchain instead of letting rust-toolchain.toml make rustup
# download components the build doesn't need.
ENV RUSTUP_TOOLCHAIN=1.99.1 \
    SQLX_OFFLINE=true
COPY server/ ./
# The cache mounts keep the crate registry and build artifacts between builds,
# so a code change recompiles only our crates.
RUN --mount=type=secret,id=proxy_ca,required=false \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    if [ -f /run/secrets/proxy_ca ]; then export CARGO_HTTP_CAINFO=/run/secrets/proxy_ca; fi; \
    cargo build --release --locked -p server && cp target/release/server /server

# --- Runtime ------------------------------------------------------------------
# Distroless: glibc, CA certificates and nothing else. No shell, no package
# manager; runs as an unprivileged user.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:08476c244888bfb425bae1ae4dd7c17f093e972aab8426883d97f3c6f280f7c1
COPY --from=server /server /app/server
COPY --from=web /web/dist /app/web
ENV BIND_ADDR=0.0.0.0:8080 \
    STATIC_DIR=/app/web
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=60s --retries=3 \
    CMD ["/app/server", "healthcheck"]
ENTRYPOINT ["/app/server"]
CMD ["serve"]
