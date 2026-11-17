# syntax=docker/dockerfile:1
# Multi-platform zero-QEMU build:
# - Stage 1 runs 100% natively on $BUILDPLATFORM (arm64 or amd64), cross-compiling
#   minymaton-server to x86_64-unknown-linux-gnu when host != amd64, and downloading +
#   SHA-256 verifying Litestream and Caddy amd64 release binaries natively.
# - Stage 2 (linux/amd64 python:3.12-slim, ported from mcp-shelf-broker/Containerfile)
#   contains zero RUN instructions so Podman/Docker on ARM64 hosts never invokes
#   qemu-x86_64 user-mode emulation.
FROM --platform=$BUILDPLATFORM docker.io/library/debian:bookworm-slim AS builder

ARG MINYMATON_VERSION=0.1.6
ARG MINYMATON_GIT_URL=""
ARG MINYMATON_GIT_REV=""
ARG LITESTREAM_VERSION=0.5.18

ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH="/opt/cargo/bin:${PATH}"

RUN set -eux; \
    HOST_ARCH="$(dpkg --print-architecture)"; \
    apt-get update -qq; \
    apt-get install -y -qq --no-install-recommends \
      build-essential pkg-config cmake curl ca-certificates git tar gzip; \
    if [ "${HOST_ARCH}" != "amd64" ]; then \
      apt-get install -y -qq --no-install-recommends \
        gcc-x86-64-linux-gnu g++-x86-64-linux-gnu libc6-dev-amd64-cross; \
    fi; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    HOST_ARCH="$(dpkg --print-architecture)"; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      sh -s -- -y --profile minimal --default-toolchain stable; \
    if [ "${HOST_ARCH}" != "amd64" ]; then \
      rustup target add x86_64-unknown-linux-gnu; \
    fi

RUN set -eux; \
    HOST_ARCH="$(dpkg --print-architecture)"; \
    TARGET_FLAG=""; \
    if [ "${HOST_ARCH}" != "amd64" ]; then \
      export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc; \
      export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc; \
      export CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++; \
      export AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar; \
      TARGET_FLAG="--target x86_64-unknown-linux-gnu"; \
    fi; \
    if [ -n "${MINYMATON_GIT_URL}" ]; then \
      REV_ARGS=""; \
      if [ -n "${MINYMATON_GIT_REV}" ]; then REV_ARGS="--rev ${MINYMATON_GIT_REV}"; fi; \
      cargo install --locked --git "${MINYMATON_GIT_URL}" ${REV_ARGS} ${TARGET_FLAG} minymaton-server --root /out; \
    else \
      cargo install --locked --version "${MINYMATON_VERSION}" ${TARGET_FLAG} minymaton-server --root /out; \
    fi; \
    if [ "${HOST_ARCH}" != "amd64" ]; then \
      x86_64-linux-gnu-strip /out/bin/minymaton-server; \
    else \
      strip /out/bin/minymaton-server; \
    fi

COPY litestream.yml /tmp/src/litestream.yml
COPY blob_sync.py gateway.py entrypoint.py /tmp/src/

RUN set -eux; \
    install -d -m 0755 /stage/usr/local/bin /stage/opt/minymaton /stage/etc/ssl/certs; \
    install -d -m 0700 /stage/var/lib/minymaton /stage/run/minymaton; \
    cp -a /etc/ssl/certs/. /stage/etc/ssl/certs/; \
    install -m 0755 /out/bin/minymaton-server /stage/usr/local/bin/minymaton-server; \
    install -m 0644 /tmp/src/litestream.yml /stage/etc/litestream.yml; \
    install -m 0755 /tmp/src/blob_sync.py /tmp/src/gateway.py /tmp/src/entrypoint.py /stage/opt/minymaton/; \
    tmpdir="$(mktemp -d)"; \
    cd "${tmpdir}"; \
    curl -fsSLO "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-${LITESTREAM_VERSION}-linux-x86_64.tar.gz"; \
    curl -fsSLO "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/checksums.txt"; \
    grep "litestream-${LITESTREAM_VERSION}-linux-x86_64.tar.gz$" checksums.txt | sha256sum -c -; \
    tar -xzf "litestream-${LITESTREAM_VERSION}-linux-x86_64.tar.gz" -C /stage/usr/local/bin litestream; \
    chmod 0755 /stage/usr/local/bin/litestream; \
    cd /; \
    rm -rf "${tmpdir}"


FROM --platform=linux/amd64 docker.io/library/python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

COPY --from=builder /stage/ /

EXPOSE 8080

ENTRYPOINT ["python3", "/opt/minymaton/entrypoint.py"]
