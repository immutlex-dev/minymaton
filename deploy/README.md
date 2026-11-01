# Minymaton Operator Deployment Guide

This document describes how to install `minymaton-server` and its helpers,
deploy a shared server, and monitor its storage.

## Deployment Topologies

`minymaton-server` binds only to a loopback address (`--listen 127.0.0.1:PORT`). Remote workstations and external HTTPS clients reach a loopback listener either through an SSH local port forward or through an authenticated reverse proxy on the same host.

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 420}}}%%
flowchart LR
    subgraph workstations["Workstations (SSH loopback forwarding)"]
        windows["Windows workstation<br/>127.0.0.1:18990 -&gt; ssh -&gt; server host<br/>%USERPROFILE%\.local\bin\minymaton-wait.exe"]
        unix["Linux or macOS workstation<br/>127.0.0.1:18990 -&gt; ssh -&gt; server host<br/>~/.local/bin/minymaton-wait"]
    end

    subgraph host["Server host (Linux)"]
        srv["minymaton-server --listen 127.0.0.1:18990"]
        db[("SQLite WAL database<br/>~/.local/share/minymaton/minymaton.sqlite3")]
        chk["minymaton-storage-check.{service,timer}<br/>1-minute capacity check (&lt; 10% free alerts)"]
        proxy["Optional HTTPS reverse proxy<br/>https://mcp.example.com/minymaton/mcp<br/>--bearer-token-file"]
        proxy --> srv
        srv --> db
        chk -.-> db
    end

    windows ==>|"SSH -L 127.0.0.1:18990:127.0.0.1:18990"| srv
    unix ==>|"SSH -L 127.0.0.1:18990:127.0.0.1:18990"| srv
```

### 1. Shared loopback server

Run one `minymaton-server` per queue, for example as a systemd user service:

- **Loopback listener:** `127.0.0.1:18990` (`/mcp` and `/mcp/agent/<name>`).
- **Durable SQLite WAL database:** for example `~/.local/share/minymaton/minymaton.sqlite3`, kept across binary upgrades.
- **Filesystem capacity monitoring:** `deploy/minymaton-storage-check.service` and `deploy/minymaton-storage-check.timer` check the database's filesystem every minute and post low-space (`< 10%` free) and recovery transitions to the webhook in `~/.config/minymaton/webhook` (mode `0600`), persisting state in `~/.local/state/minymaton/storage.json`.
- **Workstations:** forward a local port to the server's loopback listener, for example `ssh -N -L 127.0.0.1:18990:127.0.0.1:18990 server-host`, and install the `minymaton-wait` helper matching the workstation's target.

### 2. HTTPS behind a reverse proxy

To serve clients that cannot use SSH, run a reverse proxy on the server host that terminates TLS and forwards to the loopback listener:

- **Endpoint:** for example `https://mcp.example.com/minymaton/mcp` (and `/minymaton/mcp/agent/<name>`).
- **Authentication:** start the server with `--bearer-token-file` (only SHA-256 token digests are kept in memory).
- **Long polls:** a parked `receive` holds its request open until a commit arrives, so the proxy and anything in front of it must not cut idle requests short.

### 3. Hosted isolated channels ("Try it" / multi-tenant)

A single `minymaton-server` instance can host isolated private channels alongside or instead of unscoped team callers:

- **Channel-bound token lines:** add a third tab-separated column in `--bearer-token-file` (`CALLER<TAB>TOKEN<TAB>CHANNEL`). Multiple callers may share the same `CHANNEL` (for example `alice-claude` and `alice-codex` bound to `trial-7f3a`), while callers on different channels cannot see or address each other's streams.
- **Enrollment activation and hot reload:** before starting (or when rotating enrollment offline), run `minymaton-server --activate-enrollment /path/to/tokens.conf [--expected-enrollment-digest <prev_hex>] --db /path/to/minymaton.sqlite3`. On a running Unix SQLite server, update `tokens.conf` and send `SIGHUP` (`systemctl reload` or `kill -HUP <pid>`) to provision newly added channels and immediately cancel in-flight requests and MCP sessions for removed or rotated tokens.
- **Per-channel and per-principal caps:** bound storage and concurrency with `--max-retained-events-per-channel` (default `1024`), `--max-subscriptions-per-channel` (default `16`), `--max-bound-channels` (default `256`), `--max-enrolled-tokens` (default `1024`), `--max-sessions-per-principal` (default `16`), and `--max-concurrent-requests-per-principal` (default `16`).
- **Retiring expired channels:** remove the channel's token lines from `tokens.conf`, reload the server (`SIGHUP`), and run `minymaton-server --retire-channel <channel> --db /path/to/minymaton.sqlite3` to delete the channel's stream, events, batches, subscriptions, and idempotency records.

### 4. Install the public server and helpers

The public `AGPL-3.0-only` SQLite packages are published on
[crates.io](https://crates.io/crates/minymaton), with source and build
instructions in [immutlex-dev/minymaton](https://github.com/immutlex-dev/minymaton).
Install the server, waiter and storage checker together:

```sh
cargo install --locked --version 0.1.6 minymaton-server
```

This compiles for the host OS and architecture; see the root README for native
build prerequisites. `cargo install minymaton` installs only the server under
the shorter command name. The portable waiter remains available as
[`tools/minymaton-wait.py`](../tools/minymaton-wait.py) in source.

There is no separate hosted binary download site. Operators distributing their
own qualified binaries can still use `tools/stage_helpers.py` to assemble a
local bundle with corresponding source, license and checksums.

## Storage Architecture

Minymaton separates its MCP queue coordinator (`Queue` + `Signals` in `minymaton-core`) from durable storage through the `QueueStore` and `QueueReader` traits:

- **Bundled backend:** `Queue::open` runs a single-node `minymaton-server` over a local SQLite WAL database (`synchronous=FULL`, `busy_timeout=0`, one writer connection plus a concurrent read-only `Reader`), in `crates/minymaton-core/src/store.rs`.
- **Other backends:** `Queue::with_backend` accepts any implementation of `QueueStore` and `QueueReader`. `Signals` wake receives parked in the same process; a backend shared by several server processes must also raise them from the other processes' commits.
