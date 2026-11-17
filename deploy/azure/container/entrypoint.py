#!/usr/bin/env python3
"""Container PID-1 bootstrapper for Minymaton on Azure Container Apps.

1. Materializes $BEARER_TOKENS_B64 to /run/minymaton/tokens.tsv (0600).
2. Waits for Private Endpoint Blob DNS + Managed Identity RBAC readiness.
3. Restores the persisted Litestream SQLite WAL replica from Private Blob Storage.
4. Launches gateway.py (:8080 SSE keepalive proxy behind ACA HTTPS :443 ingress).
5. Replaces PID 1 via os.execv with `litestream replicate -exec minymaton-server`
   so SIGTERM on revision stop cleanly checkpoints and flushes WAL to Blob Storage.
"""

from __future__ import annotations

import base64
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

import blob_sync


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[entrypoint {ts}] {msg}", file=sys.stderr, flush=True)


def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise SystemExit(f"Missing required environment variable: {name}")
    return val


def main() -> int:
    _require_env("PUBLIC_FQDN")
    _require_env("STORAGE_ACCOUNT")
    os.environ.setdefault("BLOB_CONTAINER", "wal")
    tokens_b64 = _require_env("BEARER_TOKENS_B64")

    os.environ["MINYMATON_BOOT_ID"] = secrets.token_hex(8)

    _log("1/4 Writing bearer token file to /run/minymaton/tokens.tsv")
    for d in ("/run/minymaton", "/var/lib/minymaton"):
        Path(d).mkdir(parents=True, exist_ok=True)
        os.chmod(d, 0o700)

    tokens_path = Path("/run/minymaton/tokens.tsv")
    tokens_path.write_bytes(base64.b64decode(tokens_b64))
    os.chmod(tokens_path, 0o600)

    _log("2/4 Waiting for Private Endpoint DNS + Managed Identity Blob RBAC")
    if blob_sync.cmd_wait_ready() != 0:
        return 1

    _log("3/4 Restoring Litestream SQLite WAL replica and starting gateway (:8080)")
    subprocess.run(
        [
            "/usr/local/bin/litestream",
            "restore",
            "-if-replica-exists",
            "-config",
            "/etc/litestream.yml",
            "/var/lib/minymaton/minymaton.sqlite3",
        ],
        check=True,
    )
    subprocess.Popen(["python3", "/opt/minymaton/gateway.py"])

    _log("4/4 Launching Litestream continuous replication wrapping minymaton-server")
    os.execv(
        "/usr/local/bin/litestream",
        [
            "/usr/local/bin/litestream",
            "replicate",
            "-config",
            "/etc/litestream.yml",
            "-exec",
            "/usr/local/bin/minymaton-server --listen 127.0.0.1:18990 --db /var/lib/minymaton/minymaton.sqlite3 --bearer-token-file /run/minymaton/tokens.tsv",
        ],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
