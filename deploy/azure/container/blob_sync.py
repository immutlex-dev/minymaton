#!/usr/bin/env python3
"""Private Endpoint Azure Blob readiness check over ACA Managed Identity.

Uses Azure Container Apps' Managed Identity endpoint (IDENTITY_ENDPOINT + IDENTITY_HEADER +
AZURE_CLIENT_ID) over the VNet Private Endpoint (<account>.blob.core.windows.net -> 10.64.2.x)
with zero storage account keys and zero third-party Python packages.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import time
import urllib.parse
import urllib.request

BLOB_API_VERSION = "2023-11-03"
STORAGE_RESOURCE = "https://storage.azure.com/"


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[blob-sync {ts}] {msg}", file=sys.stderr, flush=True)


def _get_msi_token() -> str:
    client_id = os.environ.get("AZURE_CLIENT_ID", "").strip()
    identity_endpoint = os.environ.get("IDENTITY_ENDPOINT", "").strip()
    identity_header = os.environ.get("IDENTITY_HEADER", "").strip()

    if identity_endpoint and identity_header:
        params = {"api-version": "2019-08-01", "resource": STORAGE_RESOURCE}
        if client_id:
            params["client_id"] = client_id
        url = f"{identity_endpoint}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"X-IDENTITY-HEADER": identity_header})
    else:
        params = {"api-version": "2018-02-01", "resource": STORAGE_RESOURCE}
        if client_id:
            params["client_id"] = client_id
        url = f"http://169.254.169.254/metadata/identity/oauth2/token?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Metadata": "true"})

    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return str(data["access_token"])


def _blob_base_url() -> tuple[str, str]:
    account = os.environ["STORAGE_ACCOUNT"].strip()
    container = os.environ.get("BLOB_CONTAINER", "wal").strip()
    host = f"{account}.blob.core.windows.net"
    return host, f"https://{host}/{container}"


def cmd_wait_ready(timeout_seconds: int = 180) -> int:
    host, base_url = _blob_base_url()
    deadline = time.monotonic() + timeout_seconds
    last_err = ""
    while time.monotonic() < deadline:
        try:
            infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
            resolved_ips = sorted({info[4][0] for info in infos})
            token = _get_msi_token()
            req = urllib.request.Request(
                f"{base_url}?restype=container&comp=list&maxresults=1",
                headers={
                    "Authorization": f"Bearer {token}",
                    "x-ms-version": BLOB_API_VERSION,
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    _log(f"Private Blob storage ready ({host} -> {resolved_ips})")
                    return 0
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)
        time.sleep(3.0)
    _log(f"ERROR: Private Blob storage not ready after {timeout_seconds}s: {last_err}")
    return 1


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] != "wait-ready":
        raise SystemExit("usage: blob_sync.py wait-ready")
    return cmd_wait_ready()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
