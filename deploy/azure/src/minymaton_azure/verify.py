"""End-to-end Streamable HTTP MCP, >250s long-poll, CLI waiter, and WAL persistence verifier."""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import secrets
import socket
import ssl
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .azure_cli import AzureArmClient
from .config import DeploymentConfig, ensure_bearer_tokens, save_binding
from .tofu import TofuDriver


class VerificationError(RuntimeError):
    pass


class StreamableHttpMcpClient:
    """Minimal stdlib Streamable HTTP MCP client speaking minymaton's exact wire contract."""

    def __init__(self, mcp_url: str, bearer_token: str | None = None) -> None:
        parts = urlsplit(mcp_url)
        if parts.scheme != "https" or not parts.hostname:
            raise ValueError(f"Expected https://<fqdn>/mcp URL, got {mcp_url!r}")
        self.url = mcp_url
        self.host = parts.hostname
        self.port = parts.port or 443
        self.path = parts.path or "/mcp"
        self.bearer_token = bearer_token
        self._req_id = 0

    def tls_leaf_sha256(self, timeout: float = 15.0) -> str:
        ctx = ssl.create_default_context()
        with socket.create_connection((self.host, self.port), timeout=timeout) as raw_sock:
            with ctx.wrap_socket(raw_sock, server_hostname=self.host) as tls_sock:
                der = tls_sock.getpeercert(binary_form=True)
                if not der:
                    raise VerificationError("No peer TLS certificate returned")
                return hashlib.sha256(der).hexdigest()

    def raw_post(
        self,
        payload: dict[str, Any],
        *,
        bearer_override: str | None = "__USE_DEFAULT__",
        timeout: float | None = 60.0,
    ) -> tuple[int, dict[str, str], bytes]:
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection(self.host, self.port, timeout=timeout, context=ctx)
        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2026-07-28",
                "Mcp-Method": str(payload.get("method", "initialize")),
            }
            if payload.get("method") == "tools/call":
                headers["Mcp-Name"] = str((payload.get("params") or {}).get("name", ""))
            tok = self.bearer_token if bearer_override == "__USE_DEFAULT__" else bearer_override
            if tok:
                headers["Authorization"] = f"Bearer {tok}"
            enriched = {
                **payload,
                "params": {
                    **(payload.get("params") or {}),
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientInfo": {
                            "name": "minymaton-azure-verifier",
                            "version": "0.1.0",
                        },
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                },
            }
            body = json.dumps(enriched, separators=(",", ":")).encode("utf-8")
            conn.request("POST", self.path, body=body, headers=headers)
            resp = conn.getresponse()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            if resp.status >= 400:
                return resp.status, resp_headers, resp.read()

            ctype = resp_headers.get("content-type", "").lower()
            if "text/event-stream" in ctype:
                data_lines: list[str] = []
                while True:
                    raw_line = resp.readline()
                    if not raw_line:
                        break
                    line = raw_line.decode("utf-8").rstrip("\r\n")
                    if line.startswith("data:"):
                        data_lines.append(line[5:].lstrip(" "))
                    elif line == "" and data_lines:
                        joined = "\n".join(data_lines)
                        data_lines.clear()
                        parsed = json.loads(joined)
                        if isinstance(parsed, dict) and parsed.get("id") == payload.get("id"):
                            return resp.status, resp_headers, json.dumps(parsed).encode("utf-8")
                raise VerificationError("SSE stream ended without matching JSON-RPC message")

            return resp.status, resp_headers, resp.read()
        finally:
            conn.close()

    def rpc(self, method: str, params: dict[str, Any] | None = None, *, timeout: float | None = 60.0) -> dict[str, Any]:
        self._req_id += 1
        req_id = self._req_id
        status, _, raw = self.raw_post(
            {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}},
            timeout=timeout,
        )
        if status != 200:
            raise VerificationError(
                f"Expected HTTP 200 for {method}, got HTTP {status}: {raw.decode('utf-8', errors='replace')[:400]}"
            )
        msg = json.loads(raw.decode("utf-8"))
        if "error" in msg:
            raise VerificationError(f"JSON-RPC error on {method}: {msg['error']}")
        return msg["result"]

    def call_tool(
        self,
        name: str,
        arguments: dict[str, Any],
        *,
        timeout: float | None = 60.0,
    ) -> dict[str, Any]:
        result = self.rpc(
            "tools/call",
            {"name": name, "arguments": arguments},
            timeout=timeout,
        )
        if result.get("isError"):
            raise VerificationError(f"Tool {name} returned isError=true: {result}")
        structured = result.get("structuredContent")
        if isinstance(structured, dict):
            return structured
        for block in result.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text":
                return json.loads(block.get("text") or "{}")
        raise VerificationError(f"Tool {name} returned no structured JSON payload: {result}")


def wait_for_https_ready(mcp_url: str, operator_token: str, timeout_seconds: int = 240) -> str:
    """Wait until ACA HTTPS :443 ingress is ready and proxies to minymaton-server."""
    client = StreamableHttpMcpClient(mcp_url, operator_token)
    deadline = time.monotonic() + timeout_seconds
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            leaf_fp = client.tls_leaf_sha256(timeout=10.0)
            init_res = client.rpc(
                "initialize",
                {
                    "protocolVersion": "2026-07-28",
                    "capabilities": {},
                    "clientInfo": {"name": "minymaton-azure-readiness", "version": "0.1.0"},
                },
                timeout=15.0,
            )
            if isinstance(init_res, dict) and "capabilities" in init_res:
                return leaf_fp
        except Exception as exc:  # noqa: BLE001 - retry while DNS/ACME settles
            last_err = exc
            time.sleep(5.0)
    raise VerificationError(
        f"HTTPS endpoint {mcp_url} did not become ready within {timeout_seconds}s: {last_err}"
    )


def _get_healthz_boot_id(fqdn: str, timeout: float = 10.0) -> str:
    ctx = ssl.create_default_context()
    conn = http.client.HTTPSConnection(fqdn, 443, timeout=timeout, context=ctx)
    try:
        conn.request("GET", "/healthz")
        resp = conn.getresponse()
        raw = resp.read()
        if resp.status != 200:
            raise VerificationError(f"/healthz returned HTTP {resp.status}: {raw!r}")
        doc = json.loads(raw.decode("utf-8"))
        return str(doc.get("boot_id") or "")
    finally:
        conn.close()


def _assert_http_80_redirects_to_https(fqdn: str, timeout: float = 10.0) -> None:
    conn = http.client.HTTPConnection(fqdn, 80, timeout=timeout)
    try:
        conn.request("GET", "/mcp")
        resp = conn.getresponse()
        resp.read()
        loc = resp.getheader("Location") or ""
        if resp.status not in {301, 302, 307, 308} or not loc.startswith(f"https://{fqdn}"):
            raise VerificationError(
                f"Expected HTTP :80 /mcp redirect to https://{fqdn}, got {resp.status} Location={loc!r}"
            )
    finally:
        conn.close()


def _verify_zero_deployer_nsgs_and_private_storage(
    cfg: DeploymentConfig,
    arm: AzureArmClient,
    rg_name: str,
    storage_account_name: str,
) -> dict[str, Any]:
    _, _, nsg_list = arm.request(
        "GET",
        f"/subscriptions/{cfg.lane.subscription_id}/resourceGroups/{rg_name}"
        "/providers/Microsoft.Network/networkSecurityGroups?api-version=2024-01-01",
    )
    nsgs = (nsg_list or {}).get("value") or []
    deployer_nsgs = [
        str(item.get("name"))
        for item in nsgs
        if isinstance(item, dict)
        and (item.get("tags") or {}).get("managed_by") == "minymaton-azure"
    ]
    if deployer_nsgs:
        raise VerificationError(
            f"Zero-NSG architecture violation: found deployer-created NSGs {deployer_nsgs}"
        )

    _, _, st_doc = arm.request(
        "GET",
        f"/subscriptions/{cfg.lane.subscription_id}/resourceGroups/{rg_name}"
        f"/providers/Microsoft.Storage/storageAccounts/{storage_account_name}?api-version=2023-05-01",
    )
    props = (st_doc or {}).get("properties") or {}
    pna = str(props.get("publicNetworkAccess") or "")
    shared_key = props.get("allowSharedKeyAccess")
    if pna.lower() != "disabled" or shared_key is not False:
        raise VerificationError(
            f"Storage posture violation on {storage_account_name}: "
            f"publicNetworkAccess={pna!r}, allowSharedKeyAccess={shared_key!r}"
        )
    return {
        "deployer_nsg_count": 0,
        "policy_attached_nsgs": [str(item.get("name")) for item in nsgs if isinstance(item, dict)],
        "storage_public_network_access": pna.lower(),
        "storage_shared_key_enabled": bool(shared_key),
    }


def _find_minymaton_wait_script() -> Path:
    repo_candidate = Path(__file__).resolve().parents[4] / "tools" / "minymaton-wait.py"
    if repo_candidate.is_file():
        return repo_candidate
    raise VerificationError(f"Could not find tools/minymaton-wait.py at {repo_candidate}")


def verify_deployment(
    cfg: DeploymentConfig,
    arm: AzureArmClient,
    outputs: dict[str, Any],
    *,
    long_poll_seconds: float = 255.0,
) -> dict[str, Any]:
    mcp_url = str(outputs["mcp_url"])
    fqdn = str(outputs["fqdn"])
    static_ip = str(outputs["static_ip"])
    rg_name = str(outputs["resource_group_name"])
    storage_account_name = str(outputs["storage_account_name"])

    tokens = ensure_bearer_tokens(cfg.tokens_file)
    operator_token = tokens["operator"]
    peer_token = tokens["peer"]

    # 1. Verify Microsoft-managed HTTPS :443 WebPKI TLS ingress and HTTP :80 -> HTTPS redirect.
    cert_sha256_before = wait_for_https_ready(mcp_url, operator_token, timeout_seconds=180)
    _assert_http_80_redirects_to_https(fqdn)
    boot_id_before = _get_healthz_boot_id(fqdn)

    # 2. Verify zero deployer-created NSGs and locked-down Private Endpoint Storage posture.
    posture = _verify_zero_deployer_nsgs_and_private_storage(
        cfg, arm, rg_name, storage_account_name
    )

    op_client = StreamableHttpMcpClient(mcp_url, operator_token)
    peer_client = StreamableHttpMcpClient(mcp_url, peer_token)

    # 3. Auth boundary checks on HTTPS :443: missing token -> 401, wrong token -> 401.
    status_no_auth, hdrs_no_auth, _ = op_client.raw_post(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        bearer_override=None,
    )
    if status_no_auth != 401 or "bearer" not in hdrs_no_auth.get("www-authenticate", "").lower():
        raise VerificationError(
            f"Unauthenticated request expected 401 + WWW-Authenticate: Bearer on {op_client.url}, "
            f"got {status_no_auth} {hdrs_no_auth}"
        )

    status_bad_auth, _, _ = op_client.raw_post(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        bearer_override="wrong_" + ("x" * 40),
    )
    if status_bad_auth != 401:
        raise VerificationError(f"Invalid bearer token expected 401, got {status_bad_auth}")

    # 4. Verify tools/list exposes all 4 minymaton tools on HTTPS :443.
    expected_tools = ["publish", "receive", "stream", "subscription"]
    tools_res = op_client.rpc("tools/list", {})
    tool_names = sorted(str(t.get("name")) for t in (tools_res.get("tools") or []))
    if tool_names != expected_tools:
        raise VerificationError(f"Unexpected MCP tools {tool_names}, expected {expected_tools}")

    # 5. Create stream + subscription for the >240s ACA-killer parked receive(wait=true) test.
    run_tag = secrets.token_hex(3)
    stream_name = f"azure.e2e.{run_tag}"
    stream_res = op_client.call_tool("stream", {"action": "create", "name": stream_name})
    if stream_res.get("status") not in {"created", "exists"}:
        raise VerificationError(f"Unexpected stream create response: {stream_res}")

    sub_res = op_client.call_tool(
        "subscription",
        {"action": "create", "stream": stream_name, "label": f"longpoll-{run_tag}"},
    )
    sub_handle = str(sub_res["handle"])

    longpoll_box: dict[str, Any] = {}

    def _parked_receiver() -> None:
        wait_client = StreamableHttpMcpClient(mcp_url, operator_token)
        t0 = time.monotonic()
        try:
            res = wait_client.call_tool(
                "receive",
                {
                    "handle": sub_handle,
                    "max_events": 10,
                    "max_bytes": 16384,
                    "wait": True,
                },
                timeout=long_poll_seconds + 90.0,
            )
            longpoll_box["result"] = res
            longpoll_box["elapsed_s"] = round(time.monotonic() - t0, 2)
        except Exception as exc:  # noqa: BLE001
            longpoll_box["error"] = exc
            longpoll_box["elapsed_s"] = round(time.monotonic() - t0, 2)

    park_start = time.monotonic()
    recv_thread = threading.Thread(target=_parked_receiver, daemon=True)
    recv_thread.start()

    parked_confirmed = False
    for _ in range(40):
        listed = peer_client.call_tool(
            "subscription", {"action": "list", "stream": stream_name}
        )
        subs = listed.get("subscriptions") or []
        matching = [s for s in subs if s.get("handle") == sub_handle]
        if matching and matching[0].get("waiting") is True:
            parked_confirmed = True
            break
        time.sleep(0.5)
    if not parked_confirmed:
        raise VerificationError(f"Subscription {sub_handle} never entered waiting=true state")

    while True:
        elapsed = time.monotonic() - park_start
        if not recv_thread.is_alive():
            err = longpoll_box.get("error")
            raise VerificationError(
                f"Parked receive(wait=true) dropped prematurely after {elapsed:.1f}s: {err or longpoll_box}"
            )
        remaining = long_poll_seconds - elapsed
        if remaining <= 0:
            break
        time.sleep(min(remaining, 5.0))

    wake_payload = {
        "kind": "aca_240s_killer_passed",
        "tag": run_tag,
        "held_seconds": round(long_poll_seconds, 1),
    }
    publish_t0 = time.monotonic()
    pub1 = peer_client.call_tool(
        "publish",
        {
            "stream": stream_name,
            "idempotency_key": f"longpoll-{run_tag}",
            "payload": wake_payload,
        },
    )
    if pub1.get("status") != "published":
        raise VerificationError(f"Unexpected publish response: {pub1}")

    recv_thread.join(timeout=20.0)
    wake_latency_ms = round((time.monotonic() - publish_t0) * 1000.0, 1)
    if recv_thread.is_alive():
        raise VerificationError("Parked receive(wait=true) did not wake within 20s after publish")
    if "error" in longpoll_box:
        raise VerificationError(f"Parked receive(wait=true) failed: {longpoll_box['error']}")

    recv1 = longpoll_box["result"]
    events1 = recv1.get("events") or []
    if len(events1) != 1 or events1[0].get("payload") != wake_payload:
        raise VerificationError(f"Mismatched long-poll event payload: {recv1}")
    batch1_id = str(recv1["batch_id"])

    # 6. Real CLI waiter test using `tools/minymaton-wait.py` over HTTPS :443 (with --ack batch1_id).
    wait_script = _find_minymaton_wait_script()
    wait_env = os.environ.copy()
    wait_env["MINYMATON_TOKEN"] = operator_token
    cli_proc = subprocess.Popen(
        [
            sys.executable,
            str(wait_script),
            "--handle",
            sub_handle,
            "--ack",
            batch1_id,
            "--url",
            mcp_url,
            "--token-env",
            "MINYMATON_TOKEN",
        ],
        env=wait_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        cli_parked = False
        for _ in range(40):
            if cli_proc.poll() is not None:
                out, err = cli_proc.communicate(timeout=5)
                raise VerificationError(
                    f"minymaton-wait.py exited early (rc={cli_proc.returncode}): stdout={out} stderr={err}"
                )
            listed = peer_client.call_tool(
                "subscription", {"action": "list", "stream": stream_name}
            )
            matching = [
                s for s in (listed.get("subscriptions") or []) if s.get("handle") == sub_handle
            ]
            if matching and matching[0].get("waiting") is True:
                cli_parked = True
                break
            time.sleep(0.5)
        if not cli_parked:
            raise VerificationError("minymaton-wait.py never entered waiting=true state")

        cli_payload = {"kind": "cli_waiter_wakeup", "tag": run_tag}
        peer_client.call_tool(
            "publish",
            {
                "stream": stream_name,
                "idempotency_key": f"cli-{run_tag}",
                "payload": cli_payload,
            },
        )
        cli_out, cli_err = cli_proc.communicate(timeout=30)
        if cli_proc.returncode != 0:
            raise VerificationError(
                f"minymaton-wait.py failed (rc={cli_proc.returncode}): stdout={cli_out} stderr={cli_err}"
            )
        cli_batch = json.loads(cli_out.strip())
        cli_events = cli_batch.get("events") or []
        if len(cli_events) != 1 or cli_events[0].get("payload") != cli_payload:
            raise VerificationError(f"Unexpected minymaton-wait.py output: {cli_batch}")
        batch2_id = str(cli_batch["batch_id"])
    finally:
        if cli_proc.poll() is None:
            cli_proc.kill()

    # 7. Litestream Private Endpoint Blob SQLite WAL persistence + bearer token rotation
    #    across a real Container App revision replacement (fresh container ephemeral filesystem).
    durable_payload = {"kind": "sqlite_wal_survives_aca_revision_restart", "tag": run_tag}
    peer_client.call_tool(
        "publish",
        {
            "stream": stream_name,
            "idempotency_key": f"wal-{run_tag}",
            "payload": durable_payload,
        },
    )
    # Allow Litestream's 1s sync loop to flush WAL frames to Private Endpoint Blob before revision swap.
    time.sleep(3.5)

    saved_doc = json.loads(cfg.binding_file.read_text(encoding="utf-8"))
    container_image = str(saved_doc["container_image"])
    rotated_token = secrets.token_urlsafe(36)
    updated_tsv = (
        cfg.tokens_file.read_text(encoding="utf-8").replace("\r\n", "\n").rstrip("\n")
        + f"\nrotated-{run_tag}\t{rotated_token}\n"
    )
    updated_b64 = base64.b64encode(updated_tsv.encode("utf-8")).decode("ascii")

    tofu = TofuDriver(cfg, arm)
    tofu.apply_app(
        container_image=container_image,
        bearer_tokens_b64=updated_b64,
        revision_nonce=f"verify-{run_tag}",
    )

    # Confirm traffic has cut over to the newly booted container instance (distinct boot_id).
    boot_id_after = boot_id_before
    deadline = time.monotonic() + 180.0
    while time.monotonic() < deadline:
        try:
            candidate_boot = _get_healthz_boot_id(fqdn, timeout=5.0)
            if candidate_boot and candidate_boot != boot_id_before:
                boot_id_after = candidate_boot
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(3.0)
    if boot_id_after == boot_id_before:
        raise VerificationError(
            f"Container App revision restart did not cut over to a new container boot_id ({boot_id_before})"
        )

    cert_sha256_after = wait_for_https_ready(mcp_url, operator_token, timeout_seconds=120)
    if cert_sha256_after != cert_sha256_before:
        raise VerificationError(
            "TLS leaf certificate changed unexpectedly across ACA revision replacement "
            f"({cert_sha256_before} != {cert_sha256_after})"
        )

    # ACK batch2_id and receive durable_event_3 from the restored SQLite WAL replica.
    post_restart_recv = op_client.call_tool(
        "receive",
        {
            "handle": sub_handle,
            "ack": batch2_id,
            "max_events": 10,
            "max_bytes": 16384,
            "wait": False,
        },
    )
    events3 = post_restart_recv.get("events") or []
    if len(events3) != 1 or events3[0].get("payload") != durable_payload:
        raise VerificationError(
            f"Litestream SQLite WAL durability check failed after ACA revision replacement: {post_restart_recv}"
        )

    rotated_client = StreamableHttpMcpClient(mcp_url, rotated_token)
    rot_tools = rotated_client.rpc("tools/list", {})
    if len(rot_tools.get("tools") or []) != 4:
        raise VerificationError("Rotated bearer token failed to authenticate on new ACA revision")

    report = {
        "fqdn": fqdn,
        "mcp_url": mcp_url,
        "static_ip": static_ip,
        "boot_id_before": boot_id_before,
        "boot_id_after": boot_id_after,
        "tls_cert_sha256": cert_sha256_after,
        "tls_cert_persisted_across_restart": True,
        "http_80_redirect_verified": True,
        "https_443_mcp_verified": True,
        "auth_401_verified": True,
        "long_poll_elapsed_seconds": longpoll_box["elapsed_s"],
        "long_poll_wake_latency_ms": wake_latency_ms,
        "cli_waiter_verified": True,
        "sqlite_wal_persisted_across_restart": True,
        "token_rotation_verified": True,
        **posture,
    }
    save_binding(cfg, {"phase": "verified", "verification": report})
    return report
