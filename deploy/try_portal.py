#!/usr/bin/env python3
"""Public self-serve 'Try Minymaton' channel minting portal and TTL reaper.

Mints isolated 24-hour capability channels (`CALLER\\tTOKEN\\tCHANNEL`),
hot-reloads `minymaton-server` via SIGHUP, and reaps expired channels with
`minymaton-server --retire-channel`.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import secrets
import signal
import subprocess
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ANCHOR_CALLER = "try-anchor"
ANCHOR_CHANNEL = "ch-anchor"


class PortalConfig:
    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        state_dir: Path,
        server_bin: Path,
        mcp_internal_url: str,
        mcp_public_url: str,
        portal_public_url: str,
        max_active_channels: int = 120,
        ttl_seconds: int = 86400,
        per_ip_hourly_limit: int = 5,
        global_hourly_limit: int = 60,
    ) -> None:
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.state_dir = state_dir
        self.db_path = state_dir / "minymaton.sqlite3"
        self.tokens_path = state_dir / "tokens.conf"
        self.leases_path = state_dir / "leases.json"
        self.server_bin = server_bin
        self.mcp_internal_url = mcp_internal_url
        self.mcp_public_url = mcp_public_url
        self.portal_public_url = portal_public_url.rstrip("/")
        self.max_active_channels = max_active_channels
        self.ttl_seconds = ttl_seconds
        self.per_ip_hourly_limit = per_ip_hourly_limit
        self.global_hourly_limit = global_hourly_limit


class LeaseStore:
    def __init__(self, cfg: PortalConfig) -> None:
        self.cfg = cfg
        self.lock = threading.Lock()
        self.ip_mint_history: dict[str, list[float]] = {}
        self.global_mint_history: list[float] = []
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        self.cfg.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.cfg.leases_path.exists():
            anchor_token = "mnm_anchor_" + secrets.token_hex(24)
            initial = {
                "anchor_token": anchor_token,
                "leases": [],
                "total_minted": 0,
            }
            self._write_json_atomic(self.cfg.leases_path, initial)
        data = self._read_state()
        if not self.cfg.tokens_path.exists():
            self._write_tokens_file(data)

    def _read_state(self) -> dict[str, Any]:
        with self.cfg.leases_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _write_json_atomic(self, path: Path, payload: dict[str, Any]) -> None:
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)

    def _write_tokens_file(self, state: dict[str, Any]) -> None:
        lines = [
            "# Managed by try_portal.py - DO NOT EDIT BY HAND",
            f"{ANCHOR_CALLER}\t{state['anchor_token']}\t{ANCHOR_CHANNEL}",
        ]
        for lease in state.get("leases", []):
            lines.append(f"{lease['caller']}\t{lease['token']}\t{lease['channel']}")
        tmp = self.cfg.tokens_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.cfg.tokens_path)

    def _sighup_minymaton_try(self) -> bool:
        """Send SIGHUP to the minymaton-try server process owned by our UID."""
        my_uid = os.getuid()
        sent = False
        proc_root = Path("/proc")
        for entry in proc_root.iterdir():
            if not entry.name.isdigit():
                continue
            try:
                stat = entry.stat()
                if stat.st_uid != my_uid:
                    continue
                cmdline_raw = (entry / "cmdline").read_bytes()
                args = [part.decode("utf-8", "ignore") for part in cmdline_raw.split(b"\0") if part]
                if not args:
                    continue
                if "minymaton-server" in Path(args[0]).name and str(self.cfg.db_path) in args:
                    os.kill(int(entry.name), signal.SIGHUP)
                    sent = True
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
        return sent

    def status_snapshot(self) -> dict[str, Any]:
        with self.lock:
            state = self._read_state()
            active = len(state.get("leases", []))
            return {
                "active_channels": active,
                "max_active_channels": self.cfg.max_active_channels,
                "ttl_seconds": self.cfg.ttl_seconds,
                "retained_events_per_channel": 256,
                "max_subscriptions_per_channel": 8,
                "total_minted": state.get("total_minted", 0),
                "mcp_url": self.cfg.mcp_public_url,
            }

    def mint_channel(self, client_ip: str) -> tuple[int, dict[str, Any]]:
        now = time.time()
        cutoff = now - 3600.0
        with self.lock:
            self.global_mint_history = [t for t in self.global_mint_history if t >= cutoff]
            ip_hist = [t for t in self.ip_mint_history.get(client_ip, []) if t >= cutoff]
            self.ip_mint_history[client_ip] = ip_hist

            if len(ip_hist) >= self.cfg.per_ip_hourly_limit:
                return HTTPStatus.TOO_MANY_REQUESTS, {
                    "error": "rate_limit_exceeded",
                    "message": (
                        f"Per-IP limit reached ({self.cfg.per_ip_hourly_limit} channels/hour). "
                        "Use your existing channel token or wait before minting another."
                    ),
                }
            if len(self.global_mint_history) >= self.cfg.global_hourly_limit:
                return HTTPStatus.TOO_MANY_REQUESTS, {
                    "error": "global_rate_limit_exceeded",
                    "message": "Hourly trial mint capacity reached. Try again shortly.",
                }

            state = self._read_state()
            leases: list[dict[str, Any]] = state.get("leases", [])
            if len(leases) >= self.cfg.max_active_channels:
                return HTTPStatus.SERVICE_UNAVAILABLE, {
                    "error": "channel_capacity_reached",
                    "message": (
                        f"All {self.cfg.max_active_channels} public trial slots are currently in use. "
                        "Slots recycle automatically as 24-hour leases expire."
                    ),
                }

            existing_channels = {item["channel"] for item in leases}
            while True:
                short_id = secrets.token_hex(4)
                channel = f"ch-{short_id}"
                caller = f"trial-{short_id}"
                if channel not in existing_channels:
                    break

            token = f"mnm_try_{short_id}_{secrets.token_urlsafe(28)}"
            expires_at = int(now + self.cfg.ttl_seconds)
            lease = {
                "caller": caller,
                "channel": channel,
                "token": token,
                "created_at": int(now),
                "expires_at": expires_at,
                "ip_hash": secrets.token_hex(4),
            }
            leases.append(lease)
            state["leases"] = leases
            state["total_minted"] = int(state.get("total_minted", 0)) + 1

            self._write_json_atomic(self.cfg.leases_path, state)
            self._write_tokens_file(state)
            reloaded = self._sighup_minymaton_try()

            ip_hist.append(now)
            self.ip_mint_history[client_ip] = ip_hist
            self.global_mint_history.append(now)

        # Wait briefly for SIGHUP handler to finish provisioning the channel in SQLite
        if reloaded:
            time.sleep(0.08)

        return HTTPStatus.CREATED, {
            "channel": channel,
            "caller": caller,
            "token": token,
            "mcp_url": self.cfg.mcp_public_url,
            "expires_at": expires_at,
            "ttl_seconds": self.cfg.ttl_seconds,
            "limits": {
                "retained_events": 256,
                "subscriptions": 8,
                "sessions": 8,
                "concurrent_requests": 8,
            },
        }

    def reap_expired(self) -> list[str]:
        now = time.time()
        expired_channels: list[str] = []
        with self.lock:
            state = self._read_state()
            leases = state.get("leases", [])
            keep = []
            for lease in leases:
                if now >= float(lease.get("expires_at", 0)):
                    expired_channels.append(lease["channel"])
                else:
                    keep.append(lease)
            if not expired_channels:
                return []
            state["leases"] = keep
            self._write_json_atomic(self.cfg.leases_path, state)
            self._write_tokens_file(state)
            self._sighup_minymaton_try()

        time.sleep(0.1)
        for ch in expired_channels:
            try:
                subprocess.run(
                    [
                        str(self.cfg.server_bin),
                        "--retire-channel",
                        ch,
                        "--db",
                        str(self.cfg.db_path),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
            except Exception:
                pass
        return expired_channels


def verify_channel_against_local_mcp(
    internal_url: str,
    token: str,
    action: str,
    message_text: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """Run a live MCP initialize + tool call against the local minymaton-try server."""
    if not token or len(token) < 32 or any(ch.isspace() for ch in token):
        return HTTPStatus.BAD_REQUEST, {
            "error": "invalid_token",
            "message": "Provide the 32+ character bearer token minted for your channel.",
        }

    t0 = time.monotonic()
    # internal_url is e.g. http://127.0.0.1:18992/mcp
    without_scheme = internal_url.removeprefix("http://").removeprefix("https://")
    host_port, _, path = without_scheme.partition("/")
    path = "/" + path if path else "/mcp"
    host, _, port_str = host_port.partition(":")
    port = int(port_str) if port_str else 18992

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": f"Bearer {token}",
    }

    def parse_mcp_body(raw_text: str) -> Any:
        for line in raw_text.splitlines():
            if line.startswith("data:"):
                data_str = line[5:].strip()
                if data_str:
                    return json.loads(data_str)
        return json.loads(raw_text)

    try:
        conn = http.client.HTTPConnection(host, port, timeout=5)
        init_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {"name": "minymaton-try-verifier", "version": "0.1.2"},
                },
            }
        )
        conn.request("POST", path, body=init_payload, headers=headers)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", "replace")
        if resp.status != 200:
            return resp.status, {
                "error": "mcp_auth_failed",
                "message": f"MCP server returned HTTP {resp.status}",
            }
        session_id = resp.getheader("mcp-session-id")
        if session_id:
            headers["Mcp-Session-Id"] = session_id

        notif_payload = json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"})
        conn.request("POST", path, body=notif_payload, headers=headers)
        resp2 = conn.getresponse()
        resp2.read()

        # First discover the caller's bound channel via stream list
        list_call = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": "stream", "arguments": {"action": "list"}},
            }
        )
        conn.request("POST", path, body=list_call, headers=headers)
        resp3 = conn.getresponse()
        raw3 = resp3.read().decode("utf-8", "replace")
        parsed3 = parse_mcp_body(raw3)
        content_text = (
            parsed3.get("result", {})
            .get("content", [{}])[0]
            .get("text", "{}")
        )
        stream_info = json.loads(content_text)
        streams = stream_info.get("streams", [])
        bound_channel = streams[0] if streams else None

        result_payload: dict[str, Any] = {
            "ok": True,
            "isolated_streams": streams,
        }

        if action == "ping" and bound_channel:
            idem_key = f"web-probe-{secrets.token_hex(6)}"
            pub_call = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": "publish",
                        "arguments": {
                            "stream": bound_channel,
                            "idempotency_key": idem_key,
                            "payload": {
                                "kind": "channel_probe",
                                "message": message_text
                                or "Hello from the Minymaton Try It console!",
                                "sent_at": int(time.time()),
                            },
                        },
                    },
                }
            )
            conn.request("POST", path, body=pub_call, headers=headers)
            resp4 = conn.getresponse()
            raw4 = resp4.read().decode("utf-8", "replace")
            parsed4 = parse_mcp_body(raw4)
            pub_text = (
                parsed4.get("result", {})
                .get("content", [{}])[0]
                .get("text", "{}")
            )
            result_payload["published"] = json.loads(pub_text)

        # Cleanly close the temporary MCP session so we don't consume a session slot
        if session_id:
            conn.request("DELETE", path, headers=headers)
            conn.getresponse().read()
        conn.close()

        elapsed_ms = round((time.monotonic() - t0) * 1000.0, 1)
        result_payload["latency_ms"] = elapsed_ms
        return HTTPStatus.OK, result_payload
    except Exception as exc:
        return HTTPStatus.BAD_GATEWAY, {
            "error": "probe_failed",
            "message": f"Probe could not reach local MCP bus: {exc}",
        }


INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Minymaton — Mint a Private Agent Channel</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@400;500;600&family=Space+Grotesk:wght@600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-chassis: #0B1120;
      --bg-panel: #111A2E;
      --bg-well: #070C17;
      --border-grid: #1E3A8A;
      --border-subtle: rgba(59, 130, 246, 0.22);
      --text-primary: #E8F0FE;
      --text-muted: #8BA3C7;
      --accent-amber: #F59E0B;
      --accent-amber-soft: rgba(245, 158, 11, 0.14);
      --accent-seafoam: #5EEAD4;
      --accent-seafoam-soft: rgba(94, 234, 212, 0.12);
      --accent-cobalt: #3B82F6;
      --danger: #F87171;
      --font-display: 'Space Grotesk', system-ui, sans-serif;
      --font-body: 'Plus Jakarta Sans', system-ui, sans-serif;
      --font-mono: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      background-color: var(--bg-chassis);
      background-image:
        radial-gradient(circle at 14% 12%, rgba(30, 58, 138, 0.32), transparent 38%),
        linear-gradient(to right, rgba(30, 58, 138, 0.12) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(30, 58, 138, 0.12) 1px, transparent 1px);
      background-size: auto, 32px 32px, 32px 32px;
      color: var(--text-primary);
      font-family: var(--font-body);
      line-height: 1.55;
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
    }

    a {
      color: var(--accent-seafoam);
      text-decoration: none;
    }
    a:hover {
      text-decoration: underline;
    }

    .shell {
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px 24px 64px;
    }

    /* Top Telemetry Strip */
    .topbar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 20px;
      background: rgba(17, 26, 46, 0.88);
      border: 1px solid var(--border-grid);
      border-radius: 10px;
      margin-bottom: 32px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .brand svg {
      width: 46px;
      height: 36px;
      flex-shrink: 0;
    }

    .brand-title {
      font-family: var(--font-display);
      font-size: 1.25rem;
      font-weight: 700;
      letter-spacing: -0.03em;
      color: var(--text-primary);
    }

    .brand-sub {
      font-family: var(--font-mono);
      font-size: 0.74rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .telemetry-strip {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 18px;
      font-family: var(--font-mono);
      font-size: 0.78rem;
      font-variant-numeric: tabular-nums;
      color: var(--text-muted);
    }

    .telemetry-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 5px 10px;
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
    }

    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--accent-seafoam);
      box-shadow: 0 0 10px var(--accent-seafoam);
    }

    .telemetry-val {
      color: var(--text-primary);
      font-weight: 600;
    }

    /* Main Asymmetric Dispatch Console */
    .console-grid {
      display: grid;
      grid-template-columns: 5fr 7fr;
      gap: 28px;
      align-items: start;
    }

    @media (max-width: 960px) {
      .console-grid {
        grid-template-columns: 1fr;
      }
    }

    /* Left Pitch & Action Column */
    .pitch-col {
      display: flex;
      flex-direction: column;
      gap: 22px;
    }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-family: var(--font-mono);
      font-size: 0.76rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--accent-amber);
    }

    h1 {
      font-family: var(--font-display);
      font-size: clamp(2rem, 3.3vw, 2.75rem);
      font-weight: 700;
      line-height: 1.08;
      letter-spacing: -0.03em;
      color: var(--text-primary);
    }

    .lead {
      font-size: 1.02rem;
      color: var(--text-muted);
      max-width: 54ch;
    }

    .lead strong {
      color: var(--text-primary);
      font-weight: 600;
    }

    .mint-action-box {
      background: var(--bg-panel);
      border: 1px solid var(--border-grid);
      border-radius: 12px;
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }

    .btn-mint {
      appearance: none;
      width: 100%;
      border: 2px solid #92400E;
      background: linear-gradient(180deg, #FBBF24 0%, #F59E0B 100%);
      color: #0B1120;
      font-family: var(--font-display);
      font-size: 1.08rem;
      font-weight: 700;
      letter-spacing: -0.01em;
      padding: 14px 20px;
      border-radius: 8px;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      transition: transform 0.12s ease, filter 0.12s ease;
    }

    .btn-mint:hover:not(:disabled) {
      filter: brightness(1.06);
      transform: translateY(-1px);
    }

    .btn-mint:disabled {
      opacity: 0.65;
      cursor: progress;
    }

    .cli-mint-bar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 9px 12px;
      font-family: var(--font-mono);
      font-size: 0.78rem;
      color: var(--text-muted);
      overflow-x: auto;
    }

    .cli-mint-bar code {
      color: var(--accent-seafoam);
      white-space: nowrap;
    }

    .copy-mini {
      background: transparent;
      border: 1px solid var(--border-subtle);
      color: var(--text-primary);
      font-family: var(--font-mono);
      font-size: 0.72rem;
      padding: 4px 8px;
      border-radius: 4px;
      cursor: pointer;
      flex-shrink: 0;
    }

    .copy-mini:hover {
      border-color: var(--accent-seafoam);
      color: var(--accent-seafoam);
    }

    /* Isolation Architecture Card */
    .spec-panel {
      background: rgba(17, 26, 46, 0.65);
      border: 1px solid var(--border-subtle);
      border-radius: 10px;
      padding: 18px;
    }

    .spec-title {
      font-family: var(--font-mono);
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--text-muted);
      margin-bottom: 12px;
    }

    .spec-list {
      list-style: none;
      display: grid;
      gap: 10px;
      font-size: 0.9rem;
      color: var(--text-muted);
    }

    .spec-list li {
      display: flex;
      align-items: baseline;
      gap: 10px;
    }

    .spec-tag {
      font-family: var(--font-mono);
      font-size: 0.74rem;
      color: var(--accent-seafoam);
      background: var(--accent-seafoam-soft);
      padding: 2px 6px;
      border-radius: 4px;
      flex-shrink: 0;
    }

    /* Right Signature Element: Live Channel Dispatch Ticket */
    .ticket-card {
      background: var(--bg-panel);
      border: 2px solid var(--border-grid);
      border-radius: 14px;
      overflow: hidden;
      box-shadow: 0 22px 48px rgba(0, 0, 0, 0.45);
    }

    .ticket-header {
      background: linear-gradient(90deg, #0F172A 0%, #172554 100%);
      border-bottom: 1px solid var(--border-grid);
      padding: 16px 22px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }

    .ticket-title-group {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .ticket-badge {
      font-family: var(--font-mono);
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      padding: 4px 9px;
      border-radius: 4px;
      background: rgba(139, 163, 199, 0.16);
      color: var(--text-muted);
    }

    .ticket-badge.live {
      background: var(--accent-seafoam-soft);
      color: var(--accent-seafoam);
      border: 1px solid rgba(94, 234, 212, 0.4);
    }

    .ticket-callsign {
      font-family: var(--font-display);
      font-size: 1.2rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }

    .ticket-ttl {
      font-family: var(--font-mono);
      font-size: 0.82rem;
      font-variant-numeric: tabular-nums;
      color: var(--accent-amber);
    }

    .ticket-body {
      padding: 22px;
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    /* Credential Stamp Grid */
    .stamp-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
    }

    @media (max-width: 640px) {
      .stamp-grid {
        grid-template-columns: 1fr;
      }
    }

    .stamp-cell {
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 10px 12px;
    }

    .stamp-label {
      font-family: var(--font-mono);
      font-size: 0.68rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--text-muted);
      margin-bottom: 4px;
    }

    .stamp-value {
      font-family: var(--font-mono);
      font-size: 0.86rem;
      font-weight: 600;
      color: var(--text-primary);
      word-break: break-all;
    }

    .stamp-value.amber {
      color: var(--accent-amber);
    }

    .stamp-value.seafoam {
      color: var(--accent-seafoam);
    }

    /* Token Row */
    .token-strip {
      background: var(--bg-well);
      border: 1px solid var(--border-grid);
      border-radius: 8px;
      padding: 12px 14px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }

    .token-value {
      font-family: var(--font-mono);
      font-size: 0.84rem;
      color: var(--accent-amber);
      overflow-x: auto;
      white-space: nowrap;
    }

    /* Harness Config Tabs */
    .tabs-bar {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      border-bottom: 1px solid var(--border-subtle);
      padding-bottom: 10px;
    }

    .tab-btn {
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      color: var(--text-muted);
      font-family: var(--font-mono);
      font-size: 0.76rem;
      font-weight: 600;
      padding: 7px 12px;
      border-radius: 6px;
      cursor: pointer;
    }

    .tab-btn.active {
      background: rgba(59, 130, 246, 0.18);
      border-color: var(--accent-cobalt);
      color: var(--text-primary);
    }

    .code-box {
      position: relative;
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 14px;
    }

    .code-box pre {
      font-family: var(--font-mono);
      font-size: 0.78rem;
      line-height: 1.5;
      color: var(--text-primary);
      overflow-x: auto;
      white-space: pre;
    }

    .code-copy-btn {
      position: absolute;
      top: 10px;
      right: 10px;
      background: var(--bg-panel);
      border: 1px solid var(--border-grid);
      color: var(--text-primary);
      font-family: var(--font-mono);
      font-size: 0.72rem;
      padding: 5px 10px;
      border-radius: 5px;
      cursor: pointer;
    }

    .code-copy-btn:hover {
      border-color: var(--accent-amber);
      color: var(--accent-amber);
    }

    /* Two-Terminal Handshake Prompt */
    .handshake-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }

    @media (max-width: 640px) {
      .handshake-grid {
        grid-template-columns: 1fr;
      }
    }

    .terminal-card {
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      border-radius: 8px;
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .terminal-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-family: var(--font-mono);
      font-size: 0.72rem;
      color: var(--accent-seafoam);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }

    .terminal-prompt {
      font-family: var(--font-mono);
      font-size: 0.76rem;
      color: var(--text-primary);
      line-height: 1.45;
    }

    /* Live Bus Probe Bar */
    .probe-bar {
      border-top: 1px dashed var(--border-grid);
      padding-top: 16px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .probe-controls {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 10px;
    }

    .btn-probe {
      background: var(--bg-well);
      border: 1px solid var(--accent-seafoam);
      color: var(--accent-seafoam);
      font-family: var(--font-mono);
      font-size: 0.78rem;
      font-weight: 600;
      padding: 8px 13px;
      border-radius: 6px;
      cursor: pointer;
    }

    .btn-probe:hover:not(:disabled) {
      background: var(--accent-seafoam-soft);
    }

    .btn-probe:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }

    .probe-output {
      font-family: var(--font-mono);
      font-size: 0.76rem;
      background: var(--bg-well);
      border: 1px solid var(--border-subtle);
      border-radius: 6px;
      padding: 10px 12px;
      color: var(--text-muted);
      min-height: 42px;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .error-banner {
      display: none;
      background: rgba(248, 113, 113, 0.12);
      border: 1px solid var(--danger);
      color: #FCA5A5;
      padding: 10px 14px;
      border-radius: 8px;
      font-size: 0.85rem;
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand">
        <svg viewBox="0 0 320 240" role="img" aria-label="Minymaton automaton">
          <defs>
            <linearGradient id="body" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stop-color="#3b82f6"/>
              <stop offset="1" stop-color="#1d4ed8"/>
            </linearGradient>
            <linearGradient id="card" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stop-color="#fde68a"/>
              <stop offset="1" stop-color="#f59e0b"/>
            </linearGradient>
          </defs>
          <g stroke="#92400e" stroke-width="4" stroke-linejoin="round">
            <rect x="34" y="96" width="66" height="44" rx="7" fill="url(#card)" opacity="0.65"/>
            <rect x="88" y="96" width="66" height="44" rx="7" fill="url(#card)"/>
            <path d="M88 102 L121 124 L154 102" fill="none"/>
          </g>
          <line x1="236" y1="44" x2="236" y2="20" stroke="#60a5fa" stroke-width="6" stroke-linecap="round"/>
          <circle cx="236" cy="16" r="10" fill="#f59e0b"/>
          <rect x="170" y="44" width="132" height="150" rx="30" fill="url(#body)" stroke="#60a5fa" stroke-width="5"/>
          <rect x="188" y="66" width="96" height="58" rx="16" fill="#0f172a"/>
          <circle cx="216" cy="95" r="10" fill="#5eead4"/>
          <circle cx="256" cy="95" r="10" fill="#5eead4"/>
          <rect x="200" y="142" width="72" height="12" rx="6" fill="#bfdbfe"/>
          <circle cx="264" cy="170" r="9" fill="#22c55e"/>
        </svg>
        <div>
          <div class="brand-title">minymaton</div>
          <div class="brand-sub">Isolated Agent Bus // Public Trial Sandbox</div>
        </div>
      </div>

      <div class="telemetry-strip">
        <div class="telemetry-pill">
          <span class="pulse-dot"></span>
          <span>ACTIVE CHANNELS: <span class="telemetry-val" id="stat-channels">-- / --</span></span>
        </div>
        <div class="telemetry-pill">
          <span>RING BUFFER: <span class="telemetry-val">256 EVENTS</span></span>
        </div>
        <div class="telemetry-pill">
          <span>LEASE TTL: <span class="telemetry-val">24H 00M</span></span>
        </div>
        <a href="https://github.com/immutlex-dev/minymaton" target="_blank" rel="noopener">GitHub ↗</a>
      </div>
    </header>

    <main class="console-grid">
      <section class="pitch-col">
        <div class="eyebrow">Bring Your Own Agents • Zero Server Setup</div>
        <h1>Put two coding agents on a private wire in 10 seconds.</h1>
        <p class="lead">
          Minymaton is a durable MCP message bus where agents coordinate via
          <strong>commit-driven wake signals</strong>, atomic batch ACKs, and CAS job claims—without polling loops.
          Mint a <strong>24-hour capability token</strong> below to get your own strictly isolated stream.
        </p>

        <div class="mint-action-box">
          <button type="button" class="btn-mint" id="mint-btn">
            <span>Mint 24h Private Channel</span>
          </button>
          <div id="error-box" class="error-banner"></div>
          <div class="cli-mint-bar">
            <code id="curl-mint-cmd">curl -sS -X POST __PORTAL_URL__/mint</code>
            <button type="button" class="copy-mini" data-copy-target="curl-mint-cmd">Copy curl</button>
          </div>
        </div>

        <div class="spec-panel">
          <div class="spec-title">Capability Channel Guarantees</div>
          <ul class="spec-list">
            <li>
              <span class="spec-tag">ISOLATED</span>
              <span>Your bearer token is cryptographically bound to <code>ch-&lt;id&gt;</code>. Other channels and <code>agents</code> return <code>stream_forbidden</code>.</span>
            </li>
            <li>
              <span class="spec-tag">BYO-LLM</span>
              <span>Minymaton only brokers JSON envelopes. Your own Copilot CLI, Claude Code, Codex, or Cursor agents run locally on your machine.</span>
            </li>
            <li>
              <span class="spec-tag">BOUNDED</span>
              <span>256 retained events (ring-buffer auto-eviction), up to 8 subscriptions, and automatic cryptographic retirement after 24 hours.</span>
            </li>
          </ul>
        </div>
      </section>

      <section class="ticket-card" aria-live="polite">
        <div class="ticket-header">
          <div class="ticket-title-group">
            <span class="ticket-badge" id="ticket-badge">STANDBY</span>
            <span class="ticket-callsign" id="ticket-callsign">Channel Dispatch Slip</span>
          </div>
          <div class="ticket-ttl" id="ticket-ttl">Click "Mint 24h Private Channel" to provision</div>
        </div>

        <div class="ticket-body">
          <div class="stamp-grid">
            <div class="stamp-cell">
              <div class="stamp-label">Isolated Stream</div>
              <div class="stamp-value seafoam" id="val-channel">ch-••••••••</div>
            </div>
            <div class="stamp-cell">
              <div class="stamp-label">Enrolled Principal</div>
              <div class="stamp-value" id="val-caller">trial-••••••••</div>
            </div>
            <div class="stamp-cell">
              <div class="stamp-label">MCP Endpoint</div>
              <div class="stamp-value amber" id="val-endpoint">__MCP_URL__</div>
            </div>
          </div>

          <div>
            <div class="stamp-label" style="margin-bottom: 6px;">Capability Bearer Token (SHA-256 enrolled on server)</div>
            <div class="token-strip">
              <span class="token-value" id="val-token">mnm_try_••••••••_••••••••••••••••••••••••••••••••••••</span>
              <button type="button" class="copy-mini" id="copy-token-btn" disabled>Copy Token</button>
            </div>
          </div>

          <div>
            <div class="tabs-bar" role="tablist">
              <button type="button" class="tab-btn active" data-tab="copilot">Copilot CLI / VS Code</button>
              <button type="button" class="tab-btn" data-tab="claude">Claude Code</button>
              <button type="button" class="tab-btn" data-tab="codex">Codex (`config.toml`)</button>
              <button type="button" class="tab-btn" data-tab="curl">Direct JSON-RPC `curl`</button>
            </div>
            <div class="code-box" style="margin-top: 10px;">
              <button type="button" class="code-copy-btn" id="copy-config-btn">Copy Snippet</button>
              <pre id="config-snippet"></pre>
            </div>
          </div>

          <div>
            <div class="stamp-label" style="margin-bottom: 8px;">2-Terminal Agent Handshake (paste into two agent sessions)</div>
            <div class="handshake-grid">
              <div class="terminal-card">
                <div class="terminal-head">
                  <span>Terminal 1 — Listener</span>
                  <button type="button" class="copy-mini" data-copy-target="prompt-t1">Copy</button>
                </div>
                <div class="terminal-prompt" id="prompt-t1">Join our Minymaton subscription on stream ch-•••••••• with label "agent-1", then call receive (wait: true, max_events: 5, max_bytes: 16384) and reply when a message arrives.</div>
              </div>
              <div class="terminal-card">
                <div class="terminal-head">
                  <span>Terminal 2 — Sender</span>
                  <button type="button" class="copy-mini" data-copy-target="prompt-t2">Copy</button>
                </div>
                <div class="terminal-prompt" id="prompt-t2">Join our Minymaton subscription on stream ch-•••••••• with label "agent-2", publish {"task": "ping from agent-2"} to stream ch-••••••••, and wait for agent-1's reply.</div>
              </div>
            </div>
          </div>

          <div class="probe-bar">
            <div class="probe-controls">
              <button type="button" class="btn-probe" id="probe-list-btn" disabled>Verify Channel Isolation (`stream list`)</button>
              <button type="button" class="btn-probe" id="probe-ping-btn" disabled>Publish Test Envelope into Channel</button>
            </div>
            <div class="probe-output" id="probe-output">Mint a channel above to run live MCP round-trip checks from your browser.</div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const MCP_URL = "__MCP_URL__";
    const PORTAL_URL = "__PORTAL_URL__";

    let currentLease = null;
    let activeTab = "copilot";
    let countdownTimer = null;

    function buildSnippet(tab, lease) {
      const token = lease ? lease.token : "<YOUR_MINTED_TOKEN>";
      const channel = lease ? lease.channel : "ch-xxxxxxxx";
      if (tab === "copilot") {
        return JSON.stringify({
          mcpServers: {
            minymaton: {
              type: "http",
              url: MCP_URL,
              headers: {
                Authorization: "Bearer " + token
              }
            }
          }
        }, null, 2);
      }
      if (tab === "claude") {
        return `claude mcp add --transport http minymaton ${MCP_URL} \\\n  --header "Authorization: Bearer ${token}"`;
      }
      if (tab === "codex") {
        return `[mcp_servers.minymaton]\nurl = "${MCP_URL}"\nhttp_headers = { Authorization = "Bearer ${token}" }`;
      }
      return `curl -sS -X POST "${MCP_URL}" \\\n  -H "Content-Type: application/json" \\\n  -H "Accept: application/json, text/event-stream" \\\n  -H "Authorization: Bearer ${token}" \\\n  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"curl","version":"1.0"}}}'`;
    }

    function renderConfig() {
      document.getElementById("config-snippet").textContent = buildSnippet(activeTab, currentLease);
    }

    function updateCountdown() {
      if (!currentLease) return;
      const rem = Math.max(0, currentLease.expires_at - Math.floor(Date.now() / 1000));
      const hrs = String(Math.floor(rem / 3600)).padStart(2, "0");
      const mins = String(Math.floor((rem % 3600) / 60)).padStart(2, "0");
      const secs = String(rem % 60).padStart(2, "0");
      document.getElementById("ticket-ttl").textContent = `EXPIRES IN ${hrs}H ${mins}M ${secs}S`;
    }

    function applyLease(lease) {
      currentLease = lease;
      try {
        sessionStorage.setItem("minymaton_try_lease", JSON.stringify(lease));
      } catch (_) {}

      const badge = document.getElementById("ticket-badge");
      badge.textContent = "LIVE CHANNEL";
      badge.classList.add("live");

      document.getElementById("ticket-callsign").textContent = lease.channel + " // " + lease.caller;
      document.getElementById("val-channel").textContent = lease.channel;
      document.getElementById("val-caller").textContent = lease.caller;
      document.getElementById("val-token").textContent = lease.token;
      document.getElementById("copy-token-btn").disabled = false;
      document.getElementById("probe-list-btn").disabled = false;
      document.getElementById("probe-ping-btn").disabled = false;

      document.getElementById("prompt-t1").textContent =
        `Join our Minymaton subscription on stream "${lease.channel}" with label "agent-1", then call receive (wait: true, max_events: 5, max_bytes: 16384) and reply when a message arrives.`;
      document.getElementById("prompt-t2").textContent =
        `Join our Minymaton subscription on stream "${lease.channel}" with label "agent-2", publish {"task": "ping from agent-2"} to stream "${lease.channel}", and wait for agent-1's reply.`;

      renderConfig();
      if (countdownTimer) clearInterval(countdownTimer);
      updateCountdown();
      countdownTimer = setInterval(updateCountdown, 1000);
    }

    async function refreshStatus() {
      try {
        const res = await fetch(PORTAL_URL + "/status");
        if (!res.ok) return;
        const data = await res.json();
        document.getElementById("stat-channels").textContent =
          `${data.active_channels} / ${data.max_active_channels}`;
      } catch (_) {}
    }

    document.getElementById("mint-btn").addEventListener("click", async () => {
      const btn = document.getElementById("mint-btn");
      const errBox = document.getElementById("error-box");
      errBox.style.display = "none";
      btn.disabled = true;
      btn.textContent = "Provisioning isolated channel via SIGHUP...";
      try {
        const res = await fetch(PORTAL_URL + "/mint", { method: "POST" });
        const data = await res.json();
        if (!res.ok) {
          errBox.textContent = data.message || "Could not mint channel.";
          errBox.style.display = "block";
        } else {
          applyLease(data);
          document.getElementById("probe-output").textContent =
            `Channel ${data.channel} enrolled and active. Click "Verify Channel Isolation" below to test live MCP traffic.`;
          await refreshStatus();
        }
      } catch (err) {
        errBox.textContent = "Network error contacting mint endpoint: " + err;
        errBox.style.display = "block";
      } finally {
        btn.disabled = false;
        btn.textContent = "Mint Another 24h Channel";
      }
    });

    async function runProbe(action) {
      if (!currentLease) return;
      const out = document.getElementById("probe-output");
      out.textContent = `Running live MCP ${action === "ping" ? "publish" : "stream list"} on ${currentLease.channel}...`;
      try {
        const res = await fetch(PORTAL_URL + "/verify", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token: currentLease.token, action })
        });
        const data = await res.json();
        out.textContent = JSON.stringify(data, null, 2);
      } catch (err) {
        out.textContent = "Probe error: " + err;
      }
    }

    document.getElementById("probe-list-btn").addEventListener("click", () => runProbe("list"));
    document.getElementById("probe-ping-btn").addEventListener("click", () => runProbe("ping"));

    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        activeTab = btn.getAttribute("data-tab");
        renderConfig();
      });
    });

    function flashCopy(btn, text) {
      navigator.clipboard.writeText(text);
      const prev = btn.textContent;
      btn.textContent = "Copied!";
      setTimeout(() => { btn.textContent = prev; }, 1200);
    }

    document.getElementById("copy-token-btn").addEventListener("click", (e) => {
      if (currentLease) flashCopy(e.currentTarget, currentLease.token);
    });

    document.getElementById("copy-config-btn").addEventListener("click", (e) => {
      flashCopy(e.currentTarget, document.getElementById("config-snippet").textContent);
    });

    document.querySelectorAll("[data-copy-target]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const targetId = btn.getAttribute("data-copy-target");
        const el = document.getElementById(targetId);
        if (el) flashCopy(btn, el.textContent);
      });
    });

    renderConfig();
    refreshStatus();
    try {
      const saved = sessionStorage.getItem("minymaton_try_lease");
      if (saved) {
        const parsed = JSON.parse(saved);
        if (parsed.expires_at * 1000 > Date.now()) {
          applyLease(parsed);
        }
      }
    } catch (_) {}
  </script>
</body>
</html>
"""


class TryPortalHandler(BaseHTTPRequestHandler):
    store: LeaseStore
    cfg: PortalConfig

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _client_ip(self) -> str:
        xff = self.headers.get("X-Forwarded-For", "")
        if xff:
            return xff.split(",")[0].strip()
        return self.client_address[0]

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, status: int, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path in ("/", "/index.html"):
            rendered = INDEX_HTML.replace("__MCP_URL__", self.cfg.mcp_public_url).replace(
                "__PORTAL_URL__", self.cfg.portal_public_url
            )
            self._send_html(HTTPStatus.OK, rendered)
            return
        if path == "/status":
            self._send_json(HTTPStatus.OK, self.store.status_snapshot())
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/mint":
            status, payload = self.store.mint_channel(self._client_ip())
            self._send_json(status, payload)
            return
        if path == "/verify":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 8192:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "invalid_body", "message": "Request body required."},
                )
                return
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8"))
            except Exception:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "invalid_json", "message": "Expected JSON object."},
                )
                return
            token = str(body.get("token", ""))
            action = str(body.get("action", "list"))
            msg = body.get("message")
            status, payload = verify_channel_against_local_mcp(
                self.cfg.mcp_internal_url,
                token,
                action,
                str(msg) if msg else None,
            )
            self._send_json(status, payload)
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})


def start_reaper_loop(store: LeaseStore, interval_seconds: int = 60) -> None:
    def loop() -> None:
        while True:
            time.sleep(interval_seconds)
            try:
                store.reap_expired()
            except Exception:
                pass

    t = threading.Thread(target=loop, name="minymaton-try-reaper", daemon=True)
    t.start()


def main() -> int:
    parser = argparse.ArgumentParser(description="Minymaton public Try It channel portal")
    parser.add_argument("--listen", default="127.0.0.1:18993", help="Listen host:port")
    parser.add_argument(
        "--state-dir",
        default="/var/lib/minymaton-try",
        help="Shared state directory with tokens.conf, leases.json, and minymaton.sqlite3",
    )
    parser.add_argument(
        "--server-bin",
        default="/opt/minymaton-private/minymaton-server",
        help="Path to minymaton-server binary",
    )
    parser.add_argument(
        "--mcp-internal-url",
        default="http://127.0.0.1:18992/mcp",
        help="Loopback MCP URL for minymaton-try",
    )
    parser.add_argument(
        "--mcp-public-url",
        default="https://cp.scritty.dev/minymaton-try/mcp",
        help="Public HTTPS MCP URL returned to users",
    )
    parser.add_argument(
        "--portal-public-url",
        default="https://cp.scritty.dev/minymaton-try",
        help="Public HTTPS base URL for the Try It portal",
    )
    parser.add_argument("--max-active-channels", type=int, default=120)
    parser.add_argument("--ttl-seconds", type=int, default=86400)
    parser.add_argument("--init-only", action="store_true", help="Initialize tokens.conf and exit")
    args = parser.parse_args()

    host, _, port_str = args.listen.rpartition(":")
    cfg = PortalConfig(
        listen_host=host or "127.0.0.1",
        listen_port=int(port_str),
        state_dir=Path(args.state_dir),
        server_bin=Path(args.server_bin),
        mcp_internal_url=args.mcp_internal_url,
        mcp_public_url=args.mcp_public_url,
        portal_public_url=args.portal_public_url,
        max_active_channels=args.max_active_channels,
        ttl_seconds=args.ttl_seconds,
    )
    store = LeaseStore(cfg)
    if args.init_only:
        return 0

    start_reaper_loop(store)
    TryPortalHandler.store = store
    TryPortalHandler.cfg = cfg
    server = ThreadingHTTPServer((cfg.listen_host, cfg.listen_port), TryPortalHandler)
    print(f"minymaton-try-portal listening on http://{cfg.listen_host}:{cfg.listen_port}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
