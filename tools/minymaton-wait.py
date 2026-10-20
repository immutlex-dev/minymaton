#!/usr/bin/env python3
"""Zero-token background CLI waiter for minymaton subscriptions.

Blocks on `receive(wait: true)` over Streamable HTTP until a batch arrives or a
foreground `receive` supersedes the wait. Only a delivered batch is printed to
stdout with exit 0. Supersession and errors are terminal, reported as JSON on
stderr with distinct nonzero exit codes; nothing is retried or auto-acknowledged.
Wakeups require a harness that notifies on background-command completion.

Reads options from CLI flags, `MINYMATON_*` environment variables, or an
explicitly supplied INI file (`--config` / `MINYMATON_CONFIG` `[minymaton]`
section with `[DEFAULT]` fallback). Reads the bearer token from
`MINYMATON_TOKEN` (or the environment variable named by `--token-env` /
`MINYMATON_TOKEN_ENV` / INI `token_env`), never from argv or config files.
"""

from __future__ import annotations

import argparse
import configparser
import http.client
import json
import os
import pathlib
import ssl
import sys
from urllib.parse import urlsplit


class ConfigError(ValueError):
    pass


def load_ini(config_path: str | None) -> tuple[pathlib.Path, configparser.ConfigParser] | None:
    raw_path = config_path if config_path is not None else (os.environ.get("MINYMATON_CONFIG") or None)
    if not raw_path:
        return None
    path = pathlib.Path(raw_path)
    try:
        text = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as error:
        raise ConfigError(f"cannot read INI config in {path}: {error}") from error
    parser = configparser.ConfigParser()
    try:
        parser.read_string(text, source=str(path))
    except configparser.Error as error:
        raise ConfigError(f"{error.__class__.__name__}: {error} in {path}") from error
    return path, parser


def ini_get(ini: tuple[pathlib.Path, configparser.ConfigParser] | None, key: str) -> str | None:
    if ini is None:
        return None
    path, parser = ini
    if not parser.has_section("minymaton"):
        return None
    try:
        val = parser.get("minymaton", key, fallback=None)
    except configparser.Error as error:
        raise ConfigError(f"{error.__class__.__name__}: {error} in {path}") from error
    return val if val else None


def parse_int_option(raw: str, label: str, path: pathlib.Path | None = None) -> int:
    try:
        return int(raw.strip(), 10)
    except ValueError as error:
        suffix = f" in {path}" if path is not None else ""
        raise ConfigError(f"invalid {label}: {error}{suffix}") from error


def resolve_options(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    secrets: list[str] | None = None,
) -> tuple[str, str | None, int, int, str, str | None, dict[str, str], list[str]]:
    if secrets is None:
        secrets = []
    ini = load_ini(args.config)
    ini_path = ini[0] if ini is not None else None

    handle = args.handle or os.environ.get("MINYMATON_HANDLE") or ini_get(ini, "handle")
    if not handle:
        parser.error("the following arguments are required: --handle")

    ack = args.ack or os.environ.get("MINYMATON_ACK") or ini_get(ini, "ack")

    if args.max_events is not None:
        max_events = args.max_events
    elif os.environ.get("MINYMATON_MAX_EVENTS"):
        max_events = parse_int_option(os.environ["MINYMATON_MAX_EVENTS"], "MINYMATON_MAX_EVENTS")
    else:
        raw_events = ini_get(ini, "max_events")
        max_events = parse_int_option(raw_events, "max_events", ini_path) if raw_events is not None else 10

    if args.max_bytes is not None:
        max_bytes = args.max_bytes
    elif os.environ.get("MINYMATON_MAX_BYTES"):
        max_bytes = parse_int_option(os.environ["MINYMATON_MAX_BYTES"], "MINYMATON_MAX_BYTES")
    else:
        raw_bytes = ini_get(ini, "max_bytes")
        max_bytes = parse_int_option(raw_bytes, "max_bytes", ini_path) if raw_bytes is not None else 16384

    ca_file = args.ca_file or os.environ.get("MINYMATON_CA_FILE") or ini_get(ini, "ca_file")

    url = args.url or os.environ.get("MINYMATON_URL") or ini_get(ini, "url")
    if not url:
        raise ConfigError(
            "missing endpoint URL: specify --url, MINYMATON_URL, or url in --config / MINYMATON_CONFIG"
        )

    explicit_token_env = (
        args.token_env
        if args.token_env is not None
        else (os.environ.get("MINYMATON_TOKEN_ENV") or ini_get(ini, "token_env"))
    )
    if explicit_token_env is not None:
        var_name = explicit_token_env.strip()
        if not var_name:
            raise ConfigError("token_env cannot be empty")
        token = os.environ.get(var_name)
        if not token:
            raise ConfigError(
                f"environment variable {var_name} specified by token_env is not set or is empty"
            )
    else:
        token = os.environ.get("MINYMATON_TOKEN") or None

    headers: dict[str, str] = {}
    if token:
        secrets.append(token)
        bearer_prefix = "Bearer" + " "
        headers["Authorization"] = token if token.startswith(bearer_prefix) else f"{bearer_prefix}{token}"

    return handle, ack, max_events, max_bytes, url, ca_file, headers, secrets


def build_ssl_context(ca_file: str | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    env_cert_file = os.environ.get("SSL_CERT_FILE")
    if env_cert_file and pathlib.Path(env_cert_file).is_file():
        ctx.load_verify_locations(cafile=env_cert_file)
    env_cert_dir = os.environ.get("SSL_CERT_DIR")
    if env_cert_dir:
        for entry in env_cert_dir.split(os.pathsep):
            if entry and pathlib.Path(entry).is_dir():
                ctx.load_verify_locations(capath=entry)
    if ca_file:
        try:
            ctx.load_verify_locations(cafile=ca_file)
        except (OSError, ssl.SSLError) as error:
            raise ConfigError(f"cannot load CA file {ca_file}: {error}") from error
    return ctx


def post_jsonrpc(
    url: str,
    base_headers: dict[str, str],
    payload: dict,
    ca_file: str | None = None,
) -> dict:
    if not isinstance(url, str):
        raise ValueError("MCP endpoint URL must be a string")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("MCP endpoint must be an absolute HTTP or HTTPS URL")
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = parts.path or "/mcp"
    if parts.query:
        path = f"{path}?{parts.query}"

    if parts.scheme == "https":
        conn: http.client.HTTPConnection = http.client.HTTPSConnection(
            host, port, timeout=None, context=build_ssl_context(ca_file)
        )
    else:
        conn = http.client.HTTPConnection(host, port, timeout=None)
    try:
        headers = {
            **base_headers,
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": payload["method"],
        }
        if payload["method"] == "tools/call":
            headers["Mcp-Name"] = payload["params"]["name"]
        payload = {
            **payload,
            "params": {
                **payload.get("params", {}),
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientInfo": {"name": "minymaton-wait", "version": "0.1.0"},
                    "io.modelcontextprotocol/clientCapabilities": {},
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        conn.request("POST", path, body=body, headers=headers)
        resp = conn.getresponse()
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")

        ctype = (resp.getheader("Content-Type") or "").lower()
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
                    event_text = "\n".join(data_lines)
                    data_lines.clear()
                    msg = json.loads(event_text)
                    if not isinstance(msg, dict):
                        raise ValueError("JSON-RPC response must be an object")
                    if msg.get("id") == payload.get("id") and ("result" in msg or "error" in msg):
                        return msg
            raise RuntimeError("SSE stream closed before JSON-RPC response arrived")

        raw = resp.read().decode("utf-8")
        msg = json.loads(raw)
        if not isinstance(msg, dict) or msg.get("id") != payload.get("id") or not (
            "result" in msg or "error" in msg
        ):
            raise ValueError("missing or mismatched JSON-RPC response")
        return msg
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for the next minymaton batch on a subscription.")
    parser.add_argument("--handle", default=None, help="Subscription handle (sub_...)")
    parser.add_argument("--ack", default=None, help="Optional batch_id (bat_...) to acknowledge first")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to seal (default: 10)")
    parser.add_argument("--max-bytes", type=int, default=None, help="Maximum wire bytes (default: 16384)")
    parser.add_argument("--url", default=None, help="Optional MCP endpoint URL override")
    parser.add_argument("--ca-file", default=None, help="Optional PEM CA certificate bundle for HTTPS verification")
    parser.add_argument("--config", default=None, help="Optional INI configuration file")
    parser.add_argument("--token-env", default=None, help="Bearer token environment variable name (default: MINYMATON_TOKEN)")
    args = parser.parse_args()
    secrets: list[str] = []
    if args.token_env:
        cli_secret = os.environ.get(args.token_env.strip())
        if cli_secret:
            secrets.append(cli_secret)

    try:
        return run(parser, args, secrets)
    except ConfigError as error:
        return fail({"code": "config_failed", "message": str(error)}, 1, secrets)
    except (RuntimeError, OSError, http.client.HTTPException, ssl.SSLError) as error:
        return fail({"code": "transport_failed", "message": str(error)}, 7, secrets)
    except ValueError as error:
        return fail({"code": "protocol_failed", "message": str(error)}, 1, secrets)
    except KeyboardInterrupt:
        return fail({"code": "interrupted", "message": "waiter interrupted"}, 130, secrets)


def fail(error: dict, exit_code: int, secrets: list[str] | tuple[str, ...] = ()) -> int:
    msg = error.get("message")
    if isinstance(msg, str):
        candidates = list(secrets)
        for env_key in ("MINYMATON_TOKEN", os.environ.get("MINYMATON_TOKEN_ENV") or ""):
            if env_key:
                env_secret = os.environ.get(env_key)
                if env_secret:
                    candidates.append(env_secret)
        for secret in candidates:
            if not secret:
                continue
            for variant in (
                secret,
                repr(secret)[1:-1],
                repr(secret.encode("latin-1", errors="backslashreplace"))[2:-1],
                *secret.splitlines(),
            ):
                if variant:
                    msg = msg.replace(variant, "[redacted]")
        error = {**error, "message": msg}
    print(json.dumps({**error, "status": "error"}, separators=(",", ":")), file=sys.stderr, flush=True)
    return exit_code


def run(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    secrets: list[str] | None = None,
) -> int:
    if secrets is None:
        secrets = []
    handle, ack, max_events, max_bytes, url, ca_file, headers, secrets = resolve_options(
        parser, args, secrets
    )
    receive_args: dict[str, object] = {
        "handle": handle,
        "max_events": max_events,
        "max_bytes": max_bytes,
        "wait": True,
    }
    if ack:
        receive_args["ack"] = ack

    call_resp = post_jsonrpc(
        url,
        headers,
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "receive", "arguments": receive_args},
        },
        ca_file=ca_file,
    )
    if not call_resp:
        raise ValueError("empty response from receive")
    if "error" in call_resp:
        return fail(
            {"code": "jsonrpc_error", "message": "receive request failed", "details": call_resp["error"]},
            1,
            secrets,
        )

    result = call_resp.get("result")
    if not isinstance(result, dict):
        raise ValueError("tools/call result must be an object")
    structured = result.get("structuredContent")
    if structured is None:
        for block in result.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text":
                structured = json.loads(block.get("text") or "{}")
                break
    if not isinstance(structured, dict):
        raise ValueError("receive result must contain a JSON object")
    if result.get("isError"):
        if not isinstance(structured.get("code"), str) or not isinstance(structured.get("message"), str):
            raise ValueError("tool error must contain a code and message")
        exit_code = {
            "batch_no_longer_pending": 4,
            "subscription_not_found": 5,
            "batch_exceeds_bounds": 6,
        }.get(structured.get("code"), 1)
        return fail(structured, exit_code, secrets)
    if structured.get("status") == "superseded":
        print(json.dumps(structured, separators=(",", ":")), file=sys.stderr, flush=True)
        return 3
    if structured.get("status") == "events":
        if (
            not isinstance(structured.get("events"), list) or not structured["events"]
            or not isinstance(structured.get("batch_id"), str)
            or structured.get("ack_after_handling") != structured["batch_id"]
        ):
            raise ValueError("events result is missing a batch or its acknowledgement")
        print(json.dumps(structured, separators=(",", ":")), flush=True)
        return 0
    return fail(
        {"code": "unexpected_status", "message": "waiting receive returned no batch", "details": structured},
        1,
        secrets,
    )


if __name__ == "__main__":
    sys.exit(main())
