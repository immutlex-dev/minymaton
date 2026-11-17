#!/usr/bin/env python3
"""Streamable HTTP MCP gateway and SSE keepalive proxy behind ACA HTTPS :443 ingress.

Listens on 0.0.0.0:8080:
  * /healthz -> readiness check against 127.0.0.1:18990 returning boot_id
  * /mcp and /mcp/* -> 127.0.0.1:18990 with Host rewrite + 25s SSE comment keepalives
    (keeps ACA Envoy's 240s idle request timer reset indefinitely so parked
    `receive(wait=true)` and `minymaton-wait` hold open across multi-hour waits).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from urllib.parse import urlsplit

UPSTREAM_MCP_HOST = "127.0.0.1"
UPSTREAM_MCP_PORT = 18990

EARLY_HEADER_WAIT_SECONDS = 1.5
SSE_KEEPALIVE_INTERVAL_SECONDS = 25.0
MAX_HEADER_BYTES = 65536

HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[gateway {ts}] {msg}", file=sys.stderr, flush=True)


def _http_chunk(data: bytes) -> bytes:
    return f"{len(data):x}\r\n".encode("ascii") + data + b"\r\n"


async def _read_headers(reader: asyncio.StreamReader) -> tuple[str, list[tuple[str, str]]] | None:
    try:
        raw = await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError as err:
        if not err.partial:
            return None
        raise
    if len(raw) > MAX_HEADER_BYTES:
        raise ValueError("HTTP header block exceeds limit")
    header_text = raw[:-4].decode("latin-1")
    lines = header_text.split("\r\n")
    start_line = lines[0]
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers.append((k.strip(), v.strip()))
    return start_line, headers


async def _read_chunked_stream(reader: asyncio.StreamReader):
    """Async generator yielding each decoded payload chunk from an HTTP chunked body."""
    while True:
        size_line = await reader.readuntil(b"\r\n")
        size_token = size_line[:-2].split(b";", 1)[0].strip()
        chunk_len = int(size_token, 16)
        if chunk_len == 0:
            # Consume trailing headers (usually empty \r\n)
            while True:
                trailer = await reader.readuntil(b"\r\n")
                if trailer == b"\r\n":
                    break
            return
        payload = await reader.readexactly(chunk_len)
        await reader.readexactly(2)  # trailing \r\n
        if payload:
            yield payload


async def _read_body_bytes(
    reader: asyncio.StreamReader,
    headers: list[tuple[str, str]],
    *,
    read_until_eof_default: bool,
) -> bytes:
    hdr_map = {k.lower(): v for k, v in headers}
    if "chunked" in hdr_map.get("transfer-encoding", "").lower():
        parts: list[bytes] = []
        async for chunk in _read_chunked_stream(reader):
            parts.append(chunk)
        return b"".join(parts)
    if "content-length" in hdr_map:
        length = int(hdr_map["content-length"])
        return await reader.readexactly(length) if length > 0 else b""
    if read_until_eof_default:
        return await reader.read()
    return b""


async def _send_simple_response(
    writer: asyncio.StreamWriter,
    status_line: str,
    headers: list[tuple[str, str]],
    body: bytes,
) -> None:
    lines = [status_line]
    for k, v in headers:
        lines.append(f"{k}: {v}")
    lines.append(f"Content-Length: {len(body)}")
    lines.append("Connection: close")
    header_bytes = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
    writer.write(header_bytes + body)
    await writer.drain()


async def _handle_healthz(writer: asyncio.StreamWriter, boot_id: str) -> None:
    try:
        _, up_writer = await asyncio.wait_for(
            asyncio.open_connection(UPSTREAM_MCP_HOST, UPSTREAM_MCP_PORT),
            timeout=1.0,
        )
        up_writer.close()
        await up_writer.wait_closed()
        payload = f'{{"ok":true,"boot_id":"{boot_id}"}}\n'.encode("utf-8")
        await _send_simple_response(
            writer,
            "HTTP/1.1 200 OK",
            [("Content-Type", "application/json"), ("Cache-Control", "no-store")],
            payload,
        )
    except Exception:  # noqa: BLE001
        await _send_simple_response(
            writer,
            "HTTP/1.1 503 Service Unavailable",
            [("Content-Type", "application/json")],
            b'{"ok":false}\n',
        )


async def _stream_upstream_sse_to_chunked_client(
    up_reader: asyncio.StreamReader,
    resp_headers: list[tuple[str, str]],
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    hdr_map = {k.lower(): v for k, v in resp_headers}
    is_chunked = "chunked" in hdr_map.get("transfer-encoding", "").lower()
    chunk_iter = _read_chunked_stream(up_reader) if is_chunked else None

    async def _next_piece() -> bytes:
        if chunk_iter is not None:
            try:
                return await chunk_iter.__anext__()
            except StopAsyncIteration:
                return b""
        return await up_reader.read(8192)

    client_eof_task = asyncio.create_task(client_reader.read(1))
    piece_task = asyncio.create_task(_next_piece())
    try:
        while True:
            done, _ = await asyncio.wait(
                {piece_task, client_eof_task},
                timeout=SSE_KEEPALIVE_INTERVAL_SECONDS,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if client_eof_task in done:
                # Downstream client closed socket; cancel upstream read so minymaton drops waiter
                piece_task.cancel()
                return
            if piece_task in done:
                piece = piece_task.result()
                if not piece:
                    client_writer.write(b"0\r\n\r\n")
                    await client_writer.drain()
                    return
                client_writer.write(_http_chunk(piece))
                await client_writer.drain()
                piece_task = asyncio.create_task(_next_piece())
            else:
                # Idle interval elapsed; send SSE comment keepalive chunk
                client_writer.write(_http_chunk(b": keepalive\n\n"))
                await client_writer.drain()
    finally:
        if not client_eof_task.done():
            client_eof_task.cancel()
        if not piece_task.done():
            piece_task.cancel()


async def _proxy_mcp(
    method: str,
    target: str,
    req_headers: list[tuple[str, str]],
    req_body: bytes,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    up_reader, up_writer = await asyncio.wait_for(
        asyncio.open_connection(UPSTREAM_MCP_HOST, UPSTREAM_MCP_PORT),
        timeout=5.0,
    )
    try:
        accept_val = ""
        out_lines = [f"{method} {target} HTTP/1.1", f"Host: {UPSTREAM_MCP_HOST}:{UPSTREAM_MCP_PORT}"]
        for k, v in req_headers:
            kl = k.lower()
            if kl == "accept":
                accept_val = v.lower()
            if kl in HOP_BY_HOP_HEADERS or kl in {"host", "content-length"}:
                continue
            out_lines.append(f"{k}: {v}")
        out_lines.append(f"Content-Length: {len(req_body)}")
        out_lines.append("Connection: close")
        up_writer.write(("\r\n".join(out_lines) + "\r\n\r\n").encode("latin-1") + req_body)
        await up_writer.drain()

        can_sse = "text/event-stream" in accept_val and method.upper() in {"POST", "GET"}
        header_task = asyncio.create_task(_read_headers(up_reader))

        if not can_sse:
            parsed = await header_task
            if parsed is None:
                await _send_simple_response(client_writer, "HTTP/1.1 502 Bad Gateway", [], b"")
                return
            resp_status, resp_headers = parsed
            resp_body = await _read_body_bytes(
                up_reader, resp_headers, read_until_eof_default=True
            )
            filtered = [
                (k, v)
                for k, v in resp_headers
                if k.lower() not in HOP_BY_HOP_HEADERS and k.lower() != "content-length"
            ]
            await _send_simple_response(client_writer, resp_status, filtered, resp_body)
            return

        done, _ = await asyncio.wait({header_task}, timeout=EARLY_HEADER_WAIT_SECONDS)
        if header_task in done:
            parsed = header_task.result()
            if parsed is None:
                await _send_simple_response(client_writer, "HTTP/1.1 502 Bad Gateway", [], b"")
                return
            resp_status, resp_headers = parsed
            ctype = next(
                (v.lower() for k, v in resp_headers if k.lower() == "content-type"),
                "",
            )
            if "text/event-stream" not in ctype:
                resp_body = await _read_body_bytes(
                    up_reader, resp_headers, read_until_eof_default=True
                )
                filtered = [
                    (k, v)
                    for k, v in resp_headers
                    if k.lower() not in HOP_BY_HOP_HEADERS and k.lower() != "content-length"
                ]
                await _send_simple_response(client_writer, resp_status, filtered, resp_body)
                return

            # Upstream started an SSE stream immediately: preserve all upstream headers
            # (including Mcp-Session-Id) and inject 25s SSE comment keepalives while idle.
            out_hdrs = [resp_status]
            for k, v in resp_headers:
                if k.lower() in HOP_BY_HOP_HEADERS or k.lower() == "content-length":
                    continue
                out_hdrs.append(f"{k}: {v}")
            out_hdrs.append("Cache-Control: no-cache")
            out_hdrs.append("X-Accel-Buffering: no")
            out_hdrs.append("Transfer-Encoding: chunked")
            out_hdrs.append("Connection: close")
            client_writer.write(("\r\n".join(out_hdrs) + "\r\n\r\n").encode("latin-1"))
            await client_writer.drain()
            await _stream_upstream_sse_to_chunked_client(
                up_reader, resp_headers, client_reader, client_writer
            )
            return

        # Upstream has already passed auth_gate/session_gate (<5ms) and is parked inside
        # stateless receive(wait=true) before writing response headers. Flush SSE 200 OK
        # immediately and emit `: keepalive` every 25s until minymaton-server responds.
        sse_preface = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/event-stream\r\n"
            "Cache-Control: no-cache\r\n"
            "X-Accel-Buffering: no\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n\r\n"
        ).encode("latin-1")
        client_writer.write(sse_preface + _http_chunk(b": keepalive\n\n"))
        await client_writer.drain()

        client_eof_task = asyncio.create_task(client_reader.read(1))
        try:
            while True:
                done, _ = await asyncio.wait(
                    {header_task, client_eof_task},
                    timeout=SSE_KEEPALIVE_INTERVAL_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if client_eof_task in done:
                    header_task.cancel()
                    return
                if header_task in done:
                    break
                client_writer.write(_http_chunk(b": keepalive\n\n"))
                await client_writer.drain()
        finally:
            if not client_eof_task.done():
                client_eof_task.cancel()

        parsed = header_task.result()
        if parsed is None:
            client_writer.write(b"0\r\n\r\n")
            await client_writer.drain()
            return
        _, resp_headers = parsed
        ctype = next(
            (v.lower() for k, v in resp_headers if k.lower() == "content-type"),
            "",
        )
        if "text/event-stream" in ctype:
            await _stream_upstream_sse_to_chunked_client(
                up_reader, resp_headers, client_reader, client_writer
            )
            return

        resp_body = await _read_body_bytes(
            up_reader, resp_headers, read_until_eof_default=True
        )
        text_lines = resp_body.decode("utf-8").splitlines() or [""]
        sse_frame = "".join(f"data: {line}\n" for line in text_lines) + "\n"
        client_writer.write(_http_chunk(sse_frame.encode("utf-8")) + b"0\r\n\r\n")
        await client_writer.drain()
    finally:
        up_writer.close()
        try:
            await up_writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    public_fqdn: str,
    boot_id: str,
) -> None:
    try:
        parsed = await asyncio.wait_for(_read_headers(client_reader), timeout=30.0)
        if parsed is None:
            return
        start_line, req_headers = parsed
        parts = start_line.split(" ")
        if len(parts) < 2:
            await _send_simple_response(client_writer, "HTTP/1.1 400 Bad Request", [], b"")
            return
        method, target = parts[0].upper(), parts[1]
        req_body = await asyncio.wait_for(
            _read_body_bytes(client_reader, req_headers, read_until_eof_default=False),
            timeout=30.0,
        )
        path = urlsplit(target).path or "/"

        if path == "/healthz":
            await _handle_healthz(client_writer, boot_id)
            return

        if path == "/mcp" or path.startswith("/mcp/"):
            await _proxy_mcp(
                method, target, req_headers, req_body, client_reader, client_writer
            )
            return

        await _send_simple_response(client_writer, "HTTP/1.1 404 Not Found", [], b"")
    except Exception as exc:  # noqa: BLE001
        _log(f"Request error (fqdn={public_fqdn}): {exc}")
    finally:
        client_writer.close()
        try:
            await client_writer.wait_closed()
        except Exception:  # noqa: BLE001
            pass


async def _serve(public_fqdn: str, boot_id: str) -> None:
    srv_ext = await asyncio.start_server(
        lambda r, w: _handle_client(r, w, public_fqdn=public_fqdn, boot_id=boot_id),
        host="0.0.0.0",
        port=8080,
    )
    _log(f"Listening on 0.0.0.0:8080 (fqdn={public_fqdn}, boot_id={boot_id})")
    async with srv_ext:
        await srv_ext.serve_forever()


def main() -> int:
    public_fqdn = os.environ["PUBLIC_FQDN"].strip()
    boot_id = os.environ.get("MINYMATON_BOOT_ID", "boot")
    asyncio.run(_serve(public_fqdn, boot_id))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
