#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Type messages and send them to an MCP server.

By default each line you type is published through the server's `publish` tool,
so you can put messages on a minymaton queue while an agent is receiving from it.
Slash commands send any tool call or any raw JSON-RPC message.

JSON-RPC 2.0 is hand-rolled on the standard library, following the framing rules
of vanilla-ui's MCP server (mcp/src/framing.mjs and mcp/src/sse.mjs):
newline-delimited JSON over stdio, and Streamable HTTP whose replies are either
one JSON document or a Server-Sent Events stream.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import queue
import select
import signal
import socket
import ssl
import subprocess
import sys
import threading
import uuid
from collections.abc import Callable
from urllib.parse import urlsplit

MAX_MESSAGE_BYTES = 8 * 1024 * 1024
SUPPORTED_VERSIONS = ("2025-11-25", "2025-06-18")
CLIENT_INFO = {"name": "mcp-send", "version": "1.0.0"}
DEFAULT_TEMPLATE = '{"stream": "{stream}", "idempotency_key": "{key}", "payload": "{message}"}'

HELP = """\
Type a line and press Enter to send it as a message.
  /tools               list the server's tools
  /call NAME {json}    call any tool with these arguments
  /raw {json}          send one raw JSON-RPC message; include "id" to wait for its reply
  /help                show this help
  /quit                disconnect; so do Ctrl+C and end of input (Ctrl+Z then Enter on Windows)
Ctrl+C while a call is waiting sends the server a cancellation and exits with
status 130; whether that call took effect is then unknown.
In the publish template and in /call arguments, {message}, {key} and {stream}
are replaced with the typed line, a fresh random key, and --stream."""

print_lock = threading.Lock()


def say(text: str) -> None:
    with print_lock:
        print(text, flush=True)


def encode(message: object) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class TransportError(Exception):
    pass


class SessionExpired(TransportError):
    """The server rejected a request carrying an expired HTTP session."""


class Interrupted(Exception):
    """Ctrl+C arrived while a request was waiting for its reply."""

    def __init__(self, request: dict) -> None:
        super().__init__(request.get("method"))
        self.request = request


class Doorbell:
    """Lets the main thread wait for other threads without becoming deaf to Ctrl+C.

    On Windows, Ctrl+C cannot interrupt a thread blocked in a socket read or on a lock,
    so the main thread blocks in neither. Network I/O runs in worker threads, and the
    main thread waits in select() on this socket pair. A worker rings it when it
    finishes; Python rings it when Ctrl+C arrives (signal.set_wakeup_fd), so the
    KeyboardInterrupt is raised when the key is pressed, not when the server answers."""

    def __init__(self) -> None:
        self._reader, self._writer = socket.socketpair()
        self._reader.setblocking(False)
        self._writer.setblocking(False)
        signal.set_wakeup_fd(self._writer.fileno(), warn_on_full_buffer=False)

    def ring(self) -> None:
        try:
            self._writer.send(b"\0")
        except BlockingIOError:
            pass  # Already ringing; the waiter looks again when it wakes.

    def wait(self, ready: Callable[[], bool]) -> None:
        while not ready():
            select.select([self._reader], [], [])
            try:
                while self._reader.recv(4096):
                    pass
            except BlockingIOError:
                pass

    def run(self, work: Callable[[], object]) -> object:
        """Runs work in a worker thread and returns its result, re-raising its error here."""
        outcome: list[tuple[bool, object]] = []

        def worker() -> None:
            try:
                outcome.append((True, work()))
            except BaseException as error:  # Handed to the waiting thread, which re-raises it.
                outcome.append((False, error))
            finally:
                self.ring()

        threading.Thread(target=worker, daemon=True).start()
        self.wait(lambda: bool(outcome))
        succeeded, value = outcome[0]
        if not succeeded:
            raise value
        return value


def parse_json(text: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise TransportError(f"server sent something that is not JSON ({error}): {text[:200]!r}") from None


class LineFramer:
    """Newline-delimited JSON, with the size limit enforced while a message is still arriving."""

    def __init__(self, limit: int = MAX_MESSAGE_BYTES) -> None:
        self.limit = limit
        self.pending = bytearray()
        self.discarding = False

    def push(self, chunk: bytes) -> list[str]:
        lines: list[str] = []
        start = 0
        while start < len(chunk):
            end = chunk.find(b"\n", start)
            piece = chunk[start:] if end == -1 else chunk[start:end]
            if not self.discarding:
                if len(self.pending) + len(piece) > self.limit:
                    say(f"!! refused a server message over {self.limit} bytes")
                    self.pending.clear()
                    self.discarding = True
                else:
                    self.pending += piece
            if end == -1:
                break
            if self.discarding:
                self.discarding = False
            else:
                self._emit(lines)
            start = end + 1
        return lines

    def finish(self) -> list[str]:
        lines: list[str] = []
        if not self.discarding:
            self._emit(lines)
        return lines

    def _emit(self, lines: list[str]) -> None:
        line = bytes(self.pending).removesuffix(b"\r")
        self.pending.clear()
        if not line:
            return
        try:
            lines.append(line.decode("utf-8"))
        except UnicodeDecodeError:
            say("!! refused a server message that is not valid UTF-8")


class SseDecoder:
    """Server-Sent Events: a blank line ends an event, and one space after the colon is framing."""

    def __init__(self, limit: int = MAX_MESSAGE_BYTES) -> None:
        self.limit = limit
        self._reset()

    def _reset(self) -> None:
        self.event = ""
        self.buffer = ""

    def feed(self, line: str) -> tuple[str, str] | None:
        if line == "":
            # Per the SSE spec, one trailing newline is removed and an empty data buffer dispatches nothing.
            # rmcp opens its streams with such an event: an id and an empty data field.
            data = self.buffer.removesuffix("\n")
            event = (self.event or "message", data) if data else None
            self._reset()
            return event
        if line.startswith(":"):
            return None
        name, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]
        if name == "data":
            if len(self.buffer) + len(value) + 1 > self.limit:
                raise TransportError(f"server event over {self.limit} bytes")
            self.buffer += value + "\n"
        elif name == "event":
            self.event = value
        return None


class HttpTransport:
    """Streamable HTTP: one POST per message; the reply is JSON, an SSE stream, or 202 Accepted."""

    def __init__(self, url: str, headers: dict[str, str], cafile: str | None) -> None:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise SystemExit(f"not an http:// or https:// URL: {url}")
        self.scheme = parts.scheme
        self.host = parts.hostname
        self.port = parts.port
        self.path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        self.extra_headers = headers
        self.tls = ssl.create_default_context(cafile=cafile) if parts.scheme == "https" else None
        self.session_id: str | None = None
        self.protocol_version: str | None = None

    def _connect(self) -> http.client.HTTPConnection:
        if self.tls is not None:
            return http.client.HTTPSConnection(self.host, self.port, context=self.tls)
        return http.client.HTTPConnection(self.host, self.port)

    def _headers(self, extra: dict[str, str]) -> dict[str, str]:
        headers = dict(self.extra_headers)
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        if self.protocol_version:
            headers["MCP-Protocol-Version"] = self.protocol_version
        headers.update(extra)
        return headers

    def exchange(self, message: dict, deliver: Callable[[dict], bool]) -> None:
        """POST one message and pass each server message in the reply to deliver() until it returns True."""
        connection = self._connect()
        try:
            connection.request("POST", self.path, body=encode(message), headers=self._headers({
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }))
            response = connection.getresponse()
            if message.get("method") == "initialize":
                self.session_id = response.getheader("Mcp-Session-Id")
            if response.status == 202:
                response.read()
                return
            if response.status >= 400:
                body = response.read(64 * 1024).decode("utf-8", "replace").strip()
                if response.status == 404 and self.session_id and message.get("method") != "initialize":
                    raise SessionExpired(f"HTTP 404 {response.reason}: {body}")
                raise TransportError(f"HTTP {response.status} {response.reason}: {body}")
            media_type = (response.getheader("Content-Type") or "").split(";")[0].strip().lower()
            if media_type == "application/json":
                body = response.read(MAX_MESSAGE_BYTES + 1)
                if len(body) > MAX_MESSAGE_BYTES:
                    raise TransportError(f"server reply over {MAX_MESSAGE_BYTES} bytes")
                parsed = parse_json(body.decode("utf-8"))
                for item in parsed if isinstance(parsed, list) else [parsed]:
                    if deliver(item):
                        return
                return
            if media_type == "text/event-stream":
                decoder = SseDecoder()
                while raw := response.readline(MAX_MESSAGE_BYTES + 1):
                    if len(raw) > MAX_MESSAGE_BYTES:
                        raise TransportError(f"server event line over {MAX_MESSAGE_BYTES} bytes")
                    event = decoder.feed(raw.decode("utf-8").removesuffix("\n").removesuffix("\r"))
                    if event is not None and event[0] == "message" and deliver(parse_json(event[1])):
                        return
                return
            raise TransportError(f"unexpected reply type {media_type!r} with HTTP {response.status}")
        finally:
            connection.close()

    def close(self) -> None:
        if not self.session_id:
            return
        connection = self._connect()
        try:
            connection.request("DELETE", self.path, headers=self._headers({}))
            response = connection.getresponse()
            response.read()
            say(f"session ended (HTTP {response.status})")
        except OSError as error:
            say(f"!! could not end the session: {error}")
        finally:
            connection.close()


class StdioTransport:
    """A server process spawned by this script, speaking newline-delimited JSON on its stdin and stdout."""

    def __init__(self, command: list[str], deliver: Callable[[dict | None], None]) -> None:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, creationflags=flags)
        self.write_lock = threading.Lock()
        self.deliver = deliver
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        framer = LineFramer()
        stdout = self.process.stdout
        while chunk := stdout.read1(65536):
            self._deliver_lines(framer.push(chunk))
        self._deliver_lines(framer.finish())
        self.deliver(None)

    def _deliver_lines(self, lines: list[str]) -> None:
        for line in lines:
            try:
                message = json.loads(line)
            except json.JSONDecodeError as error:
                say(f"!! server sent a line that is not JSON ({error}): {line[:200]}")
                continue
            self.deliver(message)

    def send(self, message: dict) -> None:
        with self.write_lock:
            self.process.stdin.write(encode(message) + b"\n")
            self.process.stdin.flush()

    def close(self) -> None:
        self.process.stdin.close()
        say(f"server exited with code {self.process.wait()}")


def http_headers(arguments: argparse.Namespace) -> dict[str, str]:
    """The extra HTTP headers: each --header, and the bearer token read from the
    environment variable --token-env names, so the token never sits in argv."""
    headers = {}
    for header in arguments.header:
        name, colon, value = header.partition(":")
        if not colon:
            raise SystemExit(f"--header needs 'Name: value', got {header!r}")
        headers[name.strip()] = value.strip()
    if arguments.token_env is not None:
        token = os.environ.get(arguments.token_env)
        if not token:
            raise SystemExit(f"environment variable {arguments.token_env} named by --token-env is not set or is empty")
        headers["Authorization"] = token if token.startswith("Bearer ") else f"Bearer {token}"
    return headers


class Session:
    def __init__(self, arguments: argparse.Namespace) -> None:
        self.doorbell = Doorbell()
        self.next_id = 0
        self.replies: queue.Queue[dict | None] = queue.Queue()
        if arguments.transport == "http":
            self.http: HttpTransport | None = HttpTransport(
                arguments.url, http_headers(arguments), arguments.cafile)
            self.stdio: StdioTransport | None = None
        else:
            command = arguments.command[1:] if arguments.command[:1] == ["--"] else arguments.command
            if not command:
                raise SystemExit("stdio needs the server command after --")
            self.http = None
            self.stdio = StdioTransport(command, self._from_stdio)

    def fresh_id(self) -> int:
        self.next_id += 1
        return self.next_id

    def _from_stdio(self, message: dict | None) -> None:
        if message is None or ("id" in message and "method" not in message):
            self.replies.put(message)
            self.doorbell.ring()
        else:
            self._incoming(message)

    def _incoming(self, message: dict) -> None:
        method = message.get("method")
        if "id" in message:
            if method == "ping":
                reply = {"jsonrpc": "2.0", "id": message["id"], "result": {}}
            else:
                reply = {"jsonrpc": "2.0", "id": message["id"],
                         "error": {"code": -32601, "message": f"mcp-send does not handle {method}"}}
            say(f"<- server request {method}; answered")
            self._answer(reply)
        else:
            say(f"<- {method} {json.dumps(message.get('params', {}), ensure_ascii=False)}")

    def _answer(self, reply: dict) -> None:
        """Replies to a server request, from the thread that received it."""
        if self.http is not None:
            self.http.exchange(reply, self._unexpected)
        else:
            self.stdio.send(reply)

    def _unexpected(self, incoming: dict) -> bool:
        say(f"<- unexpected message {json.dumps(incoming, ensure_ascii=False)}")
        return False

    def send(self, message: dict) -> dict | None:
        """Send one message. For a request (it has an id and a method), wait for its reply
        and return it; Ctrl+C while waiting cancels the request and raises Interrupted."""
        is_request = "id" in message and "method" in message
        try:
            try:
                return self._exchange(message, is_request)
            except SessionExpired:
                if self.http is None or message.get("method") in ("initialize", "notifications/initialized"):
                    raise
                version = self.http.protocol_version
                if version is None:
                    raise
                self.http.session_id = None
                self.initialize(version)
                return self._exchange(message, is_request)
        except KeyboardInterrupt:
            if not is_request:
                raise
            self._cancel(message["id"])
            raise Interrupted(message) from None

    def _exchange(self, message: dict, is_request: bool) -> dict | None:
        if self.http is not None:
            reply: dict | None = None

            def deliver(incoming: dict) -> bool:
                nonlocal reply
                if "method" in incoming:
                    self._incoming(incoming)
                    return False
                if is_request and incoming.get("id") == message["id"]:
                    reply = incoming
                    return True
                say(f"<- unexpected reply {json.dumps(incoming, ensure_ascii=False)}")
                return False

            self.doorbell.run(lambda: self.http.exchange(message, deliver))
            if is_request and reply is None:
                raise TransportError("the server ended its reply without answering the request")
            return reply
        self.doorbell.run(lambda: self.stdio.send(message))
        if not is_request:
            return None
        while True:
            self.doorbell.wait(lambda: not self.replies.empty())
            reply = self.replies.get_nowait()
            if reply is None:
                raise TransportError("the server closed its output")
            if reply.get("id") == message["id"]:
                return reply
            say(f"<- unexpected reply {json.dumps(reply, ensure_ascii=False)}")

    def _cancel(self, request_id: object) -> None:
        """Tells the server to stop an interrupted request. Only an explicit cancellation
        does that in the 2025 revisions; a dropped connection does not."""
        try:
            self.notify("notifications/cancelled", {"requestId": request_id, "reason": "interrupted by Ctrl+C"})
        except (TransportError, OSError, http.client.HTTPException) as error:
            say(f"!! could not send the cancellation: {error}")

    def request(self, method: str, params: dict) -> dict:
        reply = self.send({"jsonrpc": "2.0", "id": self.fresh_id(), "method": method, "params": params})
        if "error" in reply:
            error = reply["error"]
            raise TransportError(f"{method} failed: {error.get('code')} {error.get('message')}")
        return reply["result"]

    def notify(self, method: str, params: dict | None = None) -> None:
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self.send(message)

    def initialize(self, version: str) -> dict:
        result = self.request("initialize", {"protocolVersion": version, "capabilities": {}, "clientInfo": CLIENT_INFO})
        negotiated = result.get("protocolVersion")
        if negotiated not in SUPPORTED_VERSIONS:
            raise TransportError(f"server chose protocol {negotiated!r}; this script speaks {', '.join(SUPPORTED_VERSIONS)}")
        if self.http is not None:
            self.http.protocol_version = negotiated
        self.notify("notifications/initialized")
        return result

    def close(self) -> None:
        transport = self.http if self.http is not None else self.stdio
        self.doorbell.run(transport.close)


def fill(template: object, values: dict[str, str]) -> object:
    if isinstance(template, str):
        for name, value in values.items():
            template = template.replace("{" + name + "}", value)
        return template
    if isinstance(template, list):
        return [fill(item, values) for item in template]
    if isinstance(template, dict):
        return {key: fill(value, values) for key, value in template.items()}
    return template


def show_tool_result(reply: dict) -> None:
    if "error" in reply:
        error = reply["error"]
        say(f"   error {error.get('code')}: {error.get('message')}")
        if "data" in error:
            say(f"   {json.dumps(error['data'], ensure_ascii=False)}")
        return
    result = reply.get("result", {})
    shown = False
    for item in result.get("content", []):
        say(f"   {item['text']}" if item.get("type") == "text" else f"   {json.dumps(item, ensure_ascii=False)}")
        shown = True
    if not shown and "structuredContent" in result:
        say(f"   {json.dumps(result['structuredContent'], ensure_ascii=False)}")
    if result.get("isError"):
        say("   (the tool reported an error)")


def call_tool(session: Session, name: str, arguments: object) -> None:
    reply = session.send({"jsonrpc": "2.0", "id": session.fresh_id(), "method": "tools/call",
                          "params": {"name": name, "arguments": arguments}})
    show_tool_result(reply)


def list_tools(session: Session) -> None:
    cursor = None
    while True:
        result = session.request("tools/list", {"cursor": cursor} if cursor else {})
        for tool in result.get("tools", []):
            summary = (tool.get("description") or "").strip().splitlines()
            say(f"   {tool['name']}" + (f": {summary[0]}" if summary else ""))
        cursor = result.get("nextCursor")
        if not cursor:
            return


class ReplyListener:
    """Follows a reply stream on its own HTTP session and prints each reply once.

    It runs in a background thread with its own minymaton subscription, so the agent's
    replies reach this terminal. If the server ends its session or cuts its waiting
    receive, it starts a new session and receives again. A batch is acknowledged on the
    next receive, after its replies are printed; one delivered again after a cut is not
    printed twice."""

    LABEL = "mcp-send-replies"

    def __init__(self, arguments: argparse.Namespace, stream: str) -> None:
        self.http = HttpTransport(arguments.url, http_headers(arguments), arguments.cafile)
        self.protocol = arguments.protocol
        self.stream = stream
        self.next_id = 0
        self.printed: set[str] = set()

    def _request(self, method: str, params: dict) -> dict:
        self.next_id += 1
        message = {"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}
        reply: dict | None = None

        def deliver(incoming: dict) -> bool:
            nonlocal reply
            if "method" not in incoming and incoming.get("id") == message["id"]:
                reply = incoming
                return True
            return False

        self.http.exchange(message, deliver)
        if reply is None:
            raise TransportError("the server ended its reply without answering the request")
        if "error" in reply:
            error = reply["error"]
            raise TransportError(f"{method} failed: {error.get('code')} {error.get('message')}")
        return reply["result"]

    def _connect(self) -> None:
        self.http.session_id = None
        self.http.protocol_version = None
        result = self._request("initialize", {"protocolVersion": self.protocol, "capabilities": {},
                                              "clientInfo": CLIENT_INFO})
        self.http.protocol_version = result.get("protocolVersion")
        self.http.exchange({"jsonrpc": "2.0", "method": "notifications/initialized"}, lambda _: False)

    def _tool(self, name: str, arguments: dict) -> dict:
        result = self._request("tools/call", {"name": name, "arguments": arguments})
        structured = result.get("structuredContent")
        if structured is None:
            raise TransportError(f"{name} returned no structured result")
        return structured

    def _handle(self) -> str:
        recovered = self._tool("subscription", {"action": "recover", "stream": self.stream, "label": self.LABEL})
        if recovered.get("code") == "stream_not_found":
            created = self._tool("stream", {"action": "create", "name": self.stream})
            if created.get("code") not in (None, "stream_exists"):
                raise TransportError(f"cannot create stream {self.stream}: {created}")
            recovered = self._tool("subscription", {"action": "recover", "stream": self.stream,
                                                    "label": self.LABEL})
        if recovered.get("code") == "subscription_not_found":
            recovered = self._tool("subscription", {"action": "create", "stream": self.stream,
                                                    "label": self.LABEL})
            if recovered.get("code") == "subscription_exists":
                recovered = self._tool("subscription", {"action": "recover", "stream": self.stream,
                                                        "label": self.LABEL})
        if "handle" not in recovered:
            raise TransportError(f"cannot follow {self.stream}: {recovered}")
        return recovered["handle"]

    def start(self) -> None:
        """Connects and subscribes before returning, so replies published afterwards are seen."""
        self._connect()
        handle = self._handle()
        say(f"following replies on {self.stream}")
        threading.Thread(target=self._run, args=(handle,), daemon=True).start()

    def _run(self, handle: str) -> None:
        ack: str | None = None
        failures = 0
        while True:
            try:
                arguments = {"handle": handle, "max_events": 100, "max_bytes": 16384, "wait": True}
                if ack is not None:
                    arguments["ack"] = ack
                try:
                    result = self._tool("receive", arguments)
                except SessionExpired:
                    self._connect()
                    continue
                failures = 0
                if "ack_result" in result and result["ack_result"] is not None:
                    ack = None
                if result.get("status") == "events":
                    for event in result["events"]:
                        if event["message_id"] in self.printed:
                            continue
                        self.printed.add(event["message_id"])
                        payload = event["payload"]
                        text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
                        say(f"<- reply: {text}")
                    ack = result["ack_after_handling"]
                elif result.get("code"):
                    say(f"!! receive on {self.stream} failed: {result}")
                    return
            except (TransportError, OSError, http.client.HTTPException) as cut:
                # A cut receive or an ended session: the next
                # request is answered by the replacement, which reports the session gone.
                failures += 1
                if failures > 5:
                    say(f"!! reply stream {self.stream} keeps failing: {cut}")
                    return
                try:
                    self._connect()
                except (TransportError, OSError, http.client.HTTPException) as error:
                    say(f"!! reply stream {self.stream} disconnected: {error}")
                    return


def repl(session: Session, tool: str, template: object, stream: str | None) -> int:
    """Reads lines until the user quits, and returns the exit status."""
    while True:
        try:
            line = input("> ")
        except EOFError:
            return 0
        except KeyboardInterrupt:
            say("")  # Ctrl+C at the prompt quits; finish the prompt's line first.
            return 0
        except UnicodeDecodeError as error:
            say(f"!! input is not UTF-8 text ({error}); nothing was sent for that line, ending")
            return 0
        if not line.strip():
            continue
        values = {"message": line, "key": uuid.uuid4().hex, "stream": stream or ""}
        try:
            if line.startswith("/"):
                command, _, rest = line.partition(" ")
                if command == "/quit":
                    return 0
                if command == "/help":
                    say(HELP)
                elif command == "/tools":
                    list_tools(session)
                elif command == "/call":
                    name, _, arguments = rest.strip().partition(" ")
                    if not name:
                        say("   usage: /call NAME {json arguments}")
                        continue
                    call_tool(session, name, fill(json.loads(arguments or "{}"), values))
                elif command == "/raw":
                    reply = session.send(json.loads(rest))
                    say(f"   {json.dumps(reply, ensure_ascii=False)}" if reply is not None else "   sent")
                else:
                    say(f"   unknown command {command}; /help lists them")
                continue
            call_tool(session, tool, fill(template, values))
        except Interrupted as interrupted:
            request = interrupted.request
            what = request.get("params", {}).get("name") if request.get("method") == "tools/call" else None
            what = what or request.get("method")
            say(f"!! interrupted before {what} answered; a cancellation was sent, "
                f"and whether {what} took effect is unknown")
            return 130
        except KeyboardInterrupt:
            say("")
            return 0
        except json.JSONDecodeError as error:
            say(f"   not valid JSON: {error}")
        except TransportError as error:
            say(f"!! {error}")


def main() -> int:
    # Piped input would otherwise be decoded with the Windows ANSI code page, corrupting non-ASCII messages.
    sys.stdin.reconfigure(encoding="utf-8")
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(
        description="Type messages and send them to an MCP server.",
        epilog=HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    transports = parser.add_subparsers(dest="transport", required=True)
    http_parser = transports.add_parser("http", help="Streamable HTTP server, JSON or SSE replies")
    http_parser.add_argument("url", help="the server's MCP endpoint, e.g. https://127.0.0.1:8443/mcp")
    http_parser.add_argument("--header", action="append", default=[], metavar="'Name: value'",
                             help="extra HTTP header; repeatable")
    http_parser.add_argument("--token-env", metavar="VAR",
                             help="send the bearer token held in environment variable VAR (fails if unset or empty)")
    http_parser.add_argument("--cafile", help="CA certificate to trust for https://")
    http_parser.add_argument("--replies", metavar="STREAM",
                             help="also follow this minymaton stream and print each reply published to it")
    stdio_parser = transports.add_parser("stdio", help="spawn a stdio server: stdio [options] -- COMMAND ARGS...")
    stdio_parser.add_argument("command", nargs=argparse.REMAINDER, help="the server command, after --")
    for sub in (http_parser, stdio_parser):
        sub.add_argument("--protocol", choices=SUPPORTED_VERSIONS, default=SUPPORTED_VERSIONS[0])
        sub.add_argument("--tool", default="publish", help="tool each typed line is sent to (default: publish)")
        sub.add_argument("--args", default=DEFAULT_TEMPLATE, help="JSON arguments template for --tool")
        sub.add_argument("--stream", help="stream name for {stream} in the template")
    arguments = parser.parse_args()

    try:
        template = json.loads(arguments.args)
    except json.JSONDecodeError as error:
        parser.error(f"--args is not valid JSON: {error}")
    if "{stream}" in arguments.args and not arguments.stream:
        parser.error("the --args template uses {stream}; pass --stream NAME")

    session = Session(arguments)
    status = 0
    try:
        result = session.initialize(arguments.protocol)
        server = result.get("serverInfo", {})
        say(f"connected to {server.get('name', '?')} {server.get('version', '')} "
            f"(protocol {result.get('protocolVersion')}); lines go to tool '{arguments.tool}'. /help for commands.")
        if getattr(arguments, "replies", None):
            ReplyListener(arguments, arguments.replies).start()
        status = repl(session, arguments.tool, template, arguments.stream)
    except (Interrupted, KeyboardInterrupt):
        say("!! interrupted while connecting")
        status = 130
    except TransportError as error:
        say(f"!! {error}")
        status = 1
    except (OSError, http.client.HTTPException) as error:
        say(f"!! connection failed: {error}")
        status = 1
    finally:
        try:
            session.close()
        except KeyboardInterrupt:
            say("!! interrupted while ending the session; the server may still hold it")
            status = 130
    return status


if __name__ == "__main__":
    sys.exit(main())
