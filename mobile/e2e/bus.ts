// Real buses for the Detox suite: each Bus is a minymaton-server process on
// a loopback port with its own database and enrolled tokens, reached from the
// emulator through `adb reverse`. The Desk is the other agent on the bus,
// speaking MCP Streamable HTTP exactly as crates/minymaton-mobile-core's
// Endpoint does (src/mcp_client.rs), and mirrors the Desk helper in
// crates/minymaton-mobile-core/tests/mobile_core.rs.
import { ChildProcess, execFileSync, spawn } from 'child_process';
import { randomBytes } from 'crypto';
import fs from 'fs';
import os from 'os';
import path from 'path';
import readline from 'readline';

export const PHONE_TOKEN = 'mny_phone_0123456789abcdef0123456789abcdef';
export const DESK_TOKEN = 'mny_desk_fedcba9876543210fedcba9876543210';
const PROTOCOL = '2026-07-28';

function isAndroid(): boolean {
  try {
    const { device } = require('detox');
    return device.getPlatform() === 'android';
  } catch {
    return true;
  }
}

function appContainer(udid: string): string {
  return execFileSync('xcrun', ['simctl', 'get_app_container', udid, 'com.minymaton', 'data'])
    .toString()
    .trim();
}

function iosFile(udid: string, relative: string): string {
  // Android `files/minymaton` is the core's data dir; on iOS that is
  // Documents/minymaton inside the app container.
  const rel = relative.replace(/^files\//, 'Documents/');
  return path.join(appContainer(udid), rel);
}

function serverBinary(): string {
  if (process.env.MINYMATON_SERVER_BIN) return process.env.MINYMATON_SERVER_BIN;
  const exe = process.platform === 'win32' ? 'minymaton-server.exe' : 'minymaton-server';
  return path.resolve(__dirname, '..', '..', 'target', 'release', exe);
}

export class Bus {
  private constructor(
    readonly url: string,
    readonly port: number,
    private readonly child: ChildProcess,
    private readonly dir: string,
    private readonly serial: string,
  ) {}

  /** Starts a server and maps its port into the device at the same number. */
  static async start(serial: string): Promise<Bus> {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'minymaton-bus-'));
    const tokens = path.join(dir, 'tokens.tsv');
    fs.writeFileSync(tokens, `phone\t${PHONE_TOKEN}\ndesk\t${DESK_TOKEN}\n`);
    const child = spawn(
      serverBinary(),
      ['--listen', '127.0.0.1:0', '--db', path.join(dir, 'bus.sqlite3'), '--bearer-token-file', tokens],
      { stdio: ['ignore', 'pipe', 'inherit'] },
    );
    const url = await new Promise<string>((resolve, reject) => {
      child.once('exit', (code) => reject(new Error(`minymaton-server exited with ${code} before listening`)));
      child.once('error', reject);
      const lines = readline.createInterface({ input: child.stdout! });
      lines.once('line', (line) => {
        const match = /^listening on (http:\/\/\S+\/mcp)$/.exec(line.trim());
        if (match) resolve(match[1]);
        else reject(new Error(`unexpected first server line: ${line}`));
      });
    });
    const port = Number(new URL(url).port);
    // Android emulators reach the host through `adb reverse`; iOS simulators
    // (UUID serials) share the host loopback, so no mapping is needed.
    if (isAndroid()) {
      execFileSync('adb', ['-s', serial, 'reverse', `tcp:${port}`, `tcp:${port}`]);
    }
    return new Bus(url, port, child, dir, serial);
  }

  desk(): Desk {
    return new Desk(this.url);
  }

  async stop(): Promise<void> {
    if (isAndroid()) {
      execFileSync('adb', ['-s', this.serial, 'reverse', '--remove', `tcp:${this.port}`]);
    }
    if (this.child.exitCode === null) {
      const exited = new Promise((resolve) => this.child.once('exit', resolve));
      this.child.kill();
      await exited;
    }
    fs.rmSync(this.dir, { recursive: true, force: true });
  }
}

type RpcResponse = {
  id?: number;
  result?: { structuredContent?: Record<string, unknown>; content?: { type: string; text?: string }[]; isError?: boolean };
  error?: { code: number; message: string };
};

function frames(body: string): RpcResponse[] {
  const out: RpcResponse[] = [];
  for (const block of body.split(/\r?\n\r?\n/)) {
    const data = block
      .split(/\r?\n/)
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice(5).trimStart())
      .join('\n');
    if (data) out.push(JSON.parse(data));
  }
  return out;
}

export class Desk {
  private nextId = 1;

  constructor(private readonly url: string) {}

  async call(tool: string, args: Record<string, unknown>): Promise<Record<string, any>> {
    const id = this.nextId++;
    const request = {
      jsonrpc: '2.0',
      id,
      method: 'tools/call',
      params: {
        name: tool,
        arguments: args,
        _meta: {
          'io.modelcontextprotocol/protocolVersion': PROTOCOL,
          'io.modelcontextprotocol/clientInfo': { name: 'minymaton-detox-desk', version: '0.1.0' },
          'io.modelcontextprotocol/clientCapabilities': {},
        },
      },
    };
    // A stream that carried only keepalives and then closed is answered by
    // sending the call again, as Endpoint::call does.
    for (;;) {
      const response = await fetch(this.url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json, text/event-stream',
          'MCP-Protocol-Version': PROTOCOL,
          'Mcp-Method': 'tools/call',
          'Mcp-Name': tool,
          Authorization: `Bearer ${DESK_TOKEN}`,
        },
        body: JSON.stringify(request),
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`desk ${tool}: HTTP ${response.status}: ${body}`);
      const type = response.headers.get('content-type') ?? '';
      const message = type.includes('text/event-stream')
        ? frames(body).find((frame) => frame.id === id)
        : (JSON.parse(body) as RpcResponse);
      if (!message) continue;
      if (message.error) throw new Error(`desk ${tool}: ${message.error.message}`);
      const result = message.result!;
      const structured =
        result.structuredContent ?? JSON.parse(result.content?.find((block) => block.type === 'text')?.text ?? '{}');
      if (result.isError) throw new Error(`desk ${tool}: ${JSON.stringify(structured)}`);
      return structured;
    }
  }

  async createStream(name: string): Promise<void> {
    await this.call('stream', { action: 'create', name });
  }

  async join(stream: string, label = 'desk'): Promise<string> {
    const created = await this.call('subscription', { action: 'create', stream, label, start_at: 'latest' });
    return created.handle as string;
  }

  async publish(stream: string, payload: Record<string, unknown>, to?: string, from = 'desk'): Promise<number> {
    const args: Record<string, unknown> = {
      stream,
      idempotency_key: randomBytes(16).toString('hex'),
      payload,
      from,
    };
    if (to) args.to = to;
    const published = await this.call('publish', args);
    return published.sequence as number;
  }

  async roster(stream: string): Promise<{ label: string; waiting: boolean }[]> {
    const listed = await this.call('subscription', { action: 'list', stream, limit: 100 });
    return listed.subscriptions as { label: string; waiting: boolean }[];
  }

  /**
   * Parks one `receive` with wait:true and resolves `ready` as soon as the bus
   * emits the `"window open"` progress notification over SSE, then resolves
   * `done` once the batch arrives and is acknowledged.
   */
  async park(handle: string): Promise<{ done: Promise<Record<string, any>[]> }> {
    const id = this.nextId++;
    const request = {
      jsonrpc: '2.0',
      id,
      method: 'tools/call',
      params: {
        name: 'receive',
        arguments: { handle, max_events: 16, max_bytes: 16384, wait: true },
        _meta: {
          progressToken: id,
          'io.modelcontextprotocol/protocolVersion': PROTOCOL,
          'io.modelcontextprotocol/clientInfo': { name: 'minymaton-detox-desk', version: '0.1.0' },
          'io.modelcontextprotocol/clientCapabilities': {},
        },
      },
    };
    const response = await fetch(this.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json, text/event-stream',
        'MCP-Protocol-Version': PROTOCOL,
        'Mcp-Method': 'tools/call',
        'Mcp-Name': 'receive',
        Authorization: `Bearer ${DESK_TOKEN}`,
      },
      body: JSON.stringify(request),
    });
    if (!response.ok || !response.body) {
      throw new Error(`desk park: HTTP ${response.status}`);
    }
    const type = response.headers.get('content-type') ?? '';
    if (!type.includes('text/event-stream')) {
      const message = (await response.json()) as RpcResponse;
      if (message.error) throw new Error(`desk park: ${message.error.message}`);
      const res = message.result!;
      const structured =
        res.structuredContent ?? JSON.parse(res.content?.find((b) => b.type === 'text')?.text ?? '{}');
      if (res.isError) throw new Error(`desk park: ${JSON.stringify(structured)}`);
      if (structured.status !== 'events') {
        throw new Error(`unexpected desk receive: ${JSON.stringify(structured)}`);
      }
      await this.call('receive', { handle, max_events: 0, ack: structured.ack_after_handling as string });
      return { done: Promise.resolve(structured.events as Record<string, any>[]) };
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let resolveReady!: () => void;
    let rejectReady!: (err: unknown) => void;
    const ready = new Promise<void>((res, rej) => {
      resolveReady = res;
      rejectReady = rej;
    });

    const done = (async (): Promise<Record<string, any>[]> => {
      try {
        for (;;) {
          const { value, done: eof } = await reader.read();
          if (value) buffer += decoder.decode(value, { stream: true });
          let idx: number;
          while ((idx = buffer.search(/\r?\n\r?\n/)) !== -1) {
            const block = buffer.slice(0, idx);
            const sepLen = buffer.slice(idx).startsWith('\r\n') ? 4 : 2;
            buffer = buffer.slice(idx + sepLen);
            const data = block
              .split(/\r?\n/)
              .filter((line) => line.startsWith('data:'))
              .map((line) => line.slice(5).trimStart())
              .join('\n');
            if (!data) continue;
            const frame = JSON.parse(data) as RpcResponse & { method?: string; params?: { message?: string } };
            if (frame.method === 'notifications/progress' && frame.params?.message === 'window open') {
              resolveReady();
            } else if (frame.id === id) {
              resolveReady();
              if (frame.error) throw new Error(`desk park: ${frame.error.message}`);
              const res = frame.result!;
              const structured =
                res.structuredContent ?? JSON.parse(res.content?.find((b) => b.type === 'text')?.text ?? '{}');
              if (res.isError) throw new Error(`desk park: ${JSON.stringify(structured)}`);
              if (structured.status !== 'events') {
                throw new Error(`unexpected desk receive: ${JSON.stringify(structured)}`);
              }
              await this.call('receive', { handle, max_events: 0, ack: structured.ack_after_handling as string });
              return structured.events as Record<string, any>[];
            }
          }
          if (eof) throw new Error('desk park: SSE closed before response');
        }
      } catch (err) {
        rejectReady(err);
        throw err;
      }
    })();

    await ready;
    return { done };
  }

  /**
   * Parks one `receive` with wait:true until the bus delivers a batch to the
   * desk, acknowledges that batch and returns its events.
   */
  async waitForEvent(handle: string): Promise<Record<string, any>[]> {
    const result = await this.call('receive', { handle, max_events: 16, max_bytes: 16384, wait: true });
    if (result.status !== 'events') throw new Error(`unexpected desk receive: ${JSON.stringify(result)}`);
    await this.call('receive', { handle, max_events: 0, ack: result.ack_after_handling });
    return result.events;
  }

  async checkEmpty(handle: string): Promise<void> {
    const result = await this.call('receive', { handle, max_events: 16, max_bytes: 16384, wait: false });
    if (result.status !== 'empty') {
      throw new Error(`expected empty receive, got: ${JSON.stringify(result)}`);
    }
  }
}

/** Reads a file from the app's private storage through `run-as`. */
export function readAppFile(serial: string, relative: string): Buffer {
  if (!isAndroid()) return fs.readFileSync(iosFile(serial, relative));
  return execFileSync('adb', ['-s', serial, 'exec-out', 'run-as', 'com.minymaton', 'cat', relative]);
}

export function listAppFiles(serial: string, relative: string): string[] {
  if (!isAndroid())
    return fs
      .readdirSync(iosFile(serial, relative), { withFileTypes: true })
      .filter((entry) => entry.isFile())
      .map((entry) => entry.name);
  return execFileSync('adb', ['-s', serial, 'shell', 'run-as', 'com.minymaton', 'ls', relative])
    .toString()
    .split(/\s+/)
    .filter(Boolean);
}
