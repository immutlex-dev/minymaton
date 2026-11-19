// The JS face of the Rust core (crates/minymaton-mobile-core). Android and
// iOS both expose a native module named MinymatonModule with the same
// methods: request(commandJson) -> responseJson, and saveToken / loadToken /
// deleteToken against the platform keystore. Events arrive as JSON strings
// on the "MinymatonEvent" channel.
import { NativeEventEmitter, NativeModules } from 'react-native';

type NativeMinymaton = {
  request(command: string): Promise<string>;
  saveToken(profileId: string, token: string): Promise<null>;
  loadToken(profileId: string): Promise<string | null>;
  deleteToken(profileId: string): Promise<null>;
  addListener(eventName: string): void;
  removeListeners(count: number): void;
};

const native = NativeModules.MinymatonModule as NativeMinymaton;
const emitter = new NativeEventEmitter(NativeModules.MinymatonModule);

export type Profile = { id: string; name: string; url: string; has_ca: boolean };

export type RosterEntry = {
  label: string;
  waiting: boolean;
  last_active_at?: string;
  metadata?: Record<string, unknown>;
};

export type Message = {
  message_id: string;
  sequence: number;
  published_at: string;
  payload: unknown;
  publisher?: string | null;
  direction: 'in' | 'out';
};

export type Joined = { stream: string; label: string };

export type StreamsPage = {
  streams: string[];
  next_after: string | null;
};

export type MessagesPage = {
  stream: string;
  messages: Message[];
  next_before: number | null;
  has_newer: boolean;
  total_retained: number;
};

export type NudgeKind = 'yes_go' | 'no_stop' | 'catch_up' | 'custom';

export type CoreEvent =
  | { event: 'waiting'; profile_id: string; stream: string; label: string }
  | { event: 'messages'; profile_id: string; stream: string; messages: Message[] }
  | { event: 'acked'; profile_id: string; stream: string; batch_id: string; outcome: string }
  | { event: 'watch_ended'; profile_id: string; stream: string; code: string; message: string }
  | { event: 'paused'; profile_id: string | null; stopped: string[] };

export class CoreError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
  }
}

async function call<T>(command: Record<string, unknown>): Promise<T> {
  const response = JSON.parse(await native.request(JSON.stringify(command)));
  if (!response.ok) {
    throw new CoreError(response.error.code, response.error.message);
  }
  return response.result as T;
}

export const core = {
  profiles: () => call<{ profiles: Profile[]; active_profile: string | null }>({ cmd: 'profiles' }),
  addProfile: (name: string, url: string) =>
    call<{ profile: Profile }>({ cmd: 'add_profile', name, url }),
  removeProfile: (profileId: string) => call({ cmd: 'remove_profile', profile_id: profileId }),
  activate: (profileId: string, token: string | null) =>
    call<{ profile: Profile; streams: string[]; next_after: string | null; started: string[] }>(
      token === null ? { cmd: 'activate', profile_id: profileId } : { cmd: 'activate', profile_id: profileId, token },
    ),
  streams: (opts?: { after?: string | null; limit?: number }) => {
    const command: Record<string, unknown> = { cmd: 'streams' };
    if (opts?.after) command.after = opts.after;
    if (opts?.limit !== undefined) command.limit = opts.limit;
    return call<StreamsPage>(command);
  },
  deleteStreams: (streams: string[]) =>
    call<{ status: string; deleted: string[]; deleted_count: number; streams: string[]; next_after: string | null }>({
      cmd: 'delete_streams',
      streams,
    }),
  sweepStreams: (keep?: string[]) =>
    call<{ status: string; deleted: string[]; deleted_count: number; streams: string[]; next_after: string | null }>(
      keep ? { cmd: 'sweep_streams', keep } : { cmd: 'sweep_streams' },
    ),
  roster: (stream: string) => call<{ stream: string; subscriptions: RosterEntry[] }>({ cmd: 'roster', stream }),
  deleteSubscriptions: (stream: string, labels: string[]) =>
    call<{ status: string; stream: string; deleted: string[]; deleted_count: number; subscriptions: RosterEntry[] }>({
      cmd: 'delete_subscriptions',
      stream,
      labels,
    }),
  sweepSubscriptions: (stream: string, keep?: string[]) =>
    call<{ status: string; stream: string; deleted: string[]; deleted_count: number; subscriptions: RosterEntry[] }>(
      keep ? { cmd: 'sweep_subscriptions', stream, keep } : { cmd: 'sweep_subscriptions', stream },
    ),
  join: (stream: string, label: string) =>
    call<{ status: string; subscription: Joined }>({ cmd: 'join', stream, label }),
  joined: () => call<{ subscriptions: Joined[] }>({ cmd: 'joined' }),
  publish: (stream: string, payload: Record<string, unknown>, to: string | null) =>
    call<{ message: Message }>(to ? { cmd: 'publish', stream, payload, to } : { cmd: 'publish', stream, payload }),
  nudge: (stream: string, kind: NudgeKind, text: string | null, to: string | null) => {
    const command: Record<string, unknown> = { cmd: 'nudge', stream, kind };
    if (text) command.text = text;
    if (to) command.to = to;
    return call<{ message: Message }>(command);
  },
  messages: (stream: string, opts?: { before?: number | null; limit?: number }) => {
    const command: Record<string, unknown> = { cmd: 'messages', stream };
    if (opts?.before !== undefined && opts?.before !== null) command.before = opts.before;
    if (opts?.limit !== undefined) command.limit = opts.limit;
    return call<MessagesPage>(command);
  },
  syncHistory: (stream: string, opts?: { before?: number | null; limit?: number }) => {
    const command: Record<string, unknown> = { cmd: 'sync_history', stream };
    if (opts?.before !== undefined && opts?.before !== null) command.before = opts.before;
    if (opts?.limit !== undefined) command.limit = opts.limit;
    return call<MessagesPage>(command);
  },
  pause: () => call<{ paused: true; stopped: string[] }>({ cmd: 'pause' }),
  resume: () => call({ cmd: 'resume' }),
};

export const tokens = {
  save: (profileId: string, token: string) => native.saveToken(profileId, token),
  load: (profileId: string) => native.loadToken(profileId),
  remove: (profileId: string) => native.deleteToken(profileId),
};

export function onCoreEvent(handler: (event: CoreEvent) => void): () => void {
  const subscription = emitter.addListener('MinymatonEvent', (raw: string) => handler(JSON.parse(raw)));
  return () => subscription.remove();
}

export function messageText(payload: unknown): string {
  if (payload && typeof payload === 'object') {
    for (const key of ['text', 'body', 'summary'] as const) {
      const value = (payload as Record<string, unknown>)[key];
      if (typeof value === 'string') return value;
    }
  }
  return JSON.stringify(payload, null, 2);
}
