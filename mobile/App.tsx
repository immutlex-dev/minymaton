import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  AppState,
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import {
  core,
  CoreError,
  Message,
  MessagesPage,
  messageText,
  NudgeKind,
  onCoreEvent,
  Profile,
  RosterEntry,
  tokens,
} from './src/minymaton';

// Precision Avionics / RF Telemetry palette, ported from minymaton-desk theme.rs.
const CHASSIS_DEEP = '#0B0E14';
const PANEL_SLATE = '#131822';
const SURFACE_RAISED = '#1B2230';
const BEZEL_LINE = '#283246';
const INK_PRIMARY = '#E6ECF5';
const INK_MUTED = '#8B98AF';
const SIGNAL_GO = '#10B981';
const SIGNAL_STOP = '#EF4444';
const KNOB_CURSOR = '#A78BFA';
const MONO = Platform.select({ ios: 'Menlo', android: 'monospace' }) as string;

const BUS_ACCENTS = ['#2DD4BF', '#38BDF8', '#F59E0B'] as const;
const COLLAPSED_IDLE_LIMIT = 8;
const STREAM_PAGE_SIZE = 20;
const MESSAGE_PAGE_SIZE = 20;

function busAccent(profile: Profile | undefined): string {
  if (!profile) return BUS_ACCENTS[0];
  let hash = 0;
  for (let i = 0; i < profile.id.length; i += 1) {
    hash = (hash * 31 + profile.id.charCodeAt(i)) >>> 0;
  }
  return BUS_ACCENTS[hash % BUS_ACCENTS.length];
}

function describe(error: unknown): string {
  if (error instanceof CoreError) return `${error.code}: ${error.message}`;
  return error instanceof Error ? error.message : String(error);
}

function stamp(iso: string): string {
  return iso.length >= 16 ? iso.slice(5, 16).replace('T', ' ') : iso;
}

function cleanTarget(raw: string): string {
  return raw.trim().replace(/^@+/, '').trim();
}

function toggleItem(list: string[], item: string): string[] {
  return list.includes(item) ? list.filter((entry) => entry !== item) : [...list, item];
}

export default function App() {
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState<string | null>(null);
  const [name, setName] = useState('');
  const [url, setUrl] = useState('');
  const [token, setToken] = useState('');
  const [showAdd, setShowAdd] = useState(false);
  const [streams, setStreams] = useState<string[]>([]);
  const [streamAfter, setStreamAfter] = useState<string | null>(null);
  const [streamCursorStack, setStreamCursorStack] = useState<(string | null)[]>([]);
  const [streamNextAfter, setStreamNextAfter] = useState<string | null>(null);
  const [stream, setStream] = useState<string | null>(null);
  const [roster, setRoster] = useState<RosterEntry[]>([]);
  const [joinedMap, setJoinedMap] = useState<Record<string, boolean>>({});
  const [joinedLabels, setJoinedLabels] = useState<Record<string, string>>({});
  const [messages, setMessages] = useState<Message[]>([]);
  const [msgBefore, setMsgBefore] = useState<number | null>(null);
  const [msgCursorStack, setMsgCursorStack] = useState<(number | null)[]>([]);
  const [msgNextBefore, setMsgNextBefore] = useState<number | null>(null);
  const [msgHasNewer, setMsgHasNewer] = useState(false);
  const [msgTotalRetained, setMsgTotalRetained] = useState(0);
  const [joinLabel, setJoinLabel] = useState('phone');
  const [composeText, setComposeText] = useState('');
  const [composeTo, setComposeTo] = useState('');
  const [watchStatus, setWatchStatus] = useState('idle');
  const [ackStatus, setAckStatus] = useState('none');
  const [pauseStatus, setPauseStatus] = useState('running');
  const [selectingStreams, setSelectingStreams] = useState(false);
  const [selectedStreams, setSelectedStreams] = useState<string[]>([]);
  const [selectingRoster, setSelectingRoster] = useState(false);
  const [selectedRoster, setSelectedRoster] = useState<string[]>([]);
  const [showAllIdleRoster, setShowAllIdleRoster] = useState(false);
  const streamRef = useRef<string | null>(null);
  streamRef.current = stream;
  const msgBeforeRef = useRef<number | null>(null);
  msgBeforeRef.current = msgBefore;

  const guard = useCallback(async (work: () => Promise<void>, label = 'Working') => {
    setBusy(label);
    try {
      setError('');
      await work();
    } catch (failure) {
      setError(describe(failure));
    } finally {
      setBusy(null);
    }
  }, []);

  const applyStreamResetPage = useCallback((streamsList: string[], nextAfter: string | null) => {
    setStreams(streamsList);
    setStreamAfter(null);
    setStreamCursorStack([]);
    setStreamNextAfter(nextAfter);
  }, []);

  const applyMessagesPage = useCallback((page: MessagesPage) => {
    setMessages(page.messages);
    setMsgNextBefore(page.next_before);
    setMsgHasNewer(page.has_newer);
    setMsgTotalRetained(page.total_retained);
  }, []);

  const resetMessagesCursor = useCallback(() => {
    setMsgBefore(null);
    msgBeforeRef.current = null;
    setMsgCursorStack([]);
  }, []);

  const loadStream = useCallback(
    async (streamName: string, cursorOverride?: number | null) => {
      const cursor = cursorOverride !== undefined ? cursorOverride : msgBeforeRef.current;
      const [listed, history] = await Promise.all([
        core.roster(streamName),
        core.messages(streamName, { before: cursor, limit: MESSAGE_PAGE_SIZE }),
      ]);
      if (streamRef.current !== streamName) return;
      setRoster(listed.subscriptions);
      applyMessagesPage(history);
    },
    [applyMessagesPage],
  );

  const refreshJoined = useCallback(async () => {
    const joined = await core.joined();
    const map: Record<string, boolean> = {};
    const labels: Record<string, string> = {};
    for (const entry of joined.subscriptions) {
      map[entry.stream] = true;
      labels[entry.stream] = entry.label;
    }
    setJoinedMap(map);
    setJoinedLabels(labels);
    return map;
  }, []);

  const resetStream = useCallback(() => {
    setStream(null);
    streamRef.current = null;
    setRoster([]);
    setMessages([]);
    resetMessagesCursor();
    setMsgNextBefore(null);
    setMsgHasNewer(false);
    setMsgTotalRetained(0);
    setComposeTo('');
    setWatchStatus('idle');
    setAckStatus('none');
    setSelectingRoster(false);
    setSelectedRoster([]);
    setShowAllIdleRoster(false);
  }, [resetMessagesCursor]);

  const activate = useCallback(
    async (profileId: string, secret: string | null) => {
      const result = await core.activate(profileId, secret);
      setActiveId(result.profile.id);
      applyStreamResetPage(result.streams, result.next_after);
      setSelectingStreams(false);
      setSelectedStreams([]);
      resetStream();
      setShowAdd(false);
      await refreshJoined();
    },
    [applyStreamResetPage, refreshJoined, resetStream],
  );

  const reloadProfiles = useCallback(async () => {
    const listed = await core.profiles();
    setProfiles(listed.profiles);
    return listed;
  }, []);

  useEffect(() => {
    guard(async () => {
      const listed = await reloadProfiles();
      if (listed.active_profile) {
        await activate(listed.active_profile, await tokens.load(listed.active_profile));
      }
    });
  }, [guard, reloadProfiles, activate]);

  useEffect(
    () =>
      onCoreEvent((event) => {
        if (event.event === 'waiting') {
          setWatchStatus(`waiting ${event.stream} as ${event.label}`);
        } else if (event.event === 'messages') {
          setWatchStatus(`handling ${event.stream}`);
          setAckStatus('pending');
        } else if (event.event === 'acked') {
          setAckStatus(event.outcome);
        } else if (event.event === 'watch_ended') {
          setWatchStatus(`ended ${event.code}`);
        }
        if ('stream' in event && event.stream === streamRef.current) {
          loadStream(event.stream).catch((failure) => setError(describe(failure)));
        }
      }),
    [loadStream],
  );

  useEffect(() => {
    const subscription = AppState.addEventListener('change', (state) => {
      if (state === 'background') {
        core
          .pause()
          .then(({ stopped }) => setPauseStatus(`stopped ${stopped.join(' ') || 'nothing'}`))
          .catch((failure) => setError(describe(failure)));
      } else if (state === 'active') {
        guard(async () => {
          await core.resume();
          if (streamRef.current) await loadStream(streamRef.current);
        });
      }
    });
    return () => subscription.remove();
  }, [guard, loadStream]);

  const addProfile = () => {
    const cleanName = name;
    const cleanUrl = url.trim();
    const secret = token.trim() || null;
    setName('');
    setUrl('');
    setToken('');
    return guard(async () => {
      const { profile } = await core.addProfile(cleanName, cleanUrl);
      try {
        await activate(profile.id, secret);
      } catch (failure) {
        await core.removeProfile(profile.id);
        throw failure;
      }
      if (secret) await tokens.save(profile.id, secret);
      await reloadProfiles();
    });
  };

  const switchTo = (profile: Profile) =>
    guard(async () => {
      await activate(profile.id, await tokens.load(profile.id));
    });

  const openStream = (streamName: string) =>
    guard(async () => {
      if (streamRef.current !== streamName) {
        setComposeTo('');
        setSelectingRoster(false);
        setSelectedRoster([]);
        setShowAllIdleRoster(false);
      }
      resetMessagesCursor();
      setStream(streamName);
      streamRef.current = streamName;
      const [listed, existing, joinedList] = await Promise.all([
        core.roster(streamName),
        core.messages(streamName, { limit: MESSAGE_PAGE_SIZE }),
        core.joined(),
      ]);
      if (streamRef.current !== streamName) return;
      const map: Record<string, boolean> = {};
      const labels: Record<string, string> = {};
      for (const entry of joinedList.subscriptions) {
        map[entry.stream] = true;
        labels[entry.stream] = entry.label;
      }
      setJoinedMap(map);
      setJoinedLabels(labels);
      setRoster(listed.subscriptions);
      // A stream the phone has not joined has no local history yet: pull
      // the bus's retained history once so tapping it is never empty.
      if (existing.total_retained === 0 && !map[streamName]) {
        const synced = await core.syncHistory(streamName, { limit: MESSAGE_PAGE_SIZE });
        if (streamRef.current !== streamName) return;
        applyMessagesPage(synced);
      } else {
        applyMessagesPage(existing);
      }
    });

  const syncHistory = () =>
    guard(async () => {
      if (!stream) return;
      resetMessagesCursor();
      const synced = await core.syncHistory(stream, { limit: MESSAGE_PAGE_SIZE });
      if (streamRef.current !== stream) return;
      applyMessagesPage(synced);
      await loadStream(stream, null);
    });

  const join = () =>
    guard(async () => {
      if (!stream) return;
      await core.join(stream, joinLabel.trim());
      await refreshJoined();
      await loadStream(stream);
    });

  const send = () =>
    guard(async () => {
      if (!stream) return;
      const text = composeText.trim();
      if (!text) {
        setError('invalid_command: enter a message before sending');
        return;
      }
      const recipient = cleanTarget(composeTo) || null;
      await core.publish(stream, { text }, recipient);
      setComposeText('');
      resetMessagesCursor();
      await loadStream(stream, null);
    });

  const nudge = (kind: NudgeKind) =>
    guard(async () => {
      if (!stream) return;
      await core.nudge(stream, kind, kind === 'custom' ? composeText : null, cleanTarget(composeTo) || null);
      if (kind === 'custom') setComposeText('');
      resetMessagesCursor();
      await loadStream(stream, null);
    });

  const refreshStreams = () =>
    guard(async () => {
      const page = await core.streams({ limit: STREAM_PAGE_SIZE });
      applyStreamResetPage(page.streams, page.next_after);
    });

  const nextStreamsPage = () => {
    if (!streamNextAfter) return;
    const nextCursor = streamNextAfter;
    return guard(async () => {
      const page = await core.streams({ after: nextCursor, limit: STREAM_PAGE_SIZE });
      setStreamCursorStack((prev) => [...prev, streamAfter]);
      setStreamAfter(nextCursor);
      setStreams(page.streams);
      setStreamNextAfter(page.next_after);
    });
  };

  const prevStreamsPage = () => {
    if (streamAfter === null) return;
    const prevStack = streamCursorStack.slice(0, -1);
    const prevCursor = streamCursorStack.length > 0 ? streamCursorStack[streamCursorStack.length - 1] : null;
    return guard(async () => {
      const page = await core.streams({ after: prevCursor, limit: STREAM_PAGE_SIZE });
      setStreamCursorStack(prevStack);
      setStreamAfter(prevCursor);
      setStreams(page.streams);
      setStreamNextAfter(page.next_after);
    });
  };

  const firstStreamsPage = () => {
    if (streamAfter === null) return;
    return guard(async () => {
      const page = await core.streams({ limit: STREAM_PAGE_SIZE });
      applyStreamResetPage(page.streams, page.next_after);
    });
  };

  const olderMessagesPage = () => {
    if (!stream || msgNextBefore === null) return;
    const targetStream = stream;
    const nextCursor = msgNextBefore;
    return guard(async () => {
      const page = await core.messages(targetStream, { before: nextCursor, limit: MESSAGE_PAGE_SIZE });
      if (streamRef.current !== targetStream) return;
      setMsgCursorStack((prev) => [...prev, msgBefore]);
      setMsgBefore(nextCursor);
      msgBeforeRef.current = nextCursor;
      applyMessagesPage(page);
    });
  };

  const newerMessagesPage = () => {
    if (!stream || (msgBefore === null && !msgHasNewer)) return;
    const targetStream = stream;
    const prevStack = msgCursorStack.slice(0, -1);
    const prevCursor = msgCursorStack.length > 0 ? msgCursorStack[msgCursorStack.length - 1] : null;
    return guard(async () => {
      const page = await core.messages(targetStream, { before: prevCursor, limit: MESSAGE_PAGE_SIZE });
      if (streamRef.current !== targetStream) return;
      setMsgCursorStack(prevStack);
      setMsgBefore(prevCursor);
      msgBeforeRef.current = prevCursor;
      applyMessagesPage(page);
    });
  };

  const latestMessagesPage = () => {
    if (!stream || msgBefore === null) return;
    const targetStream = stream;
    return guard(async () => {
      const page = await core.messages(targetStream, { limit: MESSAGE_PAGE_SIZE });
      if (streamRef.current !== targetStream) return;
      resetMessagesCursor();
      applyMessagesPage(page);
    });
  };

  const sweepStreams = () =>
    guard(async () => {
      const res = await core.sweepStreams();
      applyStreamResetPage(res.streams, res.next_after);
      setSelectedStreams([]);
      setSelectingStreams(false);
      if (stream && res.deleted.includes(stream)) {
        resetStream();
      }
    });

  const deleteSelectedStreams = () =>
    guard(async () => {
      if (selectedStreams.length === 0) return;
      const res = await core.deleteStreams(selectedStreams);
      applyStreamResetPage(res.streams, res.next_after);
      await refreshJoined();
      setSelectedStreams([]);
      setSelectingStreams(false);
      if (stream && res.deleted.includes(stream)) {
        resetStream();
      }
    });

  const sweepRoster = () =>
    guard(async () => {
      if (!stream) return;
      const res = await core.sweepSubscriptions(stream);
      setRoster(res.subscriptions);
      setSelectedRoster([]);
      setSelectingRoster(false);
      if (targetRecipient && !res.subscriptions.some((entry) => entry.label === targetRecipient)) {
        setComposeTo('');
      }
    });

  const deleteSelectedRoster = () =>
    guard(async () => {
      if (!stream || selectedRoster.length === 0) return;
      const res = await core.deleteSubscriptions(stream, selectedRoster);
      setRoster(res.subscriptions);
      await refreshJoined();
      setSelectedRoster([]);
      setSelectingRoster(false);
      if (targetRecipient && !res.subscriptions.some((entry) => entry.label === targetRecipient)) {
        setComposeTo('');
      }
    });

  const active = profiles.find((profile) => profile.id === activeId);
  const accent = busAccent(active);
  const watching = watchStatus.startsWith('waiting');
  const myLabel = stream ? joinedLabels[stream] : undefined;
  const targetRecipient = cleanTarget(composeTo);
  const sortedRoster = [...roster].sort((a, b) => {
    if (a.waiting !== b.waiting) return a.waiting ? -1 : 1;
    return (b.last_active_at ?? '').localeCompare(a.last_active_at ?? '');
  });
  const waitingRoster = sortedRoster.filter((entry) => entry.waiting);
  const idleRoster = sortedRoster.filter((entry) => !entry.waiting);
  const visibleIdleRoster = showAllIdleRoster
    ? idleRoster
    : idleRoster.filter(
        (entry, index) =>
          index < COLLAPSED_IDLE_LIMIT ||
          entry.label === targetRecipient ||
          entry.label === myLabel ||
          selectedRoster.includes(entry.label),
      );
  const hiddenIdleCount = idleRoster.length - visibleIdleRoster.length;
  const visibleRoster = [...waitingRoster, ...visibleIdleRoster];
  const newestFirst = [...messages].sort((a, b) => b.sequence - a.sequence);

  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle="light-content" />
      <ScrollView testID="main_scroll" contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled" keyboardDismissMode="on-drag">
        <View style={[styles.rail, { borderColor: `${accent}8C` }]}>
          <View style={styles.railRow}>
            <Text style={[styles.brand, { color: accent }]}>MINY MOBILE</Text>
            <View style={[styles.activePill, { borderColor: accent }]}>
              <Text testID="active_bus" style={[styles.activePillText, { color: accent }]}>
                {active ? active.name : 'no bus'}
              </Text>
            </View>
            <View style={[styles.badge, { backgroundColor: `${watching ? SIGNAL_GO : INK_MUTED}24`, borderColor: `${watching ? SIGNAL_GO : INK_MUTED}8C` }]}>
              <Text style={[styles.badgeText, { color: watching ? SIGNAL_GO : INK_MUTED }]}>{watching ? 'PARKED' : 'IDLE'}</Text>
            </View>
          </View>
          <View style={styles.railRow}>
            <Pressable
              testID="refresh_streams"
              onPress={refreshStreams}
              android_ripple={{ color: accent }}
              style={({ pressed }) => [styles.actionBtn, { backgroundColor: `${accent}38`, borderColor: accent }, pressed && styles.actionBtnPressed]}
            >
              <Text style={styles.actionText}>{busy ? 'REFRESHING...' : 'REFRESH'}</Text>
            </Pressable>
            {active && !showAdd ? (
              <Pressable
                testID="show_add_bus"
                onPress={() => setShowAdd(true)}
                android_ripple={{ color: accent }}
                style={({ pressed }) => [styles.actionBtn, { backgroundColor: `${accent}38`, borderColor: accent }, pressed && styles.actionBtnPressed]}
              >
                <Text style={styles.actionText}>+ BUS</Text>
              </Pressable>
            ) : null}
            {busy ? <ActivityIndicator color={accent} size="small" /> : null}
            {busy ? <Text style={styles.busyText}>{busy}</Text> : null}
          </View>
          <View style={styles.railRow}>
            {profiles.map((profile) => {
              const isActive = profile.id === activeId;
              const pAccent = busAccent(profile);
              return (
                <Pressable
                  key={profile.id}
                  testID={`profile_${profile.name}`}
                  onPress={() => switchTo(profile)}
                  android_ripple={{ color: pAccent }}
                  style={({ pressed }) => [
                    styles.toggle,
                    isActive
                      ? { backgroundColor: pressed ? `${pAccent}61` : `${pAccent}3D`, borderColor: pAccent, borderWidth: 1.5 }
                      : { backgroundColor: pressed ? SURFACE_RAISED : CHASSIS_DEEP, borderColor: BEZEL_LINE, borderWidth: 1 },
                  ]}
                >
                  <Text style={[styles.mono, { color: isActive ? pAccent : INK_MUTED }]}>{profile.name}</Text>
                </Pressable>
              );
            })}
          </View>
          {!active || showAdd ? (
            <View>
              <TextInput testID="profile_name" placeholder="Name" placeholderTextColor={INK_MUTED} textContentType="none" autoComplete="off" value={name} onChangeText={setName} style={styles.input} />
              <TextInput testID="profile_url" placeholder="https://host/mcp" placeholderTextColor={INK_MUTED} autoCapitalize="none" textContentType="none" autoComplete="off" value={url} onChangeText={setUrl} style={styles.input} />
              <TextInput
                testID="profile_token"
                placeholder="Bearer token"
                placeholderTextColor={INK_MUTED}
                autoCapitalize="none"
                autoCorrect={false}
                spellCheck={false}
                textContentType="none"
                autoComplete="off"
                importantForAutofill="no"
                secureTextEntry={Platform.OS === 'android'}
                value={token}
                onChangeText={setToken}
                style={styles.input}
              />
              <Pressable testID="add_profile" onPress={addProfile} android_ripple={{ color: accent }} style={({ pressed }) => [styles.actionBtn, { backgroundColor: `${accent}38`, borderColor: accent }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.actionText}>Connect</Text>
              </Pressable>
              {showAdd && active ? (
                <Pressable testID="cancel_add_bus" onPress={() => setShowAdd(false)} android_ripple={{ color: INK_MUTED }} style={({ pressed }) => [styles.actionBtn, styles.actionPlain, pressed && styles.actionBtnPressed]}>
                  <Text style={styles.actionText}>Cancel</Text>
                </Pressable>
              ) : null}
            </View>
          ) : null}
        </View>

        {error ? (
          <View style={styles.urgent}>
            <Text style={styles.errorCaption}>BUS ERROR:</Text>
            <Text testID="error_text" style={styles.errorText}>
              {error}
            </Text>
          </View>
        ) : null}

        {active && stream ? (
          <View style={styles.deckPanel}>
            <View style={styles.deckHead}>
              <Text style={styles.panelTitle}>DISPATCH DECK</Text>
              <View style={[styles.badge, styles.deckTargetPill, { backgroundColor: 'rgba(167,139,250,0.14)', borderColor: 'rgba(167,139,250,0.55)' }]}>
                <Text testID="target_pill" style={[styles.badgeText, { color: KNOB_CURSOR, fontSize: 12 }]}>
                  {`TARGET: ${targetRecipient ? '@' + targetRecipient : '#' + stream + ' (BROADCAST)'}`}
                </Text>
              </View>
            </View>
            <TextInput testID="compose_text" placeholder="Message" placeholderTextColor={INK_MUTED} returnKeyType="send" onSubmitEditing={send} value={composeText} onChangeText={setComposeText} style={styles.compactInput} />
            <TextInput testID="compose_to" placeholder="To (optional label)" placeholderTextColor={INK_MUTED} autoCapitalize="none" value={composeTo} onChangeText={setComposeTo} style={styles.compactInput} />
            <View style={styles.railRow}>
              <Pressable testID="nudge_yes_go" onPress={() => nudge('yes_go')} android_ripple={{ color: SIGNAL_GO }} style={({ pressed }) => [styles.compactBtn, { backgroundColor: pressed ? `${SIGNAL_GO}73` : `${SIGNAL_GO}38`, borderColor: SIGNAL_GO, borderWidth: pressed ? 2 : 1.5 }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.compactBtnText}>YES / GO</Text>
              </Pressable>
              <Pressable testID="nudge_no_stop" onPress={() => nudge('no_stop')} android_ripple={{ color: SIGNAL_STOP }} style={({ pressed }) => [styles.compactBtn, { backgroundColor: pressed ? `${SIGNAL_STOP}73` : `${SIGNAL_STOP}38`, borderColor: SIGNAL_STOP, borderWidth: pressed ? 2 : 1.5 }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.compactBtnText}>NO / STOP</Text>
              </Pressable>
              <Pressable testID="nudge_catch_up" onPress={() => nudge('catch_up')} android_ripple={{ color: KNOB_CURSOR }} style={({ pressed }) => [styles.compactBtn, { backgroundColor: pressed ? `${KNOB_CURSOR}73` : `${KNOB_CURSOR}38`, borderColor: KNOB_CURSOR, borderWidth: pressed ? 2 : 1.5 }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.compactBtnText}>CATCH UP</Text>
              </Pressable>
              <Pressable testID="nudge_custom" onPress={() => nudge('custom')} android_ripple={{ color: '#38BDF8' }} style={({ pressed }) => [styles.compactBtn, { backgroundColor: pressed ? '#38BDF873' : '#38BDF838', borderColor: '#38BDF8', borderWidth: pressed ? 2 : 1.5 }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.compactBtnText}>NUDGE</Text>
              </Pressable>
              <Pressable testID="send_button" onPress={send} android_ripple={{ color: accent }} style={({ pressed }) => [styles.compactBtn, { backgroundColor: pressed ? `${accent}73` : `${accent}38`, borderColor: accent, borderWidth: pressed ? 2 : 1.5 }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.compactBtnText}>SEND</Text>
              </Pressable>
            </View>
          </View>
        ) : null}

        {active && stream ? (
          <View style={styles.panel}>
            <View style={styles.panelHead}>
              <Text style={styles.panelTitle}>NUDGE TARGETS & ROSTER</Text>
              <View style={styles.panelActions}>
                <Pressable testID="sweep_roster" onPress={sweepRoster} android_ripple={{ color: '#F59E0B' }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: '#F59E0B26', borderColor: '#F59E0B' }, pressed && styles.actionBtnPressed]}>
                  <Text style={[styles.miniBtnText, { color: '#F59E0B' }]}>SWEEP IDLE</Text>
                </Pressable>
                <Pressable
                  testID="toggle_select_roster"
                  onPress={() => {
                    setSelectingRoster((prev) => {
                      if (prev) setSelectedRoster([]);
                      return !prev;
                    });
                  }}
                  android_ripple={{ color: accent }}
                  style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${accent}24`, borderColor: accent }, pressed && styles.actionBtnPressed]}
                >
                  <Text style={[styles.miniBtnText, { color: accent }]}>{selectingRoster ? 'DONE' : 'SELECT'}</Text>
                </Pressable>
                {selectingRoster && selectedRoster.length > 0 ? (
                  <Pressable testID="delete_selected_roster" onPress={deleteSelectedRoster} android_ripple={{ color: SIGNAL_STOP }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${SIGNAL_STOP}38`, borderColor: SIGNAL_STOP }, pressed && styles.actionBtnPressed]}>
                    <Text style={[styles.miniBtnText, { color: SIGNAL_STOP }]}>{`DELETE (${selectedRoster.length})`}</Text>
                  </Pressable>
                ) : null}
              </View>
            </View>
            <View style={styles.strip}>
              <Text testID="open_stream" style={styles.mono}>{stream}</Text>
              <Text testID="watch_status" style={styles.monoMuted}>{watchStatus}</Text>
              <Text testID="pause_status" style={styles.monoMuted}>{pauseStatus}</Text>
              <Text testID="ack_status" style={styles.monoMuted}>{ackStatus}</Text>
            </View>
            <View style={styles.railRow}>
              <TextInput testID="join_label" placeholder="Label" placeholderTextColor={INK_MUTED} autoCapitalize="none" value={joinLabel} onChangeText={setJoinLabel} style={styles.compactInput} />
              <Pressable testID="join_button" onPress={join} android_ripple={{ color: SIGNAL_GO }} style={({ pressed }) => [styles.compactBtn, { backgroundColor: `${SIGNAL_GO}38`, borderColor: SIGNAL_GO }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.compactBtnText}>JOIN</Text>
              </Pressable>
              {joinedMap[stream] ? (
                <View style={[styles.badge, { backgroundColor: `${SIGNAL_GO}24`, borderColor: `${SIGNAL_GO}8C` }]}>
                  <Text style={[styles.badgeText, { color: SIGNAL_GO }]}>{`PARKED AS ${joinedLabels[stream] ?? ''}`}</Text>
                </View>
              ) : null}
            </View>
            <Pressable
              testID="compose_broadcast"
              onPress={() => setComposeTo('')}
              android_ripple={{ color: KNOB_CURSOR }}
              style={({ pressed }) => [
                styles.selectableRow,
                targetRecipient === ''
                  ? { backgroundColor: pressed ? 'rgba(167,139,250,0.30)' : 'rgba(167,139,250,0.18)', borderColor: KNOB_CURSOR, borderWidth: 1.5 }
                  : { backgroundColor: pressed ? '#242E40' : SURFACE_RAISED, borderColor: BEZEL_LINE, borderWidth: 1 },
              ]}
            >
              <Text style={styles.mono}>{`BROADCAST STREAM  #${stream}`}</Text>
            </Pressable>
            {visibleRoster.map((entry) => {
              const isChecked = selectedRoster.includes(entry.label);
              const selected = selectingRoster ? isChecked : targetRecipient === entry.label;
              const meta = entry.metadata ?? {};
              const metaLine = ['harness', 'host', 'model']
                .map((key) => meta[key])
                .filter((value): value is string => typeof value === 'string' && value.length > 0)
                .join(' \u00b7 ');
              return (
                <Pressable
                  key={entry.label}
                  testID={selectingRoster ? `select_roster_${entry.label}` : `target_${entry.label}`}
                  onPress={() => {
                    if (selectingRoster) {
                      setSelectedRoster((prev) => toggleItem(prev, entry.label));
                    } else {
                      setComposeTo(selected ? '' : entry.label);
                    }
                  }}
                  android_ripple={{ color: KNOB_CURSOR }}
                  style={({ pressed }) => [
                    styles.rosterCard,
                    selected
                      ? { backgroundColor: pressed ? 'rgba(167,139,250,0.30)' : 'rgba(167,139,250,0.18)', borderColor: selectingRoster ? SIGNAL_STOP : KNOB_CURSOR, borderWidth: 1.5 }
                      : { backgroundColor: pressed ? '#242E40' : SURFACE_RAISED, borderColor: BEZEL_LINE, borderWidth: 1 },
                  ]}
                >
                  <View style={styles.rosterTop}>
                    <Text testID={`roster_${entry.label}`} style={styles.mono}>
                      {selectingRoster
                        ? `${isChecked ? '[x]' : '[ ]'} ${entry.label} ${entry.waiting ? 'waiting' : 'idle'}`
                        : `${entry.label} ${entry.waiting ? 'waiting' : 'idle'}`}
                    </Text>
                    <View style={[styles.badge, { backgroundColor: `${entry.waiting ? SIGNAL_GO : INK_MUTED}24`, borderColor: `${entry.waiting ? SIGNAL_GO : INK_MUTED}8C` }]}>
                      <Text style={[styles.badgeText, { color: entry.waiting ? SIGNAL_GO : INK_MUTED }]}>{entry.waiting ? 'WAITING' : 'IDLE'}</Text>
                    </View>
                    <Text style={styles.monoMuted}>{entry.last_active_at ? stamp(entry.last_active_at) : ''}</Text>
                  </View>
                  {metaLine ? <Text style={styles.metaLine}>{metaLine}</Text> : null}
                </Pressable>
              );
            })}
            {hiddenIdleCount > 0 || (showAllIdleRoster && idleRoster.length > COLLAPSED_IDLE_LIMIT) ? (
              <Pressable
                testID="toggle_all_idle_roster"
                onPress={() => setShowAllIdleRoster((prev) => !prev)}
                android_ripple={{ color: accent }}
                style={({ pressed }) => [styles.actionBtn, styles.actionPlain, pressed && styles.actionBtnPressed]}
              >
                <Text style={styles.actionText}>
                  {showAllIdleRoster ? 'HIDE EXTRA IDLE' : `SHOW +${hiddenIdleCount} IDLE`}
                </Text>
              </Pressable>
            ) : null}
          </View>
        ) : null}

        {active && stream ? (
          <View style={styles.panel}>
            <View style={styles.panelHead}>
              <Text style={[styles.panelTitle, { color: accent }]}>{`HISTORY #${stream}`}</Text>
              <Text testID="messages_page_info" style={styles.monoMuted}>
                {msgTotalRetained === 0 ? '0 retained' : `Page ${msgCursorStack.length + 1} (${newestFirst.length}/${msgTotalRetained})`}
              </Text>
              <Pressable testID="sync_history" onPress={syncHistory} android_ripple={{ color: accent }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${accent}38`, borderColor: accent }, pressed && styles.actionBtnPressed]}>
                <Text style={styles.miniBtnText}>{busy ? 'SYNCING...' : 'REPLAY HISTORY'}</Text>
              </Pressable>
            </View>
            {msgBefore !== null || msgHasNewer || msgNextBefore !== null ? (
              <View style={styles.pagerBar}>
                {msgCursorStack.length > 1 ? (
                  <Pressable testID="messages_latest" onPress={latestMessagesPage} android_ripple={{ color: accent }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${accent}24`, borderColor: accent, marginLeft: 0 }, pressed && styles.actionBtnPressed]}>
                    <Text style={[styles.miniBtnText, { color: accent }]}>LATEST</Text>
                  </Pressable>
                ) : null}
                <Pressable
                  testID="messages_newer"
                  disabled={msgBefore === null && !msgHasNewer}
                  onPress={newerMessagesPage}
                  android_ripple={{ color: accent }}
                  style={({ pressed }) => [
                    styles.miniBtn,
                    { marginLeft: 0 },
                    msgBefore === null && !msgHasNewer ? styles.miniBtnDisabled : { backgroundColor: `${accent}24`, borderColor: accent },
                    pressed && styles.actionBtnPressed,
                  ]}
                >
                  <Text style={[styles.miniBtnText, msgBefore === null && !msgHasNewer ? styles.pagerTextDisabled : { color: accent }]}>{'< NEWER'}</Text>
                </Pressable>
                <Text testID="messages_cursor_range" style={styles.monoMuted}>
                  {newestFirst.length > 0
                    ? `#${String(newestFirst[0].sequence).padStart(4, '0')}..#${String(newestFirst[newestFirst.length - 1].sequence).padStart(4, '0')}`
                    : ''}
                </Text>
                <Pressable
                  testID="messages_older"
                  disabled={msgNextBefore === null}
                  onPress={olderMessagesPage}
                  android_ripple={{ color: accent }}
                  style={({ pressed }) => [
                    styles.miniBtn,
                    { marginLeft: 0 },
                    msgNextBefore === null ? styles.miniBtnDisabled : { backgroundColor: `${accent}24`, borderColor: accent },
                    pressed && styles.actionBtnPressed,
                  ]}
                >
                  <Text style={[styles.miniBtnText, msgNextBefore === null ? styles.pagerTextDisabled : { color: accent }]}>{'OLDER >'}</Text>
                </Pressable>
              </View>
            ) : null}
            {newestFirst.length === 0 ? <Text style={styles.empty}>No retained messages.</Text> : null}
            {newestFirst.map((message) => {
              const payload = (typeof message.payload === 'object' && message.payload !== null
                ? message.payload
                : {}) as Record<string, unknown>;
              const from =
                (typeof payload.from === 'string' && payload.from) ||
                message.publisher ||
                (message.direction === 'out' ? myLabel ?? 'phone' : 'bus');
              const to = typeof payload.to === 'string' && payload.to ? payload.to : null;
              const isFromMe = myLabel ? from === myLabel : message.direction === 'out';
              const isDirectToMe = Boolean(myLabel && to === myLabel);
              const cardBg = isDirectToMe ? '#2E141F' : isFromMe ? '#142923' : SURFACE_RAISED;
              const cardBorder = isDirectToMe ? SIGNAL_STOP : isFromMe ? SIGNAL_GO : BEZEL_LINE;
              const extra = Object.fromEntries(
                Object.entries(payload).filter(([key]) => !['text', 'body', 'summary', 'from', 'to', 'kind'].includes(key)),
              );
              return (
                <View key={message.message_id} style={[styles.messageCard, { backgroundColor: cardBg, borderColor: cardBorder }]}>
                  <View style={styles.messageTop}>
                    <Text style={[styles.mono, { color: accent }]}>{`#${String(message.sequence).padStart(4, '0')}`}</Text>
                    <Text testID={`message_from_${message.sequence}`} style={styles.mono}>
                      {from}
                    </Text>
                    <Text testID={`message_route_${message.sequence}`} style={styles.monoMuted}>{to ? `${from} -> @${to}` : '-> BROADCAST'}</Text>
                    <Text style={styles.monoMuted}>{stamp(message.published_at)}</Text>
                  </View>
                  <Text selectable testID={`message_${message.sequence}`} style={styles.messageText}>
                    {messageText(message.payload)}
                  </Text>
                  {Object.keys(extra).length > 0 ? (
                    <Text style={styles.preview}>{JSON.stringify(extra, null, 2).slice(0, 180)}</Text>
                  ) : null}
                </View>
              );
            })}
          </View>
        ) : null}

        {active ? (
          <View style={styles.panel}>
            <View style={styles.panelHead}>
              <View style={styles.panelActions}>
                <Text style={styles.panelTitle}>STREAMS</Text>
                <Text style={[styles.panelCount, { color: accent }]}>
                  {streamAfter !== null || streamNextAfter !== null ? `${streams.length} (P${streamCursorStack.length + 1})` : streams.length}
                </Text>
              </View>
              <View style={styles.panelActions}>
                <Pressable testID="sweep_streams" onPress={sweepStreams} android_ripple={{ color: '#F59E0B' }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: '#F59E0B26', borderColor: '#F59E0B' }, pressed && styles.actionBtnPressed]}>
                  <Text style={[styles.miniBtnText, { color: '#F59E0B' }]}>SWEEP IDLE</Text>
                </Pressable>
                <Pressable
                  testID="toggle_select_streams"
                  onPress={() => {
                    setSelectingStreams((prev) => {
                      if (prev) setSelectedStreams([]);
                      return !prev;
                    });
                  }}
                  android_ripple={{ color: accent }}
                  style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${accent}24`, borderColor: accent }, pressed && styles.actionBtnPressed]}
                >
                  <Text style={[styles.miniBtnText, { color: accent }]}>{selectingStreams ? 'DONE' : 'SELECT'}</Text>
                </Pressable>
                {selectingStreams && selectedStreams.length > 0 ? (
                  <Pressable testID="delete_selected_streams" onPress={deleteSelectedStreams} android_ripple={{ color: SIGNAL_STOP }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${SIGNAL_STOP}38`, borderColor: SIGNAL_STOP }, pressed && styles.actionBtnPressed]}>
                    <Text style={[styles.miniBtnText, { color: SIGNAL_STOP }]}>{`DELETE (${selectedStreams.length})`}</Text>
                  </Pressable>
                ) : null}
              </View>
            </View>
            {streamAfter !== null || streamNextAfter !== null ? (
              <View style={styles.pagerBar}>
                {streamCursorStack.length > 1 ? (
                  <Pressable testID="streams_first" onPress={firstStreamsPage} android_ripple={{ color: accent }} style={({ pressed }) => [styles.miniBtn, { backgroundColor: `${accent}24`, borderColor: accent, marginLeft: 0 }, pressed && styles.actionBtnPressed]}>
                    <Text style={[styles.miniBtnText, { color: accent }]}>FIRST</Text>
                  </Pressable>
                ) : null}
                <Pressable
                  testID="streams_prev"
                  disabled={streamAfter === null}
                  onPress={prevStreamsPage}
                  android_ripple={{ color: accent }}
                  style={({ pressed }) => [
                    styles.miniBtn,
                    { marginLeft: 0 },
                    streamAfter === null ? styles.miniBtnDisabled : { backgroundColor: `${accent}24`, borderColor: accent },
                    pressed && styles.actionBtnPressed,
                  ]}
                >
                  <Text style={[styles.miniBtnText, streamAfter === null ? styles.pagerTextDisabled : { color: accent }]}>{'< PREV'}</Text>
                </Pressable>
                <Text testID="streams_page_info" style={styles.monoMuted}>
                  {`PAGE ${streamCursorStack.length + 1}${streamNextAfter ? '+' : ''} (${streams.length})`}
                </Text>
                <Pressable
                  testID="streams_next"
                  disabled={streamNextAfter === null}
                  onPress={nextStreamsPage}
                  android_ripple={{ color: accent }}
                  style={({ pressed }) => [
                    styles.miniBtn,
                    { marginLeft: 0 },
                    streamNextAfter === null ? styles.miniBtnDisabled : { backgroundColor: `${accent}24`, borderColor: accent },
                    pressed && styles.actionBtnPressed,
                  ]}
                >
                  <Text style={[styles.miniBtnText, streamNextAfter === null ? styles.pagerTextDisabled : { color: accent }]}>{'NEXT >'}</Text>
                </Pressable>
              </View>
            ) : null}
            {streams.map((streamName) => {
              const isChecked = selectedStreams.includes(streamName);
              const selected = selectingStreams ? isChecked : stream === streamName;
              return (
                <Pressable
                  key={streamName}
                  testID={selectingStreams ? `select_stream_${streamName}` : `stream_${streamName}`}
                  onPress={() => {
                    if (selectingStreams) {
                      setSelectedStreams((prev) => toggleItem(prev, streamName));
                    } else {
                      openStream(streamName);
                    }
                  }}
                  android_ripple={{ color: KNOB_CURSOR }}
                  style={({ pressed }) => [
                    styles.selectableRow,
                    selected
                      ? { backgroundColor: pressed ? 'rgba(167,139,250,0.30)' : 'rgba(167,139,250,0.18)', borderColor: selectingStreams ? SIGNAL_STOP : KNOB_CURSOR, borderWidth: 1.5 }
                      : { backgroundColor: pressed ? '#242E40' : SURFACE_RAISED, borderColor: BEZEL_LINE, borderWidth: 1 },
                  ]}
                >
                  <Text style={styles.mono}>
                    {selectingStreams ? `${isChecked ? '[x]' : '[ ]'} #${streamName}` : `#${streamName}`}
                  </Text>
                  {joinedMap[streamName] ? (
                    <View style={[styles.badge, { backgroundColor: `${SIGNAL_GO}24`, borderColor: `${SIGNAL_GO}8C` }]}>
                      <Text style={[styles.badgeText, { color: SIGNAL_GO }]}>LIVE</Text>
                    </View>
                  ) : null}
                </Pressable>
              );
            })}
          </View>
        ) : null}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: CHASSIS_DEEP },
  content: { padding: 10, paddingBottom: 400 },
  rail: { backgroundColor: PANEL_SLATE, borderWidth: 1, borderRadius: 6, padding: 8 },
  railRow: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', marginVertical: 2 },
  brand: { fontFamily: MONO, fontSize: 13, fontWeight: '700', marginRight: 10 },
  activePill: { borderWidth: 1, borderRadius: 4, paddingVertical: 2, paddingHorizontal: 8, marginRight: 8 },
  activePillText: { fontFamily: MONO, fontSize: 13, fontWeight: '700' },
  badge: { borderWidth: 1, borderRadius: 4, paddingVertical: 2, paddingHorizontal: 7, marginRight: 6 },
  badgeText: { fontFamily: MONO, fontSize: 11 },
  toggle: { borderRadius: 5, paddingVertical: 6, paddingHorizontal: 10, margin: 2 },
  selectableRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', borderRadius: 5, paddingVertical: 8, paddingHorizontal: 10, marginVertical: 2 },
  rosterCard: { borderRadius: 5, paddingVertical: 6, paddingHorizontal: 10, marginVertical: 2 },
  rosterTop: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
  metaLine: { fontFamily: MONO, fontSize: 10, color: INK_MUTED, marginTop: 2 },
  actionBtn: { borderRadius: 6, paddingVertical: 8, paddingHorizontal: 12, margin: 3 },
  compactBtn: { borderRadius: 6, paddingVertical: 6, paddingHorizontal: 10, margin: 2 },
  compactBtnText: { fontFamily: MONO, fontWeight: '700', color: INK_PRIMARY, fontSize: 12 },
  miniBtn: { borderRadius: 4, borderWidth: 1, paddingVertical: 4, paddingHorizontal: 8, marginLeft: 6 },
  miniBtnDisabled: { backgroundColor: SURFACE_RAISED, borderColor: BEZEL_LINE, opacity: 0.45 },
  miniBtnText: { fontFamily: MONO, fontWeight: '700', fontSize: 11, color: INK_PRIMARY },
  pagerTextDisabled: { color: INK_MUTED },
  pagerBar: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6, gap: 6 },
  actionPlain: { backgroundColor: SURFACE_RAISED, borderColor: BEZEL_LINE, borderWidth: 1 },
  actionBtnPressed: { opacity: 0.85 },
  actionText: { fontFamily: MONO, fontWeight: '700', color: INK_PRIMARY, fontSize: 13 },
  busyText: { fontFamily: MONO, color: INK_MUTED, fontSize: 12, marginLeft: 4 },
  urgent: { backgroundColor: '#520F17', borderWidth: 1.5, borderColor: SIGNAL_STOP, borderRadius: 6, padding: 10, marginTop: 8 },
  errorText: { color: INK_PRIMARY, fontFamily: MONO, fontSize: 13 },
  errorCaption: { color: SIGNAL_STOP, fontFamily: MONO, fontWeight: '700', fontSize: 11, marginTop: 4 },
  panel: { backgroundColor: PANEL_SLATE, borderWidth: 1, borderColor: BEZEL_LINE, borderRadius: 6, padding: 8, marginTop: 8 },
  deckPanel: { backgroundColor: PANEL_SLATE, borderWidth: 1, borderColor: BEZEL_LINE, borderRadius: 6, padding: 8, marginTop: 8 },
  deckHead: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 },
  deckTargetPill: { marginRight: 0 },
  panelHead: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', marginBottom: 5, gap: 6 },
  panelActions: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 4 },
  panelTitle: { fontFamily: MONO, fontSize: 12, fontWeight: '700', color: INK_MUTED, letterSpacing: 1 },
  panelCount: { fontFamily: MONO, fontSize: 12, fontWeight: '700', marginLeft: 6 },
  strip: { backgroundColor: CHASSIS_DEEP, borderWidth: 1, borderColor: BEZEL_LINE, borderRadius: 5, padding: 6, marginBottom: 6 },
  mono: { fontFamily: MONO, color: INK_PRIMARY, fontSize: 13 },
  monoMuted: { fontFamily: MONO, color: INK_MUTED, fontSize: 11 },
  input: { borderWidth: 1, borderColor: BEZEL_LINE, backgroundColor: CHASSIS_DEEP, color: INK_PRIMARY, fontFamily: MONO, borderRadius: 5, padding: 9, marginVertical: 3, minWidth: 150 },
  compactInput: { borderWidth: 1, borderColor: BEZEL_LINE, backgroundColor: CHASSIS_DEEP, color: INK_PRIMARY, fontFamily: MONO, borderRadius: 5, paddingVertical: 6, paddingHorizontal: 9, marginVertical: 2, minWidth: 140 },
  messageCard: { borderWidth: 1, borderRadius: 5, padding: 7, marginVertical: 2 },
  messageTop: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 8, marginBottom: 3 },
  messageText: { color: INK_PRIMARY, fontSize: 14 },
  preview: { fontFamily: MONO, fontSize: 11, color: INK_MUTED, marginTop: 4 },
  empty: { color: INK_MUTED, fontStyle: 'italic', fontFamily: MONO, fontSize: 12, marginVertical: 4 },
});
