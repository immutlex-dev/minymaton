//! Bus profiles, the active bus and its receive waits.
//!
//! Each joined subscription has at most one waiting `receive` in flight.
//! A received batch is persisted, together with the acknowledgement it now
//! owes, before the next `receive` carries that acknowledgement: nothing is
//! acknowledged that the device has not stored. The stored acknowledgement is
//! sent on its own before the next wait parks. Pausing cancels the waits and
//! returns once each has ended and closed its connection; resuming starts them
//! again: an owed acknowledgement goes first, then the wait returns whatever
//! arrived since.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use minymaton_core::api::{
    EventEnvelope, ReceiveResult, StreamResult, SubscriptionEntry, SubscriptionResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::CoreError;
use crate::mcp_client::Endpoint;
use crate::storage::{Joined, MAX_MESSAGE_PAGE_LIMIT, Profile, Storage, StoredMessage};

/// Receives each event as JSON, on a core thread.
pub type Listener = Box<dyn Fn(String) + Send + Sync>;

const MAX_EVENTS: u64 = 16;
const MAX_BYTES: u64 = 16_384;
const PAGE_LIMIT: u64 = 100;
const DEFAULT_STREAM_PAGE_LIMIT: u64 = 20;
const DEFAULT_MESSAGE_PAGE_LIMIT: u32 = 20;
const NUDGE_SOURCE: &str = "minymaton-mobile";

#[derive(Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Profiles,
    AddProfile {
        name: String,
        url: String,
        ca_pem: Option<String>,
    },
    RemoveProfile {
        profile_id: String,
    },
    Activate {
        profile_id: String,
        token: Option<String>,
    },
    Deactivate,
    Status,
    Streams {
        after: Option<String>,
        limit: Option<u64>,
    },
    CreateStream {
        name: String,
    },
    DeleteStreams {
        streams: Vec<String>,
    },
    SweepStreams {
        keep: Option<Vec<String>>,
    },
    Roster {
        stream: String,
    },
    DeleteSubscriptions {
        stream: String,
        labels: Vec<String>,
    },
    SweepSubscriptions {
        stream: String,
        keep: Option<Vec<String>>,
    },
    Join {
        stream: String,
        label: String,
        start_at: Option<String>,
        metadata: Option<BTreeMap<String, Value>>,
    },
    Leave {
        stream: String,
    },
    Joined,
    Publish {
        stream: String,
        payload: Value,
        to: Option<String>,
    },
    Nudge {
        stream: String,
        kind: String,
        text: Option<String>,
        to: Option<String>,
    },
    Messages {
        stream: String,
        before: Option<u64>,
        limit: Option<u32>,
    },
    SyncHistory {
        stream: String,
        before: Option<u64>,
        limit: Option<u32>,
    },
    Pause,
    Resume,
}

struct Watcher {
    id: u64,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

struct Active {
    profile_id: String,
    endpoint: Arc<Endpoint>,
    watchers: HashMap<String, Watcher>,
}

struct Shared {
    storage: Storage,
    listener: Listener,
    active: Mutex<Option<Active>>,
    paused: Mutex<bool>,
    next_watcher: AtomicU64,
}

impl Shared {
    fn emit(&self, event: &Value) {
        (self.listener)(event.to_string());
    }

    fn lock_active(&self) -> std::sync::MutexGuard<'_, Option<Active>> {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_paused(&self) -> bool {
        *self
            .paused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The mobile core: one instance per app process.
pub struct MobileCore {
    runtime: Runtime,
    shared: Arc<Shared>,
}

impl MobileCore {
    /// Opens the device database in `data_dir` and starts paused, with no
    /// active bus: the app activates the stored profile with its token.
    pub fn open(data_dir: &Path, listener: Listener) -> Result<Self, CoreError> {
        std::fs::create_dir_all(data_dir).map_err(|error| {
            CoreError::new("storage_failed", format!("cannot create data dir: {error}"))
        })?;
        let storage = Storage::open(&data_dir.join("minymaton-mobile.sqlite3"))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| {
                CoreError::new("runtime_failed", format!("cannot start runtime: {error}"))
            })?;
        Ok(Self {
            runtime,
            shared: Arc::new(Shared {
                storage,
                listener,
                active: Mutex::new(None),
                paused: Mutex::new(false),
                next_watcher: AtomicU64::new(1),
            }),
        })
    }

    /// Runs one JSON command to completion. Blocking: call it from a worker
    /// thread, never the UI thread.
    pub fn request(&self, command: &str) -> String {
        let outcome = match serde_json::from_str::<Command>(command) {
            Ok(command) => self.runtime.block_on(self.run(command)),
            Err(error) => Err(CoreError::new(
                "invalid_command",
                format!("invalid command: {error}"),
            )),
        };
        match outcome {
            Ok(result) => json!({"ok": true, "result": result}).to_string(),
            Err(error) => json!({"ok": false, "error": error}).to_string(),
        }
    }

    /// Cancels every receive wait. The core can be dropped afterwards.
    pub fn close(&self) {
        if let Some(active) = self.shared.lock_active().take() {
            for watcher in active.watchers.values() {
                watcher.cancel.cancel();
            }
        }
    }

    async fn run(&self, command: Command) -> Result<Value, CoreError> {
        let storage = &self.shared.storage;
        match command {
            Command::Profiles => {
                let active = storage.active_profile()?;
                Ok(json!({"profiles": storage.profiles()?, "active_profile": active}))
            }
            Command::AddProfile { name, url, ca_pem } => {
                Endpoint::new(&url, None, ca_pem.as_deref())?;
                if name.trim().is_empty() {
                    return Err(CoreError::new(
                        "invalid_profile",
                        "the profile needs a name",
                    ));
                }
                let profile = Profile {
                    id: random_hex(8)?,
                    name: name.trim().to_owned(),
                    url,
                    has_ca: ca_pem.is_some(),
                    ca_pem,
                };
                storage.add_profile(&profile)?;
                Ok(json!({"profile": profile}))
            }
            Command::RemoveProfile { profile_id } => {
                let mut active = self.shared.lock_active();
                if active
                    .as_ref()
                    .is_some_and(|active| active.profile_id == profile_id)
                {
                    stop_watchers(active.take());
                }
                drop(active);
                Ok(json!({"removed": storage.remove_profile(&profile_id)?}))
            }
            Command::Activate { profile_id, token } => self.activate(&profile_id, token).await,
            Command::Deactivate => {
                stop_watchers(self.shared.lock_active().take());
                storage.set_active_profile(None)?;
                Ok(json!({}))
            }
            Command::Status => {
                let active = self.shared.lock_active();
                let (profile_id, watching) = match active.as_ref() {
                    Some(active) => {
                        let mut watching: Vec<&String> = active.watchers.keys().collect();
                        watching.sort();
                        (Some(active.profile_id.clone()), json!(watching))
                    }
                    None => (None, json!([])),
                };
                Ok(json!({
                    "active_profile": profile_id,
                    "paused": self.shared.is_paused(),
                    "watching": watching,
                }))
            }
            Command::Streams { after, limit } => {
                let (_, endpoint) = self.active()?;
                let (streams, next_after) =
                    list_streams_page(&endpoint, after.as_deref(), clamp_stream_limit(limit))
                        .await?;
                Ok(json!({"streams": streams, "next_after": next_after}))
            }
            Command::CreateStream { name } => {
                let (_, endpoint) = self.active()?;
                let result: StreamResult = decode(
                    endpoint
                        .call("stream", json!({"action": "create", "name": name}))
                        .await?,
                )?;
                Ok(serde_json::to_value(result).unwrap_or_default())
            }
            Command::DeleteStreams { streams } => self.delete_streams(streams).await,
            Command::SweepStreams { keep } => self.sweep_streams(keep).await,
            Command::Roster { stream } => {
                let (_, endpoint) = self.active()?;
                let subscriptions = list_roster(&endpoint, &stream).await?;
                Ok(json!({"stream": stream, "subscriptions": subscriptions}))
            }
            Command::DeleteSubscriptions { stream, labels } => {
                self.delete_subscriptions(stream, labels).await
            }
            Command::SweepSubscriptions { stream, keep } => {
                self.sweep_subscriptions(stream, keep).await
            }
            Command::Join {
                stream,
                label,
                start_at,
                metadata,
            } => self.join(stream, label, start_at, metadata).await,
            Command::Leave { stream } => {
                let (profile_id, endpoint) = self.active()?;
                let joined = storage
                    .subscriptions(&profile_id)?
                    .into_iter()
                    .find(|joined| joined.stream == stream)
                    .ok_or_else(|| not_joined(&stream))?;
                if let Some(active) = self.shared.lock_active().as_mut()
                    && let Some(watcher) = active.watchers.remove(&stream)
                {
                    watcher.cancel.cancel();
                }
                endpoint
                    .call(
                        "subscription",
                        json!({"action": "delete", "stream": stream, "label": joined.label}),
                    )
                    .await?;
                storage.remove_subscription(&profile_id, &stream)?;
                Ok(json!({"stream": stream, "label": joined.label}))
            }
            Command::Joined => {
                let (profile_id, _) = self.active()?;
                Ok(json!({"subscriptions": storage.subscriptions(&profile_id)?}))
            }
            Command::Publish {
                stream,
                payload,
                to,
            } => self.publish(stream, payload, to).await,
            Command::Nudge {
                stream,
                kind,
                text,
                to,
            } => {
                let text = nudge_text(&kind, text.as_deref())?;
                let payload = json!({"nudge": kind, "text": text, "source": NUDGE_SOURCE});
                self.publish(stream, payload, to).await
            }
            Command::Messages {
                stream,
                before,
                limit,
            } => {
                let (profile_id, _) = self.active()?;
                messages_response(storage, &profile_id, &stream, before, limit)
            }
            Command::SyncHistory {
                stream,
                before,
                limit,
            } => self.sync_history(stream, before, limit).await,
            Command::Pause => {
                *self
                    .shared
                    .paused
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
                let (profile_id, watchers) = match self.shared.lock_active().as_mut() {
                    Some(active) => (
                        Some(active.profile_id.clone()),
                        active.watchers.drain().collect::<Vec<_>>(),
                    ),
                    None => (None, Vec::new()),
                };
                // Pausing returns only once every wait has ended and dropped
                // its request, so the device holds no wait while paused.
                let mut stopped = Vec::with_capacity(watchers.len());
                for (stream, watcher) in watchers {
                    watcher.cancel.cancel();
                    watcher.task.await.map_err(|error| {
                        CoreError::new(
                            "watch_failed",
                            format!("the wait on {stream} failed: {error}"),
                        )
                    })?;
                    stopped.push(stream);
                }
                stopped.sort();
                self.shared.emit(&json!({
                    "event": "paused",
                    "profile_id": profile_id,
                    "stopped": stopped,
                }));
                Ok(json!({"paused": true, "stopped": stopped}))
            }
            Command::Resume => {
                *self
                    .shared
                    .paused
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
                let started = self.start_watchers()?;
                Ok(json!({"paused": false, "started": started}))
            }
        }
    }

    fn active(&self) -> Result<(String, Arc<Endpoint>), CoreError> {
        self.shared
            .lock_active()
            .as_ref()
            .map(|active| (active.profile_id.clone(), Arc::clone(&active.endpoint)))
            .ok_or_else(|| CoreError::new("no_active_bus", "no bus is active"))
    }

    async fn activate(&self, profile_id: &str, token: Option<String>) -> Result<Value, CoreError> {
        let storage = &self.shared.storage;
        let profile = storage.profile(profile_id)?.ok_or_else(|| {
            CoreError::new("profile_not_found", format!("no profile {profile_id}"))
        })?;
        let endpoint = Arc::new(Endpoint::new(
            &profile.url,
            token.as_deref(),
            profile.ca_pem.as_deref(),
        )?);
        // Proves the URL, the trust store and the token before switching.
        let (streams, next_after) =
            list_streams_page(&endpoint, None, DEFAULT_STREAM_PAGE_LIMIT).await?;
        stop_watchers(self.shared.lock_active().replace(Active {
            profile_id: profile.id.clone(),
            endpoint,
            watchers: HashMap::new(),
        }));
        storage.set_active_profile(Some(&profile.id))?;
        let started = self.start_watchers()?;
        Ok(json!({
            "profile": profile,
            "streams": streams,
            "next_after": next_after,
            "started": started,
        }))
    }

    async fn join(
        &self,
        stream: String,
        label: String,
        start_at: Option<String>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let mut create = json!({
            "action": "create",
            "stream": stream,
            "label": label,
            "start_at": start_at.unwrap_or_else(|| "earliest".to_owned()),
        });
        let mut recover = json!({"action": "recover", "stream": stream, "label": label});
        if let Some(metadata) = &metadata {
            create["metadata"] = json!(metadata);
            recover["metadata"] = json!(metadata);
        }
        let result = match endpoint.call("subscription", create).await {
            Err(error) if error.code == "subscription_exists" => {
                endpoint.call("subscription", recover).await?
            }
            other => other?,
        };
        let (status, handle) = match decode(result)? {
            SubscriptionResult::Created { handle, .. } => ("created", handle),
            SubscriptionResult::Recovered { handle, .. } => ("recovered", handle),
            _ => {
                return Err(CoreError::protocol(
                    "subscription create returned no handle",
                ));
            }
        };
        let joined = Joined {
            stream,
            label,
            handle,
        };
        self.shared
            .storage
            .save_subscription(&profile_id, &joined)?;
        self.start_watchers()?;
        Ok(json!({"status": status, "subscription": joined}))
    }

    async fn delete_streams(&self, streams: Vec<String>) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let (status, deleted, deleted_count) = if streams.is_empty() {
            ("deleted_bulk", Vec::new(), 0)
        } else if streams.len() == 1 {
            let result = endpoint
                .call("stream", json!({"action": "delete", "name": streams[0]}))
                .await?;
            let StreamResult::Deleted { stream } = decode(result)? else {
                return Err(CoreError::protocol(
                    "stream delete returned unexpected status",
                ));
            };
            ("deleted", vec![stream], 1)
        } else {
            let result = endpoint
                .call("stream", json!({"action": "delete_bulk", "names": streams}))
                .await?;
            let StreamResult::DeletedBulk {
                deleted,
                deleted_count,
            } = decode(result)?
            else {
                return Err(CoreError::protocol(
                    "stream delete_bulk returned unexpected status",
                ));
            };
            ("deleted_bulk", deleted, deleted_count)
        };
        if let Some(active) = self.shared.lock_active().as_mut()
            && active.profile_id == profile_id
        {
            for stream in &deleted {
                if let Some(watcher) = active.watchers.remove(stream) {
                    watcher.cancel.cancel();
                }
            }
        }
        self.shared.storage.remove_streams(&profile_id, &deleted)?;
        let (updated, next_after) =
            list_streams_page(&endpoint, None, DEFAULT_STREAM_PAGE_LIMIT).await?;
        Ok(json!({
            "status": status,
            "deleted": deleted,
            "deleted_count": deleted_count,
            "streams": updated,
            "next_after": next_after,
        }))
    }

    async fn sweep_streams(&self, keep: Option<Vec<String>>) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let mut merged_keep = keep.unwrap_or_default();
        for joined in self.shared.storage.subscriptions(&profile_id)? {
            if !merged_keep.contains(&joined.stream) {
                merged_keep.push(joined.stream);
            }
        }
        let result = endpoint
            .call(
                "stream",
                json!({"action": "sweep_idle", "keep": merged_keep}),
            )
            .await?;
        let StreamResult::Swept {
            deleted,
            deleted_count,
        } = decode(result)?
        else {
            return Err(CoreError::protocol(
                "stream sweep_idle returned unexpected status",
            ));
        };
        if let Some(active) = self.shared.lock_active().as_mut()
            && active.profile_id == profile_id
        {
            for stream in &deleted {
                if let Some(watcher) = active.watchers.remove(stream) {
                    watcher.cancel.cancel();
                }
            }
        }
        self.shared.storage.remove_streams(&profile_id, &deleted)?;
        let (updated, next_after) =
            list_streams_page(&endpoint, None, DEFAULT_STREAM_PAGE_LIMIT).await?;
        Ok(json!({
            "status": "swept",
            "deleted": deleted,
            "deleted_count": deleted_count,
            "streams": updated,
            "next_after": next_after,
        }))
    }

    async fn delete_subscriptions(
        &self,
        stream: String,
        labels: Vec<String>,
    ) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let (status, res_stream, deleted, deleted_count) = if labels.is_empty() {
            ("deleted_bulk", stream, Vec::new(), 0)
        } else if labels.len() == 1 {
            let result = endpoint
                .call(
                    "subscription",
                    json!({"action": "delete", "stream": stream, "label": labels[0]}),
                )
                .await?;
            let SubscriptionResult::Deleted { stream, label } = decode(result)? else {
                return Err(CoreError::protocol(
                    "subscription delete returned unexpected status",
                ));
            };
            ("deleted", stream, vec![label], 1)
        } else {
            let result = endpoint
                .call(
                    "subscription",
                    json!({"action": "delete_bulk", "stream": stream, "labels": labels}),
                )
                .await?;
            let SubscriptionResult::DeletedBulk {
                stream,
                deleted,
                deleted_count,
            } = decode(result)?
            else {
                return Err(CoreError::protocol(
                    "subscription delete_bulk returned unexpected status",
                ));
            };
            ("deleted_bulk", stream, deleted, deleted_count)
        };
        let removed_local =
            self.shared
                .storage
                .remove_subscriptions(&profile_id, &res_stream, &deleted)?;
        if !removed_local.is_empty()
            && let Some(active) = self.shared.lock_active().as_mut()
            && active.profile_id == profile_id
            && let Some(watcher) = active.watchers.remove(&res_stream)
        {
            watcher.cancel.cancel();
        }
        let subscriptions = list_roster(&endpoint, &res_stream).await?;
        Ok(json!({
            "status": status,
            "stream": res_stream,
            "deleted": deleted,
            "deleted_count": deleted_count,
            "subscriptions": subscriptions,
        }))
    }

    async fn sweep_subscriptions(
        &self,
        stream: String,
        keep: Option<Vec<String>>,
    ) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let mut merged_keep = keep.unwrap_or_default();
        if let Some(joined) = self
            .shared
            .storage
            .subscriptions(&profile_id)?
            .into_iter()
            .find(|entry| entry.stream == stream)
            && !merged_keep.contains(&joined.label)
        {
            merged_keep.push(joined.label);
        }
        let result = endpoint
            .call(
                "subscription",
                json!({"action": "sweep_idle", "stream": stream, "keep": merged_keep}),
            )
            .await?;
        let SubscriptionResult::Swept {
            stream: res_stream,
            deleted,
            deleted_count,
        } = decode(result)?
        else {
            return Err(CoreError::protocol(
                "subscription sweep_idle returned unexpected status",
            ));
        };
        let removed_local =
            self.shared
                .storage
                .remove_subscriptions(&profile_id, &res_stream, &deleted)?;
        if !removed_local.is_empty()
            && let Some(active) = self.shared.lock_active().as_mut()
            && active.profile_id == profile_id
            && let Some(watcher) = active.watchers.remove(&res_stream)
        {
            watcher.cancel.cancel();
        }
        let subscriptions = list_roster(&endpoint, &res_stream).await?;
        Ok(json!({
            "status": "swept",
            "stream": res_stream,
            "deleted": deleted,
            "deleted_count": deleted_count,
            "subscriptions": subscriptions,
        }))
    }

    /// Loads the stream's retained history into local storage through a
    /// temporary subscription started at the earliest retained message,
    /// then deletes that subscription so the roster is left as it was.
    async fn sync_history(
        &self,
        stream: String,
        before: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let created = endpoint
            .call(
                "subscription",
                json!({"action": "create", "stream": stream, "start_at": "earliest"}),
            )
            .await?;
        let (temp_label, handle) = match decode(created)? {
            SubscriptionResult::Created { label, handle, .. } => (label, handle),
            _ => {
                return Err(CoreError::protocol(
                    "history subscription create returned no handle",
                ));
            }
        };
        let loaded = self
            .drain_history(&endpoint, &profile_id, &stream, &handle)
            .await;
        // The temporary subscription goes away whatever the drain did, so a
        // failed sync can never leave a phantom subscriber behind.
        let deleted = endpoint
            .call(
                "subscription",
                json!({"action": "delete", "stream": stream, "label": temp_label}),
            )
            .await;
        loaded?;
        deleted?;
        messages_response(&self.shared.storage, &profile_id, &stream, before, limit)
    }

    async fn drain_history(
        &self,
        endpoint: &Endpoint,
        profile_id: &str,
        stream: &str,
        handle: &str,
    ) -> Result<(), CoreError> {
        let mut ack: Option<String> = None;
        loop {
            let mut arguments = json!({
                "handle": handle,
                "max_events": MAX_EVENTS,
                "max_bytes": MAX_BYTES,
                "wait": false,
            });
            if let Some(ack) = &ack {
                arguments["ack"] = json!(ack);
            }
            let result: ReceiveResult = decode(endpoint.call("receive", arguments).await?)?;
            match result {
                ReceiveResult::Events {
                    events,
                    ack_after_handling,
                    has_more,
                    ..
                } => {
                    let messages: Vec<StoredMessage> = events.into_iter().map(received).collect();
                    self.shared
                        .storage
                        .record_history(profile_id, stream, &messages)?;
                    ack = Some(ack_after_handling);
                    if !has_more {
                        break;
                    }
                }
                ReceiveResult::Empty { .. } => break,
                ReceiveResult::Acked { .. } => break,
                ReceiveResult::Superseded { .. } => {
                    return Err(CoreError::new(
                        "superseded",
                        "another receive took over the history subscription",
                    ));
                }
            }
        }
        if let Some(ack) = ack {
            endpoint
                .call(
                    "receive",
                    json!({"handle": handle, "max_events": 0, "ack": ack}),
                )
                .await?;
        }
        Ok(())
    }

    async fn publish(
        &self,
        stream: String,
        mut payload: Value,
        to: Option<String>,
    ) -> Result<Value, CoreError> {
        let (profile_id, endpoint) = self.active()?;
        let from = self
            .shared
            .storage
            .subscriptions(&profile_id)?
            .into_iter()
            .find(|joined| joined.stream == stream)
            .map(|joined| joined.label);
        let to = normalize_recipient(to, &payload);
        if let Value::Object(map) = &mut payload {
            if let Some(from) = &from {
                map.entry("from").or_insert_with(|| json!(from));
            }
            match &to {
                Some(recipient) => {
                    map.insert("to".to_owned(), json!(recipient));
                }
                None => {
                    map.remove("to");
                }
            }
        }
        let mut arguments = Map::new();
        arguments.insert("stream".to_owned(), json!(stream));
        arguments.insert("idempotency_key".to_owned(), json!(random_hex(16)?));
        arguments.insert("payload".to_owned(), payload.clone());
        if let Some(from) = &from {
            arguments.insert("from".to_owned(), json!(from));
        }
        if let Some(to) = &to {
            arguments.insert("to".to_owned(), json!(to));
        }
        let result = endpoint.call("publish", Value::Object(arguments)).await?;
        let (message_id, sequence, published_at) = (
            result["message_id"].as_str().map(str::to_owned),
            result["sequence"].as_u64(),
            result["published_at"].as_str().map(str::to_owned),
        );
        let (Some(message_id), Some(sequence), Some(published_at)) =
            (message_id, sequence, published_at)
        else {
            return Err(CoreError::protocol("publish returned no message"));
        };
        let message = StoredMessage {
            message_id,
            sequence,
            published_at,
            payload,
            publisher: from,
            direction: "out".to_owned(),
        };
        self.shared
            .storage
            .record_sent(&profile_id, &stream, &message)?;
        Ok(json!({"stream": stream, "message": message}))
    }

    /// Starts a receive wait for every joined subscription of the active bus
    /// that has none, unless the app is paused.
    fn start_watchers(&self) -> Result<Vec<String>, CoreError> {
        if self.shared.is_paused() {
            return Ok(Vec::new());
        }
        let mut guard = self.shared.lock_active();
        let Some(active) = guard.as_mut() else {
            return Ok(Vec::new());
        };
        let mut started = Vec::new();
        for joined in self.shared.storage.subscriptions(&active.profile_id)? {
            if active.watchers.contains_key(&joined.stream) {
                continue;
            }
            let id = self.shared.next_watcher.fetch_add(1, Ordering::Relaxed);
            let cancel = CancellationToken::new();
            let stream = joined.stream.clone();
            let task = self.runtime.spawn(watch(
                Arc::clone(&self.shared),
                Arc::clone(&active.endpoint),
                active.profile_id.clone(),
                joined,
                id,
                cancel.clone(),
            ));
            started.push(stream.clone());
            active.watchers.insert(stream, Watcher { id, cancel, task });
        }
        Ok(started)
    }
}

impl Drop for MobileCore {
    fn drop(&mut self) {
        self.close();
    }
}

fn stop_watchers(active: Option<Active>) {
    if let Some(active) = active {
        for watcher in active.watchers.values() {
            watcher.cancel.cancel();
        }
    }
}

async fn watch(
    shared: Arc<Shared>,
    endpoint: Arc<Endpoint>,
    profile_id: String,
    joined: Joined,
    id: u64,
    cancel: CancellationToken,
) {
    let ended = watch_loop(&shared, &endpoint, &profile_id, &joined, &cancel).await;
    if let Some(error) = ended {
        if cancel.is_cancelled() {
            return;
        }
        {
            let mut guard = shared.lock_active();
            if let Some(active) = guard.as_mut()
                && active.profile_id == profile_id
                && active
                    .watchers
                    .get(&joined.stream)
                    .is_some_and(|watcher| watcher.id == id)
            {
                active.watchers.remove(&joined.stream);
            }
        }
        shared.emit(&json!({
            "event": "watch_ended",
            "profile_id": profile_id,
            "stream": joined.stream,
            "code": error.code,
            "message": error.message,
        }));
    }
}

/// Returns `None` when cancelled, or why the wait ended.
async fn watch_loop(
    shared: &Shared,
    endpoint: &Endpoint,
    profile_id: &str,
    joined: &Joined,
    cancel: &CancellationToken,
) -> Option<CoreError> {
    loop {
        let ack = match shared.storage.pending_ack(profile_id, &joined.stream) {
            Ok(ack) => ack,
            Err(error) => return Some(error),
        };
        // A stored batch is acknowledged on its own first, so the device
        // learns at once that the bus committed it, before parking.
        if let Some(ack) = ack {
            let arguments = json!({"handle": joined.handle, "max_events": 0, "ack": ack});
            let outcome = tokio::select! {
                biased;
                () = cancel.cancelled() => return None,
                outcome = endpoint.call("receive", arguments) => outcome,
            };
            let ack_result = match outcome.and_then(decode::<ReceiveResult>) {
                Ok(ReceiveResult::Acked { ack_result }) => Some(ack_result),
                Ok(_) => return Some(CoreError::protocol("an ack-only receive returned events")),
                // The bus already holds this acknowledgement or redelivers the
                // batch, whose messages are stored: the owed ack is spent.
                Err(error) if error.code == "batch_no_longer_pending" => None,
                Err(error) => return Some(error),
            };
            if let Err(error) = shared.storage.clear_pending_ack(profile_id, &joined.stream) {
                return Some(error);
            }
            if let Some(ack_result) = ack_result {
                shared.emit(&json!({
                    "event": "acked",
                    "profile_id": profile_id,
                    "stream": joined.stream,
                    "batch_id": ack_result.batch_id,
                    "outcome": ack_result.outcome,
                }));
            }
            continue;
        }
        let arguments = json!({
            "handle": joined.handle,
            "max_events": MAX_EVENTS,
            "max_bytes": MAX_BYTES,
            "wait": true,
        });
        // The bus reports progress once it holds the wait, so `waiting`
        // means the roster shows this label as waiting.
        let waiting = || {
            shared.emit(&json!({
                "event": "waiting",
                "profile_id": profile_id,
                "stream": joined.stream,
                "label": joined.label,
            }));
        };
        let outcome = tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            outcome = endpoint.call_with_progress("receive", arguments, &waiting) => outcome,
        };
        let result: ReceiveResult = match outcome.and_then(decode) {
            Ok(result) => result,
            Err(error) => return Some(error),
        };
        match result {
            ReceiveResult::Events {
                events,
                ack_after_handling,
                ..
            } => {
                let messages: Vec<StoredMessage> = events.into_iter().map(received).collect();
                // Persist before acknowledging: on failure the wait stops
                // and the bus keeps the batch for redelivery.
                let fresh = match shared.storage.record_batch(
                    profile_id,
                    &joined.stream,
                    &messages,
                    &ack_after_handling,
                ) {
                    Ok(fresh) => fresh,
                    Err(error) => return Some(error),
                };
                // A redelivered batch adds nothing the device has not shown.
                if fresh.is_empty() {
                    continue;
                }
                shared.emit(&json!({
                    "event": "messages",
                    "profile_id": profile_id,
                    "stream": joined.stream,
                    "messages": fresh,
                }));
            }
            ReceiveResult::Superseded { .. } => {
                return Some(CoreError::new(
                    "superseded",
                    "another receive took over this subscription",
                ));
            }
            ReceiveResult::Empty { .. } | ReceiveResult::Acked { .. } => {}
        }
    }
}

fn received(event: EventEnvelope) -> StoredMessage {
    StoredMessage {
        message_id: event.message_id,
        sequence: event.sequence,
        published_at: event.published_at,
        payload: event.payload,
        publisher: event.publisher,
        direction: "in".to_owned(),
    }
}

async fn list_roster(
    endpoint: &Endpoint,
    stream: &str,
) -> Result<Vec<SubscriptionEntry>, CoreError> {
    let mut subscriptions = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut arguments = json!({"action": "list", "stream": stream, "limit": PAGE_LIMIT});
        if let Some(after) = &after {
            arguments["after"] = json!(after);
        }
        let result: SubscriptionResult = decode(endpoint.call("subscription", arguments).await?)?;
        let SubscriptionResult::Listed {
            subscriptions: page,
            next_after,
            ..
        } = result
        else {
            return Err(CoreError::protocol("subscription list returned no list"));
        };
        subscriptions.extend(page);
        match next_after {
            Some(next) => after = Some(next),
            None => return Ok(subscriptions),
        }
    }
}

fn clamp_stream_limit(limit: Option<u64>) -> u64 {
    limit
        .unwrap_or(DEFAULT_STREAM_PAGE_LIMIT)
        .clamp(1, PAGE_LIMIT)
}

fn clamp_message_limit(limit: Option<u32>) -> u32 {
    limit
        .unwrap_or(DEFAULT_MESSAGE_PAGE_LIMIT)
        .clamp(1, MAX_MESSAGE_PAGE_LIMIT)
}

fn messages_response(
    storage: &Storage,
    profile_id: &str,
    stream: &str,
    before: Option<u64>,
    limit: Option<u32>,
) -> Result<Value, CoreError> {
    let page = storage.messages_page(profile_id, stream, before, clamp_message_limit(limit))?;
    Ok(json!({
        "stream": stream,
        "messages": page.messages,
        "next_before": page.next_before,
        "has_newer": page.has_newer,
        "total_retained": page.total_retained,
    }))
}

async fn list_streams_page(
    endpoint: &Endpoint,
    after: Option<&str>,
    limit: u64,
) -> Result<(Vec<String>, Option<String>), CoreError> {
    let mut arguments = json!({"action": "list", "limit": limit});
    if let Some(after) = after {
        arguments["after"] = json!(after);
    }
    let StreamResult::Listed {
        streams,
        next_after,
    } = decode(endpoint.call("stream", arguments).await?)?
    else {
        return Err(CoreError::protocol("stream list returned no list"));
    };
    Ok((streams, next_after))
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, CoreError> {
    serde_json::from_value(value).map_err(|error| {
        CoreError::new("protocol_error", format!("unexpected tool result: {error}"))
    })
}

fn clean_recipient(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_start_matches('@').trim();
    if trimmed.is_empty()
        || trimmed == "*"
        || trimmed.eq_ignore_ascii_case("all")
        || trimmed.eq_ignore_ascii_case("broadcast")
    {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn normalize_recipient(to: Option<String>, payload: &Value) -> Option<String> {
    to.as_deref().and_then(clean_recipient).or_else(|| {
        payload
            .get("to")
            .and_then(Value::as_str)
            .and_then(clean_recipient)
    })
}

fn not_joined(stream: &str) -> CoreError {
    CoreError::new("not_joined", format!("not joined to stream {stream}"))
}

/// The quick replies shared with minymaton-desk (app.rs, model.rs).
fn nudge_text(kind: &str, text: Option<&str>) -> Result<String, CoreError> {
    Ok(match kind {
        "yes_go" => "yes / go".to_owned(),
        "no_stop" => "no / stop".to_owned(),
        "catch_up" => "check stream: receive(wait:false), handle + ack every pending batch until empty, catch up to head, and re-park waiter".to_owned(),
        "custom" => {
            let text = text.map(str::trim).unwrap_or_default();
            if text.is_empty() {
                return Err(CoreError::new("invalid_command", "a custom nudge needs text"));
            }
            text.to_owned()
        }
        other => {
            return Err(CoreError::new(
                "invalid_command",
                format!("unknown nudge kind {other}"),
            ));
        }
    })
}

fn random_hex(bytes: usize) -> Result<String, CoreError> {
    let mut buffer = vec![0u8; bytes];
    getrandom::fill(&mut buffer)
        .map_err(|error| CoreError::new("random_failed", format!("no randomness: {error}")))?;
    Ok(buffer.iter().map(|byte| format!("{byte:02x}")).collect())
}
