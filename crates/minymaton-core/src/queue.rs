//! The queue's operations, including the receive wait.
//!
//! Durable state sits behind [`QueueStore`] (writes) and [`QueueReader`]
//! (read-only discovery). Backend calls run on the blocking pool, so a
//! backend may block. A receive subscribes to its stream's commit signal and
//! its subscription's ownership signal before it first looks at storage, so a
//! commit or a newer receive that lands after that look always wakes it.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use rmcp::model::{ReadResourceResult, RequestId};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::api::{
    AckToolResult, FollowArgs, FollowResult, PublishArgs, ReceiveArgs, ReceiveResult, StreamArgs,
    SubscriptionArgs, WaitEnd, WaitReport,
};
use crate::error::{ErrorCode, ToolError};
use crate::ids::inbox_handle;
use crate::policy::{self, Access, Limits, Membership};
use crate::signals::Signals;
use crate::store::{
    Admission, Bounds, DeletedStream, Publication, QueueReader, QueueStore, Reader, Selection,
    Store, inbox_not_found,
};
use crate::wire::{RESULT_LIMIT, widest_single_event_bytes};

/// Stream names, labels and idempotency keys are 1 to this many bytes.
const NAME_LIMIT: usize = 128;

/// Subscription identity metadata is at most this many serialized JSON bytes.
const METADATA_LIMIT: usize = 1024;

type SharedStore = Arc<Mutex<Box<dyn QueueStore>>>;
type SharedReader = Arc<Mutex<Box<dyn QueueReader>>>;
type SharedRefresher = Arc<Mutex<Option<AccessRefresher>>>;

#[derive(Clone)]
pub struct Queue {
    store: SharedStore,
    reader: SharedReader,
    signals: Arc<Signals>,
    access_refresh: SharedRefresher,
}

/// The call a receive answers.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// The JSON-RPC ID the result will be sent under.
    pub request_id: RequestId,
    /// Whether the exchange uses the 2026-07-28 revision.
    pub modern: bool,
}

pub enum Admitted {
    /// The result is ready.
    Done(Value),
    /// Nothing to deliver yet. The receive is subscribed to commits; call
    /// [`Waiter::wait`] to wait for events or a newer receive.
    Waiting(Box<Waiter>),
}

/// A subscription's inbox resource (decision 17).
#[derive(Clone, Debug)]
pub struct Inbox {
    pub uri: String,
    subscription_id: i64,
    stream_id: i64,
    access: Access,
}

impl Inbox {
    /// Clears the enrollment digest on this inbox snapshot when token and
    /// enrollment revocation are already enforced by the session/request layer.
    pub fn without_enrollment_digest(mut self) -> Self {
        self.access.enrollment_digest = None;
        self
    }
}

impl Queue {
    /// Observes active wait guards in this process, including their removal on
    /// cancellation. This is not a remote-client liveness guarantee.
    pub fn active_waits(&self) -> watch::Receiver<usize> {
        self.signals.active_waits()
    }

    /// The SQLite backend: one writer and one read-only WAL connection on
    /// the database at `path`.
    pub fn open(path: &Path) -> Result<Self, String> {
        Self::open_with_limits(path, Limits::default())
    }

    pub fn open_with_limits(path: &Path, limits: Limits) -> Result<Self, String> {
        let store = Store::open_with_limits(path, limits)?;
        let reader = Reader::open(path)?;
        Ok(Self::with_backend(Box::new(store), Box::new(reader)))
    }

    /// A queue over any backend. `reader` must observe every command `store`
    /// has committed by the time that command returns.
    pub fn with_backend(store: Box<dyn QueueStore>, reader: Box<dyn QueueReader>) -> Self {
        Self::with_signals(store, reader, Arc::new(Signals::default()))
    }

    fn with_signals(
        store: Box<dyn QueueStore>,
        reader: Box<dyn QueueReader>,
        signals: Arc<Signals>,
    ) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            reader: Arc::new(Mutex::new(reader)),
            signals,
            access_refresh: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_access_refresh(&self, refresher: AccessRefresher) {
        *self
            .access_refresh
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(refresher);
    }

    pub fn signal_tick(&self) -> u64 {
        self.signals.tick()
    }

    pub fn binding_changed_since(
        &self,
        stream_id: i64,
        subscription_id: i64,
        since_tick: u64,
    ) -> bool {
        self.signals
            .binding_changed_since(stream_id, subscription_id, since_tick)
    }

    pub fn commits_signal(&self, stream_id: i64) -> watch::Receiver<u64> {
        self.signals.commits(stream_id)
    }

    pub fn owners_signal(&self, subscription_id: i64) -> watch::Receiver<i64> {
        self.signals.owners(subscription_id)
    }

    pub fn acked_signal(&self, subscription_id: i64) -> watch::Receiver<u64> {
        self.signals.acked(subscription_id)
    }

    pub fn access_signal(&self, stream_id: i64) -> watch::Receiver<u64> {
        self.signals.access_changed(stream_id)
    }

    pub fn agent_bound_signal(&self, agent_key: &str) -> watch::Receiver<i64> {
        self.signals.agent_bound(agent_key)
    }

    pub fn enrollment_signal(&self) -> watch::Receiver<u64> {
        self.signals.enrollment_changed()
    }

    pub fn active_enrollment(&self) -> Option<[u8; 32]> {
        self.signals.active_enrollment()
    }

    pub fn provision_channel_blocking(&self, channel: &str) -> Result<(), ToolError> {
        check_name("channel", channel)?;
        self.store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .provision_channel(channel)
    }

    pub fn store_enrollment_blocking(
        &self,
        channels: &[String],
        digest: [u8; 32],
        expected: Option<[u8; 32]>,
    ) -> Result<bool, ToolError> {
        for channel in channels {
            check_name("channel", channel)?;
        }
        self.store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .activate_enrollment(channels, digest, expected)
    }

    /// Commits a SQLite enrollment transition and runs `on_committed` while
    /// holding BOTH the writer store lock and the reader lock so neither
    /// `with_store_access` nor `with_reader_access` can observe a mismatch
    /// between the stored enrollment digest and the in-memory authenticator
    /// (`Opus RR3-01`, `Opus RR4-01`, `Sol RR-12`).
    pub fn store_enrollment_with_hook_blocking<F>(
        &self,
        channels: &[String],
        digest: [u8; 32],
        expected_fn: impl FnOnce(Option<[u8; 32]>) -> Option<[u8; 32]>,
        on_committed: F,
    ) -> Result<bool, ToolError>
    where
        F: FnOnce(),
    {
        for channel in channels {
            check_name("channel", channel)?;
        }
        let mut store = self.store.lock().unwrap_or_else(PoisonError::into_inner);
        let reader = self.reader.lock().unwrap_or_else(PoisonError::into_inner);
        let stored = reader.enrollment_digest()?;
        let expected = expected_fn(stored);
        let changed = store.activate_enrollment(channels, digest, expected)?;
        on_committed();
        drop(reader);
        drop(store);
        Ok(changed)
    }

    pub fn notify_enrollment(&self, digest: [u8; 32]) {
        self.signals.set_enrollment(digest);
    }

    pub fn activate_enrollment_blocking(
        &self,
        channels: &[String],
        digest: [u8; 32],
        expected: Option<[u8; 32]>,
    ) -> Result<bool, ToolError> {
        let changed = self.store_enrollment_blocking(channels, digest, expected)?;
        self.notify_enrollment(digest);
        Ok(changed)
    }

    pub fn enrollment_digest_blocking(&self) -> Result<Option<[u8; 32]>, ToolError> {
        let digest = self
            .reader
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .enrollment_digest()?;
        if let Some(digest) = digest {
            self.signals.set_enrollment(digest);
        }
        Ok(digest)
    }

    pub fn delete_stream_blocking(&self, name: &str) -> Result<DeletedStream, ToolError> {
        check_name("stream", name)?;
        let deleted = self
            .store
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .delete_stream(name)?;
        self.notify_deleted_stream(&deleted);
        Ok(deleted)
    }

    pub async fn delete_stream(&self, name: &str) -> Result<DeletedStream, ToolError> {
        check_name("stream", name)?;
        let name = name.to_owned();
        let deleted = with_store(&self.store, move |store| store.delete_stream(&name)).await?;
        self.notify_deleted_stream(&deleted);
        Ok(deleted)
    }

    fn notify_deleted_stream(&self, deleted: &DeletedStream) {
        for &sub_id in &deleted.subscription_ids {
            self.signals.subscription_deleted(sub_id);
        }
        for agent_key in &deleted.agent_keys {
            self.signals.set_agent_bound(agent_key, 0);
        }
        self.signals.stream_access_changed(deleted.stream_id);
    }

    /// Runs a `stream` call for `access`.
    pub async fn stream(&self, arguments: Value, access: &Access) -> Result<Value, ToolError> {
        let arguments = parse(arguments)?;
        let access = access.clone();
        let result = match arguments {
            StreamArgs::Create { name, members } => {
                check_name("name", &name)?;
                for member in members.iter().flatten() {
                    check_name("members", member)?;
                }
                with_store_access(&self.store, &self.access_refresh, &access, move |store, access| {
                    store.create_stream(&name, access, members.as_deref())
                })
                .await?
            }
            StreamArgs::Grant {
                name,
                caller: member,
            } => {
                self.change_member(name, access, member, Membership::Grant)
                    .await?
            }
            StreamArgs::Revoke {
                name,
                caller: member,
            } => {
                self.change_member(name, access, member, Membership::Revoke)
                    .await?
            }
            StreamArgs::List { after, limit } => {
                let limit = page_limit(after.as_deref(), limit)?;
                with_reader_access(&self.reader, &self.access_refresh, &access, move |reader, access| {
                    reader.list_streams(access, after.as_deref(), limit)
                })
                .await?
            }
        };
        Ok(crate::json(&result))
    }

    async fn change_member(
        &self,
        name: String,
        access: Access,
        member: String,
        change: Membership,
    ) -> Result<crate::api::StreamResult, ToolError> {
        check_name("name", &name)?;
        check_name("caller", &member)?;
        let (result, changed_stream) =
            with_store_access(&self.store, &self.access_refresh, &access, move |store, access| {
                store.change_member(&name, access, &member, change)
            })
            .await?;
        if let Some(stream_id) = changed_stream {
            self.signals.stream_access_changed(stream_id);
        }
        Ok(result)
    }

    pub async fn subscription(
        &self,
        arguments: Value,
        access: &Access,
    ) -> Result<Value, ToolError> {
        let access = access.clone();
        let result = match parse(arguments)? {
            SubscriptionArgs::Create {
                stream,
                label,
                start_at,
                metadata,
            } => {
                if let Some(s) = &stream {
                    check_name("stream", s)?;
                }
                let stream = policy::resolve_stream(stream.as_deref(), &access)?.to_owned();
                check_name("stream", &stream)?;
                if let Some(label) = &label {
                    check_name("label", label)?;
                }
                if let Some(metadata) = &metadata {
                    check_metadata(metadata)?;
                }
                let (created, _, _) = with_store_access(
                    &self.store,
                    &self.access_refresh,
                    &access,
                    move |store, access| {
                        store.claim_subscription(
                            &stream,
                            access,
                            label.as_deref(),
                            start_at,
                            metadata.clone(),
                        )
                    },
                )
                .await?;
                created
            }
            SubscriptionArgs::Recover {
                stream,
                label,
                metadata,
            } => {
                if let Some(s) = &stream {
                    check_name("stream", s)?;
                }
                let stream = policy::resolve_stream(stream.as_deref(), &access)?.to_owned();
                check_name("stream", &stream)?;
                check_name("label", &label)?;
                if let Some(metadata) = &metadata {
                    check_metadata(metadata)?;
                }
                let (recovered, _, _) = with_store_access(
                    &self.store,
                    &self.access_refresh,
                    &access,
                    move |store, access| {
                        store.recover_subscription(&stream, access, &label, metadata.clone())
                    },
                )
                .await?;
                recovered
            }
            SubscriptionArgs::Delete { stream, label } => {
                if let Some(s) = &stream {
                    check_name("stream", s)?;
                }
                let stream = policy::resolve_stream(stream.as_deref(), &access)?.to_owned();
                check_name("stream", &stream)?;
                check_name("label", &label)?;
                let (deleted, subscription_id, bound_agent) = with_store_access(
                    &self.store,
                    &self.access_refresh,
                    &access,
                    move |store, access| store.delete_subscription(&stream, access, &label),
                )
                .await?;
                self.signals.subscription_deleted(subscription_id);
                if let Some(agent_key) = bound_agent {
                    self.signals.set_agent_bound(&agent_key, 0);
                }
                deleted
            }
            SubscriptionArgs::List {
                stream,
                after,
                limit,
            } => {
                if let Some(s) = &stream {
                    check_name("stream", s)?;
                }
                let stream = policy::resolve_stream(stream.as_deref(), &access)?.to_owned();
                check_name("stream", &stream)?;
                let limit = page_limit(after.as_deref(), limit)?;
                let waiting = self.signals.waiting_set();
                with_reader_access(
                    &self.reader,
                    &self.access_refresh,
                    &access,
                    move |reader, access| {
                        reader.list_subscriptions(
                            &stream,
                            access,
                            after.as_deref(),
                            limit,
                            &waiting,
                        )
                    },
                )
                .await?
            }
        };
        Ok(crate::json(&result))
    }

    pub async fn publish(
        &self,
        arguments: Value,
        access: &Access,
    ) -> Result<Value, ToolError> {
        self.publish_committed(arguments, access)
            .await
            .map(|(value, _, _)| value)
    }

    /// The ordinary result, whether this call committed a new event, and any
    /// subscription handle whose pending batch was committed by `ack`.
    pub async fn publish_committed(
        &self,
        arguments: Value,
        access: &Access,
    ) -> Result<(Value, bool, Option<String>), ToolError> {
        let PublishArgs {
            stream,
            idempotency_key,
            payload,
            from,
            to,
            ack,
            claims,
        } = parse(arguments)?;
        check_name("stream", &stream)?;
        let stream = policy::resolve_stream(Some(&stream), access)?.to_owned();
        check_name("idempotency_key", &idempotency_key)?;
        if let Some(from) = &from {
            check_name("from", from)?;
        }
        if let Some(to) = &to {
            check_name("to", to)?;
        }
        if let Some(ack) = &ack {
            check_name("ack", ack)?;
        }
        if let Some(claims) = &claims {
            check_name("claims", claims)?;
        }
        let sender_label = from.or_else(|| inferred_sender(&payload));
        let recipient_label = to
            .filter(|label| !is_broadcast_recipient(label))
            .or_else(|| inferred_recipient(&payload));
        let caller = access.caller.as_deref();
        // Until claim checks exist, every message must fit in a result by itself.
        let needed = widest_single_event_bytes(&payload, caller);
        if needed > RESULT_LIMIT {
            return Err(ToolError::new(
                ErrorCode::MessageTooLarge,
                format!("a result carrying this message needs {needed} bytes, more than the {RESULT_LIMIT}-byte limit"),
            )
            .with_details(json!({"result_bytes": needed, "limit_bytes": RESULT_LIMIT})));
        }
        let mut digest_input = canonical(&payload);
        digest_input.push('\0');
        if let Some(sender) = &sender_label {
            digest_input.push_str(sender);
        }
        digest_input.push('\0');
        if let Some(recipient) = &recipient_label {
            digest_input.push_str(recipient);
        }
        digest_input.push('\0');
        if let Some(ack) = &ack {
            digest_input.push_str(ack);
        }
        digest_input.push('\0');
        if let Some(claims) = &claims {
            digest_input.push_str(claims);
        }
        digest_input.push('\0');
        if let Some(caller) = caller {
            digest_input.push_str(caller);
        }
        let digest = Sha256::digest(digest_input.as_bytes()).to_vec();
        let text = payload.to_string();
        let published =
            with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
                store.publish(Publication {
                    stream: &stream,
                    key: &idempotency_key,
                    payload: &text,
                    sender_label: sender_label.as_deref(),
                    recipient_label: recipient_label.as_deref(),
                    ack: ack.as_deref(),
                    access,
                    claims: claims.as_deref(),
                    digest: &digest,
                })
            })
            .await?;
        if let Some((sub_id, seq)) = published.acked_subscription {
            self.signals.subscription_acked(sub_id, seq);
        }
        if let Some((stream_id, sequence)) = published.commit {
            self.signals.committed(stream_id, sequence);
        }
        Ok((
            crate::json(&published.result),
            published.commit.is_some(),
            published.acked_handle,
        ))
    }

    pub async fn prepare_description(
        &self,
        stream: &str,
        label: &str,
        access: &Access,
    ) -> Result<(i64, i64, String), ToolError> {
        check_name("stream", stream)?;
        check_name("label", label)?;
        let stream = policy::resolve_stream(Some(stream), access)?.to_owned();
        let label = label.to_owned();
        with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
            store.ensure_subscription(&stream, &label, access)
        })
        .await
    }

    /// Binds the agent to its own subscription on the stream: the label is the
    /// agent's name. The stream (when unbound) and the subscription are created
    /// if missing; an unbound subscription already claimed by another client is
    /// not taken.
    pub async fn follow(
        &self,
        agent: &str,
        access: &Access,
        arguments: Value,
    ) -> Result<Value, ToolError> {
        let FollowArgs { stream } = parse(arguments)?;
        check_name("agent", agent)?;
        check_name("stream", &stream)?;
        let stream = policy::resolve_stream(Some(&stream), access)?.to_owned();
        let agent_key = policy::agent_key(access, agent);
        let signal_key = agent_key.clone();
        let agent = agent.to_owned();
        let (result, subscription_id) =
            with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
                let (_, subscription_id, handle) =
                    store.follow_stream(&agent_key, &agent, access, &stream)?;
                Ok((
                    FollowResult::Following {
                        label: agent.clone(),
                        agent: agent.clone(),
                        stream: stream.clone(),
                        handle,
                    },
                    subscription_id,
                ))
            })
            .await?;
        self.signals.set_agent_bound(&signal_key, subscription_id);
        Ok(crate::json(&result))
    }

    /// The `(stream_id, subscription_id, stream, label, handle)` the agent
    /// follows under `access`, if it follows one and `access` is still admitted
    /// to that stream.
    pub async fn agent_binding(
        &self,
        agent: &str,
        access: &Access,
    ) -> Result<Option<(i64, i64, String, String, String)>, ToolError> {
        let agent_key = policy::agent_key(access, agent);
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, access| {
            let Some(binding) = reader.agent_binding(&agent_key)? else {
                return Ok(None);
            };
            match reader.check_access(binding.0, access) {
                Ok(()) => Ok(Some(binding)),
                Err(error) if error.code == ErrorCode::StreamForbidden => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await
    }

    /// The raw `(stream_id, subscription_id, stream, label, handle)` the agent
    /// is recorded as following under `access`, before checking stream access.
    pub async fn agent_binding_raw(
        &self,
        agent: &str,
        access: &Access,
    ) -> Result<Option<(i64, i64, String, String, String)>, ToolError> {
        let agent_key = policy::agent_key(access, agent);
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, _access| {
            reader.agent_binding(&agent_key)
        })
        .await
    }

    /// The `stream_id` the agent is recorded as following under `access`, even
    /// if `access` is currently revoked from that stream.
    pub async fn agent_binding_stream_id(
        &self,
        agent: &str,
        access: &Access,
    ) -> Result<Option<i64>, ToolError> {
        let agent_key = policy::agent_key(access, agent);
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, _access| {
            Ok(reader.agent_binding(&agent_key)?.map(|b| b.0))
        })
        .await
    }

    /// Whether `access` is admitted to `stream_id`.
    pub async fn check_stream_access(
        &self,
        stream_id: i64,
        access: &Access,
    ) -> Result<bool, ToolError> {
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, access| {
            match reader.check_access(stream_id, access) {
                Ok(()) => Ok(true),
                Err(error) if error.code == ErrorCode::StreamForbidden => Ok(false),
                Err(error) => Err(error),
            }
        })
        .await
    }

    /// Checks that `access` is admitted to `stream_id` and returns whether
    /// `subscription_id` has a pending batch or visible unread events.
    pub async fn subscription_unread(
        &self,
        subscription_id: i64,
        stream_id: i64,
        access: &Access,
    ) -> Result<bool, ToolError> {
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, access| {
            reader.check_access(stream_id, access)?;
            reader.inbox_unread(subscription_id)
        })
        .await
    }

    /// Checks that `access` is admitted to `stream_id` and returns whether
    /// `subscription_id` has at least one visible event with
    /// `sequence > after_sequence`.
    pub async fn subscription_visible_after(
        &self,
        subscription_id: i64,
        stream_id: i64,
        after_sequence: u64,
        access: &Access,
    ) -> Result<bool, ToolError> {
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, access| {
            reader.check_access(stream_id, access)?;
            reader.has_visible_after(subscription_id, after_sequence)
        })
        .await
    }

    pub async fn read_description(
        &self,
        handle: &str,
        access: &Access,
        id: RequestId,
        max_bytes: u64,
        extend: bool,
    ) -> Result<Value, ToolError> {
        let handle = handle.to_owned();
        with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
            store.read_description(&handle, access, &id, max_bytes, extend)
        })
        .await
    }

    /// Acknowledges the subscription's pending batch, the one its description
    /// shows, and returns that batch. With no pending batch it acknowledges
    /// nothing and seals nothing: a message no listing has shown stays unread.
    pub async fn acknowledge_pending(
        &self,
        handle: &str,
        access: &Access,
    ) -> Result<Value, ToolError> {
        let reading = handle.to_owned();
        let pending =
            with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
                store.pending_description(&reading, access)
            })
            .await?;
        let Some(batch_id) = pending["batch_id"].as_str().map(str::to_owned) else {
            return Ok(crate::json(&AckToolResult::Empty));
        };
        let handle = handle.to_owned();
        let admission =
            with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
                store.admit(&handle, access, Some(&batch_id), false)
            })
            .await?;
        if let Some(seq) = admission.acked_sequence {
            self.signals
                .subscription_acked(admission.subscription_id, seq);
        }
        let ack_result = admission
            .ack
            .ok_or_else(|| ToolError::internal("the acknowledgement was not applied"))?;
        Ok(crate::json(&AckToolResult::Acked {
            ack_result,
            acknowledged_batch: pending,
        }))
    }

    pub async fn receive(
        &self,
        arguments: Value,
        access: &Access,
        delivery: Delivery,
    ) -> Result<Admitted, ToolError> {
        let request: ReceiveArgs = serde_json::from_value(arguments)
            .map_err(|error| ToolError::invalid(format!("invalid arguments: {error}")))?;
        if request.max_events == 0 {
            let (Some(ack), None, None) = (request.ack, request.max_bytes, request.wait) else {
                return Err(ToolError::invalid(
                    "max_events 0 only acknowledges: pass ack, and neither max_bytes nor wait",
                ));
            };
            let handle = request.handle;
            let admission = with_store_access(
                &self.store,
                &self.access_refresh,
                access,
                move |store, access| store.admit(&handle, access, Some(&ack), false),
            )
            .await?;
            if let Some(seq) = admission.acked_sequence {
                self.signals
                    .subscription_acked(admission.subscription_id, seq);
            }
            let ack_result = admission
                .ack
                .ok_or_else(|| ToolError::internal("the acknowledgement was not applied"))?;
            return Ok(Admitted::Done(crate::json(&ReceiveResult::Acked {
                ack_result,
            })));
        }
        let max_bytes = request.max_bytes.ok_or_else(|| {
            ToolError::invalid("max_bytes is required when max_events is at least 1")
        })?;
        if max_bytes == 0 {
            return Err(ToolError::invalid("max_bytes must be at least 1"));
        }
        let wait = request.wait.unwrap_or(true);
        let handle = request.handle;
        let ack = request.ack;
        let bounds = Bounds {
            max_events: request.max_events,
            max_bytes: max_bytes.min(RESULT_LIMIT),
            wait,
        };
        let admission = with_store_access(
            &self.store,
            &self.access_refresh,
            access,
            move |store, access| store.admit(&handle, access, ack.as_deref(), true),
        )
        .await?;
        if let Some(seq) = admission.acked_sequence {
            self.signals
                .subscription_acked(admission.subscription_id, seq);
        }
        // Wakes an older receive of this subscription, which then ends as superseded.
        self.signals
            .owner_taken(admission.subscription_id, admission.generation);
        let waiter = Waiter {
            commits: self.signals.commits(admission.stream_id),
            owners: self.signals.owners(admission.subscription_id),
            access_changed: self.signals.access_changed(admission.stream_id),
            enrollment_changed: self.signals.enrollment_changed(),
            refresh_access: Arc::clone(&self.access_refresh),
            signals: Arc::clone(&self.signals),
            store: Arc::clone(&self.store),
            admission,
            bounds,
            delivery,
        };
        waiter.first_look().await
    }

    /// The inbox an address names, if `access` may read it.
    pub async fn inbox(&self, uri: &str, access: &Access) -> Result<Inbox, ToolError> {
        let handle = inbox_handle(uri).ok_or_else(inbox_not_found)?.to_owned();
        let uri = uri.to_owned();
        with_reader_access(&self.reader, &self.access_refresh, access, move |reader, access| {
            let (subscription_id, stream_id) = reader.inbox(&handle)?;
            reader.check_access(stream_id, access)?;
            Ok(Inbox {
                uri: uri.clone(),
                subscription_id,
                stream_id,
                access: access.clone(),
            })
        })
        .await
    }

    /// Whether an address names an inbox, answered on the calling thread, for
    /// callers that cannot await.
    pub fn is_inbox_blocking(&self, uri: &str) -> bool {
        inbox_handle(uri).is_some_and(|handle| {
            self.reader
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .inbox(handle)
                .is_ok()
        })
    }

    /// `resources/read` of an inbox: delivers the pending batch, sealing one
    /// from the backlog if none is pending. Acknowledges nothing.
    pub async fn read_inbox(
        &self,
        uri: &str,
        access: &Access,
        request_id: RequestId,
        modern: bool,
    ) -> Result<ReadResourceResult, ToolError> {
        let handle = inbox_handle(uri).ok_or_else(inbox_not_found)?.to_owned();
        let uri = uri.to_owned();
        with_store_access(&self.store, &self.access_refresh, access, move |store, access| {
            store.read_inbox(&handle, access, &uri, &request_id, modern)
        })
        .await
    }

    /// Calls `notify` once if the inbox already has something to read, then
    /// once for every commit to its stream, until `notify` returns false or
    /// access / subscription existence is revoked.
    /// Commits and access changes are subscribed to before storage is checked,
    /// so a commit or revocation that lands during the check is still observed.
    pub async fn watch_inbox<F, Fut>(&self, inbox: &Inbox, mut notify: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let mut commits = self.signals.commits(inbox.stream_id);
        let mut access_changed = self.signals.access_changed(inbox.stream_id);
        let mut enrollment_changed = self.signals.enrollment_changed();
        let mut owners = self.signals.owners(inbox.subscription_id);
        let _ = commits.borrow_and_update();
        let _ = access_changed.borrow_and_update();
        let _ = enrollment_changed.borrow_and_update();
        if *owners.borrow_and_update() == i64::MAX {
            return;
        }
        let subscription_id = inbox.subscription_id;
        let stream_id = inbox.stream_id;
        let check = || {
            with_reader_access(
                &self.reader,
                &self.access_refresh,
                &inbox.access,
                move |reader, access| {
                    reader.check_access(stream_id, access)?;
                    reader.inbox_unread(subscription_id)
                },
            )
        };
        match check().await {
            Ok(true) => {
                if !notify().await {
                    return;
                }
            }
            Ok(false) => {}
            Err(_) => return,
        }
        loop {
            tokio::select! {
                biased;
                changed = owners.changed() => {
                    if changed.is_err() || *owners.borrow_and_update() == i64::MAX {
                        return;
                    }
                }
                changed = enrollment_changed.changed() => {
                    if changed.is_err() || check().await.is_err() {
                        return;
                    }
                }
                changed = access_changed.changed() => {
                    if changed.is_err() || check().await.is_err() {
                        return;
                    }
                }
                changed = commits.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    match check().await {
                        Ok(true) => {
                            if !notify().await {
                                return;
                            }
                        }
                        Ok(false) => {}
                        Err(_) => return,
                    }
                }
            }
        }
    }
}

pub type AccessRefresher = Arc<dyn Fn(&Access) -> Result<Access, ToolError> + Send + Sync>;

/// A receive with nothing to deliver yet.
pub struct Waiter {
    store: SharedStore,
    signals: Arc<Signals>,
    admission: Admission,
    bounds: Bounds,
    delivery: Delivery,
    commits: watch::Receiver<u64>,
    owners: watch::Receiver<i64>,
    access_changed: watch::Receiver<u64>,
    enrollment_changed: watch::Receiver<u64>,
    refresh_access: SharedRefresher,
}

impl Waiter {
    /// Attaches a live authority refresher so a kept token's parked `receive`
    /// adopts the reloaded `enrollment_digest` across `SIGHUP` while a removed
    /// or fenced token fails closed (`Opus RR2-01`, `Sol RR-01`).
    pub fn with_access_refresh(mut self, refresh_access: AccessRefresher) -> Self {
        self.refresh_access = Arc::new(Mutex::new(Some(refresh_access)));
        self
    }

    async fn first_look(mut self) -> Result<Admitted, ToolError> {
        let _ = self.enrollment_changed.borrow_and_update();
        match self.select().await? {
            Selection::Empty if self.bounds.wait => Ok(Admitted::Waiting(Box::new(self))),
            selection => self
                .conclude(selection, WaitEnd::Backlog)
                .await
                .map(Admitted::Done),
        }
    }

    /// Waits until events are committed or a newer receive (or deletion /
    /// access revocation) takes over, whichever comes first.
    pub async fn wait(mut self) -> Result<Value, ToolError> {
        let _guard = self.signals.enter_wait(self.admission.subscription_id);
        if *self.owners.borrow_and_update() != self.admission.generation {
            return Ok(self.superseded());
        }
        loop {
            tokio::select! {
                biased;
                changed = self.owners.changed() => {
                    if changed.is_err() || *self.owners.borrow_and_update() != self.admission.generation {
                        return Ok(self.superseded());
                    }
                }
                changed = self.enrollment_changed.changed() => {
                    if changed.is_err() {
                        return Ok(self.superseded());
                    }
                    match self.select().await? {
                        Selection::Empty => {}
                        selection => return self.conclude(selection, WaitEnd::Commit).await,
                    }
                }
                changed = self.access_changed.changed() => {
                    if changed.is_err() {
                        return Ok(self.superseded());
                    }
                    match self.select().await? {
                        Selection::Empty => {}
                        selection => return self.conclude(selection, WaitEnd::Commit).await,
                    }
                }
                changed = self.commits.changed() => {
                    changed.map_err(|_| ToolError::internal("the commit signal closed"))?;
                    match self.select().await? {
                        Selection::Empty => {}
                        selection => return self.conclude(selection, WaitEnd::Commit).await,
                    }
                }
            }
        }
    }

    async fn select(&mut self) -> Result<Selection, ToolError> {
        let mut admission = self.admission.clone();
        let bounds = self.bounds.clone();
        let id = self.delivery.request_id.clone();
        let (selection, refreshed_access) = with_store_access(
            &self.store,
            &self.refresh_access,
            &self.admission.access,
            move |store, access| {
                admission.access = access.clone();
                let selection = store.select(&admission, &bounds, &id)?;
                Ok((selection, admission.access.clone()))
            },
        )
        .await?;
        self.admission.access = refreshed_access;
        Ok(selection)
    }

    async fn conclude(&mut self, selection: Selection, fresh: WaitEnd) -> Result<Value, ToolError> {
        let (batch, ended_by) = match selection {
            Selection::Superseded => return Ok(self.superseded()),
            Selection::Empty => return Ok(self.empty()),
            Selection::Pending(batch) => (batch, WaitEnd::PendingBatch),
            Selection::Sealed(batch) => (batch, fresh),
        };
        let wait = self.report(ended_by);
        let mut admission = self.admission.clone();
        let bounds = self.bounds.clone();
        let id = self.delivery.request_id.clone();
        let modern = self.delivery.modern;
        let result = with_store_access(
            &self.store,
            &self.refresh_access,
            &self.admission.access,
            move |store, access| {
                admission.access = access.clone();
                store.release(&admission, &batch, wait.clone(), &bounds, &id, modern)
            },
        )
        .await?;
        Ok(crate::json(&result))
    }

    fn superseded(&self) -> Value {
        crate::json(&ReceiveResult::Superseded {
            ack_result: self.admission.ack.clone(),
            wait: self.report(WaitEnd::Superseded),
        })
    }

    fn empty(&self) -> Value {
        crate::json(&ReceiveResult::Empty {
            ack_result: self.admission.ack.clone(),
            wait: self.report(WaitEnd::NoWait),
            next_receive: self.bounds.next_receive(&self.admission.handle),
        })
    }

    fn report(&self, ended_by: WaitEnd) -> WaitReport {
        WaitReport { ended_by }
    }
}

fn parse<T: DeserializeOwned>(arguments: Value) -> Result<T, ToolError> {
    serde_json::from_value(arguments)
        .map_err(|error| ToolError::invalid(format!("invalid arguments: {error}")))
}

fn valid_label(value: &str) -> bool {
    !value.is_empty() && value.len() <= NAME_LIMIT && !value.chars().any(char::is_control)
}

fn is_broadcast_recipient(value: &str) -> bool {
    value == "*" || value.eq_ignore_ascii_case("all") || value.eq_ignore_ascii_case("broadcast")
}

fn inferred_sender(payload: &Value) -> Option<String> {
    payload
        .as_object()?
        .get("from")?
        .as_str()
        .filter(|label| valid_label(label))
        .map(str::to_owned)
}

fn inferred_recipient(payload: &Value) -> Option<String> {
    payload
        .as_object()?
        .get("to")?
        .as_str()
        .filter(|label| valid_label(label) && !is_broadcast_recipient(label))
        .map(str::to_owned)
}

fn check_name(field: &str, value: &str) -> Result<(), ToolError> {
    if !valid_label(value) {
        return Err(ToolError::invalid(format!(
            "{field} must be 1 to {NAME_LIMIT} bytes, without control characters"
        )));
    }
    Ok(())
}

fn check_metadata(metadata: &std::collections::BTreeMap<String, Value>) -> Result<(), ToolError> {
    let encoded = serde_json::to_string(metadata)
        .map_err(|error| ToolError::invalid(format!("invalid metadata: {error}")))?;
    if encoded.len() > METADATA_LIMIT {
        return Err(ToolError::invalid(format!(
            "metadata must serialize to at most {METADATA_LIMIT} bytes"
        )));
    }
    Ok(())
}

fn page_limit(after: Option<&str>, limit: Option<u64>) -> Result<u64, ToolError> {
    if let Some(after) = after {
        check_name("after", after)?;
    }
    let limit = limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(ToolError::invalid("limit must be 1 to 100"));
    }
    Ok(limit)
}

fn refresh_access_with(
    refresher: &SharedRefresher,
    access: &Access,
) -> Result<Access, ToolError> {
    let maybe = refresher
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    match maybe {
        Some(refresh) => refresh(access),
        None => Ok(access.clone()),
    }
}

async fn with_store<T, F>(store: &SharedStore, work: F) -> Result<T, ToolError>
where
    T: Send + 'static,
    F: FnOnce(&mut dyn QueueStore) -> Result<T, ToolError> + Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || {
        let mut store = store.lock().unwrap_or_else(PoisonError::into_inner);
        work(store.as_mut())
    })
    .await
    .map_err(|error| ToolError::internal(format!("the storage task failed: {error}")))?
}

async fn with_store_access<T, F>(
    store: &SharedStore,
    refresher: &SharedRefresher,
    access: &Access,
    mut work: F,
) -> Result<T, ToolError>
where
    T: Send + 'static,
    F: FnMut(&mut dyn QueueStore, &Access) -> Result<T, ToolError> + Send + 'static,
{
    let store = Arc::clone(store);
    let refresher = Arc::clone(refresher);
    let access = access.clone();
    tokio::task::spawn_blocking(move || {
        let mut store = store.lock().unwrap_or_else(PoisonError::into_inner);
        let current = refresh_access_with(&refresher, &access)?;
        match work(store.as_mut(), &current) {
            Err(err) if err.code == ErrorCode::AuthorityFenced => {
                let refreshed = refresh_access_with(&refresher, &access)?;
                if refreshed.enrollment_digest != current.enrollment_digest {
                    work(store.as_mut(), &refreshed)
                } else {
                    Err(err)
                }
            }
            other => other,
        }
    })
    .await
    .map_err(|error| ToolError::internal(format!("the storage task failed: {error}")))?
}

async fn with_reader_access<T, F>(
    reader: &SharedReader,
    refresher: &SharedRefresher,
    access: &Access,
    mut work: F,
) -> Result<T, ToolError>
where
    T: Send + 'static,
    F: FnMut(&dyn QueueReader, &Access) -> Result<T, ToolError> + Send + 'static,
{
    let reader = Arc::clone(reader);
    let refresher = Arc::clone(refresher);
    let access = access.clone();
    tokio::task::spawn_blocking(move || {
        let reader = reader.lock().unwrap_or_else(PoisonError::into_inner);
        let current = refresh_access_with(&refresher, &access)?;
        match work(reader.as_ref(), &current) {
            Err(err) if err.code == ErrorCode::AuthorityFenced => {
                let refreshed = refresh_access_with(&refresher, &access)?;
                if refreshed.enrollment_digest != current.enrollment_digest {
                    work(reader.as_ref(), &refreshed)
                } else {
                    Err(err)
                }
            }
            other => other,
        }
    })
    .await
    .map_err(|error| ToolError::internal(format!("the reader task failed: {error}")))?
}

/// The payload with object keys sorted at every level, so equal payloads
/// always produce the same idempotency digest.
fn canonical(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            out.push('{');
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(item, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}
