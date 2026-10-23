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
use crate::policy::Membership;
use crate::signals::Signals;
use crate::store::{
    Admission, Bounds, Publication, QueueReader, QueueStore, Reader, Selection, Store,
    inbox_not_found,
};
use crate::wire::{RESULT_LIMIT, widest_single_event_bytes};

/// Stream names, labels and idempotency keys are 1 to this many bytes.
const NAME_LIMIT: usize = 128;

/// Subscription identity metadata is at most this many serialized JSON bytes.
const METADATA_LIMIT: usize = 1024;

type SharedStore = Arc<Mutex<Box<dyn QueueStore>>>;
type SharedReader = Arc<Mutex<Box<dyn QueueReader>>>;

pub struct Queue {
    store: SharedStore,
    reader: SharedReader,
    signals: Arc<Signals>,
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
    Waiting(Waiter),
}

/// A subscription's inbox resource (decision 17).
#[derive(Clone, Debug)]
pub struct Inbox {
    pub uri: String,
    subscription_id: i64,
    stream_id: i64,
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
        let store = Store::open(path)?;
        let reader = Reader::open(path)?;
        Ok(Self::with_backend(Box::new(store), Box::new(reader)))
    }

    /// A queue over any backend. `reader` must observe every command `store`
    /// has committed by the time that command returns.
    pub fn with_backend(store: Box<dyn QueueStore>, reader: Box<dyn QueueReader>) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            reader: Arc::new(Mutex::new(reader)),
            signals: Arc::new(Signals::default()),
        }
    }

    /// Runs a `stream` call for `caller`, the authenticated bearer-token
    /// caller, or `None` on a server without bearer authentication.
    pub async fn stream(&self, arguments: Value, caller: Option<&str>) -> Result<Value, ToolError> {
        let arguments = parse(arguments)?;
        let caller = caller.map(str::to_owned);
        let result = match arguments {
            StreamArgs::Create { name, members } => {
                check_name("name", &name)?;
                for member in members.iter().flatten() {
                    check_name("members", member)?;
                }
                with_store(&self.store, move |store| {
                    store.create_stream(&name, caller.as_deref(), members.as_deref())
                })
                .await?
            }
            StreamArgs::Grant {
                name,
                caller: member,
            } => {
                self.change_member(name, caller, member, Membership::Grant)
                    .await?
            }
            StreamArgs::Revoke {
                name,
                caller: member,
            } => {
                self.change_member(name, caller, member, Membership::Revoke)
                    .await?
            }
            StreamArgs::List { after, limit } => {
                let limit = page_limit(after.as_deref(), limit)?;
                with_reader(&self.reader, move |reader| {
                    reader.list_streams(caller.as_deref(), after.as_deref(), limit)
                })
                .await?
            }
        };
        Ok(crate::json(&result))
    }

    async fn change_member(
        &self,
        name: String,
        caller: Option<String>,
        member: String,
        change: Membership,
    ) -> Result<crate::api::StreamResult, ToolError> {
        check_name("name", &name)?;
        check_name("caller", &member)?;
        with_store(&self.store, move |store| {
            store.change_member(&name, caller.as_deref(), &member, change)
        })
        .await
    }

    pub async fn subscription(
        &self,
        arguments: Value,
        caller: Option<&str>,
    ) -> Result<Value, ToolError> {
        let caller = caller.map(str::to_owned);
        let result = match parse(arguments)? {
            SubscriptionArgs::Create {
                stream,
                label,
                start_at,
                metadata,
            } => {
                check_name("stream", &stream)?;
                if let Some(label) = &label {
                    check_name("label", label)?;
                }
                if let Some(metadata) = &metadata {
                    check_metadata(metadata)?;
                }
                with_store(&self.store, move |store| {
                    store.claim_subscription(
                        &stream,
                        caller.as_deref(),
                        label.as_deref(),
                        start_at,
                        metadata,
                    )
                })
                .await?
            }
            SubscriptionArgs::Recover {
                stream,
                label,
                metadata,
            } => {
                check_name("stream", &stream)?;
                check_name("label", &label)?;
                if let Some(metadata) = &metadata {
                    check_metadata(metadata)?;
                }
                with_store(&self.store, move |store| {
                    store.recover_subscription(&stream, caller.as_deref(), &label, metadata)
                })
                .await?
            }
            SubscriptionArgs::Delete { stream, label } => {
                check_name("stream", &stream)?;
                check_name("label", &label)?;
                let (deleted, subscription_id) = with_store(&self.store, move |store| {
                    store.delete_subscription(&stream, caller.as_deref(), &label)
                })
                .await?;
                self.signals.subscription_deleted(subscription_id);
                deleted
            }
            SubscriptionArgs::List {
                stream,
                after,
                limit,
            } => {
                check_name("stream", &stream)?;
                let limit = page_limit(after.as_deref(), limit)?;
                let waiting = self.signals.waiting_set();
                with_reader(&self.reader, move |reader| {
                    reader.list_subscriptions(
                        &stream,
                        caller.as_deref(),
                        after.as_deref(),
                        limit,
                        &waiting,
                    )
                })
                .await?
            }
        };
        Ok(crate::json(&result))
    }

    pub async fn publish(
        &self,
        arguments: Value,
        caller: Option<&str>,
    ) -> Result<Value, ToolError> {
        self.publish_committed(arguments, caller)
            .await
            .map(|(value, _, _)| value)
    }

    /// The ordinary result, whether this call committed a new event, and any
    /// subscription handle whose pending batch was committed by `ack`.
    pub async fn publish_committed(
        &self,
        arguments: Value,
        caller: Option<&str>,
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
        let publisher = caller.map(str::to_owned);
        let published = with_store(&self.store, move |store| {
            store.publish(Publication {
                stream: &stream,
                key: &idempotency_key,
                payload: &text,
                sender_label: sender_label.as_deref(),
                recipient_label: recipient_label.as_deref(),
                ack: ack.as_deref(),
                publisher: publisher.as_deref(),
                claims: claims.as_deref(),
                digest: &digest,
            })
        })
        .await?;
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
    ) -> Result<String, ToolError> {
        check_name("stream", stream)?;
        check_name("label", label)?;
        let stream = stream.to_owned();
        let label = label.to_owned();
        with_store(&self.store, move |store| {
            store.ensure_subscription(&stream, &label)
        })
        .await
    }

    /// Binds the agent to its own subscription on the stream: the label is the
    /// agent's name. The stream and the subscription are created if missing;
    /// an unbound subscription already claimed by another client is not taken.
    pub async fn follow(
        &self,
        agent: &str,
        caller: Option<&str>,
        arguments: Value,
    ) -> Result<Value, ToolError> {
        let FollowArgs { stream } = parse(arguments)?;
        check_name("agent", agent)?;
        check_name("stream", &stream)?;
        let agent = agent.to_owned();
        let caller = caller.map(str::to_owned);
        let result = with_store(&self.store, move |store| {
            let handle = store.follow_stream(&agent, caller.as_deref(), &stream)?;
            Ok(FollowResult::Following {
                label: agent.clone(),
                agent,
                stream,
                handle,
            })
        })
        .await?;
        Ok(crate::json(&result))
    }

    /// The stream, label and handle the agent follows, if it follows one.
    pub async fn agent_binding(
        &self,
        agent: &str,
    ) -> Result<Option<(String, String, String)>, ToolError> {
        let agent = agent.to_owned();
        with_reader(&self.reader, move |reader| reader.agent_binding(&agent)).await
    }

    pub async fn read_description(
        &self,
        handle: &str,
        caller: Option<&str>,
        id: RequestId,
        max_bytes: u64,
        extend: bool,
    ) -> Result<Value, ToolError> {
        let handle = handle.to_owned();
        let caller = caller.map(str::to_owned);
        with_store(&self.store, move |store| {
            store.read_description(&handle, caller.as_deref(), &id, max_bytes, extend)
        })
        .await
    }

    /// Acknowledges the subscription's pending batch, the one its description
    /// shows, and returns that batch. With no pending batch it acknowledges
    /// nothing and seals nothing: a message no listing has shown stays unread.
    pub async fn acknowledge_pending(
        &self,
        handle: &str,
        caller: Option<&str>,
    ) -> Result<Value, ToolError> {
        let reading = handle.to_owned();
        let caller = caller.map(str::to_owned);
        let reader = caller.clone();
        let pending = with_store(&self.store, move |store| {
            store.pending_description(&reading, reader.as_deref())
        })
        .await?;
        let Some(batch_id) = pending["batch_id"].as_str().map(str::to_owned) else {
            return Ok(crate::json(&AckToolResult::Empty));
        };
        let handle = handle.to_owned();
        let admission = with_store(&self.store, move |store| {
            store.admit(&handle, caller.as_deref(), Some(&batch_id), false)
        })
        .await?;
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
        caller: Option<&str>,
        delivery: Delivery,
    ) -> Result<Admitted, ToolError> {
        let request: ReceiveArgs = serde_json::from_value(arguments)
            .map_err(|error| ToolError::invalid(format!("invalid arguments: {error}")))?;
        let caller = caller.map(str::to_owned);
        if request.max_events == 0 {
            let (Some(ack), None, None) = (request.ack, request.max_bytes, request.wait) else {
                return Err(ToolError::invalid(
                    "max_events 0 only acknowledges: pass ack, and neither max_bytes nor wait",
                ));
            };
            let handle = request.handle;
            let admission = with_store(&self.store, move |store| {
                store.admit(&handle, caller.as_deref(), Some(&ack), false)
            })
            .await?;
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
        let admission = with_store(&self.store, move |store| {
            store.admit(&handle, caller.as_deref(), ack.as_deref(), true)
        })
        .await?;
        // Wakes an older receive of this subscription, which then ends as superseded.
        self.signals
            .owner_taken(admission.subscription_id, admission.generation);
        let waiter = Waiter {
            commits: self.signals.commits(admission.stream_id),
            owners: self.signals.owners(admission.subscription_id),
            signals: Arc::clone(&self.signals),
            store: Arc::clone(&self.store),
            admission,
            bounds,
            delivery,
        };
        waiter.first_look().await
    }

    /// The inbox an address names, if `caller` may read it.
    pub async fn inbox(&self, uri: &str, caller: Option<&str>) -> Result<Inbox, ToolError> {
        let handle = inbox_handle(uri).ok_or_else(inbox_not_found)?.to_owned();
        let uri = uri.to_owned();
        let caller = caller.map(str::to_owned);
        with_reader(&self.reader, move |reader| {
            let (subscription_id, stream_id) = reader.inbox(&handle)?;
            reader.check_access(stream_id, caller.as_deref())?;
            Ok(Inbox {
                uri,
                subscription_id,
                stream_id,
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
        caller: Option<&str>,
        request_id: RequestId,
        modern: bool,
    ) -> Result<ReadResourceResult, ToolError> {
        let handle = inbox_handle(uri).ok_or_else(inbox_not_found)?.to_owned();
        let uri = uri.to_owned();
        let caller = caller.map(str::to_owned);
        with_store(&self.store, move |store| {
            store.read_inbox(&handle, caller.as_deref(), &uri, &request_id, modern)
        })
        .await
    }

    /// Calls `notify` once if the inbox already has something to read, then
    /// once for every commit to its stream, until `notify` returns false.
    /// Commits are subscribed to before storage is checked, so a commit that
    /// lands during the check is still notified.
    pub async fn watch_inbox<F, Fut>(&self, inbox: &Inbox, mut notify: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let mut commits = self.signals.commits(inbox.stream_id);
        let subscription_id = inbox.subscription_id;
        let unread = with_reader(&self.reader, move |reader| {
            reader.inbox_unread(subscription_id)
        })
        .await;
        // If storage cannot say, an update is only a hint to read: send it.
        if !matches!(unread, Ok(false)) && !notify().await {
            return;
        }
        while commits.changed().await.is_ok() {
            let unread = with_reader(&self.reader, move |reader| {
                reader.inbox_unread(subscription_id)
            })
            .await;
            if !matches!(unread, Ok(false)) && !notify().await {
                return;
            }
        }
    }
}

/// A receive with nothing to deliver yet.
pub struct Waiter {
    store: SharedStore,
    signals: Arc<Signals>,
    admission: Admission,
    bounds: Bounds,
    delivery: Delivery,
    commits: watch::Receiver<u64>,
    owners: watch::Receiver<i64>,
}

impl Waiter {
    async fn first_look(self) -> Result<Admitted, ToolError> {
        match self.select().await? {
            Selection::Empty if self.bounds.wait => Ok(Admitted::Waiting(self)),
            selection => self
                .conclude(selection, WaitEnd::Backlog)
                .await
                .map(Admitted::Done),
        }
    }

    /// Waits until events are committed or a newer receive (or deletion) takes
    /// over, whichever comes first.
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

    async fn select(&self) -> Result<Selection, ToolError> {
        let admission = self.admission.clone();
        let bounds = self.bounds.clone();
        let id = self.delivery.request_id.clone();
        with_store(&self.store, move |store| {
            store.select(&admission, &bounds, &id)
        })
        .await
    }

    async fn conclude(&self, selection: Selection, fresh: WaitEnd) -> Result<Value, ToolError> {
        let (batch, ended_by) = match selection {
            Selection::Superseded => return Ok(self.superseded()),
            Selection::Empty => return Ok(self.empty()),
            Selection::Pending(batch) => (batch, WaitEnd::PendingBatch),
            Selection::Sealed(batch) => (batch, fresh),
        };
        let wait = self.report(ended_by);
        let admission = self.admission.clone();
        let bounds = self.bounds.clone();
        let id = self.delivery.request_id.clone();
        let modern = self.delivery.modern;
        let result = with_store(&self.store, move |store| {
            store.release(&admission, &batch, wait, &bounds, &id, modern)
        })
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

async fn with_reader<T, F>(reader: &SharedReader, work: F) -> Result<T, ToolError>
where
    T: Send + 'static,
    F: FnOnce(&dyn QueueReader) -> Result<T, ToolError> + Send + 'static,
{
    let reader = Arc::clone(reader);
    tokio::task::spawn_blocking(move || {
        let reader = reader.lock().unwrap_or_else(PoisonError::into_inner);
        work(reader.as_ref())
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
