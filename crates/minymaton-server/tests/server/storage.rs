//! The queue's durable state behind the `QueueStore` / `QueueReader` trait
//! boundary: the SQLite backend driven through `Queue::with_backend`, through
//! the traits directly, and through a backend defined outside
//! `minymaton-core`.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use minymaton_core::api::{
    AckOutcome, ReceiveResult, StartAt, StreamResult, SubscriptionResult, WaitEnd, WaitReport,
};
use minymaton_core::{
    Admission, Admitted, Batch, Bounds, Delivery, ErrorCode, Publication, Published, Queue,
    QueueReader, QueueStore, Reader, Selection, Store, ToolError, inbox_uri,
};
use rmcp::model::{ReadResourceResult, RequestId};
use serde_json::{Value, json};

fn database() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("create a temporary directory");
    let path = dir.path().join("queue.db");
    (dir, path)
}

fn sqlite_queue(path: &Path) -> Queue {
    Queue::with_backend(
        Box::new(Store::open(path).expect("open the SQLite store")),
        Box::new(Reader::open(path).expect("open the SQLite reader")),
    )
}

fn delivery(id: i64) -> Delivery {
    Delivery {
        request_id: RequestId::Number(id),
        modern: true,
    }
}

async fn receive_now(queue: &Queue, arguments: Value, id: i64) -> Value {
    match queue
        .receive(arguments, delivery(id))
        .await
        .expect("receive")
    {
        Admitted::Done(result) => result,
        Admitted::Waiting(_) => panic!("a receive with events or wait: false must not wait"),
    }
}

fn message_ids(batch: &Value) -> Vec<Value> {
    batch["events"]
        .as_array()
        .expect("events array")
        .iter()
        .map(|event| event["message_id"].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_with_the_sqlite_backend_manages_streams_and_subscriptions() {
    let (_dir, path) = database();
    let queue = sqlite_queue(&path);

    let created = queue
        .stream(json!({"action": "create", "name": "orders"}))
        .await
        .expect("create stream");
    assert_eq!(created, json!({"status": "created", "stream": "orders"}));
    let duplicate = queue
        .stream(json!({"action": "create", "name": "orders"}))
        .await
        .expect_err("a stream name is unique");
    assert_eq!(duplicate.code, ErrorCode::StreamExists);
    let listed = queue
        .stream(json!({"action": "list"}))
        .await
        .expect("list streams");
    assert_eq!(listed["status"], "listed");
    assert_eq!(listed["streams"], json!(["orders"]));

    let claimed = queue
        .subscription(json!({
            "action": "create", "stream": "orders", "label": "worker",
            "start_at": "earliest", "metadata": {"role": "consumer"}
        }))
        .await
        .expect("claim subscription");
    assert_eq!(claimed["status"], "created");
    assert_eq!(claimed["label"], "worker");
    assert_eq!(claimed["metadata"], json!({"role": "consumer"}));
    let handle = claimed["handle"].as_str().expect("handle").to_owned();

    let taken = queue
        .subscription(json!({"action": "create", "stream": "orders", "label": "worker"}))
        .await
        .expect_err("a claimed label is not claimed twice");
    assert_eq!(taken.code, ErrorCode::SubscriptionExists);

    let recovered = queue
        .subscription(json!({
            "action": "recover", "stream": "orders", "label": "worker",
            "metadata": {"role": "restarted"}
        }))
        .await
        .expect("recover subscription");
    assert_eq!(recovered["status"], "recovered");
    assert_eq!(recovered["handle"], handle.as_str());
    assert_eq!(recovered["metadata"], json!({"role": "restarted"}));

    let listed = queue
        .subscription(json!({"action": "list", "stream": "orders"}))
        .await
        .expect("list subscriptions");
    assert_eq!(listed["status"], "listed");
    let subscriptions = listed["subscriptions"].as_array().expect("subscriptions");
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0]["label"], "worker");
    assert_eq!(subscriptions[0]["handle"], handle.as_str());
    assert_eq!(subscriptions[0]["waiting"], false);

    let deleted = queue
        .subscription(json!({"action": "delete", "stream": "orders", "label": "worker"}))
        .await
        .expect("delete subscription");
    assert_eq!(
        deleted,
        json!({"status": "deleted", "stream": "orders", "label": "worker"})
    );
    let gone = queue
        .subscription(json!({"action": "recover", "stream": "orders", "label": "worker"}))
        .await
        .expect_err("a deleted subscription is not recoverable");
    assert_eq!(gone.code, ErrorCode::SubscriptionNotFound);
    let inbox = queue
        .inbox(&inbox_uri(&handle))
        .await
        .expect_err("a deleted subscription has no inbox");
    assert_eq!(inbox.code, ErrorCode::SubscriptionNotFound);
    let listed = queue
        .subscription(json!({"action": "list", "stream": "orders"}))
        .await
        .expect("list subscriptions");
    assert_eq!(listed["subscriptions"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_with_the_sqlite_backend_publishes_idempotently_and_acks_atomically() {
    let (_dir, path) = database();
    let queue = sqlite_queue(&path);
    queue
        .stream(json!({"action": "create", "name": "orders"}))
        .await
        .expect("create stream");
    let claimed = queue
        .subscription(json!({"action": "create", "stream": "orders", "label": "worker"}))
        .await
        .expect("claim subscription");
    let handle = claimed["handle"].as_str().expect("handle").to_owned();

    let first = queue
        .publish(json!({"stream": "orders", "idempotency_key": "k1", "payload": {"n": 1}}))
        .await
        .expect("publish");
    assert_eq!(first["status"], "published");
    assert_eq!(first["sequence"], 1);
    let retried = queue
        .publish(json!({"stream": "orders", "idempotency_key": "k1", "payload": {"n": 1}}))
        .await
        .expect("an identical retry succeeds");
    assert_eq!(retried, first, "a retry returns the original result");
    let conflict = queue
        .publish(json!({"stream": "orders", "idempotency_key": "k1", "payload": {"n": 2}}))
        .await
        .expect_err("a reused key with a new payload is refused");
    assert_eq!(conflict.code, ErrorCode::IdempotencyConflict);

    let receive = json!({"handle": handle, "max_events": 10, "max_bytes": 16384, "wait": false});
    let batch = receive_now(&queue, receive.clone(), 1).await;
    assert_eq!(batch["status"], "events");
    assert_eq!(batch["delivery_attempt"], 1);
    assert_eq!(batch["wait"]["ended_by"], "backlog");
    assert_eq!(message_ids(&batch), vec![first["message_id"].clone()]);
    assert_eq!(batch["events"][0]["payload"], json!({"n": 1}));
    let batch_id = batch["ack_after_handling"]
        .as_str()
        .expect("ack_after_handling")
        .to_owned();

    let redelivered = receive_now(&queue, receive.clone(), 2).await;
    assert_eq!(redelivered["status"], "events");
    assert_eq!(redelivered["batch_id"], batch_id.as_str());
    assert_eq!(redelivered["delivery_attempt"], 2);
    assert_eq!(redelivered["wait"]["ended_by"], "pending_batch");

    let second = queue
        .publish(json!({
            "stream": "orders", "idempotency_key": "k2", "payload": {"n": 2},
            "ack": batch_id
        }))
        .await
        .expect("publish with ack");
    assert_eq!(
        second["ack_result"],
        json!({"batch_id": batch_id, "outcome": "committed"})
    );
    let reacked = queue
        .publish(json!({
            "stream": "orders", "idempotency_key": "k3", "payload": {"n": 3},
            "ack": batch_id
        }))
        .await
        .expect("acking an acked batch again is harmless");
    assert_eq!(reacked["ack_result"]["outcome"], "already_acked");

    let next = receive_now(&queue, receive.clone(), 3).await;
    assert_eq!(next["delivery_attempt"], 1);
    assert_ne!(next["batch_id"], batch_id.as_str());
    assert_eq!(
        message_ids(&next),
        vec![second["message_id"].clone(), reacked["message_id"].clone()]
    );
    let acked = receive_now(
        &queue,
        json!({"handle": handle, "max_events": 0, "ack": next["ack_after_handling"]}),
        4,
    )
    .await;
    assert_eq!(
        acked,
        json!({"status": "acked", "ack_result": {"batch_id": next["batch_id"], "outcome": "committed"}})
    );
    let empty = receive_now(&queue, receive, 5).await;
    assert_eq!(empty["status"], "empty");
    assert_eq!(empty["wait"]["ended_by"], "no_wait");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_receive_over_the_backend_is_woken_by_a_commit() {
    let (_dir, path) = database();
    let queue = sqlite_queue(&path);
    queue
        .stream(json!({"action": "create", "name": "orders"}))
        .await
        .expect("create stream");
    let claimed = queue
        .subscription(json!({"action": "create", "stream": "orders", "label": "worker"}))
        .await
        .expect("claim subscription");
    let handle = claimed["handle"].as_str().expect("handle").to_owned();

    let admitted = queue
        .receive(
            json!({"handle": handle, "max_events": 10, "max_bytes": 16384}),
            delivery(1),
        )
        .await
        .expect("receive");
    let Admitted::Waiting(waiter) = admitted else {
        panic!("an empty subscription must wait");
    };
    let woken = tokio::spawn(waiter.wait());
    let published = queue
        .publish(json!({"stream": "orders", "idempotency_key": "wake", "payload": "now"}))
        .await
        .expect("publish");
    let batch = woken.await.expect("waiter task").expect("woken receive");
    assert_eq!(batch["status"], "events");
    assert_eq!(batch["wait"]["ended_by"], "commit");
    assert_eq!(message_ids(&batch), vec![published["message_id"].clone()]);
}

#[test]
fn the_sqlite_backend_serves_the_queue_commands_through_the_traits() {
    let (_dir, path) = database();
    let mut store: Box<dyn QueueStore> = Box::new(Store::open(&path).expect("open store"));
    let reader: Box<dyn QueueReader> = Box::new(Reader::open(&path).expect("open reader"));

    assert!(matches!(
        store.create_stream("orders").expect("create stream"),
        StreamResult::Created { stream } if stream == "orders"
    ));
    let metadata = BTreeMap::from([("role".to_owned(), json!("consumer"))]);
    let SubscriptionResult::Created { handle, label, .. } = store
        .claim_subscription(
            "orders",
            Some("worker"),
            Some(StartAt::Earliest),
            Some(metadata),
        )
        .expect("claim subscription")
    else {
        panic!("claim must create");
    };
    assert_eq!(label, "worker");
    let SubscriptionResult::Listed { subscriptions, .. } = reader
        .list_subscriptions("orders", None, 10, &HashSet::new())
        .expect("list subscriptions")
    else {
        panic!("list must list");
    };
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].handle, handle);

    let published: Published = store
        .publish(Publication {
            stream: "orders",
            key: "k1",
            payload: "\"hello\"",
            sender_label: None,
            recipient_label: None,
            ack: None,
            digest: b"k1-digest",
        })
        .expect("publish");
    let (_, sequence) = published.commit.expect("a new event is committed");
    assert_eq!(sequence, 1);
    let retried = store
        .publish(Publication {
            stream: "orders",
            key: "k1",
            payload: "\"hello\"",
            sender_label: None,
            recipient_label: None,
            ack: None,
            digest: b"k1-digest",
        })
        .expect("retry");
    assert!(
        retried.commit.is_none(),
        "an idempotent retry commits nothing"
    );
    let conflict = store
        .publish(Publication {
            stream: "orders",
            key: "k1",
            payload: "\"other\"",
            sender_label: None,
            recipient_label: None,
            ack: None,
            digest: b"other-digest",
        })
        .err()
        .expect("a reused key with a new digest is refused");
    assert_eq!(conflict.code, ErrorCode::IdempotencyConflict);

    let (subscription_id, stream_id) = reader.inbox(&handle).expect("inbox");
    assert!(reader.inbox_unread(subscription_id).expect("unread"));

    let bounds = Bounds {
        max_events: 10,
        max_bytes: 16384,
        wait: false,
    };
    let id = RequestId::Number(1);
    let admission: Admission = store.admit(&handle, None, true).expect("admit");
    assert_eq!(admission.subscription_id, subscription_id);
    assert_eq!(admission.stream_id, stream_id);
    assert!(admission.ack.is_none());
    let Selection::Sealed(batch) = store.select(&admission, &bounds, &id).expect("select") else {
        panic!("a backlog must seal a batch");
    };
    let released = store
        .release(
            &admission,
            &batch,
            WaitReport {
                ended_by: WaitEnd::Backlog,
            },
            &bounds,
            &id,
            true,
        )
        .expect("release");
    let ReceiveResult::Events {
        batch_id,
        delivery_attempt,
        events,
        ..
    } = released
    else {
        panic!("release must deliver events");
    };
    assert_eq!(batch_id, batch.batch_id);
    assert_eq!(delivery_attempt, 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!("hello"));

    let retaken = store.admit(&handle, None, true).expect("admit again");
    assert!(retaken.generation > admission.generation);
    let superseded = store.select(&admission, &bounds, &id).expect("select");
    assert!(
        matches!(superseded, Selection::Superseded),
        "an older admission loses ownership"
    );
    let Selection::Pending(pending) = store.select(&retaken, &bounds, &id).expect("select") else {
        panic!("an unacknowledged batch stays pending");
    };
    assert_eq!(pending.batch_id, batch.batch_id);

    let acked = store
        .admit(&handle, Some(&batch.batch_id), false)
        .expect("ack");
    let ack = acked.ack.expect("ack result");
    assert_eq!(ack.batch_id, batch.batch_id);
    assert!(matches!(ack.outcome, AckOutcome::Committed));
    assert!(!reader.inbox_unread(subscription_id).expect("unread"));
    let owner = store.admit(&handle, None, true).expect("admit");
    assert!(matches!(
        store.select(&owner, &bounds, &id).expect("select"),
        Selection::Empty
    ));

    let followed = store
        .follow_stream("agent-x", "orders")
        .expect("follow stream");
    assert_eq!(
        reader.agent_binding("agent-x").expect("binding"),
        Some(("orders".to_owned(), "agent-x".to_owned(), followed.clone()))
    );
    assert_eq!(
        store
            .ensure_subscription("orders", "agent-x")
            .expect("ensure existing"),
        followed
    );
    let fresh = store
        .ensure_subscription("fresh", "first")
        .expect("ensure creates stream and subscription");
    assert!(reader.inbox(&fresh).is_ok());
    let StreamResult::Listed { streams, .. } = reader.list_streams(None, 10).expect("list") else {
        panic!("list must list");
    };
    assert_eq!(streams, vec!["fresh".to_owned(), "orders".to_owned()]);

    let SubscriptionResult::Recovered { handle: same, .. } = store
        .recover_subscription("orders", "worker", None)
        .expect("recover")
    else {
        panic!("recover must recover");
    };
    assert_eq!(same, handle);
    let (deleted, deleted_id) = store
        .delete_subscription("orders", "worker")
        .expect("delete");
    assert!(matches!(deleted, SubscriptionResult::Deleted { .. }));
    assert_eq!(deleted_id, subscription_id);
    assert_eq!(
        reader.inbox(&handle).expect_err("deleted").code,
        ErrorCode::SubscriptionNotFound
    );
}

type Calls = Arc<Mutex<Vec<&'static str>>>;

fn take(calls: &Calls) -> Vec<&'static str> {
    std::mem::take(&mut *calls.lock().unwrap_or_else(PoisonError::into_inner))
}

/// A backend defined outside `minymaton-core`: it forwards to another backend
/// and records each queue command it receives.
struct RecordingStore {
    inner: Box<dyn QueueStore>,
    calls: Calls,
}

impl RecordingStore {
    fn record(&self, command: &'static str) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(command);
    }
}

impl QueueStore for RecordingStore {
    fn create_stream(&mut self, name: &str) -> Result<StreamResult, ToolError> {
        self.record("create_stream");
        self.inner.create_stream(name)
    }

    fn claim_subscription(
        &mut self,
        stream: &str,
        label: Option<&str>,
        start_at: Option<StartAt>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriptionResult, ToolError> {
        self.record("claim_subscription");
        self.inner
            .claim_subscription(stream, label, start_at, metadata)
    }

    fn recover_subscription(
        &mut self,
        stream: &str,
        label: &str,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriptionResult, ToolError> {
        self.record("recover_subscription");
        self.inner.recover_subscription(stream, label, metadata)
    }

    fn delete_subscription(
        &mut self,
        stream: &str,
        label: &str,
    ) -> Result<(SubscriptionResult, i64), ToolError> {
        self.record("delete_subscription");
        self.inner.delete_subscription(stream, label)
    }

    fn follow_stream(&mut self, agent: &str, stream: &str) -> Result<String, ToolError> {
        self.record("follow_stream");
        self.inner.follow_stream(agent, stream)
    }

    fn ensure_subscription(&mut self, stream: &str, label: &str) -> Result<String, ToolError> {
        self.record("ensure_subscription");
        self.inner.ensure_subscription(stream, label)
    }

    fn publish(&mut self, publication: Publication<'_>) -> Result<Published, ToolError> {
        self.record("publish");
        self.inner.publish(publication)
    }

    fn admit(
        &mut self,
        handle: &str,
        ack: Option<&str>,
        take_ownership: bool,
    ) -> Result<Admission, ToolError> {
        self.record("admit");
        self.inner.admit(handle, ack, take_ownership)
    }

    fn select(
        &mut self,
        admission: &Admission,
        bounds: &Bounds,
        id: &RequestId,
    ) -> Result<Selection, ToolError> {
        self.record("select");
        self.inner.select(admission, bounds, id)
    }

    fn release(
        &mut self,
        admission: &Admission,
        batch: &Batch,
        wait: WaitReport,
        bounds: &Bounds,
        id: &RequestId,
        modern: bool,
    ) -> Result<ReceiveResult, ToolError> {
        self.record("release");
        self.inner
            .release(admission, batch, wait, bounds, id, modern)
    }

    fn read_inbox(
        &mut self,
        handle: &str,
        uri: &str,
        id: &RequestId,
        modern: bool,
    ) -> Result<ReadResourceResult, ToolError> {
        self.record("read_inbox");
        self.inner.read_inbox(handle, uri, id, modern)
    }

    fn read_description(
        &mut self,
        handle: &str,
        id: &RequestId,
        max_bytes: u64,
        extend: bool,
    ) -> Result<Value, ToolError> {
        self.record("read_description");
        self.inner.read_description(handle, id, max_bytes, extend)
    }

    fn pending_description(&mut self, handle: &str) -> Result<Value, ToolError> {
        self.record("pending_description");
        self.inner.pending_description(handle)
    }
}

struct RecordingReader {
    inner: Box<dyn QueueReader>,
    calls: Calls,
}

impl RecordingReader {
    fn record(&self, command: &'static str) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(command);
    }
}

impl QueueReader for RecordingReader {
    fn list_streams(&self, after: Option<&str>, limit: u64) -> Result<StreamResult, ToolError> {
        self.record("list_streams");
        self.inner.list_streams(after, limit)
    }

    fn list_subscriptions(
        &self,
        stream: &str,
        after: Option<&str>,
        limit: u64,
        waiting: &HashSet<i64>,
    ) -> Result<SubscriptionResult, ToolError> {
        self.record("list_subscriptions");
        self.inner.list_subscriptions(stream, after, limit, waiting)
    }

    fn agent_binding(&self, agent: &str) -> Result<Option<(String, String, String)>, ToolError> {
        self.record("agent_binding");
        self.inner.agent_binding(agent)
    }

    fn inbox_unread(&self, subscription_id: i64) -> Result<bool, ToolError> {
        self.record("inbox_unread");
        self.inner.inbox_unread(subscription_id)
    }

    fn inbox(&self, handle: &str) -> Result<(i64, i64), ToolError> {
        self.record("inbox");
        self.inner.inbox(handle)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_queue_dispatches_every_command_through_an_external_backend() {
    let (_dir, path) = database();
    let writes = Calls::default();
    let reads = Calls::default();
    let queue = Queue::with_backend(
        Box::new(RecordingStore {
            inner: Box::new(Store::open(&path).expect("open store")),
            calls: Arc::clone(&writes),
        }),
        Box::new(RecordingReader {
            inner: Box::new(Reader::open(&path).expect("open reader")),
            calls: Arc::clone(&reads),
        }),
    );

    queue
        .stream(json!({"action": "create", "name": "orders"}))
        .await
        .expect("create stream");
    assert_eq!(take(&writes), ["create_stream"]);
    let listed = queue
        .stream(json!({"action": "list"}))
        .await
        .expect("list streams");
    assert_eq!(listed["streams"], json!(["orders"]));
    assert_eq!(take(&reads), ["list_streams"]);

    let claimed = queue
        .subscription(json!({"action": "create", "stream": "orders", "label": "worker"}))
        .await
        .expect("claim");
    let handle = claimed["handle"].as_str().expect("handle").to_owned();
    queue
        .subscription(json!({"action": "recover", "stream": "orders", "label": "worker"}))
        .await
        .expect("recover");
    assert_eq!(
        take(&writes),
        ["claim_subscription", "recover_subscription"]
    );
    queue
        .subscription(json!({"action": "list", "stream": "orders"}))
        .await
        .expect("list subscriptions");
    assert_eq!(take(&reads), ["list_subscriptions"]);

    let published = queue
        .publish(json!({"stream": "orders", "idempotency_key": "k1", "payload": "one"}))
        .await
        .expect("publish");
    assert_eq!(take(&writes), ["publish"]);

    let batch = receive_now(
        &queue,
        json!({"handle": handle, "max_events": 10, "max_bytes": 16384, "wait": false}),
        1,
    )
    .await;
    assert_eq!(message_ids(&batch), vec![published["message_id"].clone()]);
    assert_eq!(take(&writes), ["admit", "select", "release"]);
    let acked = receive_now(
        &queue,
        json!({"handle": handle, "max_events": 0, "ack": batch["ack_after_handling"]}),
        2,
    )
    .await;
    assert_eq!(acked["status"], "acked");
    assert_eq!(take(&writes), ["admit"]);

    let uri = inbox_uri(&handle);
    let inbox = queue.inbox(&uri).await.expect("inbox");
    assert!(queue.is_inbox_blocking(&uri));
    assert_eq!(take(&reads), ["inbox", "inbox"]);

    let second = queue
        .publish(json!({"stream": "orders", "idempotency_key": "k2", "payload": "two"}))
        .await
        .expect("publish");
    let mut notified = 0;
    queue
        .watch_inbox(&inbox, || {
            notified += 1;
            async { false }
        })
        .await;
    assert_eq!(notified, 1, "an unread inbox is notified at once");
    assert_eq!(take(&reads), ["inbox_unread"]);

    let read = queue
        .read_inbox(&uri, RequestId::Number(3), true)
        .await
        .expect("read inbox");
    assert!(!read.contents.is_empty());
    let description = queue
        .read_description(&handle, RequestId::Number(4), 16384, false)
        .await
        .expect("read description");
    assert!(description.is_object());
    let pending = queue
        .acknowledge_pending(&handle)
        .await
        .expect("acknowledge pending");
    assert_eq!(pending["status"], "acked");
    assert_eq!(
        message_ids(&pending["acknowledged_batch"]),
        vec![second["message_id"].clone()]
    );
    assert_eq!(
        take(&writes),
        [
            "publish",
            "read_inbox",
            "read_description",
            "pending_description",
            "admit"
        ]
    );

    let followed = queue
        .follow("agent-x", json!({"stream": "orders"}))
        .await
        .expect("follow");
    assert_eq!(followed["status"], "following");
    let binding = queue.agent_binding("agent-x").await.expect("binding");
    assert_eq!(
        binding,
        Some((
            "orders".to_owned(),
            "agent-x".to_owned(),
            followed["handle"].as_str().expect("handle").to_owned()
        ))
    );
    let ensured = queue
        .prepare_description("orders", "agent-x")
        .await
        .expect("prepare description");
    assert_eq!(ensured, followed["handle"].as_str().expect("handle"));
    assert_eq!(take(&writes), ["follow_stream", "ensure_subscription"]);
    assert_eq!(take(&reads), ["agent_binding"]);

    queue
        .subscription(json!({"action": "delete", "stream": "orders", "label": "worker"}))
        .await
        .expect("delete");
    assert_eq!(take(&writes), ["delete_subscription"]);
}

/// Real storage faults injected into the server process.
#[cfg(target_os = "linux")]
mod faults {
    use crate::common::{
        Lifecycle, Outcome, Server, ack_only, inbox_uri, message_ids, receive_args,
    };
    use rmcp::ServiceError;
    use rmcp::model::ProtocolVersion;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_real_write_failure_is_explicit_and_committed_messages_survive_recovery() {
        let mut server = Server::start_with_file_limit().await;
        let client = server
            .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
            .await;
        let handle = client.create_subscription("agents", "writer").await;
        let mut saved = Vec::new();
        let mut failure = None;
        for index in 0..200 {
            match client
                .call(
                    "publish",
                    json!({
                        "stream": "agents", "idempotency_key": format!("message-{index}"),
                        "payload": "x".repeat(2048)
                    }),
                )
                .await
            {
                Outcome::Success(published) => saved.push(published["message_id"].clone()),
                Outcome::Failure(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        let failure =
            failure.expect("the isolated child's file-size limit must reject a real write");
        assert_eq!(failure["code"], "storage_unavailable");
        assert!(failure["details"]["sqlite_extended_code"].is_number());
        assert!(
            !saved.is_empty(),
            "exercise durable writes before the refusal"
        );
        assert!(
            server.running(),
            "the failed write must not terminate the server"
        );
        let listed = client.call("stream", json!({"action": "list"})).await.ok();
        assert_eq!(listed["streams"], json!(["agents"]));
        drop(client);
        server.restart().await;
        let client = server
            .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
            .await;
        let mut restored = Vec::new();
        while restored.len() < saved.len() {
            let batch = client
                .call("receive", receive_args(&handle, 100, 16384, 0))
                .await
                .ok();
            assert_eq!(batch["status"], "events");
            restored.extend(message_ids(&batch));
            client
                .call("receive", ack_only(&handle, &batch["batch_id"]))
                .await
                .ok();
        }
        assert_eq!(
            restored, saved,
            "failed writes must not lose acknowledged publications"
        );
        client
            .publish("agents", "recovered", json!("writes work again"))
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn storage_full_and_busy_are_reported_in_tool_and_protocol_errors() {
        let server = Server::start().await;
        let client = server
            .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
            .await;
        let handle = client.create_subscription("agents", "reader").await;
        let first = client
            .publish("agents", "before-full", json!("already durable"))
            .await;

        let connection = rusqlite::Connection::open(server.db_path()).expect("open test database");
        connection
            .execute_batch(
                "CREATE TABLE full_probe (id INTEGER PRIMARY KEY AUTOINCREMENT);
                 INSERT INTO full_probe VALUES (9223372036854775807);
                 CREATE TRIGGER events_full BEFORE INSERT ON events
                 BEGIN INSERT INTO full_probe VALUES (NULL); END;
                 CREATE TRIGGER batches_full BEFORE INSERT ON batches
                 BEGIN INSERT INTO full_probe VALUES (NULL); END;",
            )
            .expect("install SQLITE_FULL triggers");

        let full_publish = client
            .call(
                "publish",
                json!({"stream": "agents", "idempotency_key": "blocked-by-full", "payload": "full"}),
            )
            .await
            .err();
        assert_eq!(full_publish["code"], "storage_full");
        assert_eq!(full_publish["details"]["sqlite_extended_code"], 13);

        let inbox_error = client
            .read_inbox(&inbox_uri(&handle))
            .await
            .expect_err("sealing a batch must report storage_full");
        let ServiceError::McpError(error_data) = inbox_error else {
            panic!("expected MCP protocol error, got {inbox_error:?}");
        };
        let data = error_data.data.expect("structured ToolError data");
        assert_eq!(data["code"], "storage_full");
        assert_eq!(data["details"]["sqlite_extended_code"], 13);

        connection
            .execute_batch(
                "DROP TRIGGER events_full; DROP TRIGGER batches_full; DROP TABLE full_probe;",
            )
            .expect("remove SQLITE_FULL triggers");

        connection
            .execute_batch("BEGIN IMMEDIATE;")
            .expect("hold writer transaction");
        let busy_publish = client
            .call(
                "publish",
                json!({"stream": "agents", "idempotency_key": "blocked-by-busy", "payload": "busy"}),
            )
            .await
            .err();
        assert_eq!(busy_publish["code"], "storage_busy");
        assert_eq!(busy_publish["details"]["sqlite_extended_code"], 5);
        connection
            .execute_batch("ROLLBACK;")
            .expect("release writer transaction");

        let received = client
            .call("receive", receive_args(&handle, 10, 16384, 0))
            .await
            .ok();
        assert_eq!(message_ids(&received), vec![first["message_id"].clone()]);
    }
}
