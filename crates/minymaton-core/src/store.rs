//! Durable state in SQLite: one writer connection in WAL mode with
//! `synchronous=FULL` and no busy timeout, plus a dedicated read-only WAL
//! connection for lock-free discovery queries.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::Duration;

use rmcp::model::{ReadResourceResult, RequestId};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, Transaction, TransactionBehavior, params,
};
use serde_json::{Value, json};

use crate::api::{
    AckOutcome, AckResult, EventEnvelope, InboxRead, NextReceive, PublishResult, ReceiveResult,
    StartAt, StreamResult, SubscriptionEntry, SubscriptionResult, WaitReport,
};
use crate::error::{ErrorCode, ToolError};
use crate::ids::{BATCH_PREFIX, HANDLE_PREFIX, MESSAGE_PREFIX, now, random_id};
use crate::wire::{
    RESULT_LIMIT, discovery_bytes, inbox_bytes, inbox_result, widest_batch_bytes, wire_bytes,
};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    last_sequence INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS events (
    stream_id INTEGER NOT NULL REFERENCES streams (id),
    sequence INTEGER NOT NULL,
    message_id TEXT NOT NULL UNIQUE,
    published_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    sender_label TEXT,
    recipient_label TEXT,
    PRIMARY KEY (stream_id, sequence)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS publishes (
    stream_id INTEGER NOT NULL REFERENCES streams (id),
    idempotency_key TEXT NOT NULL,
    request_digest BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY (stream_id, idempotency_key)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY,
    handle TEXT NOT NULL UNIQUE,
    stream_id INTEGER NOT NULL REFERENCES streams (id),
    label TEXT NOT NULL,
    committed_sequence INTEGER NOT NULL,
    owner_generation INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_active_at TEXT NOT NULL,
    metadata TEXT,
    UNIQUE (stream_id, label)
) STRICT;
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions (id),
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    sealed_at TEXT NOT NULL,
    acked_at TEXT
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS batches_one_pending
    ON batches (subscription_id) WHERE acked_at IS NULL;
CREATE TABLE IF NOT EXISTS batch_aliases (
    batch_id TEXT PRIMARY KEY,
    batch_row INTEGER NOT NULL REFERENCES batches (id),
    last_sequence INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS agents (
    name TEXT PRIMARY KEY,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions (id)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS agent_subscriptions (
    subscription_id INTEGER PRIMARY KEY REFERENCES subscriptions (id),
    agent TEXT NOT NULL
) STRICT, WITHOUT ROWID;
";

/// The queue's writes at the atomic queue-command boundary. Each method is one
/// all-or-nothing queue command; a backend decides how it stores and orders
/// them. [`crate::Queue`] owns argument validation, wake-up signals and the
/// receive wait, and reaches durable state only through this trait.
pub trait QueueStore: Send {
    /// Creates an empty stream; `stream_exists` if the name is taken.
    fn create_stream(&mut self, name: &str) -> Result<StreamResult, ToolError>;

    /// Opens a subscription labelled `label`, or the smallest free `agent-N`
    /// when `label` is `None`. `start_at` defaults to earliest for an explicit
    /// label and latest for a claimed one.
    fn claim_subscription(
        &mut self,
        stream: &str,
        label: Option<&str>,
        start_at: Option<StartAt>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriptionResult, ToolError>;

    /// Returns an existing subscription's handle, replacing its stored
    /// metadata when `metadata` is given.
    fn recover_subscription(
        &mut self,
        stream: &str,
        label: &str,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriptionResult, ToolError>;

    /// Removes a subscription with its batches and bindings, returning the
    /// result and the removed subscription's id.
    fn delete_subscription(
        &mut self,
        stream: &str,
        label: &str,
    ) -> Result<(SubscriptionResult, i64), ToolError>;

    /// Ensures `stream` exists and binds `agent` to its own subscription
    /// labelled `agent`, returning that subscription's handle.
    fn follow_stream(&mut self, agent: &str, stream: &str) -> Result<String, ToolError>;

    /// The handle of the stream's subscription labelled `label`, creating the
    /// stream and the subscription if either is missing.
    fn ensure_subscription(&mut self, stream: &str, label: &str) -> Result<String, ToolError>;

    /// Commits one event, or returns the original result for a retry with the
    /// same key and digest, applying `publication.ack` in the same commit.
    fn publish(&mut self, publication: Publication<'_>) -> Result<Published, ToolError>;

    /// Applies any ACK and, for a receiving call, takes a new ownership
    /// generation. A refused ACK changes nothing.
    fn admit(
        &mut self,
        handle: &str,
        ack: Option<&str>,
        take_ownership: bool,
    ) -> Result<Admission, ToolError>;

    /// What this receive delivers: superseded if it no longer owns the
    /// subscription, else the pending batch, else a batch sealed from the
    /// backlog under `bounds`.
    fn select(
        &mut self,
        admission: &Admission,
        bounds: &Bounds,
        id: &RequestId,
    ) -> Result<Selection, ToolError>;

    /// Snapshots `batch` for sending and records the delivery attempt in one
    /// commit, unless ownership moved, the batch was acknowledged meanwhile,
    /// or the result does not fit `bounds`.
    fn release(
        &mut self,
        admission: &Admission,
        batch: &Batch,
        wait: WaitReport,
        bounds: &Bounds,
        id: &RequestId,
        modern: bool,
    ) -> Result<ReceiveResult, ToolError>;

    /// `resources/read` of an inbox: the pending batch, or one sealed from the
    /// backlog, with its attempt recorded. Acknowledges nothing.
    fn read_inbox(
        &mut self,
        handle: &str,
        uri: &str,
        id: &RequestId,
        modern: bool,
    ) -> Result<ReadResourceResult, ToolError>;

    /// The description's pending batch, sealing one up to `max_bytes` when none
    /// is pending; with `extend`, a pending batch grows to take newer backlog.
    fn read_description(
        &mut self,
        handle: &str,
        id: &RequestId,
        max_bytes: u64,
        extend: bool,
    ) -> Result<Value, ToolError>;

    /// The pending batch exactly as the description shows it, or
    /// `{"status":"empty"}`. Seals nothing and records no attempt.
    fn pending_description(&mut self, handle: &str) -> Result<Value, ToolError>;
}

/// The queue's read-only discovery, served beside [`QueueStore`] so listings
/// never wait behind a write.
pub trait QueueReader: Send {
    fn list_streams(&self, after: Option<&str>, limit: u64) -> Result<StreamResult, ToolError>;

    /// One page of the stream's subscriptions; `waiting` holds the ids of
    /// subscriptions with a receive parked in this process.
    fn list_subscriptions(
        &self,
        stream: &str,
        after: Option<&str>,
        limit: u64,
        waiting: &HashSet<i64>,
    ) -> Result<SubscriptionResult, ToolError>;

    /// The stream, label and handle the agent follows, if it follows one.
    fn agent_binding(&self, agent: &str) -> Result<Option<(String, String, String)>, ToolError>;

    /// Whether the subscription has a pending batch or undelivered backlog.
    fn inbox_unread(&self, subscription_id: i64) -> Result<bool, ToolError>;

    /// The subscription id and stream id behind an inbox handle.
    fn inbox(&self, handle: &str) -> Result<(i64, i64), ToolError>;
}

/// One `publish` command. `digest` identifies the request for idempotent
/// retries: the same `key` with a different digest is a conflict.
#[derive(Clone, Copy, Debug)]
pub struct Publication<'a> {
    pub stream: &'a str,
    pub key: &'a str,
    /// The payload as JSON text.
    pub payload: &'a str,
    pub sender_label: Option<&'a str>,
    pub recipient_label: Option<&'a str>,
    pub ack: Option<&'a str>,
    pub digest: &'a [u8],
}

/// The SQLite [`QueueStore`]: one writer connection.
pub struct Store {
    conn: Connection,
}

/// The SQLite [`QueueReader`]: one read-only WAL connection.
pub struct Reader {
    conn: Connection,
}

/// A receive admitted to a subscription, with any ACK it applied.
#[derive(Clone, Debug)]
pub struct Admission {
    pub subscription_id: i64,
    pub stream_id: i64,
    pub handle: String,
    /// The ownership generation this receive took; ACK-only calls take none.
    pub generation: i64,
    pub ack: Option<AckResult>,
}

/// One receive's effective bounds.
#[derive(Clone, Debug)]
pub struct Bounds {
    pub max_events: u64,
    /// The caller's `max_bytes`, capped at the result limit.
    pub max_bytes: u64,
    pub wait: bool,
}

impl Bounds {
    pub fn next_receive(&self, handle: &str) -> NextReceive {
        NextReceive {
            handle: handle.to_owned(),
            max_events: self.max_events,
            max_bytes: self.max_bytes,
            wait: self.wait,
        }
    }
}

/// A sealed batch: `row_id` is the backend's own key for it and `batch_id`
/// the ID clients acknowledge.
#[derive(Clone, Debug)]
pub struct Batch {
    pub row_id: i64,
    pub batch_id: String,
}

pub enum Selection {
    /// A newer receive owns the subscription, or the subscription was deleted.
    Superseded,
    /// The subscription's unacknowledged batch.
    Pending(Batch),
    /// A batch sealed just now from the backlog.
    Sealed(Batch),
    /// Nothing to deliver.
    Empty,
}

pub struct Published {
    pub result: PublishResult,
    /// The stream and sequence committed, or `None` for an idempotent retry.
    pub commit: Option<(i64, u64)>,
    /// The subscription handle whose pending batch was committed by `ack`.
    pub acked_handle: Option<String>,
}

impl Reader {
    pub fn open(path: &Path) -> Result<Self, String> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags(path, flags)
            .map_err(|error| format!("cannot open reader {}: {error}", path.display()))?;
        conn.busy_timeout(Duration::ZERO)
            .map_err(|error| format!("cannot configure reader {}: {error}", path.display()))?;
        Ok(Self { conn })
    }
}

impl QueueReader for Reader {
    fn list_streams(&self, after: Option<&str>, limit: u64) -> Result<StreamResult, ToolError> {
        list_streams_on(&self.conn, after, limit)
    }

    fn list_subscriptions(
        &self,
        stream: &str,
        after: Option<&str>,
        limit: u64,
        waiting: &HashSet<i64>,
    ) -> Result<SubscriptionResult, ToolError> {
        list_subscriptions_on(&self.conn, stream, after, limit, waiting)
    }

    fn agent_binding(&self, agent: &str) -> Result<Option<(String, String, String)>, ToolError> {
        agent_binding_on(&self.conn, agent)
    }

    fn inbox_unread(&self, subscription_id: i64) -> Result<bool, ToolError> {
        inbox_unread_on(&self.conn, subscription_id)
    }

    fn inbox(&self, handle: &str) -> Result<(i64, i64), ToolError> {
        inbox_on(&self.conn, handle)
    }
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, String> {
        let conn = Connection::open(path)
            .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
        configure(&conn).map_err(|error| format!("cannot prepare {}: {error}", path.display()))?;
        Ok(Self { conn })
    }

    fn create_subscription(
        &mut self,
        stream: &str,
        label: &str,
    ) -> Result<SubscriptionResult, ToolError> {
        self.claim_subscription(stream, Some(label), Some(StartAt::Earliest), None)
    }
}

impl QueueStore for Store {
    fn create_stream(&mut self, name: &str) -> Result<StreamResult, ToolError> {
        let tx = self.write()?;
        let exists = tx
            .query_row("SELECT 1 FROM streams WHERE name = ?1", [name], |_| Ok(()))
            .optional()?
            .is_some();
        if exists {
            return Err(ToolError::new(
                ErrorCode::StreamExists,
                format!("a stream named {name:?} already exists"),
            ));
        }
        tx.execute(
            "INSERT INTO streams (name, created_at) VALUES (?1, ?2)",
            params![name, now()],
        )?;
        tx.commit()?;
        Ok(StreamResult::Created {
            stream: name.to_owned(),
        })
    }

    fn claim_subscription(
        &mut self,
        stream: &str,
        label: Option<&str>,
        start_at: Option<StartAt>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriptionResult, ToolError> {
        let tx = self.write()?;
        let stream_id = stream_id(&tx, stream)?;
        let effective_start = start_at.unwrap_or(match label {
            Some(_) => StartAt::Earliest,
            None => StartAt::Latest,
        });
        let assigned;
        let label = match label {
            Some(label) => label,
            None => {
                let mut statement =
                    tx.prepare("SELECT label FROM subscriptions WHERE stream_id = ?1")?;
                let labels = statement
                    .query_map([stream_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<std::collections::HashSet<_>, _>>()?;
                assigned = (1..)
                    .map(|number: u64| format!("agent-{number}"))
                    .find(|label| !labels.contains(label))
                    .ok_or_else(|| ToolError::internal("no free agent label"))?;
                &assigned
            }
        };
        let taken = tx
            .query_row(
                "SELECT 1 FROM subscriptions WHERE stream_id = ?1 AND label = ?2",
                params![stream_id, label],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if taken {
            return Err(ToolError::new(
                ErrorCode::SubscriptionExists,
                format!(
                    "stream {stream:?} already has a subscription labelled {label:?}; recover it only if this is your own earlier subscription"
                ),
            ));
        }
        let start_sequence: i64 = match effective_start {
            StartAt::Earliest => tx.query_row(
                "SELECT COALESCE(
                     (SELECT MIN(sequence) - 1 FROM events WHERE stream_id = ?1),
                     last_sequence)
                 FROM streams WHERE id = ?1",
                [stream_id],
                |row| row.get(0),
            )?,
            StartAt::Latest => tx.query_row(
                "SELECT last_sequence FROM streams WHERE id = ?1",
                [stream_id],
                |row| row.get(0),
            )?,
        };
        let handle = random_id(HANDLE_PREFIX)?;
        let timestamp = now();
        let metadata_json = metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ToolError::internal(format!("cannot serialize metadata: {error}")))?;
        tx.execute(
            "INSERT INTO subscriptions (handle, stream_id, label, committed_sequence, created_at, last_active_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)",
            params![handle, stream_id, label, start_sequence, timestamp, metadata_json],
        )?;
        tx.commit()?;
        Ok(SubscriptionResult::Created {
            handle,
            stream: stream.to_owned(),
            label: label.to_owned(),
            metadata,
        })
    }

    fn recover_subscription(
        &mut self,
        stream: &str,
        label: &str,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<SubscriptionResult, ToolError> {
        let tx = self.write()?;
        let stream_id = stream_id(&tx, stream)?;
        let (handle, stored_metadata): (String, Option<String>) = tx
            .query_row(
                "SELECT handle, metadata FROM subscriptions WHERE stream_id = ?1 AND label = ?2",
                params![stream_id, label],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                ToolError::new(
                    ErrorCode::SubscriptionNotFound,
                    format!("stream {stream:?} has no subscription labelled {label:?}"),
                )
            })?;
        let effective_metadata = match metadata {
            Some(updated) => {
                let metadata_json = serde_json::to_string(&updated).map_err(|error| {
                    ToolError::internal(format!("cannot serialize metadata: {error}"))
                })?;
                tx.execute(
                    "UPDATE subscriptions SET last_active_at = ?3, metadata = ?4 WHERE stream_id = ?1 AND label = ?2",
                    params![stream_id, label, now(), metadata_json],
                )?;
                Some(updated)
            }
            None => {
                tx.execute(
                    "UPDATE subscriptions SET last_active_at = ?3 WHERE stream_id = ?1 AND label = ?2",
                    params![stream_id, label, now()],
                )?;
                parse_metadata(stored_metadata)?
            }
        };
        tx.commit()?;
        Ok(SubscriptionResult::Recovered {
            handle,
            stream: stream.to_owned(),
            label: label.to_owned(),
            metadata: effective_metadata,
        })
    }

    fn delete_subscription(
        &mut self,
        stream: &str,
        label: &str,
    ) -> Result<(SubscriptionResult, i64), ToolError> {
        let tx = self.write()?;
        let stream_id = stream_id(&tx, stream)?;
        let subscription_id: i64 = tx
            .query_row(
                "SELECT id FROM subscriptions WHERE stream_id = ?1 AND label = ?2",
                params![stream_id, label],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| {
                ToolError::new(
                    ErrorCode::SubscriptionNotFound,
                    format!("stream {stream:?} has no subscription labelled {label:?}"),
                )
            })?;
        tx.execute(
            "DELETE FROM batch_aliases WHERE batch_row IN (
                 SELECT id FROM batches WHERE subscription_id = ?1
             )",
            [subscription_id],
        )?;
        tx.execute(
            "DELETE FROM batches WHERE subscription_id = ?1",
            [subscription_id],
        )?;
        tx.execute(
            "DELETE FROM agents WHERE subscription_id = ?1",
            [subscription_id],
        )?;
        tx.execute(
            "DELETE FROM agent_subscriptions WHERE subscription_id = ?1",
            [subscription_id],
        )?;
        tx.execute("DELETE FROM subscriptions WHERE id = ?1", [subscription_id])?;
        tx.commit()?;
        Ok((
            SubscriptionResult::Deleted {
                stream: stream.to_owned(),
                label: label.to_owned(),
            },
            subscription_id,
        ))
    }

    /// Ensures `stream` exists and binds `agent` to its own subscription
    /// labelled `agent` on `stream` in one transaction, reusing an earlier
    /// `follow`-created subscription for `agent` and refusing to hijack an
    /// unbound subscription claimed by another client.
    fn follow_stream(&mut self, agent: &str, stream: &str) -> Result<String, ToolError> {
        let tx = self.write()?;
        tx.execute(
            "INSERT INTO streams (name, created_at) VALUES (?1, ?2)
             ON CONFLICT (name) DO NOTHING",
            params![stream, now()],
        )?;
        let stream_id = stream_id(&tx, stream)?;
        let existing: Option<(i64, String, bool)> = tx
            .query_row(
                "SELECT subscriptions.id,
                        subscriptions.handle,
                        EXISTS (
                            SELECT 1 FROM agent_subscriptions
                            WHERE subscription_id = subscriptions.id AND agent = ?2
                        )
                 FROM subscriptions
                 WHERE subscriptions.stream_id = ?1 AND subscriptions.label = ?2",
                params![stream_id, agent],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (subscription_id, handle) = match existing {
            Some((subscription_id, handle, true)) => {
                tx.execute(
                    "UPDATE subscriptions SET last_active_at = ?2 WHERE id = ?1",
                    params![subscription_id, now()],
                )?;
                (subscription_id, handle)
            }
            Some((_, _, false)) => {
                return Err(ToolError::new(
                    ErrorCode::SubscriptionExists,
                    format!(
                        "stream {stream:?} already has an unbound subscription labelled {agent:?}; choose another stream or agent endpoint"
                    ),
                ));
            }
            None => {
                let floor: i64 = tx.query_row(
                    "SELECT COALESCE(
                         (SELECT MIN(sequence) - 1 FROM events WHERE stream_id = ?1),
                         last_sequence)
                     FROM streams WHERE id = ?1",
                    [stream_id],
                    |row| row.get(0),
                )?;
                let handle = random_id(HANDLE_PREFIX)?;
                let timestamp = now();
                tx.execute(
                    "INSERT INTO subscriptions (handle, stream_id, label, committed_sequence, created_at, last_active_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                    params![handle, stream_id, agent, floor, timestamp],
                )?;
                (tx.last_insert_rowid(), handle)
            }
        };
        tx.execute(
            "INSERT INTO agent_subscriptions (subscription_id, agent)
             VALUES (?1, ?2)
             ON CONFLICT (subscription_id) DO NOTHING",
            params![subscription_id, agent],
        )?;
        tx.execute(
            "INSERT INTO agents (name, subscription_id)
             VALUES (?1, ?2)
             ON CONFLICT (name) DO UPDATE SET subscription_id = excluded.subscription_id",
            params![agent, subscription_id],
        )?;
        tx.commit()?;
        Ok(handle)
    }

    fn ensure_subscription(&mut self, stream: &str, label: &str) -> Result<String, ToolError> {
        match self.create_stream(stream) {
            Ok(_) => {}
            Err(error) if error.code == ErrorCode::StreamExists => {}
            Err(error) => return Err(error),
        }
        let subscription = match self.create_subscription(stream, label) {
            Ok(created) => created,
            Err(error) if error.code == ErrorCode::SubscriptionExists => {
                self.recover_subscription(stream, label, None)?
            }
            Err(error) => return Err(error),
        };
        match subscription {
            SubscriptionResult::Created { handle, .. }
            | SubscriptionResult::Recovered { handle, .. } => Ok(handle),
            SubscriptionResult::Deleted { .. } | SubscriptionResult::Listed { .. } => Err(
                ToolError::internal("subscription creation returned an unexpected status"),
            ),
        }
    }

    fn publish(&mut self, publication: Publication<'_>) -> Result<Published, ToolError> {
        let Publication {
            stream,
            key,
            payload,
            sender_label,
            recipient_label,
            ack,
            digest,
        } = publication;
        let tx = self.write()?;
        let (stream_id, last_sequence): (i64, i64) = tx
            .query_row(
                "SELECT id, last_sequence FROM streams WHERE name = ?1",
                [stream],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| stream_not_found(stream))?;
        let prior: Option<(Vec<u8>, i64)> = tx
            .query_row(
                "SELECT request_digest, sequence FROM publishes
                 WHERE stream_id = ?1 AND idempotency_key = ?2",
                params![stream_id, key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((prior_digest, _)) = &prior
            && prior_digest != digest
        {
            return Err(ToolError::new(
                ErrorCode::IdempotencyConflict,
                format!(
                    "the idempotency key {key:?} was already used on stream {stream:?} for a different payload"
                ),
            ));
        }
        let mut acked_handle = None;
        let ack_result = match ack {
            None => None,
            Some(batch_id) => {
                let owner: Option<(i64, String, String)> = tx
                    .query_row(
                        "SELECT s.id, s.label, s.handle
                         FROM subscriptions s
                         WHERE s.stream_id = ?1
                           AND s.id = COALESCE(
                               (SELECT subscription_id FROM batches WHERE batch_id = ?2),
                               (SELECT b.subscription_id
                                FROM batch_aliases a
                                JOIN batches b ON b.id = a.batch_row
                                WHERE a.batch_id = ?2)
                           )",
                        params![stream_id, batch_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                let Some((subscription_id, owner_label, owner_handle)) = owner else {
                    return Err(ToolError::new(
                        ErrorCode::InvalidAck,
                        format!(
                            "{batch_id:?} is not a batch on stream {stream:?}; nothing was published or acknowledged"
                        ),
                    ));
                };
                if let Some(sender) = sender_label
                    && sender != owner_label
                {
                    return Err(ToolError::new(
                        ErrorCode::InvalidAck,
                        format!(
                            "{batch_id:?} belongs to subscription {owner_label:?}, not sender {sender:?}; nothing was published or acknowledged"
                        ),
                    ));
                }
                let ack_result = acknowledge(&tx, subscription_id, batch_id)?;
                tx.execute(
                    "UPDATE subscriptions SET last_active_at = ?2 WHERE id = ?1",
                    params![subscription_id, now()],
                )?;
                if matches!(ack_result.outcome, AckOutcome::Committed) {
                    acked_handle = Some(owner_handle);
                }
                Some(ack_result)
            }
        };
        if let Some((_, sequence)) = prior {
            let (message_id, published_at): (String, String) = tx.query_row(
                "SELECT message_id, published_at FROM events WHERE stream_id = ?1 AND sequence = ?2",
                params![stream_id, sequence],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.commit()?;
            return Ok(Published {
                result: PublishResult::Published {
                    stream: stream.to_owned(),
                    message_id,
                    sequence: to_u64(sequence),
                    published_at,
                    ack_result,
                },
                commit: None,
                acked_handle,
            });
        }
        let sequence = last_sequence + 1;
        let message_id = random_id(MESSAGE_PREFIX)?;
        let published_at = now();
        tx.execute(
            "INSERT INTO events (stream_id, sequence, message_id, published_at, payload, sender_label, recipient_label)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                stream_id,
                sequence,
                message_id,
                published_at,
                payload,
                sender_label,
                recipient_label
            ],
        )?;
        tx.execute(
            "UPDATE streams SET last_sequence = ?2 WHERE id = ?1",
            params![stream_id, sequence],
        )?;
        tx.execute(
            "INSERT INTO publishes (stream_id, idempotency_key, request_digest, sequence)
             VALUES (?1, ?2, ?3, ?4)",
            params![stream_id, key, digest, sequence],
        )?;
        tx.commit()?;
        Ok(Published {
            result: PublishResult::Published {
                stream: stream.to_owned(),
                message_id,
                sequence: to_u64(sequence),
                published_at,
                ack_result,
            },
            commit: Some((stream_id, to_u64(sequence))),
            acked_handle,
        })
    }

    /// Applies any ACK and, for a receiving call, takes a new ownership
    /// generation. A refused ACK changes nothing.
    fn admit(
        &mut self,
        handle: &str,
        ack: Option<&str>,
        take_ownership: bool,
    ) -> Result<Admission, ToolError> {
        let tx = self.write()?;
        let (subscription_id, stream_id): (i64, i64) = tx
            .query_row(
                "SELECT id, stream_id FROM subscriptions WHERE handle = ?1",
                [handle],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                ToolError::new(
                    ErrorCode::SubscriptionNotFound,
                    "no subscription has that handle; recover it by stream and label",
                )
            })?;
        let ack = match ack {
            None => None,
            Some(batch_id) => Some(acknowledge(&tx, subscription_id, batch_id)?),
        };
        let timestamp = now();
        let generation: i64 = if take_ownership {
            tx.query_row(
                "UPDATE subscriptions
                 SET owner_generation = owner_generation + 1, last_active_at = ?2
                 WHERE id = ?1 RETURNING owner_generation",
                params![subscription_id, timestamp],
                |row| row.get(0),
            )?
        } else {
            tx.query_row(
                "UPDATE subscriptions SET last_active_at = ?2
                 WHERE id = ?1 RETURNING owner_generation",
                params![subscription_id, timestamp],
                |row| row.get(0),
            )?
        };
        tx.commit()?;
        Ok(Admission {
            subscription_id,
            stream_id,
            handle: handle.to_owned(),
            generation,
            ack,
        })
    }

    /// Finds what this receive delivers: nothing if it no longer owns the
    /// subscription, else the pending batch, else a batch sealed from the
    /// backlog under `bounds`.
    fn select(
        &mut self,
        admission: &Admission,
        bounds: &Bounds,
        id: &RequestId,
    ) -> Result<Selection, ToolError> {
        let tx = self.write()?;
        let row: Option<(i64, i64, String)> = tx
            .query_row(
                "SELECT committed_sequence, owner_generation, label FROM subscriptions WHERE id = ?1",
                [admission.subscription_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((committed, generation, label)) = row else {
            return Ok(Selection::Superseded);
        };
        if generation != admission.generation {
            return Ok(Selection::Superseded);
        }
        if let Some(batch) = pending_batch(&tx, admission.subscription_id)? {
            return Ok(Selection::Pending(batch));
        }
        let backlog = Backlog {
            subscription_id: admission.subscription_id,
            stream_id: admission.stream_id,
            committed,
            handle: &admission.handle,
            label: &label,
        };
        let Some(batch) = seal(&tx, &backlog, bounds.max_events, bounds.max_bytes, id)
            .map_err(|error| error.tool_error(bounds.max_bytes, admission.ack.as_ref()))?
        else {
            advance_skipped(&tx, &backlog)?;
            tx.commit()?;
            return Ok(Selection::Empty);
        };
        tx.commit()?;
        Ok(Selection::Sealed(batch))
    }

    /// Snapshots the batch for sending and records the delivery attempt, in
    /// one transaction. A batch that was acknowledged meanwhile, or that does
    /// not fit this call's bounds, is not released and not counted.
    fn release(
        &mut self,
        admission: &Admission,
        batch: &Batch,
        wait: WaitReport,
        bounds: &Bounds,
        id: &RequestId,
        modern: bool,
    ) -> Result<ReceiveResult, ToolError> {
        let tx = self.write()?;
        let owner: Option<(String, i64)> = tx
            .query_row(
                "SELECT label, owner_generation FROM subscriptions WHERE id = ?1",
                [admission.subscription_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((label, _)) = owner.filter(|(_, generation)| *generation == admission.generation)
        else {
            return Ok(ReceiveResult::Superseded {
                ack_result: admission.ack.clone(),
                wait: WaitReport {
                    ended_by: crate::api::WaitEnd::Superseded,
                },
            });
        };
        let Some(contents) = batch_contents(&tx, admission.stream_id, &label, batch)? else {
            return Err(ToolError::new(
                ErrorCode::BatchNoLongerPending,
                "the batch was acknowledged by another call before it could be sent",
            ));
        };
        let result = ReceiveResult::Events {
            batch_id: batch.batch_id.clone(),
            delivery_attempt: contents.attempt,
            events: contents.events,
            has_more: contents.has_more,
            ack_result: admission.ack.clone(),
            wait,
            ack_after_handling: batch.batch_id.clone(),
            next_receive: bounds.next_receive(&admission.handle),
        };
        let needed = wire_bytes(&crate::json(&result), id, modern);
        if needed > bounds.max_bytes {
            return Err(exceeds_bounds(
                needed,
                bounds.max_bytes,
                admission.ack.as_ref(),
            ));
        }
        record_attempt(&tx, batch, contents.attempt)?;
        tx.commit()?;
        Ok(result)
    }

    /// Reads an inbox (decision 17) in one transaction: the pending batch is
    /// delivered again, or a new one is sealed from the backlog under the
    /// receive size rule, with its attempt recorded. Takes no ownership and
    /// acknowledges nothing.
    fn read_inbox(
        &mut self,
        handle: &str,
        uri: &str,
        id: &RequestId,
        modern: bool,
    ) -> Result<ReadResourceResult, ToolError> {
        self.read_batch(handle, id, Delivery::Deliver, RESULT_LIMIT, |read| {
            let result = inbox_result(uri, &read, modern);
            let needed = inbox_bytes(&result, id, modern);
            if needed > RESULT_LIMIT {
                return Err(exceeds_bounds(needed, RESULT_LIMIT, None));
            }
            Ok(result)
        })
    }

    /// The description's pending batch, sealing a new one from the backlog
    /// up to `max_bytes` when none is pending. With `extend`, a pending batch
    /// that has room for newer backlog grows to take it under a new batch id;
    /// its old id stays acknowledgeable for the events it carried.
    fn read_description(
        &mut self,
        handle: &str,
        id: &RequestId,
        max_bytes: u64,
        extend: bool,
    ) -> Result<Value, ToolError> {
        let delivery = if extend {
            Delivery::Extend
        } else {
            Delivery::Deliver
        };
        self.read_batch(handle, id, delivery, max_bytes, without_attempt)
    }

    /// The pending batch exactly as the description shows it, or
    /// `{"status":"empty"}` when none is pending. Seals nothing from the
    /// backlog and records no delivery attempt: it reads only what a listing
    /// has already shown.
    fn pending_description(&mut self, handle: &str) -> Result<Value, ToolError> {
        self.read_batch(
            handle,
            &RequestId::Number(0),
            Delivery::Peek,
            RESULT_LIMIT,
            without_attempt,
        )
    }
}

impl Store {
    fn read_batch<T>(
        &mut self,
        handle: &str,
        id: &RequestId,
        delivery: Delivery,
        max_bytes: u64,
        render: impl FnOnce(serde_json::Value) -> Result<T, ToolError>,
    ) -> Result<T, ToolError> {
        let tx = self.write()?;
        let (subscription_id, stream_id, committed, label): (i64, i64, i64, String) = tx
            .query_row(
                "SELECT id, stream_id, committed_sequence, label FROM subscriptions WHERE handle = ?1",
                [handle],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?
            .ok_or_else(inbox_not_found)?;
        let backlog = Backlog {
            subscription_id,
            stream_id,
            committed,
            handle,
            label: &label,
        };
        let batch = match (pending_batch(&tx, subscription_id)?, delivery) {
            (Some(batch), Delivery::Extend) => Some(
                extend(&tx, &backlog, batch, max_bytes, id)
                    .map_err(|error| error.tool_error(max_bytes, None))?,
            ),
            (Some(batch), _) => Some(batch),
            (None, Delivery::Peek) => None,
            (None, _) => seal(&tx, &backlog, u64::MAX, max_bytes, id)
                .map_err(|error| error.tool_error(max_bytes, None))?,
        };
        let Some(batch) = batch else {
            if delivery != Delivery::Peek {
                advance_skipped(&tx, &backlog)?;
                tx.commit()?;
            }
            return render(crate::json(&InboxRead::Empty));
        };
        let contents = batch_contents(&tx, stream_id, &label, &batch)?.ok_or_else(|| {
            ToolError::internal("the pending batch vanished inside its transaction")
        })?;
        let read = InboxRead::Events {
            batch_id: batch.batch_id.clone(),
            delivery_attempt: contents.attempt,
            events: contents.events,
            has_more: contents.has_more,
            ack_after_handling: batch.batch_id.clone(),
        };
        let result = render(crate::json(&read))?;
        if delivery != Delivery::Peek {
            record_attempt(&tx, &batch, contents.attempt)?;
            tx.commit()?;
        }
        Ok(result)
    }

    fn write(&mut self) -> Result<Transaction<'_>, ToolError> {
        Ok(self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?)
    }
}

fn list_streams_on(
    conn: &Connection,
    after: Option<&str>,
    limit: u64,
) -> Result<StreamResult, ToolError> {
    let mut statement = conn.prepare(
        "SELECT name FROM streams WHERE (?1 IS NULL OR name > ?1) ORDER BY name LIMIT ?2",
    )?;
    let mut streams = statement
        .query_map(params![after, limit + 1], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let mut more = streams.len() > limit as usize;
    if more {
        streams.pop();
    }
    loop {
        let next_after = more.then(|| streams.last().cloned()).flatten();
        let result = StreamResult::Listed {
            streams: streams.clone(),
            next_after,
        };
        if discovery_bytes(&result) <= RESULT_LIMIT {
            return Ok(result);
        }
        if streams.len() <= 1 {
            return Err(ToolError::internal(
                "a stream discovery entry exceeds the result byte limit",
            ));
        }
        streams.pop();
        more = true;
    }
}

fn list_subscriptions_on(
    conn: &Connection,
    stream: &str,
    after: Option<&str>,
    limit: u64,
    waiting: &HashSet<i64>,
) -> Result<SubscriptionResult, ToolError> {
    let stream_id = stream_id(conn, stream)?;
    let mut statement = conn.prepare(
        "SELECT id, label, handle, last_active_at, metadata FROM subscriptions
         WHERE stream_id = ?1 AND (?2 IS NULL OR label > ?2) ORDER BY label LIMIT ?3",
    )?;
    let raw_rows = statement
        .query_map(params![stream_id, after, limit + 1], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut subscriptions = Vec::with_capacity(raw_rows.len());
    for (id, label, handle, last_active_at, metadata) in raw_rows {
        subscriptions.push(SubscriptionEntry {
            label,
            handle,
            waiting: waiting.contains(&id),
            last_active_at,
            metadata: parse_metadata(metadata)?,
        });
    }
    let mut more = subscriptions.len() > limit as usize;
    if more {
        subscriptions.pop();
    }
    loop {
        let next_after = more
            .then(|| subscriptions.last().map(|entry| entry.label.clone()))
            .flatten();
        let result = SubscriptionResult::Listed {
            stream: stream.to_owned(),
            subscriptions: subscriptions.clone(),
            next_after,
        };
        if discovery_bytes(&result) <= RESULT_LIMIT {
            return Ok(result);
        }
        if subscriptions.len() <= 1 {
            return Err(ToolError::internal(
                "a subscription discovery entry exceeds the result byte limit",
            ));
        }
        subscriptions.pop();
        more = true;
    }
}

fn parse_metadata(raw: Option<String>) -> Result<Option<BTreeMap<String, Value>>, ToolError> {
    raw.map(|text| {
        serde_json::from_str(&text).map_err(|error| {
            ToolError::internal(format!("stored subscription metadata is not JSON: {error}"))
        })
    })
    .transpose()
}

fn agent_binding_on(
    conn: &Connection,
    agent: &str,
) -> Result<Option<(String, String, String)>, ToolError> {
    Ok(conn
        .query_row(
            "SELECT streams.name, subscriptions.label, subscriptions.handle
             FROM agents
             JOIN subscriptions ON subscriptions.id = agents.subscription_id
             JOIN streams ON streams.id = subscriptions.stream_id
             WHERE agents.name = ?1",
            [agent],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?)
}

fn inbox_unread_on(conn: &Connection, subscription_id: i64) -> Result<bool, ToolError> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM batches WHERE subscription_id = ?1 AND acked_at IS NULL)
             OR EXISTS (
                 SELECT 1 FROM events e
                 JOIN subscriptions s ON s.stream_id = e.stream_id
                 WHERE s.id = ?1
                   AND e.sequence > s.committed_sequence
                   AND (e.sender_label IS NULL OR e.sender_label != s.label)
                   AND (e.recipient_label IS NULL OR e.recipient_label = s.label)
             )",
        [subscription_id],
        |row| row.get(0),
    )?)
}

fn inbox_on(conn: &Connection, handle: &str) -> Result<(i64, i64), ToolError> {
    conn.query_row(
        "SELECT id, stream_id FROM subscriptions WHERE handle = ?1",
        [handle],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()?
    .ok_or_else(inbox_not_found)
}

/// What reading a subscription's batch may do.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Delivery {
    /// Show only an already-pending batch; record nothing.
    Peek,
    /// Deliver the pending batch, or seal one from the backlog.
    Deliver,
    /// As `Deliver`, and grow a pending batch to take newer backlog.
    Extend,
}

/// A batch as the description slot shows it: the attempt count stays out.
fn without_attempt(mut read: serde_json::Value) -> Result<serde_json::Value, ToolError> {
    read.as_object_mut()
        .expect("InboxRead serializes as an object")
        .remove("delivery_attempt");
    Ok(read)
}

fn configure(conn: &Connection) -> Result<(), String> {
    conn.busy_timeout(Duration::ZERO)
        .map_err(|error| error.to_string())?;
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(format!("SQLite kept journal mode {mode} instead of WAL"));
    }
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|error| error.to_string())?;
    let synchronous: i64 = conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if synchronous != 2 {
        return Err(format!(
            "SQLite kept synchronous={synchronous} instead of FULL (2)"
        ));
    }
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(|error| error.to_string())?;
    conn.execute_batch(SCHEMA)
        .map_err(|error| error.to_string())
}

/// The part of a stream a subscription has not yet committed past.
struct Backlog<'a> {
    subscription_id: i64,
    stream_id: i64,
    committed: i64,
    handle: &'a str,
    label: &'a str,
}

/// Advances `committed_sequence` past any events that were all filtered out
/// for this subscription's label (self-echo or addressed to another recipient).
fn advance_skipped(tx: &Transaction<'_>, backlog: &Backlog<'_>) -> Result<(), ToolError> {
    let head: i64 = tx.query_row(
        "SELECT last_sequence FROM streams WHERE id = ?1",
        [backlog.stream_id],
        |row| row.get(0),
    )?;
    if head > backlog.committed {
        tx.execute(
            "UPDATE subscriptions SET committed_sequence = ?2 WHERE id = ?1",
            params![backlog.subscription_id, head],
        )?;
    }
    Ok(())
}

/// A pending batch loaded for release.
struct Contents {
    events: Vec<EventEnvelope>,
    has_more: bool,
    /// The attempt this release records.
    attempt: u64,
}

enum SealError {
    Storage(ToolError),
    /// Even the next event alone needs this many bytes.
    TooLarge(u64),
}

impl SealError {
    fn tool_error(self, max_bytes: u64, ack: Option<&AckResult>) -> ToolError {
        match self {
            Self::Storage(error) => error,
            Self::TooLarge(needed) => exceeds_bounds(needed, max_bytes, ack),
        }
    }
}

impl From<ToolError> for SealError {
    fn from(error: ToolError) -> Self {
        Self::Storage(error)
    }
}

impl From<rusqlite::Error> for SealError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error.into())
    }
}

fn pending_batch(tx: &Transaction<'_>, subscription_id: i64) -> Result<Option<Batch>, ToolError> {
    Ok(tx
        .query_row(
            "SELECT id, batch_id FROM batches WHERE subscription_id = ?1 AND acked_at IS NULL",
            [subscription_id],
            |row| {
                Ok(Batch {
                    row_id: row.get(0)?,
                    batch_id: row.get(1)?,
                })
            },
        )
        .optional()?)
}

/// Seals the next batch from the backlog: as many events as fit in
/// `max_events` and, at their widest on any later delivery, in `max_bytes`.
/// Returns `None` when there is no backlog. The caller commits.
fn seal(
    tx: &Transaction<'_>,
    backlog: &Backlog<'_>,
    max_events: u64,
    max_bytes: u64,
    id: &RequestId,
) -> Result<Option<Batch>, SealError> {
    let batch_id = random_id(BATCH_PREFIX)?;
    let events = fit(
        tx,
        backlog,
        backlog.committed,
        max_events,
        max_bytes,
        &batch_id,
        id,
    )?;
    let (Some(first), Some(last)) = (events.first(), events.last()) else {
        return Ok(None);
    };
    tx.execute(
        "INSERT INTO batches (batch_id, subscription_id, first_sequence, last_sequence, sealed_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            batch_id,
            backlog.subscription_id,
            to_i64(first.sequence),
            to_i64(last.sequence),
            now()
        ],
    )?;
    Ok(Some(Batch {
        row_id: tx.last_insert_rowid(),
        batch_id,
    }))
}

/// Grows the pending batch to take as much newer backlog as fits in
/// `max_bytes`. A grown batch gets a new batch id; the old id becomes an
/// alias that acknowledges only the events it carried. The caller commits.
fn extend(
    tx: &Transaction<'_>,
    backlog: &Backlog<'_>,
    batch: Batch,
    max_bytes: u64,
    id: &RequestId,
) -> Result<Batch, SealError> {
    let (first, last): (i64, i64) = tx.query_row(
        "SELECT first_sequence, last_sequence FROM batches WHERE id = ?1",
        [batch.row_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let batch_id = random_id(BATCH_PREFIX)?;
    let events = fit(tx, backlog, first - 1, u64::MAX, max_bytes, &batch_id, id)?;
    let grown = events
        .last()
        .map(|event| to_i64(event.sequence))
        .filter(|&grown| grown > last);
    let Some(grown) = grown else {
        return Ok(batch);
    };
    tx.execute(
        "INSERT INTO batch_aliases (batch_id, batch_row, last_sequence) VALUES (?1, ?2, ?3)",
        params![batch.batch_id, batch.row_id, last],
    )?;
    tx.execute(
        "UPDATE batches SET batch_id = ?2, last_sequence = ?3 WHERE id = ?1",
        params![batch.row_id, batch_id, grown],
    )?;
    Ok(Batch {
        row_id: batch.row_id,
        batch_id,
    })
}

/// The events after `after` that are visible to `backlog.label`, fit in
/// `max_events` and, at their widest on any later delivery under `batch_id`,
/// in `max_bytes`.
fn fit(
    tx: &Transaction<'_>,
    backlog: &Backlog<'_>,
    after: i64,
    max_events: u64,
    max_bytes: u64,
    batch_id: &str,
    id: &RequestId,
) -> Result<Vec<EventEnvelope>, SealError> {
    let mut statement = tx.prepare(
        "SELECT sequence, message_id, published_at, payload FROM events
         WHERE stream_id = ?1
           AND sequence > ?2
           AND (sender_label IS NULL OR sender_label != ?3)
           AND (recipient_label IS NULL OR recipient_label = ?3)
         ORDER BY sequence",
    )?;
    let mut rows = statement.query(params![backlog.stream_id, after, backlog.label])?;
    let mut events = Vec::new();
    while u64::try_from(events.len()).unwrap_or(u64::MAX) < max_events {
        let Some(row) = rows.next()? else {
            break;
        };
        events.push(event(row)?);
        let needed = widest_batch_bytes(batch_id, &events, backlog.handle, id);
        if needed > max_bytes {
            events.pop();
            if events.is_empty() {
                return Err(SealError::TooLarge(needed));
            }
            break;
        }
    }
    Ok(events)
}

/// The batch's visible events and the attempt a release would record, or
/// `None` if the batch has been acknowledged.
fn batch_contents(
    tx: &Transaction<'_>,
    stream_id: i64,
    label: &str,
    batch: &Batch,
) -> Result<Option<Contents>, ToolError> {
    let (first, last, attempts, acked_at): (i64, i64, i64, Option<String>) = tx.query_row(
        "SELECT first_sequence, last_sequence, attempts, acked_at FROM batches WHERE id = ?1",
        [batch.row_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if acked_at.is_some() {
        return Ok(None);
    }
    let events = {
        let mut statement = tx.prepare(
            "SELECT sequence, message_id, published_at, payload FROM events
             WHERE stream_id = ?1
               AND sequence BETWEEN ?2 AND ?3
               AND (sender_label IS NULL OR sender_label != ?4)
               AND (recipient_label IS NULL OR recipient_label = ?4)
             ORDER BY sequence",
        )?;
        let mut rows = statement.query(params![stream_id, first, last, label])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(event(row)?);
        }
        events
    };
    let has_more: bool = tx.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM events
             WHERE stream_id = ?1
               AND sequence > ?2
               AND (sender_label IS NULL OR sender_label != ?3)
               AND (recipient_label IS NULL OR recipient_label = ?3)
         )",
        params![stream_id, last, label],
        |row| row.get(0),
    )?;
    Ok(Some(Contents {
        events,
        has_more,
        attempt: to_u64(attempts + 1),
    }))
}

fn record_attempt(tx: &Transaction<'_>, batch: &Batch, attempt: u64) -> Result<(), ToolError> {
    tx.execute(
        "UPDATE batches SET attempts = ?2 WHERE id = ?1",
        params![batch.row_id, to_i64(attempt)],
    )?;
    Ok(())
}

pub(crate) fn inbox_not_found() -> ToolError {
    ToolError::new(
        ErrorCode::SubscriptionNotFound,
        "no subscription has an inbox at that address",
    )
}

fn acknowledge(
    tx: &Transaction<'_>,
    subscription_id: i64,
    batch_id: &str,
) -> Result<AckResult, ToolError> {
    let batch: Option<(i64, i64, Option<String>)> = tx
        .query_row(
            "SELECT id, last_sequence, acked_at FROM batches WHERE batch_id = ?1 AND subscription_id = ?2",
            params![batch_id, subscription_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((row_id, last_sequence, acked_at)) = batch else {
        return acknowledge_alias(tx, subscription_id, batch_id);
    };
    let outcome = if acked_at.is_some() {
        AckOutcome::AlreadyAcked
    } else {
        tx.execute(
            "UPDATE batches SET acked_at = ?2 WHERE id = ?1",
            params![row_id, now()],
        )?;
        tx.execute(
            "UPDATE subscriptions SET committed_sequence = ?2 WHERE id = ?1",
            params![subscription_id, last_sequence],
        )?;
        AckOutcome::Committed
    };
    Ok(AckResult {
        batch_id: batch_id.to_owned(),
        outcome,
    })
}

/// Acknowledges a name a batch had before it grew: commits only through the
/// last event that name carried, and the batch keeps the rest pending under
/// its current name.
fn acknowledge_alias(
    tx: &Transaction<'_>,
    subscription_id: i64,
    batch_id: &str,
) -> Result<AckResult, ToolError> {
    let alias: Option<(i64, i64, i64)> = tx
        .query_row(
            "SELECT a.batch_row, a.last_sequence, s.committed_sequence
             FROM batch_aliases a
             JOIN batches b ON b.id = a.batch_row
             JOIN subscriptions s ON s.id = b.subscription_id
             WHERE a.batch_id = ?1 AND b.subscription_id = ?2",
            params![batch_id, subscription_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((batch_row, last_sequence, committed)) = alias else {
        return Err(ToolError::new(
            ErrorCode::InvalidAck,
            format!("{batch_id:?} is not a batch of this subscription; nothing was acknowledged"),
        ));
    };
    let outcome = if last_sequence <= committed {
        AckOutcome::AlreadyAcked
    } else {
        tx.execute(
            "UPDATE batches SET first_sequence = ?2 WHERE id = ?1",
            params![batch_row, last_sequence + 1],
        )?;
        tx.execute(
            "UPDATE subscriptions SET committed_sequence = ?2 WHERE id = ?1",
            params![subscription_id, last_sequence],
        )?;
        AckOutcome::Committed
    };
    Ok(AckResult {
        batch_id: batch_id.to_owned(),
        outcome,
    })
}

fn stream_id(conn: &Connection, name: &str) -> Result<i64, ToolError> {
    conn.query_row("SELECT id FROM streams WHERE name = ?1", [name], |row| {
        row.get(0)
    })
    .optional()?
    .ok_or_else(|| stream_not_found(name))
}

fn stream_not_found(name: &str) -> ToolError {
    ToolError::new(
        ErrorCode::StreamNotFound,
        format!("no stream is named {name:?}"),
    )
}

fn exceeds_bounds(required: u64, max_bytes: u64, ack: Option<&AckResult>) -> ToolError {
    let mut details = json!({"max_bytes": max_bytes, "required_bytes": required});
    if let Some(ack) = ack {
        details["ack_result"] = crate::json(ack);
    }
    ToolError::new(
        ErrorCode::BatchExceedsBounds,
        format!("the next batch needs {required} bytes but this receive allows {max_bytes}; nothing was delivered or acknowledged beyond any ack_result shown"),
    )
    .with_details(details)
}

fn event(row: &Row<'_>) -> Result<EventEnvelope, ToolError> {
    let sequence: i64 = row.get(0)?;
    let payload: String = row.get(3)?;
    Ok(EventEnvelope {
        message_id: row.get(1)?,
        sequence: to_u64(sequence),
        published_at: row.get(2)?,
        payload: serde_json::from_str(&payload).map_err(|error| {
            ToolError::internal(format!("a stored payload is not JSON: {error}"))
        })?,
    })
}

/// Sequences, attempts and generations are positive SQLite integers.
fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::WaitEnd;

    #[test]
    fn release_rechecks_ownership_before_recording_a_delivery() {
        for delete in [false, true] {
            let conn = Connection::open_in_memory().unwrap();
            conn.pragma_update(None, "foreign_keys", "ON").unwrap();
            conn.execute_batch(SCHEMA).unwrap();
            let mut store = Store { conn };
            store.create_stream("orders").unwrap();
            let SubscriptionResult::Created { handle, .. } =
                store.create_subscription("orders", "reader").unwrap()
            else {
                panic!("created subscription");
            };
            store
                .publish(Publication {
                    stream: "orders",
                    key: "first",
                    payload: "\"hello\"",
                    sender_label: None,
                    recipient_label: None,
                    ack: None,
                    digest: b"first",
                })
                .unwrap();
            let admission = store.admit(&handle, None, true).unwrap();
            let bounds = Bounds {
                max_events: 10,
                max_bytes: RESULT_LIMIT,
                wait: true,
            };
            let id = RequestId::Number(1);
            let Selection::Sealed(batch) = store.select(&admission, &bounds, &id).unwrap() else {
                panic!("sealed batch");
            };
            if delete {
                store.delete_subscription("orders", "reader").unwrap();
            } else {
                store.admit(&handle, None, true).unwrap();
            }
            let result = store
                .release(
                    &admission,
                    &batch,
                    WaitReport {
                        ended_by: WaitEnd::Backlog,
                    },
                    &bounds,
                    &id,
                    false,
                )
                .unwrap();
            assert!(matches!(result, ReceiveResult::Superseded { .. }));
            if !delete {
                let attempts: i64 = store
                    .conn
                    .query_row(
                        "SELECT attempts FROM batches WHERE id = ?1",
                        [batch.row_id],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(attempts, 0);
            }
        }
    }
}
