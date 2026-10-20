//! The tools' arguments and results. These types are the one contract: every
//! result is serialized from them and every advertised schema is generated
//! from them.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(extend("type" = "object"), transform = flat_action_schema)]
pub enum StreamArgs {
    Create {
        /// The stream's name: 1 to 128 bytes, without control characters.
        name: String,
    },
    List {
        /// Return names strictly after this name, from a previous page.
        after: Option<String>,
        /// Maximum page size, 1 to 100. Defaults to 100; byte bounds may return fewer.
        limit: Option<u64>,
    },
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StreamResult {
    /// The stream now exists.
    Created { stream: String },
    Listed {
        streams: Vec<String>,
        next_after: Option<String>,
    },
}

/// Where a newly created subscription starts in the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartAt {
    /// Start before the oldest retained event in the stream.
    Earliest,
    /// Start at the current head of the stream, skipping prior history.
    Latest,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(extend("type" = "object"), transform = flat_action_schema)]
pub enum SubscriptionArgs {
    Create {
        /// The existing stream to join. Defaults to agents.
        #[serde(default = "default_stream")]
        stream: String,
        /// An assigned unique label. Omit to atomically claim the smallest free agent-N.
        label: Option<String>,
        /// Where the subscription starts: "latest" (default when omitting label)
        /// or "earliest" (default when specifying label).
        start_at: Option<StartAt>,
        /// Optional identity metadata (for example `{"harness": "...", "model": "...", "host": "..."}`),
        /// returned by `subscription` `list` so the subscription list is the complete live roster.
        metadata: Option<BTreeMap<String, Value>>,
    },
    Recover {
        #[serde(default = "default_stream")]
        stream: String,
        label: String,
        /// Optional updated identity metadata for this subscription.
        metadata: Option<BTreeMap<String, Value>>,
    },
    Delete {
        #[serde(default = "default_stream")]
        stream: String,
        /// The subscription label to retire and remove from the stream.
        label: String,
    },
    List {
        #[serde(default = "default_stream")]
        stream: String,
        /// Return labels strictly after this label, from a previous page.
        after: Option<String>,
        /// Maximum page size, 1 to 100. Defaults to 100; byte bounds may return fewer.
        limit: Option<u64>,
    },
}

// Some MCP clients retain only the first branch of a top-level oneOf.
// Keep the advertised fields derived from the enums; serde still validates
// each action's required fields and rejects fields belonging to other actions.
fn flat_action_schema(schema: &mut schemars::Schema) {
    let object = schema.ensure_object();
    let variants = object.remove("oneOf").expect("action schema has variants");
    let mut properties = serde_json::Map::new();
    let mut actions = Vec::new();
    for variant in variants.as_array().expect("action variants are an array") {
        let fields = variant["properties"]
            .as_object()
            .expect("action variant has properties");
        let action = fields["action"]["const"]
            .as_str()
            .expect("action variant has a string constant");
        actions.push(Value::from(action));
        for (name, field) in fields {
            if name != "action" {
                let required = variant["required"]
                    .as_array()
                    .expect("action variant has required fields")
                    .contains(&Value::from(name.clone()));
                let mut description = format!(
                    "{} for {action}.",
                    if required { "Required" } else { "Optional" }
                );
                if let Some(text) = field.get("description") {
                    description.push(' ');
                    description.push_str(text.as_str().expect("field description is text"));
                }
                match properties.entry(name.clone()) {
                    serde_json::map::Entry::Vacant(entry) => {
                        let mut property = field.clone();
                        property["description"] = Value::String(description);
                        entry.insert(property);
                    }
                    serde_json::map::Entry::Occupied(mut entry) => {
                        let property = entry.get_mut();
                        let previous = property["description"]
                            .as_str()
                            .expect("merged field has a description");
                        property["description"] =
                            Value::String(format!("{previous}\n{description}"));
                    }
                }
            }
        }
    }
    properties.insert(
        "action".to_owned(),
        serde_json::json!({"type": "string", "enum": actions}),
    );
    object.insert("properties".to_owned(), Value::Object(properties));
    object.insert("required".to_owned(), serde_json::json!(["action"]));
    object.insert("additionalProperties".to_owned(), Value::Bool(false));
}

pub const DEFAULT_STREAM: &str = "agents";

fn default_stream() -> String {
    DEFAULT_STREAM.to_owned()
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SubscriptionEntry {
    pub label: String,
    pub handle: String,
    /// Point-in-time, process-local observation of an active receive wait guard.
    /// Not a durable reservation or proof that the remote client is alive.
    pub waiting: bool,
    /// RFC 3339 timestamp of the subscription's latest create, receive, or acknowledge.
    pub last_active_at: String,
    /// Optional identity metadata attached to the subscription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SubscriptionResult {
    /// A new subscription on the stream.
    Created {
        handle: String,
        stream: String,
        label: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<BTreeMap<String, Value>>,
    },
    /// The existing subscription with that label.
    Recovered {
        handle: String,
        stream: String,
        label: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<BTreeMap<String, Value>>,
    },
    /// The subscription was deleted and its label is free again.
    Deleted { stream: String, label: String },
    Listed {
        stream: String,
        subscriptions: Vec<SubscriptionEntry>,
        next_after: Option<String>,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FollowArgs {
    /// The stream whose batches this agent's tool list carries from now on.
    /// It is created if it does not exist.
    pub stream: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FollowResult {
    /// The agent's tool list now carries this subscription's pending batch.
    /// The subscription is labelled with the agent's name.
    Following {
        agent: String,
        stream: String,
        label: String,
        handle: String,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublishArgs {
    /// The stream to publish to.
    pub stream: String,
    /// A fresh key for each message, 1 to 128 bytes. Retrying with the same key
    /// and payload returns the original result instead of publishing twice.
    pub idempotency_key: String,
    /// The message: any JSON value.
    pub payload: Value,
    /// Optional sender subscription label; suppresses self-echo to that subscription.
    /// If omitted, top-level `payload.from` (when a valid label string) is used.
    pub from: Option<String>,
    /// Optional recipient subscription label; delivers only to that subscription.
    /// If omitted, top-level `payload.to` (when a valid non-broadcast label string) is used.
    pub to: Option<String>,
    /// Optional `ack_after_handling` batch ID from a handled receive batch on
    /// this stream, acknowledged atomically in the same transaction as this publish.
    pub ack: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PublishResult {
    /// The message is durably committed.
    Published {
        stream: String,
        message_id: String,
        sequence: u64,
        published_at: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack_result: Option<AckResult>,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReceiveArgs {
    /// The subscription handle.
    pub handle: String,
    /// The most events to return, at least 1. Use 0 together with `ack`, and
    /// without `max_bytes` or `wait`, to acknowledge without receiving.
    pub max_events: u64,
    /// The largest result to return, in bytes. Required unless `max_events` is
    /// 0; values above 16384 act as 16384.
    pub max_bytes: Option<u64>,
    /// Whether to wait for an event commit when nothing is currently waiting.
    /// Defaults to true; pass false for an immediate non-blocking check.
    pub wait: Option<bool>,
    /// The previous batch's `ack_after_handling`, once every event in it has
    /// been handled.
    pub ack: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReceiveResult {
    /// A batch of events. Handle all of them, then acknowledge the batch.
    Events {
        batch_id: String,
        /// 1 on first delivery; higher when the batch is delivered again
        /// because it was not acknowledged.
        delivery_attempt: u64,
        events: Vec<EventEnvelope>,
        /// Whether more events were waiting after this batch.
        has_more: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack_result: Option<AckResult>,
        wait: WaitReport,
        /// Pass this as `ack` on your next receive after handling every event.
        ack_after_handling: String,
        /// The arguments for your next receive; add `ack` yourself.
        next_receive: NextReceive,
    },
    /// Nothing was waiting when checked with `wait: false`.
    Empty {
        #[serde(skip_serializing_if = "Option::is_none")]
        ack_result: Option<AckResult>,
        wait: WaitReport,
        /// The arguments for your next receive.
        next_receive: NextReceive,
    },
    /// The acknowledgement was applied; nothing was received.
    Acked { ack_result: AckResult },
    /// A newer receive on the same subscription took over this one, or the
    /// subscription was deleted while waiting.
    Superseded {
        #[serde(skip_serializing_if = "Option::is_none")]
        ack_result: Option<AckResult>,
        wait: WaitReport,
    },
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct EventEnvelope {
    pub message_id: String,
    pub sequence: u64,
    pub published_at: String,
    /// The published message, exactly as it was published.
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct AckResult {
    pub batch_id: String,
    pub outcome: AckOutcome,
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AckOutcome {
    /// This acknowledgement committed the batch.
    Committed,
    /// The batch had already been acknowledged.
    AlreadyAcked,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct WaitReport {
    pub ended_by: WaitEnd,
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WaitEnd {
    /// An unacknowledged batch was waiting and is delivered again.
    PendingBatch,
    /// Events were already waiting when the call arrived.
    Backlog,
    /// The wait was woken by the commit of new events.
    Commit,
    /// `wait: false` checked immediately with nothing to deliver.
    NoWait,
    /// A newer receive on the same subscription took over, or the subscription was deleted.
    Superseded,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct NextReceive {
    pub handle: String,
    pub max_events: u64,
    pub max_bytes: u64,
    pub wait: bool,
}

/// What the `ack` tool returns (combined delivery, 2026-10-02).
#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AckToolResult {
    /// The pending batch was acknowledged. It is repeated here, because the
    /// description that showed it now shows the next batch.
    Acked {
        ack_result: AckResult,
        /// The acknowledged batch, exactly as the description showed it.
        acknowledged_batch: Value,
    },
    /// No batch was pending, so nothing was acknowledged.
    Empty,
}

/// What `resources/read` of an inbox returns (decision 17).
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InboxRead {
    /// The subscription's pending batch. Handle every event, then acknowledge
    /// the batch with `receive`; reading acknowledges nothing.
    Events {
        batch_id: String,
        /// 1 on first delivery; higher each time the batch is delivered again.
        delivery_attempt: u64,
        events: Vec<EventEnvelope>,
        /// Whether more events were waiting after this batch.
        has_more: bool,
        /// Pass this as `ack` to `receive` after handling every event.
        ack_after_handling: String,
    },
    /// Nothing to read.
    Empty,
}
