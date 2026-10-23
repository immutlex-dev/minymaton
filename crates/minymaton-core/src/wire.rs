//! The exact size of a tool result on the wire (decision 6).
//!
//! A result is measured as rmcp sends it: `CallToolResult::structured` (one
//! text block with the JSON, plus the same object as `structuredContent`),
//! inside the JSON-RPC response, inside one Server-Sent Event.

use rmcp::model::{
    CacheScope, CallToolResult, NumberOrString, ReadResourceResult, RequestId, ResourceContents,
    ServerJsonRpcMessage, ServerResult,
};
use serde_json::Value;

use crate::api::{
    AckOutcome, AckResult, EventEnvelope, NextReceive, ReceiveResult, WaitEnd, WaitReport,
};
use crate::ids::{BATCH_PREFIX, HANDLE_PREFIX, MESSAGE_PREFIX, SAMPLE_TIMESTAMP, sample_id};

/// Every receive result is at most 16 KiB on the wire, both copies and framing
/// included.
pub const RESULT_LIMIT: u64 = 16_384;

/// sse-stream writes a data-only event as `data: `, the message, then `\n\n`.
const SSE_FRAMING: u64 = 8;

/// A batch sealed now may be delivered again later to a request whose JSON-RPC
/// ID is longer than the current one. Sealing assumes IDs up to this many
/// characters, which covers numbers, UUIDs and ULIDs.
const ASSUMED_ID_CHARS: usize = 64;

pub(crate) fn discovery_bytes(result: &impl serde::Serialize) -> u64 {
    wire_bytes(
        &crate::json(result),
        &widest_id(&RequestId::Number(0)),
        true,
    )
}

/// Bytes on the wire for this result object, answering `id`. `modern` is a
/// 2026-07-28 exchange, whose results keep `resultType`.
pub(crate) fn wire_bytes(result: &Value, id: &RequestId, modern: bool) -> u64 {
    let result = ServerResult::CallToolResult(CallToolResult::structured(result.clone()));
    message_bytes(result, id, modern)
}

/// The `resources/read` result for an inbox: one JSON text item. A 2026-07-28
/// result says it must not be cached, because every commit changes the inbox.
pub(crate) fn inbox_result(uri: &str, content: &Value, modern: bool) -> ReadResourceResult {
    let item = ResourceContents::text(content.to_string(), uri).with_mime_type("application/json");
    let result = ReadResourceResult::new(vec![item]);
    if modern {
        result.with_ttl_ms(0).with_cache_scope(CacheScope::Private)
    } else {
        result
    }
}

/// Bytes on the wire for an inbox read result, answering `id`.
pub(crate) fn inbox_bytes(result: &ReadResourceResult, id: &RequestId, modern: bool) -> u64 {
    message_bytes(ServerResult::ReadResourceResult(result.clone()), id, modern)
}

fn message_bytes(mut result: ServerResult, id: &RequestId, modern: bool) -> u64 {
    if !modern {
        result.strip_result_type_for_legacy_peer();
    }
    let message = ServerJsonRpcMessage::response(result, id.clone());
    // A message that cannot be serialized cannot be sent, so it never fits.
    let json = serde_json::to_vec(&message).map_or(u64::MAX, |bytes| {
        u64::try_from(bytes.len()).unwrap_or(u64::MAX)
    });
    json.saturating_add(SSE_FRAMING)
}

/// The size a batch of these events can reach on any later delivery: every
/// field that varies between deliveries at its widest, and the wider of the
/// current ID and the assumed one.
pub(crate) fn widest_batch_bytes(
    batch_id: &str,
    events: &[EventEnvelope],
    handle: &str,
    id: &RequestId,
) -> u64 {
    let result = ReceiveResult::Events {
        batch_id: batch_id.to_owned(),
        delivery_attempt: u64::MAX,
        events: events.to_vec(),
        has_more: false,
        ack_result: Some(AckResult {
            batch_id: sample_id(BATCH_PREFIX),
            outcome: AckOutcome::AlreadyAcked,
        }),
        wait: WaitReport {
            ended_by: WaitEnd::PendingBatch,
        },
        ack_after_handling: batch_id.to_owned(),
        next_receive: NextReceive {
            handle: handle.to_owned(),
            max_events: u64::MAX,
            max_bytes: RESULT_LIMIT,
            wait: false,
        },
    };
    wire_bytes(&crate::json(&result), &widest_id(id), true)
}

/// The size a result carrying only this payload, from this publisher, can
/// reach on any delivery.
pub(crate) fn widest_single_event_bytes(payload: &Value, publisher: Option<&str>) -> u64 {
    let event = EventEnvelope {
        message_id: sample_id(MESSAGE_PREFIX),
        sequence: u64::MAX,
        published_at: SAMPLE_TIMESTAMP.to_owned(),
        payload: payload.clone(),
        publisher: publisher.map(str::to_owned),
    };
    widest_batch_bytes(
        &sample_id(BATCH_PREFIX),
        &[event],
        &sample_id(HANDLE_PREFIX),
        &NumberOrString::Number(0),
    )
}

fn widest_id(actual: &RequestId) -> RequestId {
    let assumed = NumberOrString::String("x".repeat(ASSUMED_ID_CHARS).into());
    if encoded_len(actual) > encoded_len(&assumed) {
        actual.clone()
    } else {
        assumed
    }
}

fn encoded_len(id: &RequestId) -> usize {
    serde_json::to_vec(id).map_or(0, |bytes| bytes.len())
}
