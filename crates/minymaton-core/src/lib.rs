//! minymaton's queue: the receive wait over a pluggable durable backend
//! ([`QueueStore`] and [`QueueReader`], with SQLite as [`Store`] and
//! [`Reader`]), and the exact wire size of the MCP results it returns.

pub mod api;
mod error;
mod ids;
mod queue;
mod signals;
mod store;
mod wire;

pub use error::{ErrorCode, ToolError};
pub use ids::{INBOX_PREFIX, inbox_uri};
pub use queue::{Admitted, Delivery, Inbox, Queue, Waiter};
pub use store::{
    Admission, Batch, Bounds, Publication, Published, QueueReader, QueueStore, Reader, Selection,
    Store,
};
pub use wire::RESULT_LIMIT;

/// Results are plain JSON built from the `api` types, which always serialize.
pub(crate) fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).expect("api types serialize to JSON")
}
