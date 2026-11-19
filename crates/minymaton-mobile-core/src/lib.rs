//! The Rust core shared by the minymaton mobile apps.
//!
//! Every platform drives [`MobileCore`] through one JSON surface:
//! [`MobileCore::request`] takes a command and returns
//! `{"ok":true,"result":...}` or `{"ok":false,"error":{"code","message"}}`,
//! and the listener given to [`MobileCore::open`] receives event JSON from
//! the receive waits. The Android bridge lives in `android`, the iOS bridge in `ios`.

mod mcp_client;
mod session;
mod storage;

#[cfg(target_os = "android")]
mod android;

#[cfg(target_os = "ios")]
mod ios;

use std::fmt::Display;

use serde::Serialize;

pub use mcp_client::Endpoint;
pub use session::{Listener, MobileCore};

/// A failure reported to the app: a stable snake_case `code`, the bus's own
/// tool error codes included, and a message that never carries a credential.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CoreError {
    pub code: String,
    pub message: String,
}

impl CoreError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
        }
    }

    fn protocol(message: &str) -> Self {
        Self::new("protocol_error", message)
    }

    fn transport(context: &str, error: &dyn Display) -> Self {
        Self::new("transport_failed", format!("{context}: {error}"))
    }

    fn redact(mut self, secrets: &[String]) -> Self {
        for secret in secrets {
            self.message = self.message.replace(secret.as_str(), "[redacted]");
        }
        self
    }
}

impl Display for CoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CoreError {}
