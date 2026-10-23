//! Tool errors: the model-visible failure results.

use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidArguments,
    StreamExists,
    StreamNotFound,
    /// The stream is private and the caller is not a member.
    StreamForbidden,
    /// Private streams need the server's bearer authentication.
    PrivateStreamNeedsAuth,
    SubscriptionExists,
    SubscriptionNotFound,
    IdempotencyConflict,
    /// The message a publish claims was already claimed.
    AlreadyClaimed,
    /// The message a publish claims is not on that stream.
    ClaimTargetNotFound,
    MessageTooLarge,
    InvalidAck,
    BatchExceedsBounds,
    BatchNoLongerPending,
    RequestCancelled,
    StorageFull,
    StorageBusy,
    StorageUnavailable,
    Internal,
}

/// A failed call, returned as a tool result with `isError: true`.
#[derive(Debug, Serialize)]
pub struct ToolError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ToolError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArguments, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|error| {
            Value::String(format!("tool error could not be serialized: {error}"))
        })
    }
}

impl From<rusqlite::Error> for ToolError {
    fn from(error: rusqlite::Error) -> Self {
        if let rusqlite::Error::SqliteFailure(failure, _) = &error {
            let code = match failure.code {
                rusqlite::ErrorCode::DiskFull => ErrorCode::StorageFull,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                    ErrorCode::StorageBusy
                }
                rusqlite::ErrorCode::SystemIoFailure
                | rusqlite::ErrorCode::ReadOnly
                | rusqlite::ErrorCode::CannotOpen => ErrorCode::StorageUnavailable,
                _ => ErrorCode::Internal,
            };
            return Self::new(code, format!("storage failed: {error}"))
                .with_details(serde_json::json!({"sqlite_extended_code": failure.extended_code}));
        }
        Self::internal(format!("storage failed: {error}"))
    }
}
