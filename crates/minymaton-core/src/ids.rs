//! Opaque identifiers and timestamps.

use std::time::SystemTime;

use crate::error::ToolError;

pub(crate) const HANDLE_PREFIX: &str = "sub_";
pub(crate) const BATCH_PREFIX: &str = "bat_";
pub(crate) const MESSAGE_PREFIX: &str = "msg_";

/// Each subscription's inbox is the resource at this prefix plus its handle.
pub const INBOX_PREFIX: &str = "minymaton://inbox/";

/// The address of a subscription's inbox.
pub fn inbox_uri(handle: &str) -> String {
    format!("{INBOX_PREFIX}{handle}")
}

/// The handle an inbox address names, if it names one.
pub(crate) fn inbox_handle(uri: &str) -> Option<&str> {
    uri.strip_prefix(INBOX_PREFIX)
        .filter(|handle| handle.starts_with(HANDLE_PREFIX) && !handle.contains('/'))
}

const RANDOM_BYTES: usize = 16;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// An unpredictable identifier: the prefix and 128 bits from the operating
/// system's random number generator, in hex.
pub(crate) fn random_id(prefix: &str) -> Result<String, ToolError> {
    let mut bytes = [0u8; RANDOM_BYTES];
    getrandom::fill(&mut bytes).map_err(|error| {
        ToolError::internal(format!("the OS random number generator failed: {error}"))
    })?;
    let mut id = String::with_capacity(prefix.len() + RANDOM_BYTES * 2);
    id.push_str(prefix);
    for byte in bytes {
        id.push(char::from(HEX[usize::from(byte >> 4)]));
        id.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(id)
}

/// An identifier of exactly the length `random_id` produces.
pub(crate) fn sample_id(prefix: &str) -> String {
    format!("{prefix}{}", "0".repeat(RANDOM_BYTES * 2))
}

/// The current time in RFC 3339 with milliseconds, always 24 characters.
pub(crate) fn now() -> String {
    humantime::format_rfc3339_millis(SystemTime::now()).to_string()
}

/// A timestamp of exactly the length `now` produces.
pub(crate) const SAMPLE_TIMESTAMP: &str = "0000-00-00T00:00:00.000Z";
