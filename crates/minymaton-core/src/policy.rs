//! The queue's decisions about private streams, bound channels, budgets,
//! enrollment transitions and claims, made once for every backend. A backend
//! gathers the facts inside its own command, asks these functions, and writes
//! what they decide, so a decision and the write it allows commit together
//! whatever the storage is.

use serde_json::{Value, json};

use crate::api::{DEFAULT_STREAM, StreamResult};
use crate::error::{ErrorCode, ToolError};

/// Prefix of the sole stored member marker for a provisioned bound channel.
/// Because caller and member names forbid control characters, no enrolled
/// caller or user-supplied member can ever equal a channel marker.
pub const CHANNEL_MARKER_PREFIX: char = '\x1f';

/// Tombstone marker written as a stream's sole member when `--retire-channel`
/// fences a bound channel in Phase 1 before deleting its batches and events in
/// Phase 2 (`Sol RR-07`). Because it begins with [`CHANNEL_MARKER_PREFIX`] and
/// has an empty channel suffix, [`is_channel_marker`] counts it toward
/// `max_bound_channels`, no valid [`Access`] can match it (`Access::is_valid`
/// rejects empty channel names), [`provision`] refuses to re-provision it until
/// retirement finishes, and [`check_retire_channel`] allows an interrupted
/// `--retire-channel` to resume and complete cleanup.
pub const RETIRING_CHANNEL_MARKER: &str = "\x1f";

pub const MAX_RETAINED_EVENTS_PER_CHANNEL: u64 = 1024;
pub const MAX_SUBSCRIPTIONS_PER_CHANNEL: u64 = 16;
pub const MAX_BOUND_CHANNELS: usize = 256;
pub const MAX_ENROLLED_TOKENS: usize = 1024;
pub const MAX_SESSIONS_PER_PRINCIPAL: usize = 16;
pub const MAX_CONCURRENT_REQUESTS_PER_PRINCIPAL: usize = 16;

/// The authenticated caller and optional bound channel of a request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Access {
    /// The caller's enrolled name, when bearer authentication is active.
    pub caller: Option<String>,
    /// The private channel this principal is bound to (`CALLER\tTOKEN\tCHANNEL`),
    /// or `None` for unscoped principals (`CALLER\tTOKEN`).
    pub channel: Option<String>,
    /// SHA-256 digest of the bearer token that authenticated this request.
    pub token_digest: Option<[u8; 32]>,
    /// SHA-256 digest of the active bearer token enrollment snapshot on the
    /// authenticating gateway, verified inside storage transactions (`O-R4-2`).
    pub enrollment_digest: Option<[u8; 32]>,
}

impl Access {
    pub fn unauthenticated() -> Self {
        Self::default()
    }

    pub fn caller(caller: impl Into<String>) -> Self {
        Self {
            caller: Some(caller.into()),
            channel: None,
            token_digest: None,
            enrollment_digest: None,
        }
    }

    pub fn bound(caller: impl Into<String>, channel: impl Into<String>) -> Self {
        Self {
            caller: Some(caller.into()),
            channel: Some(channel.into()),
            token_digest: None,
            enrollment_digest: None,
        }
    }

    pub fn with_digest(mut self, digest: [u8; 32]) -> Self {
        self.token_digest = Some(digest);
        self
    }

    pub fn with_enrollment_digest(mut self, digest: [u8; 32]) -> Self {
        self.enrollment_digest = Some(digest);
        self
    }

    /// A channel-bound principal must also carry a non-empty caller identity.
    pub fn is_valid(&self) -> bool {
        match (&self.caller, &self.channel) {
            (None, Some(_)) => false,
            (Some(caller), _) if caller.is_empty() => false,
            (_, Some(channel)) if channel.is_empty() => false,
            _ => true,
        }
    }
}

/// Resource limits enforced on channel-bound streams and channel provisioning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_retained_events_per_channel: u64,
    pub max_subscriptions_per_channel: u64,
    pub max_bound_channels: usize,
    pub max_enrolled_tokens: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_retained_events_per_channel: MAX_RETAINED_EVENTS_PER_CHANNEL,
            max_subscriptions_per_channel: MAX_SUBSCRIPTIONS_PER_CHANNEL,
            max_bound_channels: MAX_BOUND_CHANNELS,
            max_enrolled_tokens: MAX_ENROLLED_TOKENS,
        }
    }
}

/// A stream's members, as one command sees them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Members {
    /// How many callers are members; an open stream has none.
    pub count: usize,
    /// Whether the calling principal's membership key is one of them.
    pub includes_caller: bool,
}

/// A change to a private stream's members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Membership {
    Grant,
    Revoke,
}

/// What a backend writes for a membership change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberWrite {
    Insert,
    Delete,
    Nothing,
}

/// What a backend writes when provisioning a bound channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProvisionWrite {
    Create { marker: String },
    Nothing,
}

/// The first claim of a message, as stored.
#[derive(Clone, Debug)]
pub struct Claim {
    pub claim_message_id: String,
    pub claimed_by: String,
    pub publisher: Option<String>,
}

/// The reserved sole member stored for a provisioned bound channel.
pub fn channel_marker(channel: &str) -> String {
    format!("{CHANNEL_MARKER_PREFIX}{channel}")
}

/// The membership key checked in `stream_members` / `member:<stream_id>:<key>`
/// for `access`. Returns `None` if `access` is unauthenticated or invalid.
pub fn member_key(access: &Access) -> Option<String> {
    if !access.is_valid() {
        return None;
    }
    match (&access.caller, &access.channel) {
        (Some(_), Some(channel)) => Some(channel_marker(channel)),
        (Some(caller), None) => Some(caller.clone()),
        (None, _) => None,
    }
}

/// The durable agent-binding key for `agent` under `access`.
/// Unscoped principals use `agent`; channel-bound principals use
/// `"{channel}\x1f{agent}"`.
pub fn agent_key(access: &Access, agent: &str) -> String {
    match &access.channel {
        Some(channel) => format!("{channel}{CHANNEL_MARKER_PREFIX}{agent}"),
        None => agent.to_owned(),
    }
}

/// The default stream for `access` when a tool call omits `stream`.
pub fn default_stream(access: &Access) -> &str {
    access.channel.as_deref().unwrap_or(DEFAULT_STREAM)
}

/// Resolves an optional `stream` argument for `access` and enforces that a
/// channel-bound principal only targets its own bound channel.
pub fn resolve_stream<'a>(
    stream: Option<&'a str>,
    access: &'a Access,
) -> Result<&'a str, ToolError> {
    if !access.is_valid() {
        return Err(stream_forbidden(access));
    }
    let resolved = stream.unwrap_or_else(|| default_stream(access));
    if let Some(bound) = access.channel.as_deref()
        && resolved != bound
    {
        return Err(stream_forbidden(access));
    }
    Ok(resolved)
}

/// `stream_forbidden` unless `access` is permitted to use a stream with `members`.
pub fn admit(members: Members, access: &Access) -> Result<(), ToolError> {
    if !access.is_valid() {
        return Err(stream_forbidden(access));
    }
    if access.channel.is_some() {
        if members.count != 1 || !members.includes_caller {
            return Err(stream_forbidden(access));
        }
        return Ok(());
    }
    if members.count > 0 && !members.includes_caller {
        return Err(stream_forbidden(access));
    }
    Ok(())
}

/// Whether a stream with these members is listed for `access`.
pub fn listed(members: Members, access: &Access) -> bool {
    if !access.is_valid() {
        return false;
    }
    if access.channel.is_some() {
        return members.count == 1 && members.includes_caller;
    }
    members.count == 0 || members.includes_caller
}

/// The members a new stream starts with: none for an open stream, else the
/// named callers and the creating one. Channel-bound principals cannot create
/// streams.
pub fn initial_members(
    access: &Access,
    members: Option<&[String]>,
) -> Result<Vec<String>, ToolError> {
    if !access.is_valid() || access.channel.is_some() {
        return Err(stream_forbidden(access));
    }
    let Some(members) = members else {
        return Ok(Vec::new());
    };
    let creator = access
        .caller
        .as_deref()
        .ok_or_else(private_stream_needs_auth)?;
    let mut all: Vec<String> = members.to_vec();
    all.push(creator.to_owned());
    all.sort();
    all.dedup();
    Ok(all)
}

/// Decides a grant or revoke of `member` by `access` on stream `name`, whose
/// members are `members` and of which `member` is one when `target_is_member`.
pub fn change_member(
    name: &str,
    members: Members,
    access: &Access,
    member: &str,
    target_is_member: bool,
    change: Membership,
) -> Result<(MemberWrite, StreamResult), ToolError> {
    if !access.is_valid() || access.channel.is_some() {
        return Err(stream_forbidden(access));
    }
    if members.count == 0 {
        return Err(ToolError::invalid(format!(
            "stream {name:?} is open to every caller; only a stream created with members has members to grant or revoke"
        )));
    }
    admit(members, access)?;
    let stream = name.to_owned();
    let caller = member.to_owned();
    Ok(match (change, target_is_member) {
        (Membership::Grant, false) => (
            MemberWrite::Insert,
            StreamResult::Granted { stream, caller },
        ),
        (Membership::Grant, true) => (
            MemberWrite::Nothing,
            StreamResult::Granted { stream, caller },
        ),
        (Membership::Revoke, true) if members.count == 1 => {
            return Err(ToolError::invalid(format!(
                "{member:?} is the last member of private stream {name:?}; grant another caller before revoking it"
            )));
        }
        (Membership::Revoke, true) => (
            MemberWrite::Delete,
            StreamResult::Revoked { stream, caller },
        ),
        (Membership::Revoke, false) => (
            MemberWrite::Nothing,
            StreamResult::Revoked { stream, caller },
        ),
    })
}

/// Decides whether bound channel `name` may be provisioned given its existing
/// stream membership (`None` if no stream named `name` exists, or `Some(members)`
/// if it already exists).
pub fn provision(
    name: &str,
    existing: Option<&[String]>,
    bound_channels: usize,
    limits: Limits,
) -> Result<ProvisionWrite, ToolError> {
    let marker = channel_marker(name);
    match existing {
        None => {
            if bound_channels >= limits.max_bound_channels {
                return Err(ToolError::new(
                    ErrorCode::StorageFull,
                    format!(
                        "cannot provision channel {name:?}: the server has reached its bound channel limit ({}); retire an unused channel with --retire-channel first",
                        limits.max_bound_channels
                    ),
                ));
            }
            Ok(ProvisionWrite::Create { marker })
        }
        Some([sole]) if sole == &marker => Ok(ProvisionWrite::Nothing),
        Some(_) => Err(ToolError::new(
            ErrorCode::StreamExists,
            format!(
                "stream {name:?} already exists and is not bound to channel {name:?}; \
                 bound channels cannot adopt an existing open or unscoped stream"
            ),
        )),
    }
}

/// Decides whether bound channel `name` may be retired given its existing
/// stream membership. Only streams carrying `channel_marker(name)` (or the
/// in-progress [`RETIRING_CHANNEL_MARKER`] tombstone from an interrupted
/// retirement) may be retired with `--retire-channel`.
pub fn check_retire_channel(name: &str, existing: &[String]) -> Result<(), ToolError> {
    let marker = channel_marker(name);
    if existing == [marker] || existing == [RETIRING_CHANNEL_MARKER] {
        Ok(())
    } else {
        Err(ToolError::invalid(format!(
            "stream {name:?} is not a bound channel; only bound channels can be retired with --retire-channel"
        )))
    }
}

/// Enforces the per-channel retained-event cap for channel-bound principals.
pub fn check_event_budget(
    stream: &str,
    access: &Access,
    retained_events: usize,
    limits: Limits,
) -> Result<(), ToolError> {
    if access.channel.is_some() && (retained_events as u64) >= limits.max_retained_events_per_channel
    {
        return Err(ToolError::new(
            ErrorCode::StorageFull,
            format!(
                "channel {stream:?} has reached its retained event limit ({}); retire the channel with --retire-channel to reclaim space",
                limits.max_retained_events_per_channel
            ),
        ));
    }
    Ok(())
}

/// Enforces the per-channel subscription cap for channel-bound principals.
pub fn check_subscription_budget(
    stream: &str,
    access: &Access,
    subscriptions: usize,
    limits: Limits,
) -> Result<(), ToolError> {
    if access.channel.is_some() && (subscriptions as u64) >= limits.max_subscriptions_per_channel {
        return Err(ToolError::new(
            ErrorCode::StorageFull,
            format!(
                "channel {stream:?} has reached its subscription limit ({}); delete unused subscriptions before creating more",
                limits.max_subscriptions_per_channel
            ),
        ));
    }
    Ok(())
}

/// Checks whether the stored active enrollment digest may transition from
/// `expected` to `new_digest`, returning `Ok(true)` when the stored digest
/// should be written and announced.
pub fn check_enrollment_transition(
    stored: Option<[u8; 32]>,
    expected: Option<[u8; 32]>,
    new_digest: [u8; 32],
) -> Result<bool, ToolError> {
    match (stored, expected) {
        (Some(current), None) if current == new_digest => Ok(false),
        (Some(current), Some(exp)) if current == exp => Ok(current != new_digest),
        (None, None) => Ok(true),
        (Some(_), None) => Err(ToolError::new(
            ErrorCode::AuthorityFenced,
            "an active enrollment digest is already set; pass --activate-enrollment with the current digest to transition enrollment",
        )),
        (Some(_), Some(_)) => Err(ToolError::new(
            ErrorCode::AuthorityFenced,
            "the stored active enrollment digest does not match the expected predecessor",
        )),
        (None, Some(_)) => Err(ToolError::new(
            ErrorCode::AuthorityFenced,
            "no enrollment digest is stored yet, so the expected predecessor did not match",
        )),
    }
}

/// Authoritative transactional enrollment fence (`O-R4-2`): when a request
/// carries an expected `enrollment_digest` from its authenticating gateway,
/// the storage transaction verifies that the stored active enrollment digest
/// matches before reading or committing state.
pub fn check_active_enrollment(
    stored: Option<[u8; 32]>,
    access: &Access,
) -> Result<(), ToolError> {
    if let Some(expected) = access.enrollment_digest
        && stored != Some(expected)
    {
        return Err(ToolError::new(
            ErrorCode::AuthorityFenced,
            "gateway bearer token enrollment does not match active cluster enrollment",
        ));
    }
    Ok(())
}

/// Decides a claim of `claimed` on `stream`: the message must be there and
/// not yet claimed. Returns the label the claim is made for.
pub fn claim<'a>(
    claimed: &str,
    stream: &str,
    target_exists: bool,
    winner: Option<Claim>,
    sender_label: Option<&'a str>,
) -> Result<&'a str, ToolError> {
    let claimed_by = sender_label
        .ok_or_else(|| ToolError::invalid("a claim needs from, the label it claims for"))?;
    if !target_exists {
        return Err(ToolError::new(
            ErrorCode::ClaimTargetNotFound,
            format!("stream {stream:?} has no message {claimed:?} to claim; nothing was published"),
        ));
    }
    if let Some(winner) = winner {
        let mut details = json!({
            "claimed_message_id": claimed,
            "claim_message_id": winner.claim_message_id,
            "claimed_by": winner.claimed_by,
        });
        if let Some(publisher) = &winner.publisher {
            details["publisher"] = Value::from(publisher.as_str());
        }
        return Err(ToolError::new(
            ErrorCode::AlreadyClaimed,
            format!(
                "{claimed:?} was already claimed by {:?}; nothing was published or acknowledged",
                winner.claimed_by
            ),
        )
        .with_details(details));
    }
    Ok(claimed_by)
}

pub(crate) fn stream_forbidden(access: &Access) -> ToolError {
    ToolError::new(
        ErrorCode::StreamForbidden,
        match (&access.caller, &access.channel) {
            (Some(caller), Some(channel)) => format!(
                "caller {caller:?} is bound to private channel {channel:?} and cannot access other streams or modify stream membership"
            ),
            (Some(caller), None) => format!(
                "the stream is private and caller {caller:?} is not a member; a member can grant access"
            ),
            (None, _) => {
                "the stream is private and this request has no authenticated caller".to_owned()
            }
        },
    )
}

pub(crate) fn private_stream_needs_auth() -> ToolError {
    ToolError::new(
        ErrorCode::PrivateStreamNeedsAuth,
        "a private stream needs callers identified by bearer tokens; start the server with --bearer-token-file",
    )
}
