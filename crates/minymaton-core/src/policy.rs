//! The queue's decisions about private streams and claims, made once for
//! every backend. A backend gathers the facts inside its own command, asks
//! these functions, and writes what they decide, so a decision and the write
//! it allows commit together whatever the storage is.

use serde_json::{Value, json};

use crate::api::StreamResult;
use crate::error::{ErrorCode, ToolError};

/// A stream's members, as one command sees them.
#[derive(Clone, Copy, Debug)]
pub struct Members {
    /// How many callers are members; an open stream has none.
    pub count: usize,
    /// Whether the calling caller is one of them.
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

/// The first claim of a message, as stored.
#[derive(Clone, Debug)]
pub struct Claim {
    pub claim_message_id: String,
    pub claimed_by: String,
    pub publisher: Option<String>,
}

/// `stream_forbidden` unless the stream is open or `caller` is a member.
pub fn admit(members: Members, caller: Option<&str>) -> Result<(), ToolError> {
    if members.count > 0 && !members.includes_caller {
        return Err(stream_forbidden(caller));
    }
    Ok(())
}

/// Whether a stream with these members is listed for its caller.
pub fn listed(members: Members) -> bool {
    members.count == 0 || members.includes_caller
}

/// The members a new stream starts with: none for an open stream, else the
/// named callers and the creating one.
pub fn initial_members(
    caller: Option<&str>,
    members: Option<&[String]>,
) -> Result<Vec<String>, ToolError> {
    let Some(members) = members else {
        return Ok(Vec::new());
    };
    let creator = caller.ok_or_else(private_stream_needs_auth)?;
    let mut all: Vec<String> = members.to_vec();
    all.push(creator.to_owned());
    all.sort();
    all.dedup();
    Ok(all)
}

/// Decides a grant or revoke of `member` by `caller` on stream `name`, whose
/// members are `members` and of which `member` is one when `target_is_member`.
pub fn change_member(
    name: &str,
    members: Members,
    caller: Option<&str>,
    member: &str,
    target_is_member: bool,
    change: Membership,
) -> Result<(MemberWrite, StreamResult), ToolError> {
    if members.count == 0 {
        return Err(ToolError::invalid(format!(
            "stream {name:?} is open to every caller; only a stream created with members has members to grant or revoke"
        )));
    }
    admit(members, caller)?;
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

pub(crate) fn stream_forbidden(caller: Option<&str>) -> ToolError {
    ToolError::new(
        ErrorCode::StreamForbidden,
        match caller {
            Some(caller) => format!(
                "the stream is private and caller {caller:?} is not a member; a member can grant access"
            ),
            None => "the stream is private and this request has no authenticated caller".to_owned(),
        },
    )
}

pub(crate) fn private_stream_needs_auth() -> ToolError {
    ToolError::new(
        ErrorCode::PrivateStreamNeedsAuth,
        "a private stream needs callers identified by bearer tokens; start the server with --bearer-token-file",
    )
}
