//! Enrolled bearer-token authentication for the MCP Streamable HTTP boundary.
//!
//! Plaintext tokens are hashed at startup with SHA-256 and never retained in
//! memory. Every HTTP request on `/mcp` and `/mcp/agent/{agent}` must carry an
//! `Authorization: Bearer <token>` header matching an enrolled digest in
//! constant time.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::hint::black_box;
use std::path::Path;

use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use minymaton_core::api::DEFAULT_STREAM;
use minymaton_core::{Access, ErrorCode, ToolError};
use sha2::{Digest, Sha256};

const MIN_TOKEN_BYTES: usize = 32;
const NAME_LIMIT: usize = 128;

struct Entry {
    caller: String,
    channel: Option<String>,
    digest: [u8; 32],
}

pub(crate) struct BearerAuthenticator {
    entries: Vec<Entry>,
    bound_channels: Vec<String>,
    enrollment_digest: [u8; 32],
}

impl BearerAuthenticator {
    /// Loads enrolled bearer tokens from `path`.
    ///
    /// Each non-empty, non-comment (`#`) line is `CALLER\tTOKEN`,
    /// `CALLER\tTOKEN\tCHANNEL`, or a bare `TOKEN`. Every token must be at
    /// least 32 bytes with no control characters, caller and channel names must
    /// be 1 to 128 bytes without control characters, `CHANNEL` cannot be
    /// `"agents"`, and caller names and token digests must be unique.
    pub(crate) fn load(
        path: &Path,
        max_enrolled_tokens: usize,
        max_bound_channels: usize,
    ) -> Result<Self, String> {
        let raw = fs::read_to_string(path).map_err(|error| {
            format!("cannot read bearer token file {}: {error}", path.display())
        })?;
        let mut entries = Vec::new();
        let mut seen_callers = HashSet::new();
        let mut seen_digests = HashSet::new();
        let mut channels = BTreeSet::new();
        for (index, line) in raw.lines().enumerate() {
            let line_no = index + 1;
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if trimmed.trim().is_empty() || trimmed.trim_start().starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = trimmed.split('\t').collect();
            let (caller, token, channel) = match parts.as_slice() {
                [token] => (format!("token-{line_no}"), token.trim(), None),
                [caller, token] => (caller.trim().to_owned(), token.trim(), None),
                [caller, token, channel] => (
                    caller.trim().to_owned(),
                    token.trim(),
                    Some(channel.trim().to_owned()),
                ),
                _ => {
                    return Err(format!(
                        "bearer token file {}:{line_no}: expected CALLER\\tTOKEN or CALLER\\tTOKEN\\tCHANNEL",
                        path.display()
                    ));
                }
            };
            let valid_ident = |s: &str| {
                !s.is_empty()
                    && s.len() <= NAME_LIMIT
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            };
            if !valid_ident(&caller) {
                return Err(format!(
                    "bearer token file {}:{line_no}: caller label must be 1 to {NAME_LIMIT} bytes of [A-Za-z0-9._-]",
                    path.display()
                ));
            }
            if let Some(channel) = &channel {
                if !valid_ident(channel) {
                    return Err(format!(
                        "bearer token file {}:{line_no}: channel for {caller:?} must be 1 to {NAME_LIMIT} bytes of [A-Za-z0-9._-]",
                        path.display()
                    ));
                }
                if channel == DEFAULT_STREAM {
                    return Err(format!(
                        "bearer token file {}:{line_no}: channel for {caller:?} cannot be the shared {DEFAULT_STREAM:?} stream",
                        path.display()
                    ));
                }
                channels.insert(channel.clone());
            }
            if token.len() < MIN_TOKEN_BYTES {
                return Err(format!(
                    "bearer token file {}:{line_no}: token for {caller:?} must be at least {MIN_TOKEN_BYTES} bytes",
                    path.display()
                ));
            }
            if token
                .chars()
                .any(|ch| ch.is_control() || ch.is_whitespace())
            {
                return Err(format!(
                    "bearer token file {}:{line_no}: token for {caller:?} must not contain whitespace or control characters",
                    path.display()
                ));
            }
            if !seen_callers.insert(caller.clone()) {
                return Err(format!(
                    "bearer token file {}:{line_no}: duplicate caller {caller:?}",
                    path.display()
                ));
            }
            let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            if !seen_digests.insert(digest) {
                return Err(format!(
                    "bearer token file {}:{line_no}: duplicate token digest",
                    path.display()
                ));
            }
            entries.push(Entry {
                caller,
                channel,
                digest,
            });
        }
        if entries.is_empty() {
            return Err(format!(
                "bearer token file {} contains no enrolled tokens",
                path.display()
            ));
        }
        if entries.len() > max_enrolled_tokens {
            return Err(format!(
                "bearer token file {} has {} tokens, exceeding the maximum of {max_enrolled_tokens}",
                path.display(),
                entries.len()
            ));
        }
        if channels.len() > max_bound_channels {
            return Err(format!(
                "bearer token file {} has {} bound channels, exceeding the maximum of {max_bound_channels}",
                path.display(),
                channels.len()
            ));
        }
        let mut sorted: Vec<(&str, &str, &[u8; 32])> = entries
            .iter()
            .map(|entry| {
                (
                    entry.caller.as_str(),
                    entry.channel.as_deref().unwrap_or(""),
                    &entry.digest,
                )
            })
            .collect();
        sorted.sort_unstable();
        let mut hasher = Sha256::new();
        for (caller, channel, digest) in sorted {
            hasher.update(caller.as_bytes());
            hasher.update(b"\0");
            hasher.update(channel.as_bytes());
            hasher.update(b"\0");
            hasher.update(digest);
            hasher.update(b"\n");
        }
        let enrollment_digest: [u8; 32] = hasher.finalize().into();
        Ok(Self {
            entries,
            bound_channels: channels.into_iter().collect(),
            enrollment_digest,
        })
    }

    pub(crate) fn bound_channels(&self) -> &[String] {
        &self.bound_channels
    }

    pub(crate) fn enrollment_digest(&self) -> [u8; 32] {
        self.enrollment_digest
    }

    pub(crate) fn has_token_digest(&self, digest: &[u8; 32]) -> bool {
        self.entries.iter().any(|entry| &entry.digest == digest)
    }

    pub(crate) fn token_digests(&self) -> impl Iterator<Item = [u8; 32]> + '_ {
        self.entries.iter().map(|entry| entry.digest)
    }

    /// Verifies that `base`'s token digest is still present with the same
    /// caller and channel in this authenticator, and returns `base` stamped
    /// with this authenticator's `enrollment_digest` (`Sol RR-01`, `Opus RR2-01`).
    pub(crate) fn refresh_access(&self, base: &Access) -> Result<Access, ToolError> {
        let Some(digest) = base.token_digest else {
            return Err(ToolError::new(
                ErrorCode::AuthorityFenced,
                "bearer token or enrollment is no longer active",
            ));
        };
        let Some(entry) = self.entries.iter().find(|entry| entry.digest == digest) else {
            return Err(ToolError::new(
                ErrorCode::AuthorityFenced,
                "bearer token or enrollment is no longer active",
            ));
        };
        if base.caller.as_deref() != Some(entry.caller.as_str()) || base.channel != entry.channel {
            return Err(ToolError::new(
                ErrorCode::AuthorityFenced,
                "bearer token or enrollment is no longer active",
            ));
        }
        Ok(base.clone().with_enrollment_digest(self.enrollment_digest))
    }

    /// Rejects a live `SIGHUP` transition that changes an existing token digest's
    /// `caller` or `channel` in place without rotating/removing that token (`O-R4-2`).
    pub(crate) fn check_reload_transition(&self, next: &Self) -> Result<(), String> {
        for prev in &self.entries {
            if let Some(retained) = next.entries.iter().find(|entry| entry.digest == prev.digest)
                && (retained.caller != prev.caller || retained.channel != prev.channel)
            {
                return Err(format!(
                    "cannot change caller or bound channel for active token of {:?} in place; rotate the token when changing its channel binding",
                    prev.caller
                ));
            }
        }
        Ok(())
    }

    /// Authenticates `Authorization: Bearer <token>` in constant time across
    /// all enrolled token digests, returning the matched caller's [`Access`].
    pub(crate) fn authenticate(&self, headers: &HeaderMap) -> Option<Access> {
        let token = extract_bearer_token(headers)?;
        let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched: Option<&Entry> = None;
        for entry in &self.entries {
            if constant_time_eq_32(&candidate, &entry.digest) && matched.is_none() {
                matched = Some(entry);
            }
        }
        matched.map(|entry| Access {
            caller: Some(entry.caller.clone()),
            channel: entry.channel.clone(),
            token_digest: Some(entry.digest),
            enrollment_digest: Some(self.enrollment_digest),
        })
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let raw = value.to_str().ok()?.trim();
    let (scheme, rest) = raw.split_once([' ', '\t'])?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = rest.trim();
    if token.is_empty() || token.chars().any(char::is_whitespace) {
        return None;
    }
    Some(token)
}

fn constant_time_eq_32(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for index in 0..32 {
        diff |= black_box(left[index] ^ right[index]);
    }
    black_box(diff) == 0
}
