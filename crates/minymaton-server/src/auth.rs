//! Enrolled bearer-token authentication for the MCP Streamable HTTP boundary.
//!
//! Plaintext tokens are hashed at startup with SHA-256 and never retained in
//! memory. Every HTTP request on `/mcp` and `/mcp/agent/{agent}` must carry an
//! `Authorization: Bearer <token>` header matching an enrolled digest in
//! constant time.

use std::collections::HashSet;
use std::fs;
use std::hint::black_box;
use std::path::Path;

use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use sha2::{Digest, Sha256};

const MIN_TOKEN_BYTES: usize = 32;

struct Entry {
    caller: String,
    digest: [u8; 32],
}

pub(crate) struct BearerAuthenticator {
    entries: Vec<Entry>,
}

impl BearerAuthenticator {
    /// Loads enrolled bearer tokens from `path`.
    ///
    /// Each non-empty, non-comment (`#`) line is either `CALLER\tTOKEN` or a
    /// bare `TOKEN`. Every token must be at least 32 bytes with no control
    /// characters, and caller names and token digests must be unique.
    pub(crate) fn load(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path).map_err(|error| {
            format!("cannot read bearer token file {}: {error}", path.display())
        })?;
        let mut entries = Vec::new();
        let mut seen_callers = HashSet::new();
        let mut seen_digests = HashSet::new();
        for (index, line) in raw.lines().enumerate() {
            let line_no = index + 1;
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if trimmed.trim().is_empty() || trimmed.trim_start().starts_with('#') {
                continue;
            }
            let (caller, token) = match trimmed.split_once('\t') {
                Some((caller, token)) => (caller.trim().to_owned(), token.trim()),
                None => (format!("token-{line_no}"), trimmed.trim()),
            };
            if caller.is_empty() || caller.chars().any(char::is_control) {
                return Err(format!(
                    "bearer token file {}:{line_no}: caller label must be non-empty without control characters",
                    path.display()
                ));
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
            entries.push(Entry { caller, digest });
        }
        if entries.is_empty() {
            return Err(format!(
                "bearer token file {} contains no enrolled tokens",
                path.display()
            ));
        }
        Ok(Self { entries })
    }

    /// Authenticates `Authorization: Bearer <token>` in constant time across
    /// all enrolled token digests, returning the matched caller label.
    pub(crate) fn authenticate(&self, headers: &HeaderMap) -> Option<&str> {
        let token = extract_bearer_token(headers)?;
        let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched: Option<&str> = None;
        for entry in &self.entries {
            if constant_time_eq_32(&candidate, &entry.digest) && matched.is_none() {
                matched = Some(entry.caller.as_str());
            }
        }
        matched
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
