//! The request log (`--request-log`): one JSON record per incoming JSON-RPC
//! message, saying which client called what and what it declared. Fields are
//! copied out one by one, so arguments, payloads and results never reach it.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::SystemTime;

use serde_json::{Map, Value};

const META_REVISION: &str = "io.modelcontextprotocol/protocolVersion";
const META_CLIENT: &str = "io.modelcontextprotocol/clientInfo";
const META_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";

pub(crate) struct RequestLog {
    file: Mutex<File>,
}

/// What the HTTP request carrying a message says about it.
pub(crate) struct Carrier<'a> {
    pub session: Option<&'a str>,
    pub revision: Option<&'a str>,
}

impl RequestLog {
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("cannot open the request log {}: {error}", path.display()))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Writes and flushes the record for one message. Anything that is not a
    /// JSON-RPC message is not recorded.
    pub(crate) fn record(&self, message: &Value, carrier: &Carrier<'_>) -> io::Result<()> {
        let Some(record) = describe(message, carrier) else {
            return Ok(());
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        let mut file = self.file.lock().unwrap_or_else(PoisonError::into_inner);
        file.write_all(&line)?;
        file.flush()
    }
}

fn describe(message: &Value, carrier: &Carrier<'_>) -> Option<Map<String, Value>> {
    let message = message.as_object()?;
    let method = message.get("method").and_then(Value::as_str);
    let id = message.get("id").filter(|id| !id.is_null());
    let kind = match (method, id) {
        (Some(_), Some(_)) => "request",
        (Some(_), None) => "notification",
        (None, Some(_)) if message.contains_key("result") || message.contains_key("error") => {
            "response"
        }
        _ => return None,
    };
    let params = message.get("params").and_then(Value::as_object);
    let meta = params
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object);
    let initialize = method == Some("initialize");

    let mut record = Map::new();
    record.insert(
        "time".to_owned(),
        Value::String(humantime::format_rfc3339_millis(SystemTime::now()).to_string()),
    );
    if let Some(session) = carrier.session {
        record.insert("session".to_owned(), Value::String(session.to_owned()));
    }
    let declared_revision = if initialize {
        params.and_then(|params| params.get("protocolVersion"))
    } else {
        meta.and_then(|meta| meta.get(META_REVISION))
    };
    if let Some(revision) = declared_revision
        .and_then(Value::as_str)
        .or(carrier.revision)
    {
        record.insert("revision".to_owned(), Value::String(revision.to_owned()));
    }
    record.insert("kind".to_owned(), Value::String(kind.to_owned()));
    if let Some(method) = method {
        record.insert("method".to_owned(), Value::String(method.to_owned()));
    }
    if let Some(id) = id {
        record.insert("id".to_owned(), id.clone());
    }
    let named = |key: &str| {
        params
            .and_then(|params| params.get(key))
            .and_then(Value::as_str)
    };
    match method {
        Some("tools/call") => {
            if let Some(tool) = named("name") {
                record.insert("tool".to_owned(), Value::String(tool.to_owned()));
            }
        }
        Some("resources/read" | "resources/subscribe" | "resources/unsubscribe") => {
            if let Some(uri) = named("uri") {
                record.insert("resource".to_owned(), Value::String(uri.to_owned()));
            }
        }
        _ => {}
    }
    let (client, capabilities) = if initialize {
        (
            params.and_then(|params| params.get("clientInfo")),
            params.and_then(|params| params.get("capabilities")),
        )
    } else {
        (
            meta.and_then(|meta| meta.get(META_CLIENT)),
            meta.and_then(|meta| meta.get(META_CAPABILITIES)),
        )
    };
    if let Some(client) = client.and_then(Value::as_object) {
        let mut who = Map::new();
        for key in ["name", "version"] {
            if let Some(value) = client.get(key).and_then(Value::as_str) {
                who.insert(key.to_owned(), Value::String(value.to_owned()));
            }
        }
        record.insert("client".to_owned(), Value::Object(who));
    }
    if let Some(capabilities) = capabilities {
        record.insert("client_capabilities".to_owned(), capabilities.clone());
    }
    Some(record)
}
