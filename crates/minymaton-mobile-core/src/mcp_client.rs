//! A sessionless MCP client: each tool call is one `tools/call` POST at the
//! 2026-07-28 revision, with no `initialize` and no session. Ported from
//! `minymaton-wait`'s `post_jsonrpc`, `exchange` and `read_response`
//! (crates/minymaton-server/src/bin/minymaton-wait.rs), generalized to every
//! tool and to a per-profile trust store.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HOST};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use rmcp::model::{
    CallToolRequestMethod, ClientCapabilities, Implementation, JsonRpcRequest, ProtocolVersion,
    Request, RequestId, RequestMetaObject,
};
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};

use crate::CoreError;

#[derive(Serialize)]
struct ToolCall<'a> {
    name: &'a str,
    arguments: Value,
    #[serde(rename = "_meta")]
    meta: RequestMetaObject,
}

/// One bus endpoint with its credential.
pub struct Endpoint {
    url: url::Url,
    authorization: Option<String>,
    tls: Option<TlsConnector>,
    secrets: Vec<String>,
    next_id: AtomicI64,
}

impl Endpoint {
    /// Parses `url`, appending `/mcp` when it has no path, and builds the
    /// trust store from the platform roots plus an optional PEM CA bundle.
    pub fn new(url: &str, token: Option<&str>, ca_pem: Option<&str>) -> Result<Self, CoreError> {
        let has_path = url.split_once("://").is_some_and(|(_, rest)| {
            rest.split(['?', '#'])
                .next()
                .is_some_and(|authority| authority.contains('/'))
        });
        let mut parsed = url::Url::parse(url).map_err(|_| invalid_url())?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(invalid_url());
        }
        if !has_path {
            parsed.set_path("/mcp");
        }
        let mut secrets = vec![
            url.to_owned(),
            parsed.username().to_owned(),
            parsed.password().unwrap_or_default().to_owned(),
            parsed.query().unwrap_or_default().to_owned(),
        ];
        secrets.extend(parsed.query_pairs().map(|(_, value)| value.into_owned()));
        let token = token.filter(|token| !token.is_empty());
        if let Some(token) = token {
            secrets.push(token.to_owned());
        }
        parsed.set_username("").map_err(|()| invalid_url())?;
        parsed.set_password(None).map_err(|()| invalid_url())?;
        let tls = if parsed.scheme() == "https" {
            Some(TlsConnector::from(Arc::new(tls_config(ca_pem)?)))
        } else {
            None
        };
        secrets.retain(|secret| !secret.is_empty());
        // Longest first, so a secret containing another is replaced whole.
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        Ok(Self {
            url: parsed,
            authorization: token.map(|token| format!("Bearer {token}")),
            tls,
            secrets,
            next_id: AtomicI64::new(1),
        })
    }

    /// Calls one tool and returns its structured result. A tool error comes
    /// back as its own `code` and `message`; transport and protocol failures
    /// never carry the URL's credentials or the token.
    pub async fn call(&self, tool: &str, arguments: Value) -> Result<Value, CoreError> {
        self.call_inner(tool, arguments, None)
            .await
            .map_err(|error| error.redact(&self.secrets))
    }

    /// Like [`Endpoint::call`], with a progress token: `on_progress` runs for
    /// each progress notification, which the server sends for a `receive`
    /// once it holds the wait.
    pub async fn call_with_progress(
        &self,
        tool: &str,
        arguments: Value,
        on_progress: &(dyn Fn() + Sync),
    ) -> Result<Value, CoreError> {
        self.call_inner(tool, arguments, Some(on_progress))
            .await
            .map_err(|error| error.redact(&self.secrets))
    }

    async fn call_inner(
        &self,
        tool: &str,
        arguments: Value,
        on_progress: Option<&(dyn Fn() + Sync)>,
    ) -> Result<Value, CoreError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let revision = ProtocolVersion::V_2026_07_28;
        let payload = JsonRpcRequest::new(
            RequestId::Number(id),
            Request::<CallToolRequestMethod, _>::new(ToolCall {
                name: tool,
                arguments,
                meta: RequestMetaObject::with_client_context(
                    revision.clone(),
                    Implementation::new("minymaton-mobile", env!("CARGO_PKG_VERSION")),
                    ClientCapabilities::default(),
                ),
            }),
        );
        let mut payload = serde_json::to_value(&payload)
            .map_err(|_| CoreError::protocol("cannot encode the tool call"))?;
        if on_progress.is_some() {
            payload["params"]["_meta"]["progressToken"] = Value::from(id);
        }
        let body = Bytes::from(payload.to_string());
        let host = self
            .url
            .host_str()
            .ok_or_else(invalid_url)?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let port = self.url.port_or_known_default().ok_or_else(invalid_url)?;
        let uri = &self.url[url::Position::BeforePath..url::Position::AfterQuery];
        let host_header = &self.url[url::Position::BeforeHost..url::Position::AfterPort];
        let message = loop {
            let mut request = http::Request::builder()
                .method("POST")
                .uri(uri)
                .header(HOST, host_header)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json, text/event-stream")
                .header("MCP-Protocol-Version", revision.as_str())
                .header("Mcp-Method", "tools/call")
                .header("Mcp-Name", tool);
            if let Some(authorization) = &self.authorization {
                request = request.header(AUTHORIZATION, authorization);
            }
            let request = request
                .body(Full::new(body.clone()))
                .map_err(|error| CoreError::transport("cannot encode the HTTP request", &error))?;
            let stream = TcpStream::connect((host, port))
                .await
                .map_err(|error| CoreError::transport("TCP connection failed", &error))?;
            let outcome = if let Some(connector) = &self.tls {
                let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                    .map_err(|error| CoreError::transport("invalid TLS server name", &error))?;
                let stream = connector
                    .connect(name, stream)
                    .await
                    .map_err(|error| CoreError::transport("TLS handshake failed", &error))?;
                exchange(stream, request, id, on_progress).await?
            } else {
                exchange(stream, request, id, on_progress).await?
            };
            match outcome {
                Outcome::Message(message) => break message,
                // The server closed an SSE stream that carried only keepalive
                // comments: the call was never answered, so it is made again.
                Outcome::KeepaliveStreamClosed => {}
            }
        };
        tool_result(&message)
    }
}

fn invalid_url() -> CoreError {
    CoreError::new(
        "invalid_profile",
        "the bus URL must be an absolute HTTP or HTTPS URL",
    )
}

fn tool_result(message: &Value) -> Result<Value, CoreError> {
    if let Some(error) = message.get("error") {
        let detail = error["message"].as_str().unwrap_or("JSON-RPC error");
        return Err(CoreError::new("jsonrpc_error", detail));
    }
    let result = message
        .get("result")
        .filter(|result| result.is_object())
        .ok_or_else(|| CoreError::protocol("tools/call result must be an object"))?;
    let mut structured = result
        .get("structuredContent")
        .filter(|value| !value.is_null())
        .cloned();
    if structured.is_none()
        && let Some(text) = result
            .get("content")
            .and_then(Value::as_array)
            .and_then(|content| content.iter().find(|block| block["type"] == "text"))
            .and_then(|block| block["text"].as_str())
    {
        structured = Some(
            serde_json::from_str(text).map_err(|_| CoreError::protocol("tool text is not JSON"))?,
        );
    }
    let structured = structured
        .filter(Value::is_object)
        .ok_or_else(|| CoreError::protocol("tool result must contain a JSON object"))?;
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        let (Some(code), Some(text)) =
            (structured["code"].as_str(), structured["message"].as_str())
        else {
            return Err(CoreError::protocol(
                "tool error must contain a code and message",
            ));
        };
        return Err(CoreError::new(code, text));
    }
    Ok(structured)
}

enum Outcome {
    Message(Value),
    KeepaliveStreamClosed,
}

async fn exchange<T>(
    stream: T,
    request: http::Request<Full<Bytes>>,
    id: i64,
    on_progress: Option<&(dyn Fn() + Sync)>,
) -> Result<Outcome, CoreError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| CoreError::transport("HTTP connection failed", &error))?;
    let keepalive_idle = AtomicBool::new(false);
    let response = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|error| CoreError::transport("HTTP request failed", &error))?;
        read_response(response, &keepalive_idle, id, on_progress).await
    };
    tokio::pin!(connection, response);
    // The connection is owned here, so completion or cancellation closes the socket.
    tokio::select! {
        biased;
        result = &mut response => result,
        result = &mut connection => {
            if let Err(error) = result {
                if keepalive_idle.load(Ordering::Relaxed) {
                    return Ok(Outcome::KeepaliveStreamClosed);
                }
                return Err(CoreError::transport("HTTP connection failed", &error));
            }
            response.await
        }
    }
}

fn response_matches(message: &Value, id: i64) -> bool {
    message.is_object()
        && message.get("id") == Some(&Value::from(id))
        && (message.get("result").is_some() || message.get("error").is_some())
}

async fn read_response(
    response: http::Response<Incoming>,
    keepalive_idle: &AtomicBool,
    id: i64,
    on_progress: Option<&(dyn Fn() + Sync)>,
) -> Result<Outcome, CoreError> {
    let status = response.status().as_u16();
    if status == 401 || status == 403 {
        return Err(CoreError::new(
            "unauthorized",
            format!("the bus rejected the token (HTTP {status})"),
        ));
    }
    if status >= 400 {
        return Err(CoreError::new("transport_failed", format!("HTTP {status}")));
    }
    let status_ok = response.status().is_success();
    let sse = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"));
    if !sse {
        let raw = response
            .into_body()
            .collect()
            .await
            .map_err(|error| CoreError::transport("HTTP response body failed", &error))?
            .to_bytes();
        let message: Value = serde_json::from_slice(&raw)
            .map_err(|_| CoreError::protocol("invalid JSON-RPC response JSON"))?;
        if !response_matches(&message, id) {
            return Err(CoreError::protocol(
                "missing or mismatched JSON-RPC response",
            ));
        }
        return Ok(Outcome::Message(message));
    }
    let mut pending = Vec::new();
    let mut data = Vec::new();
    let mut saw_comment = false;
    let mut saw_non_comment = false;
    let mut body = response.into_body();
    let mut body_error = None;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                body_error = Some(error);
                break;
            }
        };
        let Some(chunk) = frame.data_ref() else {
            continue;
        };
        pending.extend_from_slice(chunk);
        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
            let raw: Vec<_> = pending.drain(..=end).collect();
            let line = std::str::from_utf8(&raw)
                .map_err(|_| CoreError::protocol("invalid UTF-8 in SSE response"))?
                .trim_end_matches(['\r', '\n']);
            if let Some(line) = line.strip_prefix("data:") {
                saw_non_comment = true;
                keepalive_idle.store(false, Ordering::Relaxed);
                data.push(line.trim_start_matches(' ').to_owned());
            } else if line.is_empty() {
                if !data.is_empty() {
                    let text = data.join("\n");
                    data.clear();
                    let message: Value = serde_json::from_str(&text)
                        .map_err(|_| CoreError::protocol("invalid SSE response JSON"))?;
                    if response_matches(&message, id) {
                        return Ok(Outcome::Message(message));
                    }
                    if message["method"] == "notifications/progress"
                        && message["params"]["progressToken"] == id
                        && let Some(on_progress) = on_progress
                    {
                        on_progress();
                    }
                }
            } else if line.starts_with(':') {
                saw_comment = true;
            } else {
                saw_non_comment = true;
            }
        }
        let idle = status_ok
            && saw_comment
            && !saw_non_comment
            && data.is_empty()
            && (pending.is_empty() || pending.starts_with(b":") || pending == b"\r");
        keepalive_idle.store(idle, Ordering::Relaxed);
    }
    if keepalive_idle.load(Ordering::Relaxed) {
        return Ok(Outcome::KeepaliveStreamClosed);
    }
    if let Some(error) = body_error {
        return Err(CoreError::transport("HTTP SSE body failed", &error));
    }
    Err(CoreError::new(
        "transport_failed",
        "the SSE stream closed before the JSON-RPC response arrived",
    ))
}

/// The trust store: the bundled Mozilla roots, the device's system CA
/// directory on Android, and the profile's own PEM CA bundle when it has one.
fn tls_config(ca_pem: Option<&str>) -> Result<rustls::ClientConfig, CoreError> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(target_os = "android")]
    for dir in [
        "/apex/com.android.conscrypt/cacerts",
        "/system/etc/security/cacerts",
    ] {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if let Ok(data) = std::fs::read(entry.path()) {
                for cert in CertificateDer::pem_slice_iter(&data).flatten() {
                    let _ = roots.add(cert);
                }
            }
        }
    }
    if let Some(pem) = ca_pem.filter(|pem| !pem.trim().is_empty()) {
        let mut added = 0usize;
        for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            let cert =
                cert.map_err(|error| CoreError::transport("invalid profile CA bundle", &error))?;
            roots
                .add(cert)
                .map_err(|error| CoreError::transport("invalid profile CA certificate", &error))?;
            added += 1;
        }
        if added == 0 {
            return Err(CoreError::new(
                "invalid_profile",
                "the profile CA bundle contains no PEM certificate",
            ));
        }
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| CoreError::transport("TLS configuration failed", &error))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}
