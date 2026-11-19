//! Shared sessionless MCP (`2026-07-28`) Streamable HTTP + TLS client
//! transport used by both `minymaton-wait` and the `minymaton` CLI subcommands.

use std::collections::HashSet;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use http::header::{ACCEPT, CONTENT_TYPE, HOST, HeaderMap};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use rmcp::model::{
    CallToolRequestMethod, ClientCapabilities, Implementation, JsonRpcRequest, ProtocolVersion,
    Request, RequestId, RequestMetaObject,
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};

pub struct HttpFailure {
    pub error: Value,
    pub exit: u8,
}

impl HttpFailure {
    pub fn new(code: &str, message: impl Into<String>, exit: u8) -> Self {
        Self {
            error: json!({"code": code, "message": message.into()}),
            exit,
        }
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new("protocol_failed", message, 1)
    }

    pub fn transport(stage: &str, error: &(dyn Error + 'static)) -> Self {
        let mut message = format!("{stage}: {error}");
        let mut source = error.source();
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Self::new("transport_failed", message, 7)
    }

    pub fn io(message: &str, error: std::io::Error) -> Self {
        Self::new("transport_failed", format!("{message}: {error}"), 7)
    }

    pub fn redact(mut self, secrets: &[String]) -> Self {
        if let Some(message) = self.error["message"].as_str() {
            let mut message = message.to_owned();
            for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
                message = message.replace(secret, "[redacted]");
            }
            self.error["message"] = json!(message);
        }
        self
    }

    pub fn message(&self) -> &str {
        self.error["message"].as_str().unwrap_or("request failed")
    }
}

pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn environment(name: &str) -> Result<Option<String>, HttpFailure> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(HttpFailure::new(
            "config_failed",
            format!("{name} is not valid Unicode"),
            1,
        )),
    }
}

fn response_matches(message: &Value) -> bool {
    message.is_object()
        && message.get("id") == Some(&json!(2))
        && (message.get("result").is_some() || message.get("error").is_some())
}

struct X509CertInfo {
    self_issued: bool,
    eku_allows_server_auth: bool,
}

struct DerTlv<'a> {
    tag: u8,
    body: &'a [u8],
    rest: &'a [u8],
    full_tlv: &'a [u8],
}

fn read_der_tlv(input: &[u8]) -> Option<DerTlv<'_>> {
    let tag = *input.first()?;
    let len_byte = *input.get(1)?;
    let (header_len, content_len) = if len_byte & 0x80 == 0 {
        (2usize, usize::from(len_byte))
    } else {
        let num_octets = usize::from(len_byte & 0x7f);
        if num_octets == 0 || num_octets > 4 || input.len() < 2 + num_octets {
            return None;
        }
        let mut len = 0usize;
        for &byte in &input[2..2 + num_octets] {
            len = (len << 8) | usize::from(byte);
        }
        (2 + num_octets, len)
    };
    let total_len = header_len.checked_add(content_len)?;
    if input.len() < total_len {
        return None;
    }
    let full_tlv = &input[..total_len];
    let body = &input[header_len..total_len];
    let rest = &input[total_len..];
    Some(DerTlv {
        tag,
        body,
        rest,
        full_tlv,
    })
}

fn inspect_x509_der(der: &[u8]) -> Option<X509CertInfo> {
    let cert_tlv = read_der_tlv(der)?;
    if cert_tlv.tag != 0x30 {
        return None;
    }
    let tbs_tlv = read_der_tlv(cert_tlv.body)?;
    if tbs_tlv.tag != 0x30 {
        return None;
    }
    let mut tbs = tbs_tlv.body;
    if tbs.first().copied() == Some(0xa0) {
        tbs = read_der_tlv(tbs)?.rest;
    }
    let rest = read_der_tlv(tbs)?.rest;
    let rest = read_der_tlv(rest)?.rest;
    let issuer = read_der_tlv(rest)?;
    let rest = read_der_tlv(issuer.rest)?.rest;
    let subject = read_der_tlv(rest)?;
    let mut rest = read_der_tlv(subject.rest)?.rest;

    let self_issued = issuer.full_tlv == subject.full_tlv;
    let mut eku_allows_server_auth = true;

    while !rest.is_empty() {
        let ext_tlv = read_der_tlv(rest)?;
        rest = ext_tlv.rest;
        if ext_tlv.tag == 0xa3 {
            let seq_tlv = read_der_tlv(ext_tlv.body)?;
            if seq_tlv.tag != 0x30 {
                return None;
            }
            let mut exts_seq = seq_tlv.body;
            while !exts_seq.is_empty() {
                let item_tlv = read_der_tlv(exts_seq)?;
                exts_seq = item_tlv.rest;
                if item_tlv.tag != 0x30 {
                    continue;
                }
                let oid_tlv = read_der_tlv(item_tlv.body)?;
                if oid_tlv.tag != 0x06 {
                    continue;
                }
                let mut item_rest = oid_tlv.rest;
                if item_rest.first().copied() == Some(0x01) {
                    item_rest = read_der_tlv(item_rest)?.rest;
                }
                let val_tlv = read_der_tlv(item_rest)?;
                if val_tlv.tag != 0x04 {
                    continue;
                }
                if oid_tlv.body == [0x55, 0x1d, 0x25] {
                    let eku_seq_tlv = read_der_tlv(val_tlv.body)?;
                    if eku_seq_tlv.tag != 0x30 {
                        return None;
                    }
                    let mut eku_seq = eku_seq_tlv.body;
                    let mut found_server_auth = false;
                    while !eku_seq.is_empty() {
                        let kp_tlv = read_der_tlv(eku_seq)?;
                        eku_seq = kp_tlv.rest;
                        if kp_tlv.tag == 0x06
                            && (kp_tlv.body == [0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01]
                                || kp_tlv.body == [0x55, 0x1d, 0x25, 0x00])
                        {
                            found_server_auth = true;
                        }
                    }
                    if !found_server_auth {
                        eku_allows_server_auth = false;
                    }
                }
            }
        }
    }

    Some(X509CertInfo {
        self_issued,
        eku_allows_server_auth,
    })
}

fn load_pem_bundle(
    path: &Path,
    explicit: bool,
    roots: &mut rustls::RootCertStore,
    extra_intermediates: &mut Vec<rustls::pki_types::CertificateDer<'static>>,
    seen_der: &mut HashSet<Vec<u8>>,
) -> Result<(), HttpFailure> {
    use rustls::pki_types::pem::PemObject;
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(error) => {
            if explicit {
                return Err(HttpFailure::transport(
                    &format!("cannot read CA bundle {}", path.display()),
                    &error,
                ));
            }
            return Ok(());
        }
    };
    let mut parsed_any = false;
    let mut accepted_any = false;
    for item in rustls::pki_types::CertificateDer::pem_slice_iter(&data) {
        let cert = match item {
            Ok(cert) => cert,
            Err(error) => {
                if explicit {
                    return Err(HttpFailure::transport(
                        &format!("invalid PEM certificate in {}", path.display()),
                        &error,
                    ));
                }
                continue;
            }
        };
        parsed_any = true;
        let Some(info) = inspect_x509_der(cert.as_ref()) else {
            if explicit {
                return Err(HttpFailure::new(
                    "transport_failed",
                    format!("invalid X.509 certificate in {}", path.display()),
                    7,
                ));
            }
            continue;
        };
        if !info.eku_allows_server_auth {
            continue;
        }
        accepted_any = true;
        if seen_der.insert(cert.as_ref().to_vec()) {
            if info.self_issued {
                let _ = roots.add(cert);
            } else {
                extra_intermediates.push(cert);
            }
        }
    }
    if explicit && (!parsed_any || !accepted_any) {
        return Err(HttpFailure::new(
            "transport_failed",
            format!("no valid CA certificates found in {}", path.display()),
            7,
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct ExtraIntermediatesVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    #[cfg(not(windows))]
    platform: Option<rustls_platform_verifier::Verifier>,
    extra_intermediates: Vec<rustls::pki_types::CertificateDer<'static>>,
}

impl rustls::client::danger::ServerCertVerifier for ExtraIntermediatesVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let mut combined = Vec::with_capacity(intermediates.len() + self.extra_intermediates.len());
        combined.extend_from_slice(intermediates);
        combined.extend(self.extra_intermediates.iter().cloned());
        match self
            .inner
            .verify_server_cert(end_entity, &combined, server_name, ocsp_response, now)
        {
            Ok(verified) => Ok(verified),
            Err(error) => {
                #[cfg(not(windows))]
                if let Some(platform) = &self.platform {
                    return platform.verify_server_cert(
                        end_entity,
                        &combined,
                        server_name,
                        ocsp_response,
                        now,
                    );
                }
                Err(error)
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

pub fn build_tls_config(ca_file: Option<&Path>) -> Result<rustls::ClientConfig, HttpFailure> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    let mut extra_intermediates = Vec::new();
    let mut seen_der = HashSet::new();

    #[cfg(windows)]
    {
        for (store_name, is_root_store) in [("ROOT", true), ("CA", false)] {
            for open_result in [
                schannel::cert_store::CertStore::open_current_user(store_name),
                schannel::cert_store::CertStore::open_local_machine(store_name),
            ] {
                let Ok(store) = open_result else {
                    continue;
                };
                for cert in store.certs() {
                    let valid_uses_ok = match cert.valid_uses() {
                        Ok(schannel::cert_context::ValidUses::All) => true,
                        Ok(schannel::cert_context::ValidUses::Oids(oids)) => oids
                            .iter()
                            .any(|oid| oid == "1.3.6.1.5.5.7.3.1" || oid == "2.5.29.37.0"),
                        Err(_) => false,
                    };
                    if !valid_uses_ok {
                        continue;
                    }
                    let der = cert.to_der();
                    let Some(info) = inspect_x509_der(der) else {
                        continue;
                    };
                    if !info.eku_allows_server_auth {
                        continue;
                    }
                    if !seen_der.insert(der.to_vec()) {
                        continue;
                    }
                    let cert_der = rustls::pki_types::CertificateDer::from(der.to_vec());
                    if is_root_store || info.self_issued {
                        let _ = roots.add(cert_der);
                    } else {
                        extra_intermediates.push(cert_der);
                    }
                }
            }
        }
    }

    match environment("SSL_CERT_FILE")? {
        Some(path) if !path.is_empty() => {
            let path = Path::new(&path);
            if path.is_file() {
                load_pem_bundle(
                    path,
                    false,
                    &mut roots,
                    &mut extra_intermediates,
                    &mut seen_der,
                )?;
            }
        }
        _ => {
            #[cfg(windows)]
            let default_files: &[&str] = &[r"C:\Program Files\Common Files\SSL\cert.pem"];
            #[cfg(not(windows))]
            let default_files: &[&str] = &[
                "/etc/ssl/cert.pem",
                "/etc/ssl/certs/ca-certificates.crt",
                "/etc/pki/tls/cert.pem",
            ];
            for candidate in default_files {
                let path = Path::new(candidate);
                if path.is_file() {
                    let _ = load_pem_bundle(
                        path,
                        false,
                        &mut roots,
                        &mut extra_intermediates,
                        &mut seen_der,
                    );
                    break;
                }
            }
        }
    }

    let cert_dirs: Vec<PathBuf> = match std::env::var_os("SSL_CERT_DIR") {
        Some(raw) if !raw.is_empty() => std::env::split_paths(&raw).collect(),
        _ => {
            #[cfg(windows)]
            {
                vec![PathBuf::from(r"C:\Program Files\Common Files\SSL\certs")]
            }
            #[cfg(not(windows))]
            {
                Vec::new()
            }
        }
    };
    for dir in cert_dirs {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            let mut paths: Vec<PathBuf> = entries
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|path| path.is_file())
                .collect();
            paths.sort();
            for path in paths {
                let _ = load_pem_bundle(
                    &path,
                    false,
                    &mut roots,
                    &mut extra_intermediates,
                    &mut seen_der,
                );
            }
        }
    }

    if let Some(path) = ca_file {
        load_pem_bundle(
            path,
            true,
            &mut roots,
            &mut extra_intermediates,
            &mut seen_der,
        )?;
    }

    #[cfg(windows)]
    let verifier: Arc<dyn rustls::client::danger::ServerCertVerifier> = {
        if roots.is_empty() {
            return Err(HttpFailure::new(
                "transport_failed",
                "TLS trust store failed: no trusted root certificates available",
                7,
            ));
        }
        let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|error| HttpFailure::transport("TLS trust store failed", &error))?;
        Arc::new(ExtraIntermediatesVerifier {
            inner,
            extra_intermediates,
        })
    };

    #[cfg(not(windows))]
    let verifier: Arc<dyn rustls::client::danger::ServerCertVerifier> = {
        let platform = rustls_platform_verifier::Verifier::new(Arc::clone(&provider))
            .map_err(|error| HttpFailure::transport("TLS trust store failed", &error))?;
        if roots.is_empty() {
            Arc::new(platform)
        } else {
            let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots),
                Arc::clone(&provider),
            )
            .build()
            .map_err(|error| HttpFailure::transport("TLS trust store failed", &error))?;
            Arc::new(ExtraIntermediatesVerifier {
                inner,
                platform: Some(platform),
                extra_intermediates,
            })
        }
    };

    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| HttpFailure::transport("TLS configuration failed", &error))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

#[derive(Serialize)]
struct ToolCallParams<'a, A> {
    name: &'a str,
    arguments: A,
    #[serde(rename = "_meta")]
    meta: RequestMetaObject,
}

pub async fn post_tools_call<A: Serialize>(
    url: &str,
    headers: HeaderMap,
    tool_name: &str,
    client_name: &str,
    arguments: A,
    ca_file: Option<&Path>,
) -> Result<Value, HttpFailure> {
    let has_path = url.split_once("://").is_some_and(|(_, rest)| {
        rest.split(['?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('/'))
    });
    let original_url = url.to_owned();
    let mut parsed = url::Url::parse(url)
        .map_err(|_| HttpFailure::protocol("MCP endpoint must be an absolute HTTP or HTTPS URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(HttpFailure::protocol(
            "MCP endpoint must be an absolute HTTP or HTTPS URL",
        ));
    }
    if !has_path {
        parsed.set_path("/mcp");
    }
    let mut secrets = vec![
        original_url,
        parsed.username().to_owned(),
        parsed.password().unwrap_or_default().to_owned(),
        parsed.query().unwrap_or_default().to_owned(),
    ];
    secrets.extend(parsed.query_pairs().map(|(_, value)| value.into_owned()));
    if let Some(auth) = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        secrets.push(auth.to_owned());
        secrets.push(auth.strip_prefix("Bearer ").unwrap_or(auth).to_owned());
    }
    parsed
        .set_username("")
        .map_err(|_| HttpFailure::protocol("invalid MCP endpoint"))?;
    parsed
        .set_password(None)
        .map_err(|_| HttpFailure::protocol("invalid MCP endpoint"))?;
    let revision = ProtocolVersion::V_2026_07_28;
    let payload = JsonRpcRequest::new(
        RequestId::Number(2),
        Request::<CallToolRequestMethod, _>::new(ToolCallParams {
            name: tool_name,
            arguments,
            meta: RequestMetaObject::with_client_context(
                revision.clone(),
                Implementation::new(client_name, env!("CARGO_PKG_VERSION")),
                ClientCapabilities::default(),
            ),
        }),
    );
    let body = Bytes::from(
        serde_json::to_vec(&payload)
            .map_err(|_| HttpFailure::protocol(format!("cannot encode {tool_name} request")))?,
    );
    let result: Result<Value, HttpFailure> = async {
        let host = parsed
            .host_str()
            .ok_or_else(|| HttpFailure::protocol("invalid MCP endpoint"))?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| HttpFailure::protocol("invalid MCP port"))?;
        let tls_connector = if parsed.scheme() == "https" {
            Some(TlsConnector::from(Arc::new(build_tls_config(ca_file)?)))
        } else {
            None
        };
        let uri = &parsed[url::Position::BeforePath..url::Position::AfterQuery];
        let host_header = &parsed[url::Position::BeforeHost..url::Position::AfterPort];
        loop {
            let mut request = http::Request::builder()
                .method("POST")
                .uri(uri)
                .header(HOST, host_header)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json, text/event-stream")
                .header("MCP-Protocol-Version", revision.as_str())
                .header("Mcp-Method", "tools/call")
                .header("Mcp-Name", tool_name)
                .body(Full::new(body.clone()))
                .map_err(|error| HttpFailure::transport("cannot encode HTTP request", &error))?;
            request.headers_mut().extend(headers.clone());
            let stream = TcpStream::connect((host, port))
                .await
                .map_err(|error| HttpFailure::transport("TCP connection failed", &error))?;
            let outcome = if let Some(connector) = &tls_connector {
                let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                    .map_err(|error| HttpFailure::transport("invalid TLS server name", &error))?;
                let stream = connector
                    .connect(name, stream)
                    .await
                    .map_err(|error| HttpFailure::transport("TLS handshake failed", &error))?;
                exchange(stream, request).await?
            } else {
                exchange(stream, request).await?
            };
            match outcome {
                ExchangeOutcome::Message(message) => break Ok(message),
                ExchangeOutcome::KeepaliveStreamClosed => continue,
            }
        }
    }
    .await;
    result.map_err(|error| error.redact(&secrets))
}

pub fn extract_tool_structured(
    response: &Value,
    tool_label: &str,
) -> Result<(Value, bool), HttpFailure> {
    if let Some(error) = response.get("error") {
        return Err(HttpFailure {
            error: json!({
                "code": "jsonrpc_error",
                "message": format!("{tool_label} request failed"),
                "details": error,
            }),
            exit: 1,
        });
    }
    let result = response
        .get("result")
        .filter(|result| result.is_object())
        .ok_or_else(|| HttpFailure::protocol("tools/call result must be an object"))?;
    let mut structured = result
        .get("structuredContent")
        .filter(|value| !value.is_null())
        .cloned();
    if structured.is_none()
        && let Some(block) = result
            .get("content")
            .and_then(Value::as_array)
            .and_then(|content| content.iter().find(|block| block["type"] == "text"))
    {
        let text = block.get("text").filter(|text| truthy(text));
        let text = match text {
            None => "{}",
            Some(Value::String(text)) => text,
            Some(_) => return Err(HttpFailure::protocol("tool text must be a string")),
        };
        structured = Some(
            serde_json::from_str(text)
                .map_err(|_| HttpFailure::protocol(format!("invalid {tool_label} result JSON")))?,
        );
    }
    let structured = structured
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            HttpFailure::protocol(format!("{tool_label} result must contain a JSON object"))
        })?;
    let is_error = result.get("isError").is_some_and(truthy);
    Ok((structured, is_error))
}

enum ExchangeOutcome {
    Message(Value),
    KeepaliveStreamClosed,
}

async fn exchange<T>(
    stream: T,
    request: http::Request<Full<Bytes>>,
) -> Result<ExchangeOutcome, HttpFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| HttpFailure::transport("HTTP connection failed", &error))?;
    let keepalive_idle = AtomicBool::new(false);
    let response = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|error| HttpFailure::transport("HTTP request failed", &error))?;
        read_response(response, &keepalive_idle).await
    };
    tokio::pin!(response);
    tokio::pin!(connection);
    tokio::select! {
        biased;
        result = &mut response => result,
        result = &mut connection => {
            if let Err(error) = result {
                if keepalive_idle.load(Ordering::Relaxed) {
                    return Ok(ExchangeOutcome::KeepaliveStreamClosed);
                }
                return Err(HttpFailure::transport("HTTP connection failed", &error));
            }
            response.await
        }
    }
}

async fn read_response(
    response: http::Response<Incoming>,
    keepalive_idle: &AtomicBool,
) -> Result<ExchangeOutcome, HttpFailure> {
    if response.status().as_u16() >= 400 {
        return Err(HttpFailure::new(
            "transport_failed",
            format!("HTTP {}", response.status().as_u16()),
            7,
        ));
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
            .map_err(|error| HttpFailure::transport("HTTP response body failed", &error))?
            .to_bytes();
        let message: Value = serde_json::from_slice(&raw)
            .map_err(|_| HttpFailure::protocol("invalid JSON-RPC response JSON"))?;
        if !response_matches(&message) {
            return Err(HttpFailure::protocol(
                "missing or mismatched JSON-RPC response",
            ));
        }
        return Ok(ExchangeOutcome::Message(message));
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
                .map_err(|_| HttpFailure::protocol("invalid UTF-8 in SSE response"))?
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
                        .map_err(|_| HttpFailure::protocol("invalid SSE response JSON"))?;
                    if !message.is_object() {
                        return Err(HttpFailure::protocol("JSON-RPC response must be an object"));
                    }
                    if response_matches(&message) {
                        return Ok(ExchangeOutcome::Message(message));
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
        return Ok(ExchangeOutcome::KeepaliveStreamClosed);
    }
    if let Some(error) = body_error {
        return Err(HttpFailure::transport("HTTP SSE body failed", &error));
    }
    Err(HttpFailure::new(
        "transport_failed",
        "SSE stream closed before JSON-RPC response arrived",
        7,
    ))
}
