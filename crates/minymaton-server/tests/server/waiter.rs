use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, ProtocolVersion};
use serde_json::{Value, json};

use crate::common::{Lifecycle, RawReply, Server, ack_only, receive_args};

fn command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_minymaton-wait"));
    command
        .env("PATH", "")
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("MINYMATON_URL")
        .env_remove("MINYMATON_TOKEN")
        .env_remove("MINYMATON_HANDLE")
        .env_remove("MINYMATON_ACK")
        .env_remove("MINYMATON_MAX_EVENTS")
        .env_remove("MINYMATON_MAX_BYTES")
        .env_remove("MINYMATON_CA_FILE")
        .env_remove("MINYMATON_TOKEN_ENV")
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR");
    command
}

fn openssl_command() -> Command {
    #[cfg(windows)]
    {
        for candidate in [
            r"C:\Program Files\Git\usr\bin\openssl.exe",
            r"C:\Program Files\OpenSSL-Win64\bin\openssl.exe",
        ] {
            if Path::new(candidate).is_file() {
                return Command::new(candidate);
            }
        }
    }
    Command::new("openssl")
}

fn terminal(output: &Output, exit: i32, code: &str) -> Value {
    assert_eq!(output.status.code(), Some(exit), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert_eq!(output.stderr.split(|byte| *byte == b'\n').count(), 2);
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["code"], code, "{error}");
    assert_eq!(error["status"], "error");
    error
}

struct Request {
    line: String,
    headers: HashMap<String, String>,
    body: Value,
}

/// A real HTTP/1.1 listener, including raw framing that the MCP server does not
/// normally emit. A separate finish request drains the complete request record;
/// completed chunked responses deliberately leave the accepted socket open.
struct HttpServer {
    url: String,
    thread: Option<std::thread::JoinHandle<Vec<Request>>>,
}

impl HttpServer {
    fn start(wire: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        Self::on(listener, wire)
    }

    fn on(listener: TcpListener, wire: Vec<u8>) -> Self {
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let thread = std::thread::spawn(move || {
            let mut requests = Vec::new();
            let mut connections = Vec::new();
            for stream in listener.incoming() {
                let mut reader = BufReader::new(stream.unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut headers = HashMap::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    let (name, value) = header.split_once(':').expect("HTTP header");
                    headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
                }
                if line.starts_with("POST /finish ") {
                    break;
                }
                let length = headers
                    .get("content-length")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                requests.push(Request {
                    line,
                    headers,
                    body: serde_json::from_slice(&body).unwrap(),
                });
                reader.get_mut().write_all(&wire).unwrap();
                if wire
                    .windows(19)
                    .any(|part| part == b"Connection: close\r\n")
                {
                    reader
                        .get_mut()
                        .shutdown(std::net::Shutdown::Write)
                        .unwrap();
                } else {
                    connections.push(reader);
                }
            }
            requests
        });
        Self {
            url,
            thread: Some(thread),
        }
    }

    fn json(message: Value) -> Self {
        Self::start(json_wire(message))
    }

    fn finish(&mut self) -> Vec<Request> {
        let mut stream = TcpStream::connect(
            self.url
                .strip_prefix("http://")
                .unwrap()
                .strip_suffix("/mcp")
                .unwrap(),
        )
        .unwrap();
        stream
            .write_all(b"POST /finish HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        self.thread.take().unwrap().join().unwrap()
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.finish();
        }
    }
}

fn json_wire(message: Value) -> Vec<u8> {
    let body = message.to_string();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn events() -> Value {
    json!({
        "status":"events", "events":[{"message_id":"msg_test","payload":"hello"}],
        "batch_id":"bat_test", "ack_after_handling":"bat_test",
    })
}

#[test]
fn cli_waiter_http_delivers_without_a_ca_store() {
    let home = tempfile::tempdir().unwrap();
    let empty_file = home.path().join("empty-ca.pem");
    let empty_dir = home.path().join("empty-ca-directory");
    std::fs::write(&empty_file, "").unwrap();
    std::fs::create_dir(&empty_dir).unwrap();
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    let output = command(home.path())
        .args(["--url", &server.url, "--handle", "sub_test"])
        .env("SSL_CERT_FILE", &empty_file)
        .env("SSL_CERT_DIR", &empty_dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        events()
    );
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn cli_waiter_preserves_shipped_float_bits() {
    let home = tempfile::tempdir().unwrap();
    let mut values = vec![
        1.2345678901234567_f64,
        0.845_512_408_225_570_1_f64,
        2.2250738585072014e-308_f64,
        5e-324_f64,
        -0.0_f64,
    ];
    let mut bits = 1_u64;
    for _ in 0..512 {
        bits = bits.wrapping_mul(6364136223846793005).wrapping_add(1);
        let value = f64::from_bits(bits);
        if value.is_finite() {
            values.push(value);
        }
    }
    let mut batch = events();
    batch["events"][0]["payload"] = json!(values);
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":batch}}));
    let output = command(home.path())
        .args(["--url", &server.url, "--handle", "sub_test"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let batch: Value = serde_json::from_slice(&output.stdout).unwrap();
    for (index, expected) in values.into_iter().enumerate() {
        let actual = batch["events"][0]["payload"][index].as_f64().unwrap();
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "float {index}: {actual} != {expected}"
        );
    }
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn cli_waiter_terminal_json_is_ascii_with_surrogate_pairs() {
    let home = tempfile::tempdir().unwrap();
    let mut batch = events();
    batch["events"][0]["payload"] = json!({"caf\u{e9}":"\u{1f600}\u{7f}\u{2028}\"\\\n"});
    for (result, exit) in [
        (json!({"structuredContent":batch}), 0),
        (
            json!({"isError":true,"structuredContent":{
                "code":"invalid_ack","message":"caf\u{e9} \u{1f600}","details":batch,
            }}),
            1,
        ),
        (
            json!({"structuredContent":{"status":"superseded","details":batch}}),
            3,
        ),
    ] {
        let mut server = HttpServer::json(json!({"id":2,"result":result}));
        let output = command(home.path())
            .args(["--url", &server.url, "--handle", "sub_test"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(exit), "{output:?}");
        let bytes = if exit == 0 {
            &output.stdout
        } else {
            &output.stderr
        };
        assert!(bytes.is_ascii(), "terminal JSON was not ASCII: {bytes:?}");
        let text = std::str::from_utf8(bytes).unwrap();
        assert!(text.contains("\\ud83d\\ude00"), "{text}");
        assert!(text.contains("\\u007f"), "{text}");
        let actual: Value = serde_json::from_slice(bytes).unwrap();
        if exit == 0 {
            assert_eq!(actual, batch);
        } else {
            assert_eq!(actual["details"], batch);
        }
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn cli_waiter_current_thread_ignores_worker_thread_setting() {
    let home = tempfile::tempdir().unwrap();
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    let output = command(home.path())
        .args(["--url", &server.url, "--handle", "sub_test"])
        .env("TOKIO_WORKER_THREADS", "0")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn cli_waiter_connection_errors_keep_causes_without_secrets() {
    let home = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://user:private-password@{}/mcp?secret=private-query",
        listener.local_addr().unwrap()
    );
    drop(listener);
    let output = command(home.path())
        .args(["--url", &url, "--handle", "sub_test"])
        .env("MINYMATON_TOKEN", "private-authorization")
        .output()
        .unwrap();
    let error = terminal(&output, 7, "transport_failed");
    let message = error["message"].as_str().unwrap().to_ascii_lowercase();
    assert!(
        message.contains("refused") || message.contains("10061"),
        "{message}"
    );
    for secret in [
        url.as_str(),
        "private-password",
        "private-query",
        "private-authorization",
    ] {
        assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    }
    for wire in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{".to_vec(),
        b"NOT-HTTP\r\nConnection: close\r\n\r\n".to_vec(),
    ] {
        let mut server = HttpServer::start(wire);
        let url = format!("{}?secret=private-query", server.url);
        let output = command(home.path())
            .args(["--url", &url, "--handle", "sub_test"])
            .env("MINYMATON_TOKEN", "private-authorization")
            .output()
            .unwrap();
        let error = terminal(&output, 7, "transport_failed");
        let message = error["message"].as_str().unwrap().to_ascii_lowercase();
        assert!(
            message.contains("incomplete")
                || message.contains("end of file")
                || message.contains("invalid http"),
            "{message}"
        );
        assert!(!message.contains("private-query") && !message.contains("private-authorization"));
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn cli_waiter_resolves_localhost_and_preserves_ipv6() {
    let home = tempfile::tempdir().unwrap();
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let listener = TcpListener::bind(address).unwrap();
        let mut server = HttpServer::on(
            listener,
            json_wire(json!({
                "id":2,"result":{"structuredContent":events()},
            })),
        );
        let url = if address.starts_with("127.") {
            server.url.replace("127.0.0.1", "localhost")
        } else {
            server.url.clone()
        };
        let output = command(home.path())
            .args(["--url", &url, "--handle", "sub_test"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{url}: {output:?}");
        assert_eq!(server.finish().len(), 1);
    }
}

fn tls_certificates(home: &Path) -> tokio_rustls::rustls::ServerConfig {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    for arguments in [
        vec![
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=minymaton-local-test-CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
        ],
        vec![
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=localhost",
        ],
        vec![
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            "1",
            "-extfile",
            "server.ext",
        ],
    ] {
        std::fs::write(home.join("server.ext"),
            "basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n").unwrap();
        let output = openssl_command()
            .current_dir(home)
            .args(arguments)
            .output()
            .expect("OpenSSL is required for the local-CA integration fixture");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut config = tokio_rustls::rustls::ServerConfig::builder_with_provider(
        std::sync::Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from_pem_file(home.join("server.pem")).unwrap()],
        PrivateKeyDer::from_pem_file(home.join("server.key")).unwrap(),
    )
    .unwrap();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_https_validates_trust_and_hostname_without_leaking_secrets() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let home = tempfile::tempdir().unwrap();
    let config = std::sync::Arc::new(tls_certificates(home.path()));
    let empty_dir = home.path().join("empty-ca-directory");
    std::fs::create_dir(&empty_dir).unwrap();
    for (trusted, host) in [
        (true, "localhost"),
        (false, "localhost"),
        (true, "127.0.0.1"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::clone(&config));
        let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => return Err(error),
            };
            assert_eq!(
                stream.get_ref().1.alpn_protocol(),
                Some(b"http/1.1".as_slice())
            );
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(line, "POST /mcp?secret=private-tls-query HTTP/1.1\r\n");
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await.unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(request["method"], "tools/call");
            reader
                .get_mut()
                .write_all(&json_wire(json!({
                    "id":2,"result":{"structuredContent":events()},
                })))
                .await
                .unwrap();
            let mut rest = Vec::new();
            // A request socket close need not carry TLS close_notify.
            match reader.read_to_end(&mut rest).await {
                Ok(_) => {}
                Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof),
            }
            assert!(rest.is_empty(), "no retry or cleanup request");
            Ok(())
        }));
        let url = format!("https://{host}:{port}/mcp?secret=private-tls-query");
        let mut child = command(home.path());
        child
            .args(["--url", &url, "--handle", "sub_test"])
            .env("MINYMATON_TOKEN", "private-tls-authorization");
        if trusted {
            child
                .env("SSL_CERT_FILE", home.path().join("ca.pem"))
                .env("SSL_CERT_DIR", &empty_dir);
        } else {
            child.env_remove("SSL_CERT_FILE").env_remove("SSL_CERT_DIR");
        }
        let output = child.output().unwrap();
        if trusted && host == "localhost" {
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                events()
            );
            server.await.unwrap().unwrap();
        } else {
            let error = terminal(&output, 7, "transport_failed");
            let message = error["message"].as_str().unwrap().to_ascii_lowercase();
            assert!(message.contains("certificate"), "{message}");
            assert!(!message.contains("private-tls-query"));
            assert!(!message.contains("private-tls-authorization"));
            assert!(
                server.await.unwrap().is_err(),
                "untrusted TLS must send no HTTP request"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_real_store_bounds_and_acknowledged_batch_exits() {
    let server = Server::start_with_request_log().await;
    let client = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
        .await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    client
        .publish("orders", "large", json!("b".repeat(1000)))
        .await;
    let count = server.requests().len();
    let output = command(server.db_path().parent().unwrap())
        .args([
            "--url",
            server.url(),
            "--handle",
            &handle,
            "--max-bytes",
            "1000",
        ])
        .output()
        .unwrap();
    let error = terminal(&output, 6, "batch_exceeds_bounds");
    assert_eq!(error["details"]["max_bytes"], 1000);
    assert!(error["details"]["required_bytes"].as_u64().unwrap() > 1000);
    assert_eq!(server.requests().len(), count + 1);
    let batch = client
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(batch["delivery_attempt"], 1);
    client
        .call("receive", ack_only(&handle, &batch["batch_id"]))
        .await
        .ok();
    client
        .publish("orders", "ack-race", json!("not pending at release"))
        .await;
    let connection = rusqlite::Connection::open(server.db_path()).unwrap();
    // Like storage.rs's domain-failure triggers, force the real store's acknowledged
    // state at seal, before release checks it, without racing or adding product hooks.
    connection
        .execute_batch(
            "CREATE TRIGGER acknowledge_at_seal AFTER INSERT ON batches BEGIN
             UPDATE batches SET acked_at = NEW.sealed_at WHERE id = NEW.id;
             UPDATE subscriptions SET committed_sequence = NEW.last_sequence
                 WHERE id = NEW.subscription_id;
         END;",
        )
        .unwrap();
    let count = server.requests().len();
    let output = command(server.db_path().parent().unwrap())
        .args(["--url", server.url(), "--handle", &handle])
        .output()
        .unwrap();
    terminal(&output, 4, "batch_no_longer_pending");
    assert_eq!(server.requests().len(), count + 1);
    let attempts: i64 = connection
        .query_row(
            "SELECT attempts FROM batches ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempts, 0, "failed release records no delivery");
    connection
        .execute_batch("DROP TRIGGER acknowledge_at_seal;")
        .unwrap();
}

struct WaiterProcess(Option<Child>);

impl WaiterProcess {
    fn spawn(command: &mut Command) -> Self {
        Self(Some(
            command
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ))
    }

    fn child(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }

    fn finish(&mut self) -> Output {
        self.0.take().unwrap().wait_with_output().unwrap()
    }
}

impl Drop for WaiterProcess {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_delivers_pending_batch_with_empty_path() {
    let server = Server::start_with_request_log().await;
    let client = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
        .await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    client
        .publish("orders", "first", json!("python-free"))
        .await;
    let pending = client
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .ok();
    let home = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_minymaton-wait"))
        .args(["--url", server.url(), "--handle", &handle])
        .env("PATH", "")
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .env_remove("MINYMATON_URL")
        .env_remove("MINYMATON_TOKEN")
        .output()
        .expect("run the real executable with an empty child PATH");
    assert!(
        output.status.success(),
        "waiter exited {:?}: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let batch: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(batch["status"], "events");
    assert_eq!(batch["batch_id"], pending["batch_id"]);
    assert_eq!(batch["ack_after_handling"], pending["batch_id"]);
    assert_eq!(batch["events"], pending["events"]);
    assert_eq!(batch["delivery_attempt"], 2);
    let replay = client
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(replay["batch_id"], batch["batch_id"]);
    assert_eq!(replay["delivery_attempt"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_delivers_unacknowledged_batches_without_allocating_sessions() {
    let server = Server::start_with_request_log().await;
    let client = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
        .await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    client
        .publish("orders", "first", json!("handle before acknowledging"))
        .await;
    let output = command(server.db_path().parent().unwrap())
        .args(["--url", server.url(), "--handle", &handle])
        .output()
        .expect("run the real CLI waiter");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let batch: Value = serde_json::from_slice(&output.stdout).expect("waiter batch JSON");
    assert_eq!(batch["status"], "events");
    assert_eq!(batch["delivery_attempt"], 1);
    let requests = server.requests();
    let helper_receive = requests
        .iter()
        .rev()
        .find(|record| record["tool"] == "receive")
        .unwrap();
    assert!(helper_receive.get("session").is_none_or(Value::is_null));
    let replay = client
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(replay["batch_id"], batch["batch_id"]);
    assert_eq!(replay["delivery_attempt"], 2);
    client
        .call("receive", ack_only(&handle, &batch["batch_id"]))
        .await
        .ok();

    let missing = command(server.db_path().parent().unwrap())
        .args(["--url", server.url(), "--handle", "sub_missing"])
        .output()
        .expect("run waiter for missing subscription");
    assert_eq!(missing.status.code(), Some(5));
    assert!(missing.stdout.is_empty());
    let error: Value = serde_json::from_slice(&missing.stderr).expect("waiter error JSON");
    assert_eq!(error["status"], "error");
    assert_eq!(error["code"], "subscription_not_found");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_authenticates_with_bom_config_and_reports_malformed_config() {
    let token = "waiter_test_token_0123456789abcdef0123456789";
    let server = Server::start_with_bearer_tokens(token).await;
    let http = reqwest::Client::new();
    let mut handle = String::new();
    for (id, name, arguments) in [
        (1, "stream", json!({"action":"create","name":"orders"})),
        (
            2,
            "subscription",
            json!({"action":"create","stream":"orders","label":"reader"}),
        ),
        (
            3,
            "publish",
            json!({"stream":"orders","idempotency_key":"first","payload":"authenticated"}),
        ),
    ] {
        let response = http
            .post(server.url())
            .bearer_auth(token)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2026-07-28")
            .header("Mcp-Method", "tools/call")
            .header("Mcp-Name", name)
            .body(
                json!({
                    "jsonrpc":"2.0","id":id,"method":"tools/call",
                    "params":{"name":name,"arguments":arguments,"_meta":{
                        "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                        "io.modelcontextprotocol/clientInfo":{"name":"bom-test","version":"0"},
                        "io.modelcontextprotocol/clientCapabilities":{},
                    }},
                })
                .to_string(),
            )
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let reply = RawReply::read(response).await.message();
        assert_ne!(reply["result"]["isError"], true, "{reply}");
        if name == "subscription" {
            handle = reply["result"]["structuredContent"]["handle"]
                .as_str()
                .unwrap()
                .to_owned();
        }
    }
    verify_bom_config(&server, &handle, token);
}

#[test]
fn cli_waiter_sends_one_modern_request_with_optional_ack() {
    let home = tempfile::tempdir().unwrap();
    for ack in [None, Some("bat_handled")] {
        let batch = events();
        let mut server =
            HttpServer::json(json!({"jsonrpc":"2.0","id":2,"result":{"structuredContent":batch}}));
        let mut command = command(home.path());
        command
            .args([
                "--url",
                &format!("{}?private=query", server.url),
                "--handle",
                "sub_test",
                "--max-events=+02",
                "--max-bytes",
                "16_384",
            ])
            .env("HTTP_PROXY", "http://127.0.0.1:1")
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("ALL_PROXY", "http://127.0.0.1:1")
            .env("NO_PROXY", "");
        if let Some(ack) = ack {
            command.args(["--ack", ack]);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            batch
        );
        let requests = server.finish();
        assert_eq!(
            requests.len(),
            1,
            "no initialize, retries, auto-ACK or cleanup"
        );
        let request = &requests[0];
        assert_eq!(request.line, "POST /mcp?private=query HTTP/1.1\r\n");
        assert_eq!(request.headers["content-type"], "application/json");
        assert_eq!(
            request.headers["accept"],
            "application/json, text/event-stream"
        );
        assert_eq!(
            request.headers["mcp-protocol-version"],
            ProtocolVersion::V_2026_07_28.as_str()
        );
        assert_eq!(request.headers["mcp-method"], "tools/call");
        assert_eq!(request.headers["mcp-name"], "receive");
        assert!(!request.headers.contains_key("mcp-session-id"));
        assert_eq!(request.body["jsonrpc"], "2.0");
        assert_eq!(request.body["id"], 2);
        assert_eq!(request.body["method"], "tools/call");
        assert_eq!(request.body["params"]["name"], "receive");
        let arguments = &request.body["params"]["arguments"];
        assert_eq!(arguments["handle"], "sub_test");
        assert_eq!(arguments["wait"], true);
        assert_eq!(arguments["max_events"], 2);
        assert_eq!(arguments["max_bytes"], 16384);
        assert_eq!(arguments.get("ack").and_then(Value::as_str), ack);
        assert_eq!(
            request.body["params"]["_meta"],
            json!({
                "io.modelcontextprotocol/protocolVersion": ProtocolVersion::V_2026_07_28,
                "io.modelcontextprotocol/clientInfo": {"name":"minymaton-wait","version":"0.1.0"},
                "io.modelcontextprotocol/clientCapabilities": {},
            })
        );
    }
}

#[test]
fn cli_waiter_terminal_envelopes_never_retry_or_rearm() {
    let home = tempfile::tempdir().unwrap();
    let mut cases = vec![
        (
            json!({"structuredContent":{"status":"superseded","ack_result":{"outcome":"committed"}}}),
            3,
            "",
        ),
        (
            json!({"structuredContent":{"status":"empty"}}),
            1,
            "unexpected_status",
        ),
        (json!({"structuredContent":events()}), 0, ""),
        (
            json!({"content":[{"type":"text","text":events().to_string()}]}),
            0,
            "",
        ),
    ];
    for (code, exit) in [
        ("batch_no_longer_pending", 4),
        ("subscription_not_found", 5),
        ("batch_exceeds_bounds", 6),
        ("invalid_ack", 1),
    ] {
        cases.push((
            json!({"isError":true,"structuredContent":{
                "code":code,"message":"not delivered","details":{"ack":"kept"},
            }}),
            exit,
            code,
        ));
    }
    for (result, exit, code) in cases {
        let mut server = HttpServer::json(json!({"id":2,"result":result}));
        let output = command(home.path())
            .args(["--url", &server.url, "--handle", "sub_test"])
            .output()
            .unwrap();
        if exit == 0 {
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                events()
            );
        } else if exit == 3 {
            assert_eq!(output.status.code(), Some(3));
            assert!(output.stdout.is_empty());
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stderr).unwrap(),
                result["structuredContent"]
            );
        } else {
            let error = terminal(&output, exit, code);
            if result["isError"] == true {
                assert_eq!(error["details"], result["structuredContent"]["details"]);
            }
        }
        assert_eq!(server.finish().len(), 1, "{result}");
    }
}

#[test]
fn cli_waiter_rejects_malformed_and_mismatched_responses() {
    let home = tempfile::tempdir().unwrap();
    let mut cases = vec![
        (json!({"id":3,"result":{}}), "protocol_failed"),
        (json!({"id":2,"method":"not-a-response"}), "protocol_failed"),
        (json!([]), "protocol_failed"),
        (
            json!({"id":2,"error":{"code":-32602,"message":"bad request"}}),
            "jsonrpc_error",
        ),
    ];
    for result in [
        Value::Null,
        json!([]),
        json!({}),
        json!({"structuredContent":[]}),
        json!({"structuredContent":{"status":"events","events":[],"batch_id":"bat_test","ack_after_handling":"bat_test"}}),
        json!({"structuredContent":{"status":"events","events":[{}],"batch_id":"bat_test","ack_after_handling":"bat_wrong"}}),
        json!({"isError":true,"structuredContent":{"code":"invalid_ack"}}),
        json!({"content":[{"type":"text","text":"{"}]}),
    ] {
        cases.push((json!({"id":2,"result":result}), "protocol_failed"));
    }
    for (message, code) in cases {
        let mut server = HttpServer::json(message.clone());
        let output = command(home.path())
            .args(["--url", &server.url, "--handle", "sub_test"])
            .output()
            .unwrap();
        let error = terminal(&output, 1, code);
        if code == "jsonrpc_error" {
            assert_eq!(error["details"], message["error"]);
        }
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn cli_waiter_reads_split_chunked_sse_and_stops_at_http_body_eof() {
    let home = tempfile::tempdir().unwrap();
    let mut batch = events();
    batch["events"][0]["payload"] = json!("hello \u{e9}");
    let body = format!(
        ": comment\r\ndata: {{\"id\":2,\"method\":\"not-a-response\"}}\r\n\r\ndata: {{\"id\":99,\"result\":{{}}}}\r\n\r\ndata: {{\"id\":2,\r\ndata: \"result\":{{\"structuredContent\":{batch}}}}}\r\n\r\n"
    );
    let mut wire =
        b"HTTP/1.1 200 OK\r\nContent-Type: Text/Event-Stream\r\nTransfer-Encoding: chunked\r\n\r\n"
            .to_vec();
    for chunk in body.as_bytes().chunks(1) {
        write!(&mut wire, "{:x}\r\n", chunk.len()).unwrap();
        wire.extend_from_slice(chunk);
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(b"0\r\n\r\n");
    let mut server = HttpServer::start(wire);
    let output = command(home.path())
        .args(["--url", &server.url, "--handle", "sub_test"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        batch
    );
    assert_eq!(server.finish().len(), 1);

    for (body, exit, code) in [
        ("", 7, "transport_failed"),
        ("data: {\"id\":99,\"result\":{}}\n\n", 7, "transport_failed"),
        ("data: {\"id\":2,\"result\":{}}", 7, "transport_failed"),
        ("data: []\n\n", 1, "protocol_failed"),
        ("data: {\n\n", 1, "protocol_failed"),
    ] {
        let mut wire = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        if !body.is_empty() {
            write!(&mut wire, "{:x}\r\n{body}\r\n", body.len()).unwrap();
        }
        wire.extend_from_slice(b"0\r\n\r\n");
        let mut server = HttpServer::start(wire);
        let output = command(home.path())
            .args(["--url", &server.url, "--handle", "sub_test"])
            .output()
            .unwrap();
        terminal(&output, exit, code);
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn cli_waiter_transport_failures_and_redirects_are_terminal() {
    let home = tempfile::tempdir().unwrap();
    for (wire, exit, code) in [
                (b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n".to_vec(), 7, "transport_failed"),
                (b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirected\r\nContent-Length: 0\r\n\r\n".to_vec(), 1, "protocol_failed"),
                (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{".to_vec(), 7, "transport_failed"),
                (b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10\r\ndata: {".to_vec(), 7, "transport_failed"),
                (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1\r\n\r\n{".to_vec(), 1, "protocol_failed"),
            ] {
                let mut server = HttpServer::start(wire);
                let output = command(home.path()).args(["--url", &server.url, "--handle", "sub_test"]).output().unwrap();
                terminal(&output, exit, code);
                assert_eq!(server.finish().len(), 1);
            }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "http://{}/mcp?secret=not-for-diagnostics",
        listener.local_addr().unwrap()
    );
    drop(listener);
    let output = command(home.path())
        .args(["--url", &url, "--handle", "sub_test"])
        .output()
        .unwrap();
    terminal(&output, 7, "transport_failed");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("not-for-diagnostics"));
}

#[test]
fn cli_waiter_usage_errors_are_distinct_from_runtime_errors() {
    let home = tempfile::tempdir().unwrap();
    for args in [
        vec![],
        vec!["--handle"],
        vec!["--handle", "sub_test", "--stream", "orders"],
        vec!["--handle", "sub_test", "--endpoint", "http://localhost/mcp"],
        vec!["--handle", "sub_test", "--max-events", "not-an-int"],
        vec!["--handle", "sub_test", "--max-bytes", "1.5"],
        vec!["--handle", "sub_test", "--max-events", "1__0"],
    ] {
        let output = command(home.path()).args(args).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).starts_with("usage: "));
    }
    let help = command(home.path()).arg("--help").output().unwrap();
    assert!(help.status.success());
    assert!(help.stderr.is_empty());
    assert!(String::from_utf8_lossy(&help.stdout).starts_with("usage: "));
    for url in ["not-a-url", "file:///private", "http://"] {
        let output = command(home.path())
            .args(["--handle", "sub_test", "--url", url])
            .output()
            .unwrap();
        terminal(&output, 1, "protocol_failed");
    }
    let mut server = HttpServer::json(
        json!({"id":2,"result":{"isError":true,"structuredContent":{"code":"invalid_arguments","message":"bounds"}}}),
    );
    let output = command(home.path())
        .args([
            "--handle",
            "sub_test",
            "--url",
            &server.url,
            "--max-events",
            "-1",
        ])
        .output()
        .unwrap();
    terminal(&output, 1, "invalid_arguments");
    assert_eq!(
        server.finish()[0].body["params"]["arguments"]["max_events"],
        -1
    );
}

#[test]
fn cli_waiter_review_option_values_keep_argparse_classification() {
    let home = tempfile::tempdir().unwrap();
    for arguments in [
        vec!["--handle", "-x"],
        vec!["--handle", "sub_test", "--ack", "-x"],
        vec!["--handle", "sub_test", "--url", "-x"],
        vec!["--handle", "-hvalue"],
        vec!["--handle", "sub_test", "--ack", "-1e2"],
        vec!["--handle", "sub_test", "--max-events", "-1_0"],
        vec!["--handle", "sub_test", "--ack", "--"],
    ] {
        let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
        let output = command(home.path())
            .args(["--url", &server.url])
            .args(&arguments)
            .output()
            .unwrap();
        let requests = server.finish();
        assert_eq!(
            output.status.code(),
            Some(2),
            "{arguments:?}: stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty(), "{arguments:?}");
        assert!(String::from_utf8_lossy(&output.stderr).starts_with("usage: "));
        assert!(requests.is_empty(), "usage error sent HTTP: {arguments:?}");
    }
    for value in [
        "-",
        "-1",
        "-1.5",
        "-.5",
        "-\u{0661}",
        "-1\n",
        "-x with space",
        "--x with space",
    ] {
        let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
        let output = command(home.path())
            .args(["--url", &server.url, "--handle", value, "--ack", value])
            .output()
            .unwrap();
        assert!(output.status.success(), "{value:?}: {output:?}");
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body["params"]["arguments"]["handle"], value);
        assert_eq!(requests[0].body["params"]["arguments"]["ack"], value);
    }
    for value in ["-x", "-h", "--", "-1e2"] {
        let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
        let output = command(home.path())
            .args([
                "--url",
                &server.url,
                &format!("--handle={value}"),
                &format!("--ack={value}"),
                "--max-events=-1_0",
            ])
            .output()
            .unwrap();
        assert!(output.status.success(), "{value:?}: {output:?}");
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body["params"]["arguments"]["handle"], value);
        assert_eq!(requests[0].body["params"]["arguments"]["ack"], value);
        assert_eq!(requests[0].body["params"]["arguments"]["max_events"], -10);
    }
}

#[cfg(unix)]
struct RestrictedPermissions {
    path: std::path::PathBuf,
    permissions: std::fs::Permissions,
}

#[test]
fn cli_waiter_argparse_prefixes_and_unicode_decimal_values() {
    let home = tempfile::tempdir().unwrap();
    for (arguments, max_events, max_bytes) in [
        (
            vec![
                "--handle",
                "sub_test",
                "--max-events",
                "\u{2003}+\u{ff10}\u{ff10}_\u{0662}\u{a0}",
            ],
            2,
            16384,
        ),
        (
            vec![
                "--hand",
                "sub_test",
                "--max-e",
                "+00_2",
                "--max-b",
                "\u{0661}\u{0666}_\u{0663}\u{0668}\u{0664}",
                "--a=bat_handled",
            ],
            2,
            16384,
        ),
        (
            vec![
                "--handle",
                "sub_test",
                "--max-events",
                "\u{2003}+\u{ff10}\u{ff10}_\u{0662}\u{a0}",
            ],
            2,
            16384,
        ),
        (
            vec!["--handle", "sub_test", "--max-events=-\u{0661}_\u{0660}"],
            -10,
            16384,
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "\u{1e955}"],
            5,
            16384,
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "\u{1d7e1}"],
            9,
            16384,
        ),
        (
            vec!["--handle", "sub_test", "--max-e", "\u{85}2\u{85}"],
            2,
            16384,
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "-\u{0661}"],
            -1,
            16384,
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "+0_00"],
            0,
            16384,
        ),
    ] {
        let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
        let url_flag = if arguments.contains(&"--hand") {
            "--u"
        } else {
            "--url"
        };
        let output = command(home.path())
            .args([url_flag, &server.url])
            .args(&arguments)
            .output()
            .unwrap();
        assert!(output.status.success(), "{arguments:?}: {output:?}");
        assert!(output.stderr.is_empty());
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        let actual = &requests[0].body["params"]["arguments"];
        assert_eq!(actual["handle"], "sub_test");
        assert_eq!(actual["max_events"], max_events);
        assert_eq!(actual["max_bytes"], max_bytes);
        assert_eq!(actual["wait"], true);
        if arguments.contains(&"--a=bat_handled") {
            assert_eq!(actual["ack"], "bat_handled");
        }
    }
}

#[test]
fn cli_waiter_argparse_help_and_errors_match_action_order() {
    let home = tempfile::tempdir().unwrap();
    for (arguments, exit, diagnostic) in [
        (vec!["--bogus", "--help"], 0, ""),
        (vec!["--bogus", "--hel"], 0, ""),
        (vec!["junk", "--he"], 0, ""),
        (vec!["--help", "--max"], 2, "ambiguous option"),
        (vec!["--max", "--help"], 2, "ambiguous option"),
        (vec!["--help", "--h"], 2, "ambiguous option"),
        (vec!["--h"], 2, "ambiguous option"),
        (vec!["--handle", "--help"], 2, "expected one argument"),
        (vec!["--handle", "-x", "--help"], 2, "expected one argument"),
        (
            vec!["--handle", "sub_test", "--max-e=oops", "--help"],
            2,
            "invalid int value",
        ),
        (vec!["--help", "--max-e=oops"], 0, ""),
        (vec!["--help", "--handle"], 0, ""),
        (vec!["-hx"], 0, ""),
        (vec!["-hhx"], 0, ""),
        (vec!["-hh=x"], 2, "ignored explicit argument"),
        (vec!["-hh="], 2, "ignored explicit argument"),
        (vec!["-hh-h"], 2, "ignored explicit argument"),
        (vec!["-h=x"], 2, "ignored explicit argument"),
        (vec!["-h-h"], 2, "ignored explicit argument"),
        (vec!["--help="], 2, "ignored explicit argument"),
        (vec!["-h="], 2, "ignored explicit argument"),
        (vec!["--", "--help"], 2, "--handle"),
        (
            vec!["--handle", "sub_test", "--max-events", "\u{1c}2"],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "\u{b2}"],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "+_2"],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "1__0"],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", ""],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "_1"],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "1_"],
            2,
            "invalid int value",
        ),
        (
            vec!["--handle", "sub_test", "--max-events", "0x10"],
            2,
            "invalid int value",
        ),
    ] {
        let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
        let output = command(home.path())
            .args(["--url", &server.url])
            .args(&arguments)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(exit),
            "{arguments:?}: {output:?}"
        );
        if exit == 0 {
            assert!(output.stderr.is_empty());
            assert!(String::from_utf8_lossy(&output.stdout).starts_with("usage: "));
        } else {
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.starts_with("usage: "), "{stderr}");
            assert!(stderr.contains(diagnostic), "{arguments:?}: {stderr}");
        }
        assert!(
            server.finish().is_empty(),
            "{arguments:?}: no HTTP for help or usage"
        );
    }
}

#[cfg(unix)]
impl RestrictedPermissions {
    fn new(path: &Path, mode: u32) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::metadata(path).unwrap().permissions();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        Self {
            path: path.to_owned(),
            permissions,
        }
    }
}

#[cfg(unix)]
impl Drop for RestrictedPermissions {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.path, self.permissions.clone())
            .expect("restore the temporary config path's permissions");
    }
}

#[cfg(unix)]
#[test]
fn cli_waiter_review_config_metadata_failure_never_falls_back() {
    let home = tempfile::tempdir().unwrap();
    let directory = home.path().join("restricted-config-dir");
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("minymaton.ini");
    std::fs::write(
        &path,
        "[minymaton]\nurl = http://127.0.0.1:1/mcp\nhandle = sub_test\n",
    )
    .unwrap();
    for metadata_failure in [true, false] {
        let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
        std::fs::write(
            home.path().join(".claude.json"),
            json!({"mcpServers":{"minymaton":{
                "url":server.url,
            }}})
            .to_string(),
        )
        .unwrap();
        let restricted = if metadata_failure {
            RestrictedPermissions::new(&directory, 0o600)
        } else {
            RestrictedPermissions::new(&path, 0o000)
        };
        if metadata_failure {
            assert_eq!(
                std::fs::metadata(&path).unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        } else {
            assert!(std::fs::metadata(&path).unwrap().is_file());
            assert_eq!(
                std::fs::read(&path).unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied
            );
        }
        let output = command(home.path())
            .args(["--config", path.to_str().unwrap(), "--handle", "sub_test"])
            .output()
            .unwrap();
        let requests = server.finish();
        drop(restricted);
        let error = terminal(&output, 1, "config_failed");
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains(path.to_str().unwrap())
        );
        assert!(
            requests.is_empty(),
            "config failure selected a fallback endpoint"
        );
    }
}

#[test]
fn cli_waiter_review_missing_and_nonfile_configs_still_skip() {
    let home = tempfile::tempdir().unwrap();
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    for relative in [
        ".copilot/mcp-config.json",
        ".claude.json",
        ".gemini/antigravity/mcp_config.json",
    ] {
        let path = home.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "{").unwrap();
    }
    let missing_url = command(home.path())
        .args(["--handle", "sub_test"])
        .output()
        .unwrap();
    terminal(&missing_url, 1, "config_failed");
    let output = command(home.path())
        .args(["--url", &server.url, "--handle", "sub_test"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn cli_waiter_review_home_uses_the_platform_environment() {
    let home = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    std::fs::write(
        home.path().join(".claude.json"),
        json!({"mcpServers":{"minymaton":{
            "url":server.url,
        }}})
        .to_string(),
    )
    .unwrap();
    std::fs::write(other.path().join(".claude.json"), "{").unwrap();
    let mut command_no_url = command(home.path());
    #[cfg(unix)]
    command_no_url.env("USERPROFILE", other.path());
    #[cfg(windows)]
    command_no_url.env("HOME", other.path());
    let missing_url = command_no_url
        .args(["--handle", "sub_test"])
        .output()
        .unwrap();
    terminal(&missing_url, 1, "config_failed");
    let ini_path = other.path().join("minymaton.ini");
    std::fs::write(&ini_path, format!("[minymaton]\nurl = {}\n", server.url)).unwrap();
    let output = command(home.path())
        .env("MINYMATON_CONFIG", &ini_path)
        .args(["--handle", "sub_test"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(server.finish().len(), 1);
}

#[cfg(windows)]
#[test]
fn cli_waiter_review_windows_home_preserves_empty_and_missing_variables() {
    let home = tempfile::tempdir().unwrap();
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    std::fs::write(
        home.path().join(".claude.json"),
        json!({"mcpServers":{"minymaton":{
            "url":server.url,
        }}})
        .to_string(),
    )
    .unwrap();
    let missing_url = command(home.path())
        .current_dir(home.path())
        .args(["--handle", "sub_test"])
        .output()
        .unwrap();
    terminal(&missing_url, 1, "config_failed");
    let ini_path = home.path().join("minymaton.ini");
    std::fs::write(&ini_path, format!("[minymaton]\nurl = {}\n", server.url)).unwrap();
    let output = command(home.path())
        .current_dir(home.path())
        .env_remove("USERPROFILE")
        .env_remove("HOMEPATH")
        .env("MINYMATON_CONFIG", &ini_path)
        .args(["--handle", "sub_test"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_review_stdout_failure_keeps_the_batch_unacknowledged() {
    let server = Server::start_with_request_log().await;
    let client = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
        .await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    client
        .publish(
            "orders",
            "first",
            json!("keep pending after output failure"),
        )
        .await;
    let pending = client
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .ok();
    for close_stderr in [false, true] {
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        let mut command = command(server.db_path().parent().unwrap());
        command
            .args(["--url", server.url(), "--handle", &handle])
            .stdout(writer);
        if close_stderr {
            let (reader, writer) = std::io::pipe().unwrap();
            drop(reader);
            command.stderr(writer);
        }
        let count = server.requests().len();
        let output = command.output().unwrap();
        assert_eq!(server.requests().len(), count + 1, "no auto-ACK or retry");
        if close_stderr {
            assert_eq!(output.status.code(), Some(7), "{output:?}");
            assert!(output.stderr.is_empty());
        } else {
            terminal(&output, 7, "transport_failed");
        }
    }
    let replay = client
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(replay["batch_id"], pending["batch_id"]);
    assert_eq!(replay["delivery_attempt"], 4);
}

#[test]
fn cli_waiter_review_failed_diagnostic_sinks_have_a_classified_exit() {
    let home = tempfile::tempdir().unwrap();
    for result in [
        json!({"structuredContent":{"status":"superseded"}}),
        json!({"isError":true,"structuredContent":{"code":"subscription_not_found","message":"missing"}}),
    ] {
        let mut server = HttpServer::json(json!({"id":2,"result":result}));
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        let output = command(home.path())
            .args(["--url", &server.url, "--handle", "sub_test"])
            .stderr(writer)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(7), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
        assert_eq!(server.finish().len(), 1);
    }
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let output = command(home.path())
        .arg("--unknown")
        .stderr(writer)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(7), "{output:?}");
    assert!(output.stdout.is_empty());
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let output = command(home.path())
        .arg("--help")
        .stdout(writer)
        .output()
        .unwrap();
    terminal(&output, 7, "transport_failed");
}

#[test]
fn cli_waiter_preserves_config_precedence_and_same_endpoint_credentials() {
    let home = tempfile::tempdir().unwrap();
    let bearer = |token: &str| format!("{} {token}", "Bearer");
    // Harness files in HOME (.copilot, .claude.json, .gemini) must be completely ignored
    for relative in [
        ".copilot/mcp-config.json",
        ".claude.json",
        ".gemini/antigravity/mcp_config.json",
    ] {
        let path = home.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            json!({"mcpServers":{"minymaton":{
                "httpUrl":"http://127.0.0.1:9/mcp","headers":{"authorization":bearer("harness-secret")},
            }}})
            .to_string(),
        )
        .unwrap();
    }

    // Without --url, MINYMATON_URL, or --config / MINYMATON_CONFIG, fails with config_failed (exit 1)
    let missing_url = command(home.path())
        .args(["--handle", "sub_test"])
        .output()
        .unwrap();
    terminal(&missing_url, 1, "config_failed");

    let env_ini = home.path().join("env.ini");
    let cli_ini = home.path().join("cli.ini");
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    std::fs::write(
        &env_ini,
        format!(
            "[minymaton]\nurl = {}\ntoken_env = ENV_INI_TOKEN\n",
            server.url
        ),
    )
    .unwrap();
    std::fs::write(
        &cli_ini,
        format!(
            "[minymaton]\nurl = {}\ntoken_env = CLI_INI_TOKEN\n",
            server.url
        ),
    )
    .unwrap();

    // 1. MINYMATON_CONFIG supplies INI when --config is omitted
    let output = command(home.path())
        .args(["--handle", "sub_test"])
        .env("MINYMATON_CONFIG", &env_ini)
        .env("ENV_INI_TOKEN", "from-env-ini")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    // 2. --config overrides MINYMATON_CONFIG
    let output = command(home.path())
        .args([
            "--config",
            cli_ini.to_str().unwrap(),
            "--handle",
            "sub_test",
        ])
        .env("MINYMATON_CONFIG", &env_ini)
        .env("CLI_INI_TOKEN", "from-cli-ini")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    // 3. MINYMATON_TOKEN_ENV overrides INI token_env, and --token-env overrides MINYMATON_TOKEN_ENV
    let output = command(home.path())
        .args([
            "--config",
            cli_ini.to_str().unwrap(),
            "--handle",
            "sub_test",
        ])
        .env("MINYMATON_TOKEN_ENV", "ENV_VAR_OVERRIDE")
        .env("ENV_VAR_OVERRIDE", "from-env-var-override")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let output = command(home.path())
        .args([
            "--config",
            cli_ini.to_str().unwrap(),
            "--handle",
            "sub_test",
            "--token-env",
            "CLI_FLAG_OVERRIDE",
        ])
        .env("MINYMATON_TOKEN_ENV", "ENV_VAR_OVERRIDE")
        .env("CLI_FLAG_OVERRIDE", "from-cli-flag-override")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].headers["authorization"], bearer("from-env-ini"));
    assert_eq!(requests[1].headers["authorization"], bearer("from-cli-ini"));
    assert_eq!(
        requests[2].headers["authorization"],
        bearer("from-env-var-override")
    );
    assert_eq!(
        requests[3].headers["authorization"],
        bearer("from-cli-flag-override")
    );

    // 4. Specified-but-missing or empty token_env (--token-env, MINYMATON_TOKEN_ENV, or INI token_env) fails with config_failed (exit 1)
    let missing_cli_token = command(home.path())
        .args([
            "--url",
            "http://127.0.0.1:9/mcp",
            "--handle",
            "sub_test",
            "--token-env",
            "MISSING_TOKEN_VAR",
        ])
        .env("MINYMATON_TOKEN", "should-not-fallback")
        .output()
        .unwrap();
    terminal(&missing_cli_token, 1, "config_failed");

    let empty_env_token = command(home.path())
        .args(["--url", "http://127.0.0.1:9/mcp", "--handle", "sub_test"])
        .env("MINYMATON_TOKEN_ENV", "EMPTY_TOKEN_VAR")
        .env("EMPTY_TOKEN_VAR", "")
        .env("MINYMATON_TOKEN", "should-not-fallback")
        .output()
        .unwrap();
    terminal(&empty_env_token, 1, "config_failed");

    let missing_ini_token = command(home.path())
        .args([
            "--config",
            cli_ini.to_str().unwrap(),
            "--handle",
            "sub_test",
        ])
        .env("MINYMATON_TOKEN", "should-not-fallback")
        .output()
        .unwrap();
    terminal(&missing_ini_token, 1, "config_failed");

    // 5. Default MINYMATON_TOKEN is optional when token_env is not specified
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    let output = command(home.path())
        .args(["--handle", "sub_test", "--url", &server.url])
        .env("MINYMATON_URL", "http://ignored.invalid/mcp")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    for token in ["environment", &bearer("environment")] {
        let output = command(home.path())
            .args(["--handle", "sub_test"])
            .env("MINYMATON_URL", &server.url)
            .env("MINYMATON_TOKEN", token)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(!requests[0].headers.contains_key("authorization"));
    for request in &requests[1..] {
        assert_eq!(request.headers["authorization"], bearer("environment"));
    }
}

#[test]
fn cli_waiter_config_errors_include_paths_without_credentials() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("minymaton.ini");
    for contents in [
        "{\n".to_owned(),
        "[minymaton]\nhandle = sub_test\n[minymaton]\nhandle = dup\n".to_owned(),
        "[minymaton]\nhandle = sub_test\nurl = %(private-secret)s\n".to_owned(),
        "[minymaton]\nhandle = sub_test\nmax_events = private-secret\n".to_owned(),
    ] {
        std::fs::write(&path, contents).unwrap();
        let output = command(home.path())
            .args(["--config", path.to_str().unwrap(), "--handle", "sub_test"])
            .env("MINYMATON_TOKEN", "private-secret")
            .output()
            .unwrap();
        let error = terminal(&output, 1, "config_failed");
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains(path.to_str().unwrap())
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("private-secret"));
    }
}

async fn parked_fixture() -> (
    tempfile::TempDir,
    tokio::task::JoinHandle<std::io::Result<()>>,
    String,
    String,
    tokio::sync::watch::Receiver<usize>,
) {
    let dir = tempfile::tempdir().unwrap();
    let queue = minymaton_core::Queue::open(&dir.path().join("queue.sqlite3")).unwrap();
    queue
        .stream(json!({"action":"create","name":"orders"}))
        .await
        .unwrap();
    let subscription = queue
        .subscription(json!({"action":"create","stream":"orders","label":"reader"}))
        .await
        .unwrap();
    let handle = subscription["handle"].as_str().unwrap().to_owned();
    let activity = queue.active_waits();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let server = tokio::spawn(minymaton_server::serve(listener, queue));
    (dir, server, url, handle, activity)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hard_killed_http_waiter_drops_its_guard_before_the_next_publish() {
    let (dir, server, url, handle, mut activity) = parked_fixture().await;
    let server = tokio_util::task::AbortOnDropHandle::new(server);
    let client = ()
        .serve(rmcp::transport::StreamableHttpClientTransport::from_uri(
            url.clone(),
        ))
        .await
        .unwrap();
    let mut process =
        WaiterProcess::spawn(command(dir.path()).args(["--url", &url, "--handle", &handle]));
    assert_eq!(*activity.wait_for(|count| *count == 1).await.unwrap(), 1);
    let listed = client
        .call_tool(
            CallToolRequestParams::new("subscription").with_arguments(
                json!({"action":"list","stream":"orders"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        listed.structured_content.unwrap()["subscriptions"][0]["waiting"],
        true
    );
    process.child().kill().unwrap();
    assert!(!process.finish().status.success());
    assert_eq!(*activity.wait_for(|count| *count == 0).await.unwrap(), 0);
    client
        .call_tool(
            CallToolRequestParams::new("publish").with_arguments(
                json!({"stream":"orders","idempotency_key":"after-kill","payload":"next"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    let received = client
        .call_tool(
            CallToolRequestParams::new("receive").with_arguments(
                json!({"handle":handle,"max_events":10,"max_bytes":16384,"wait":false})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap()
        .structured_content
        .unwrap();
    assert_eq!(received["status"], "events");
    assert_eq!(received["delivery_attempt"], 1);
    client.cancel().await.unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_ctrl_c_exits_130_and_drops_the_parked_guard() {
    let (dir, server, url, handle, mut activity) = parked_fixture().await;
    let server = tokio_util::task::AbortOnDropHandle::new(server);
    let mut process =
        WaiterProcess::spawn(command(dir.path()).args(["--url", &url, "--handle", &handle]));
    assert_eq!(*activity.wait_for(|count| *count == 1).await.unwrap(), 1);
    let signal = Command::new("kill")
        .args(["-INT", &process.child().id().to_string()])
        .status()
        .unwrap();
    assert!(signal.success());
    terminal(&process.finish(), 130, "interrupted");
    assert_eq!(*activity.wait_for(|count| *count == 0).await.unwrap(), 0);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_inherited_sigint_ignore_delivers_after_signal() {
    let (dir, server, url, handle, mut activity) = parked_fixture().await;
    let server = tokio_util::task::AbortOnDropHandle::new(server);
    let client = ()
        .serve(rmcp::transport::StreamableHttpClientTransport::from_uri(
            url.clone(),
        ))
        .await
        .unwrap();
    let mut child = Command::new("/bin/sh");
    child
        .args([
            "-c",
            "trap '' INT HUP TERM; exec \"$@\"",
            "inherited-int",
            env!("CARGO_BIN_EXE_minymaton-wait"),
            "--url",
            &url,
            "--handle",
            &handle,
        ])
        .env("PATH", "")
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .env_remove("MINYMATON_URL")
        .env_remove("MINYMATON_TOKEN");
    let mut process = WaiterProcess::spawn(&mut child);
    assert_eq!(*activity.wait_for(|count| *count == 1).await.unwrap(), 1);
    let pid = process.child().id();
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let ignored = status
        .lines()
        .find_map(|line| line.strip_prefix("SigIgn:"))
        .unwrap();
    let ignored = u64::from_str_radix(ignored.trim(), 16).unwrap();
    assert_ne!(
        ignored & (1 << (libc::SIGINT - 1)),
        0,
        "parked child lost inherited SIG_IGN"
    );
    for signal in [libc::SIGHUP, libc::SIGTERM] {
        assert_ne!(
            ignored & (1 << (signal - 1)),
            0,
            "SIGINT registration changed signal {signal}"
        );
    }
    assert!(
        Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    client.call_tool(CallToolRequestParams::new("publish").with_arguments(
        json!({"stream":"orders","idempotency_key":"after-ignored-int","payload":"still waiting"})
            .as_object().unwrap().clone(),
    )).await.unwrap();
    let output = process.finish();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    let batch: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(batch["events"][0]["payload"], "still waiting");
    assert_eq!(batch["delivery_attempt"], 1);
    assert_eq!(*activity.wait_for(|count| *count == 0).await.unwrap(), 0);
    let replay = client
        .call_tool(
            CallToolRequestParams::new("receive").with_arguments(
                receive_args(&handle, 10, 16384, 0)
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap()
        .structured_content
        .unwrap();
    assert_eq!(replay["batch_id"], batch["batch_id"]);
    assert_eq!(replay["delivery_attempt"], 2);
    client.cancel().await.unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_supersession_is_terminal_and_explicit_ack_advances_once() {
    let (dir, server, url, handle, mut activity) = parked_fixture().await;
    let server = tokio_util::task::AbortOnDropHandle::new(server);
    let client = ()
        .serve(rmcp::transport::StreamableHttpClientTransport::from_uri(
            url.clone(),
        ))
        .await
        .unwrap();
    let mut process =
        WaiterProcess::spawn(command(dir.path()).args(["--url", &url, "--handle", &handle]));
    assert_eq!(*activity.wait_for(|count| *count == 1).await.unwrap(), 1);
    client
        .call_tool(
            CallToolRequestParams::new("receive").with_arguments(
                receive_args(&handle, 10, 16384, 0)
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    let output = process.finish();
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(output.stdout.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stderr).unwrap()["status"],
        "superseded"
    );
    assert_eq!(*activity.wait_for(|count| *count == 0).await.unwrap(), 0);
    for key in ["first", "second"] {
        client
            .call_tool(
                CallToolRequestParams::new("publish").with_arguments(
                    json!({"stream":"orders","idempotency_key":key,"payload":key})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
            )
            .await
            .unwrap();
    }
    let first = command(dir.path())
        .args(["--url", &url, "--handle", &handle, "--max-events", "1"])
        .output()
        .unwrap();
    assert!(first.status.success(), "{first:?}");
    let first: Value = serde_json::from_slice(&first.stdout).unwrap();
    let second = command(dir.path())
        .args([
            "--url",
            &url,
            "--handle",
            &handle,
            "--ack",
            first["batch_id"].as_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(second.status.success(), "{second:?}");
    let second: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_ne!(second["batch_id"], first["batch_id"]);
    assert_eq!(second["ack_result"]["outcome"], "committed");
    assert_eq!(second["events"][0]["payload"], "second");
    assert_eq!(second["delivery_attempt"], 1);
    let replay = client
        .call_tool(
            CallToolRequestParams::new("receive").with_arguments(
                receive_args(&handle, 10, 16384, 0)
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap()
        .structured_content
        .unwrap();
    assert_eq!(replay["batch_id"], second["batch_id"]);
    assert_eq!(replay["delivery_attempt"], 2);
    client.cancel().await.unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
fn verify_bom_config(server: &Server, handle: &str, token: &str) {
    let home = tempfile::tempdir().unwrap();
    let config_path = home.path().join("minymaton.ini");
    let mut config = b"\xef\xbb\xbf".to_vec();
    config.extend(
        format!(
            "[minymaton]\nurl = {}\ntoken_env = BOM_INI_TOKEN\n",
            server.url()
        )
        .as_bytes(),
    );
    std::fs::write(&config_path, config).unwrap();
    let mut command = command(home.path());
    command
        .args([
            "--config",
            config_path.to_str().unwrap(),
            "--handle",
            handle,
        ])
        .env("BOM_INI_TOKEN", token);
    let received = command.output().unwrap();
    assert!(
        received.status.success(),
        "{}",
        String::from_utf8_lossy(&received.stderr)
    );
    let batch: Value = serde_json::from_slice(&received.stdout).unwrap();
    assert_eq!(batch["events"][0]["payload"], "authenticated");
    std::fs::write(&config_path, "{").unwrap();
    let failed = command.output().unwrap();
    assert_eq!(failed.status.code(), Some(1));
    assert!(failed.stdout.is_empty());
    let error: Value = serde_json::from_slice(&failed.stderr).unwrap();
    assert_eq!(error["code"], "config_failed");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains(config_path.to_str().unwrap())
    );
}

#[test]
fn cli_waiter_ini_config_supports_default_section_basic_interpolation_and_precedence() {
    let home = tempfile::tempdir().unwrap();
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    let addr = server
        .url
        .strip_prefix("http://")
        .unwrap()
        .strip_suffix("/mcp")
        .unwrap()
        .to_owned();
    let ini_path = home.path().join("config.ini");

    let mut bom_ini = b"\xef\xbb\xbf".to_vec();
    bom_ini.extend_from_slice(
        format!(
            "# Full-line hash comment\n\
             ; Full-line semicolon comment\n\
             [DEFAULT]\n\
             Scheme = http\n\
             HostPort : {addr}\n\
             BasePath = /mcp\n\
             Max_Bytes = 8_192\n\
             Token_Env = CUSTOM_INI_TOKEN\n\
             hop1 = %(Scheme)s://%(HostPort)s\n\
             hop2 = %(hop1)s%(BasePath)s\n\
             hop3 = %(hop2)s\n\
             hop4 = %(hop3)s\n\
             hop5 = %(hop4)s\n\
             hop6 = %(hop5)s\n\
             hop7 = %(hop6)s\n\
             hop8 = %(hop7)s\n\
             hop9 = %(hop8)s\n\
             \n\
             [minymaton]\n\
             HANDLE = sub_from_ini_%%escaped%%\n\
             ACK : bat_ini_\n  continuation_line\n\
             MAX_EVENTS = +00_4\n\
             url = %(hop9)s\n"
        )
        .as_bytes(),
    );
    std::fs::write(&ini_path, bom_ini).unwrap();

    // 1. Values loaded from INI ([minymaton] + [DEFAULT] fallback + BasicInterpolation + %% escaping + multiline + custom token_env)
    let output = command(home.path())
        .args(["--config", ini_path.to_str().unwrap()])
        .env("CUSTOM_INI_TOKEN", "ini-secret-token")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    let args = &requests[0].body["params"]["arguments"];
    assert_eq!(args["handle"], "sub_from_ini_%escaped%");
    assert_eq!(args["ack"], "bat_ini_\ncontinuation_line");
    assert_eq!(args["max_events"], 4);
    assert_eq!(args["max_bytes"], 8192);
    assert_eq!(
        requests[0].headers["authorization"],
        "Bearer ini-secret-token"
    );

    // 2. Precedence: CLI > Environment > INI > Default
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    let output = command(home.path())
        .args([
            "--config",
            ini_path.to_str().unwrap(),
            "--url",
            &server.url,
            "--handle",
            "sub_cli",
            "--max-events",
            "7",
        ])
        .env("MINYMATON_HANDLE", "sub_env")
        .env("MINYMATON_ACK", "bat_env")
        .env("MINYMATON_MAX_EVENTS", "9")
        .env("MINYMATON_MAX_BYTES", "4096")
        .env("MINYMATON_TOKEN_ENV", "ENV_OVERRIDE_TOKEN")
        .env("ENV_OVERRIDE_TOKEN", "env-override-secret")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    let args = &requests[0].body["params"]["arguments"];
    assert_eq!(args["handle"], "sub_cli");
    assert_eq!(args["ack"], "bat_env");
    assert_eq!(args["max_events"], 7);
    assert_eq!(args["max_bytes"], 4096);
    assert_eq!(
        requests[0].headers["authorization"],
        "Bearer env-override-secret"
    );

    // 2b. Repeated [DEFAULT] headers merge, lazy interpolation ignores unreferenced raw '%',
    //     SECTCRE allows trailing text after ']', and lone '\r' line endings work
    let mut server = HttpServer::json(json!({"id":2,"result":{"structuredContent":events()}}));
    std::fs::write(
        &ini_path,
        format!(
            "[DEFAULT]\r\
             part_a = alpha\r\
             unused_pct = 50%raw\r\
             [other] ; unrelated section with raw percent\r\
             ratio = 99%raw\r\
             [DEFAULT] ; second DEFAULT block merges\r\
             part_b = beta\r\
             [minymaton] ; trailing comment on header\r\
             url = {}\r\
             handle = sub_%(part_a)s_%(part_b)s\r",
            server.url
        ),
    )
    .unwrap();
    let output = command(home.path())
        .args(["--config", ini_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].body["params"]["arguments"]["handle"],
        "sub_alpha_beta"
    );

    // 2c. A [DEFAULT]-only config (without [minymaton]) does not supply [minymaton] options
    std::fs::write(
        &ini_path,
        "[DEFAULT]\nurl = http://127.0.0.1:1/mcp\nhandle = sub_default_only\n",
    )
    .unwrap();
    let output = command(home.path())
        .args(["--config", ini_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.starts_with("usage: "), "{stderr}");
    assert!(
        stderr.contains("the following arguments are required: --handle"),
        "{stderr}"
    );

    // 3. Rejection of duplicate sections, duplicate options, missing headers, bad interpolation, and depth > 10
    for (bad_ini, expected_sub) in [
        (
            "[minymaton]\nhandle = a\n[minymaton]\nhandle = b\n",
            "DuplicateSectionError",
        ),
        (
            "[DEFAULT]\nhandle = a\n[DEFAULT]\nhandle = b\n[minymaton]\nurl = http://127.0.0.1:1/mcp\n",
            "DuplicateOptionError",
        ),
        (
            "[minymaton]\nhandle = a\nHANDLE : b\n",
            "DuplicateOptionError",
        ),
        (
            "[DEFAULT]\nopt = a\nOPT = b\n[minymaton]\nhandle = sub_ok\n",
            "DuplicateOptionError",
        ),
        ("handle = sub_no_section\n", "MissingSectionHeaderError"),
        (
            "[minymaton]\nhandle = %(missing_key)s\n",
            "InterpolationMissingOptionError",
        ),
        ("[minymaton]\nhandle = 50%raw\n", "InterpolationSyntaxError"),
        (
            "[minymaton]\nhandle = %(unclosed\n",
            "InterpolationSyntaxError",
        ),
        (
            "[minymaton]\n\
             k0 = val\n\
             k1 = %(k0)s\n\
             k2 = %(k1)s\n\
             k3 = %(k2)s\n\
             k4 = %(k3)s\n\
             k5 = %(k4)s\n\
             k6 = %(k5)s\n\
             k7 = %(k6)s\n\
             k8 = %(k7)s\n\
             k9 = %(k8)s\n\
             k10 = %(k9)s\n\
             handle = %(k10)s\n",
            "InterpolationDepthError",
        ),
        (
            "[minymaton]\nhandle = sub_ok\nmax_events = not_an_int\n",
            "invalid max_events",
        ),
    ] {
        std::fs::write(&ini_path, bad_ini).unwrap();
        let output = command(home.path())
            .args(["--config", ini_path.to_str().unwrap()])
            .output()
            .unwrap();
        let error = terminal(&output, 1, "config_failed");
        let msg = error["message"].as_str().unwrap();
        assert!(
            msg.contains(expected_sub),
            "expected {expected_sub:?} in {msg:?} for {bad_ini:?}"
        );
    }

    // 4. Valid INI without handle and no CLI/env handle -> exit 2 usage error
    std::fs::write(&ini_path, "[minymaton]\nmax_events = 5\n").unwrap();
    let output = command(home.path())
        .args(["--config", ini_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("--handle"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_waiter_https_supports_ca_file_ssl_cert_dir_intermediate_chain_and_eku_filtering() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let home = tempfile::tempdir().unwrap();
    let pki = home.path().join("pki");
    std::fs::create_dir_all(&pki).unwrap();

    // Generate Root CA -> Intermediate CA -> Server Leaf Certificate, plus a bad-EKU Root CA (emailProtection only)
    std::fs::write(
        pki.join("inter.ext"),
        "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n",
    )
    .unwrap();
    std::fs::write(
        pki.join("leaf.ext"),
        "basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
    )
    .unwrap();
    std::fs::write(
        pki.join("bad_ca.ext"),
        "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nextendedKeyUsage=emailProtection\n",
    )
    .unwrap();

    for arguments in [
        vec![
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "root.key",
            "-out",
            "root.pem",
            "-days",
            "1",
            "-subj",
            "/CN=minymaton-chain-root-CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
        vec![
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "inter.key",
            "-out",
            "inter.csr",
            "-subj",
            "/CN=minymaton-chain-intermediate-CA",
        ],
        vec![
            "x509",
            "-req",
            "-in",
            "inter.csr",
            "-CA",
            "root.pem",
            "-CAkey",
            "root.key",
            "-CAcreateserial",
            "-out",
            "inter.pem",
            "-days",
            "1",
            "-extfile",
            "inter.ext",
        ],
        vec![
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "leaf.key",
            "-out",
            "leaf.csr",
            "-subj",
            "/CN=localhost",
        ],
        vec![
            "x509",
            "-req",
            "-in",
            "leaf.csr",
            "-CA",
            "inter.pem",
            "-CAkey",
            "inter.key",
            "-CAcreateserial",
            "-out",
            "leaf.pem",
            "-days",
            "1",
            "-extfile",
            "leaf.ext",
        ],
        vec![
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "bad_ca.key",
            "-out",
            "bad_ca.csr",
            "-subj",
            "/CN=minymaton-email-only-CA",
        ],
        vec![
            "x509",
            "-req",
            "-in",
            "bad_ca.csr",
            "-signkey",
            "bad_ca.key",
            "-out",
            "bad_ca.pem",
            "-days",
            "1",
            "-extfile",
            "bad_ca.ext",
        ],
    ] {
        let output = openssl_command()
            .current_dir(&pki)
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Server presents ONLY leaf.pem (NOT inter.pem), requiring the client's ExtraIntermediatesVerifier
    // to supply inter.pem during path building!
    let mut tls_server_cfg = tokio_rustls::rustls::ServerConfig::builder_with_provider(
        std::sync::Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from_pem_file(pki.join("leaf.pem")).unwrap()],
        PrivateKeyDer::from_pem_file(pki.join("leaf.key")).unwrap(),
    )
    .unwrap();
    tls_server_cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls_server_cfg = std::sync::Arc::new(tls_server_cfg);

    let dir_roots = home.path().join("dir_roots");
    let dir_inters = home.path().join("dir_inters");
    std::fs::create_dir_all(&dir_roots).unwrap();
    std::fs::create_dir_all(&dir_inters).unwrap();
    std::fs::copy(pki.join("root.pem"), dir_roots.join("root.pem")).unwrap();
    std::fs::copy(pki.join("inter.pem"), dir_inters.join("inter.pem")).unwrap();

    let combined_bundle = home.path().join("combined-chain.pem");
    let mut bundle_bytes = std::fs::read(pki.join("root.pem")).unwrap();
    bundle_bytes.extend_from_slice(&std::fs::read(pki.join("inter.pem")).unwrap());
    std::fs::write(&combined_bundle, bundle_bytes).unwrap();

    // Helper to spawn a single-request HTTPS server
    let spawn_https_once = |cfg: std::sync::Arc<tokio_rustls::rustls::ServerConfig>| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        let handle = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = acceptor.accept(stream).await?;
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let mut length = 0usize;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).await.unwrap();
                if header == "\r\n" {
                    break;
                }
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await.unwrap();
            let wire = json_wire(json!({"id":2,"result":{"structuredContent":events()}}));
            reader.get_mut().write_all(&wire).await.unwrap();
            reader.get_mut().shutdown().await.unwrap();
            Ok::<(), std::io::Error>(())
        }));
        (format!("https://localhost:{port}/mcp"), handle)
    };

    // Case 1: Multi-directory SSL_CERT_DIR (root in dir_roots, intermediate in dir_inters)
    let (https_url, srv) = spawn_https_once(std::sync::Arc::clone(&tls_server_cfg)).await;
    let joined_dirs = std::env::join_paths([&dir_roots, &dir_inters]).unwrap();
    let output = command(home.path())
        .args(["--url", &https_url, "--handle", "sub_test"])
        .env("SSL_CERT_DIR", &joined_dirs)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(srv.await.unwrap().is_ok());

    // Case 2: --ca-file with combined root + intermediate bundle
    let (https_url, srv) = spawn_https_once(std::sync::Arc::clone(&tls_server_cfg)).await;
    let output = command(home.path())
        .args([
            "--url",
            &https_url,
            "--handle",
            "sub_test",
            "--ca-file",
            combined_bundle.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(srv.await.unwrap().is_ok());

    // Case 3: INI ca_file with [DEFAULT] interpolation
    let (https_url, srv) = spawn_https_once(std::sync::Arc::clone(&tls_server_cfg)).await;
    let ini_path = home.path().join("tls.ini");
    std::fs::write(
        &ini_path,
        format!(
            "[DEFAULT]\n\
             bundle_dir = {}\n\
             [minymaton]\n\
             handle = sub_ini_tls\n\
             url = {https_url}\n\
             ca_file = %(bundle_dir)s/combined-chain.pem\n",
            home.path().display()
        ),
    )
    .unwrap();
    let output = command(home.path())
        .args(["--config", ini_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(srv.await.unwrap().is_ok());

    // Case 4: Failing closed when --ca-file is missing, has only emailProtection EKU,
    //         or contains only a non-self-issued intermediate without its root anchor
    let (unanchored_url, _unanchored_srv) =
        spawn_https_once(std::sync::Arc::clone(&tls_server_cfg)).await;
    for (bad_ca, expected_sub) in [
        (
            home.path().join("nonexistent-ca.pem"),
            "cannot read ca bundle",
        ),
        (pki.join("bad_ca.pem"), "no valid ca certificates"),
        (pki.join("inter.pem"), "certificate"),
    ] {
        let output = command(home.path())
            .args([
                "--url",
                &unanchored_url,
                "--handle",
                "sub_test",
                "--ca-file",
                bad_ca.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        let error = terminal(&output, 7, "transport_failed");
        let msg = error["message"].as_str().unwrap().to_lowercase();
        assert!(
            msg.contains(expected_sub),
            "expected {expected_sub:?} for {bad_ca:?}, got: {error}"
        );
    }
}

fn python_waiter_command(home: &Path) -> Command {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/minymaton-wait.py");
    let program = if cfg!(windows) { "python" } else { "python3" };
    let mut command = Command::new(program);
    command
        .arg(script)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("MINYMATON_URL")
        .env_remove("MINYMATON_TOKEN")
        .env_remove("MINYMATON_HANDLE")
        .env_remove("MINYMATON_ACK")
        .env_remove("MINYMATON_MAX_EVENTS")
        .env_remove("MINYMATON_MAX_BYTES")
        .env_remove("MINYMATON_CA_FILE")
        .env_remove("MINYMATON_TOKEN_ENV")
        .env_remove("MINYMATON_SERVER")
        .env_remove("MINYMATON_CONFIG")
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR");
    command
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn python_waiter_fallback_delivers_events_and_selects_private_endpoint() {
    // 1. Explicit private-endpoint selection with bearer auth via MINYMATON_URL + MINYMATON_TOKEN,
    //    without borrowing mcpServers["minymaton"]'s token when the URL differs
    let private_token = "mny_private_pete_0123456789abcdef0123456789abcdef";
    let auth_server = Server::start_with_bearer_tokens(&format!("pete\t{private_token}\n")).await;
    let (dir, server, url, handle, mut activity) = parked_fixture().await;
    let server = tokio_util::task::AbortOnDropHandle::new(server);

    // Against a home with only "minymaton" pointing elsewhere, explicit --url does NOT borrow public token -> 401 -> exit 7
    let public_only_home = tempfile::tempdir().unwrap();
    let public_only_copilot = public_only_home.path().join(".copilot");
    std::fs::create_dir_all(&public_only_copilot).unwrap();
    std::fs::write(
        public_only_copilot.join("mcp-config.json"),
        json!({
            "mcpServers": {
                "minymaton": {
                    "url": "http://127.0.0.1:9/mcp",
                    "headers": {
                        "Authorization": format!("Bearer {private_token}")
                    }
                }
            }
        })
        .to_string(),
    )
    .unwrap();
    // Without --url, MINYMATON_URL, or --config / MINYMATON_CONFIG, fails with config_failed (exit 1)
    // and never reads .copilot/mcp-config.json
    let no_url = python_waiter_command(public_only_home.path())
        .args(["--handle", "sub_missing"])
        .output()
        .unwrap();
    terminal(&no_url, 1, "config_failed");

    let unauth = python_waiter_command(public_only_home.path())
        .args(["--url", auth_server.url(), "--handle", "sub_missing"])
        .output()
        .unwrap();
    terminal(&unauth, 7, "transport_failed");

    // Specified-but-missing token_env fails with config_failed (exit 1)
    let py_ini = public_only_home.path().join("py-waiter.ini");
    std::fs::write(
        &py_ini,
        format!(
            "[minymaton]\nurl = {}\ntoken_env = PY_CUSTOM_TOKEN\nhandle = sub_missing\n",
            auth_server.url()
        ),
    )
    .unwrap();
    let missing_py_token = python_waiter_command(public_only_home.path())
        .env("MINYMATON_CONFIG", &py_ini)
        .env("MINYMATON_TOKEN", private_token)
        .output()
        .unwrap();
    terminal(&missing_py_token, 1, "config_failed");

    let authed_ini = python_waiter_command(public_only_home.path())
        .env("MINYMATON_CONFIG", &py_ini)
        .env("PY_CUSTOM_TOKEN", private_token)
        .output()
        .unwrap();
    terminal(&authed_ini, 5, "subscription_not_found");

    // Synthetic custom-env error cases (--token-env and INI token_env) redact custom secrets from stderr
    let leaked_cli = python_waiter_command(public_only_home.path())
        .args([
            "--url",
            auth_server.url(),
            "--handle",
            "sub_missing",
            "--token-env",
            "PY_CLI_CUSTOM_TOKEN",
        ])
        .env(
            "PY_CLI_CUSTOM_TOKEN",
            "synthetic-cli-secret\r\nx-injected: 1",
        )
        .output()
        .unwrap();
    terminal(&leaked_cli, 1, "protocol_failed");
    assert!(
        !String::from_utf8_lossy(&leaked_cli.stderr).contains("synthetic-cli-secret"),
        "stderr leaked custom CLI token_env secret: {}",
        String::from_utf8_lossy(&leaked_cli.stderr)
    );

    let leaked_ini = python_waiter_command(public_only_home.path())
        .env("MINYMATON_CONFIG", &py_ini)
        .env("PY_CUSTOM_TOKEN", "synthetic-ini-secret\r\nx-injected: 1")
        .output()
        .unwrap();
    terminal(&leaked_ini, 1, "protocol_failed");
    assert!(
        !String::from_utf8_lossy(&leaked_ini.stderr).contains("synthetic-ini-secret"),
        "stderr leaked custom INI token_env secret: {}",
        String::from_utf8_lossy(&leaked_ini.stderr)
    );

    // With MINYMATON_URL + MINYMATON_TOKEN, authenticates to private endpoint -> subscription_not_found (exit 5)
    let authed = python_waiter_command(public_only_home.path())
        .args(["--handle", "sub_missing"])
        .env("MINYMATON_URL", auth_server.url())
        .env("MINYMATON_TOKEN", private_token)
        .output()
        .unwrap();
    terminal(&authed, 5, "subscription_not_found");

    // 2. Live parked wait, event-driven delivery without auto-ACK, then explicit --ack + supersession
    let client = ()
        .serve(rmcp::transport::StreamableHttpClientTransport::from_uri(
            url.clone(),
        ))
        .await
        .unwrap();

    let mut process = WaiterProcess::spawn(
        python_waiter_command(dir.path()).args(["--url", &url, "--handle", &handle]),
    );
    assert_eq!(*activity.wait_for(|count| *count == 1).await.unwrap(), 1);
    client
        .call_tool(
            CallToolRequestParams::new("publish").with_arguments(
                json!({"stream":"orders","idempotency_key":"py-1","payload":"from-python-fallback"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    let output = process.finish();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let first_batch: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(first_batch["status"], "events");
    assert_eq!(first_batch["events"][0]["payload"], "from-python-fallback");
    let first_batch_id = first_batch["batch_id"].as_str().unwrap().to_owned();
    assert_eq!(first_batch["ack_after_handling"], first_batch_id);

    // Without --ack, the batch remains unacknowledged and redelivers on the next receive
    let redelivered = python_waiter_command(dir.path())
        .args(["--url", &url, "--handle", &handle])
        .output()
        .unwrap();
    assert!(redelivered.status.success(), "{redelivered:?}");
    let redelivered_batch: Value = serde_json::from_slice(&redelivered.stdout).unwrap();
    assert_eq!(redelivered_batch["batch_id"], first_batch_id);
    assert_eq!(redelivered_batch["delivery_attempt"], 2);

    // Park again with explicit --ack, wait for active WaitGuard, then supersede via foreground receive
    let mut process = WaiterProcess::spawn(python_waiter_command(dir.path()).args([
        "--url",
        &url,
        "--handle",
        &handle,
        "--ack",
        &first_batch_id,
    ]));
    assert_eq!(*activity.wait_for(|count| *count == 1).await.unwrap(), 1);
    client
        .call_tool(
            CallToolRequestParams::new("receive").with_arguments(
                receive_args(&handle, 10, 16384, 0)
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    let superseded = process.finish();
    assert_eq!(superseded.status.code(), Some(3), "{superseded:?}");
    assert!(superseded.stdout.is_empty());
    let sup_json: Value = serde_json::from_slice(&superseded.stderr).unwrap();
    assert_eq!(sup_json["status"], "superseded");
    assert_eq!(sup_json["ack_result"]["batch_id"], first_batch_id);
    assert_eq!(sup_json["ack_result"]["outcome"], "committed");
    assert_eq!(*activity.wait_for(|count| *count == 0).await.unwrap(), 0);

    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
