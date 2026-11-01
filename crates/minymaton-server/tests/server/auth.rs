//! Enrolled bearer-token authentication on `/mcp` and `/mcp/agent/{agent}`.

use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use rmcp::model::{ClientCapabilities, ClientConfig, Implementation, ProtocolVersion};
use serde_json::json;

use crate::common::{Lifecycle, Server, payloads, receive_args};

const ALPHA_TOKEN: &str = "mny_alpha_0123456789abcdef0123456789abcdef";
const BRAVO_TOKEN: &str = "mny_bravo_fedcba9876543210fedcba9876543210";

fn initialize_body() -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::new("auth-test", "0"),
        ).with_protocol_version(ProtocolVersion::V_2025_11_25),
    })
    .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bearer_token_gate_rejects_unauthenticated_and_admits_enrolled_tokens() {
    let tokens = format!("# Enrolled tokens\nalpha\t{ALPHA_TOKEN}\nbravo\t{BRAVO_TOKEN}\n");
    let server = Server::start_with_bearer_tokens(&tokens).await;
    let http = reqwest::Client::new();

    // 1. Missing Authorization header on /mcp and /mcp/agent/worker -> 401 + WWW-Authenticate: Bearer.
    for url in [server.url().to_owned(), server.agent_url("worker")] {
        let response = http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .body(initialize_body())
            .send()
            .await
            .expect("POST without Authorization");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );
    }

    // 2. Wrong token and non-Bearer scheme -> 401.
    for header in [
        "Bearer mny_wrong_00000000000000000000000000000000",
        "Basic dXNlcjpwYXNz",
        "Bearer",
    ] {
        let response = http
            .post(server.url())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .header(AUTHORIZATION, header)
            .body(initialize_body())
            .send()
            .await
            .expect("POST with invalid Authorization");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{header}");
    }

    // Unauthenticated requests are rejected before the request log records anything.
    assert!(
        server.requests().is_empty(),
        "unauthenticated requests must not be logged: {:?}",
        server.requests()
    );

    // 3. Enrolled tokens succeed on /mcp and /mcp/agent/worker (including lowercase `bearer`).
    for (url, header) in [
        (server.url().to_owned(), format!("Bearer {ALPHA_TOKEN}")),
        (server.agent_url("worker"), format!("bearer {BRAVO_TOKEN}")),
    ] {
        let response = http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .header(AUTHORIZATION, header)
            .body(initialize_body())
            .send()
            .await
            .expect("POST with valid bearer token");
        assert_eq!(response.status(), StatusCode::OK, "{url}");
        assert!(
            response.headers().get("mcp-session-id").is_some(),
            "expected mcp-session-id header from {url}"
        );
        let text = response.text().await.expect("SSE response text");
        assert!(
            text.contains("\"protocolVersion\":\"2025-11-25\""),
            "unexpected initialize body: {text}"
        );
    }

    assert_eq!(server.requests().len(), 2);
}

#[test]
fn invalid_bearer_token_files_are_refused_before_opening_the_database() {
    for (case, content) in [
        ("empty", "# comments only\n\n"),
        ("short-token", "alpha\ttoo_short_token\n"),
        (
            "duplicate-caller",
            "alpha\tmny_first_0123456789abcdef0123456789abcdef\nalpha\tmny_second_0123456789abcdef0123456789abcdef\n",
        ),
        (
            "duplicate-digest",
            "alpha\tmny_same_0123456789abcdef0123456789abcdef\nbravo\tmny_same_0123456789abcdef0123456789abcdef\n",
        ),
        (
            "reserved-agents-channel",
            "alpha\tmny_first_0123456789abcdef0123456789abcdef\tagents\n",
        ),
        (
            "invalid-channel-chars",
            "alpha\tmny_first_0123456789abcdef0123456789abcdef\tbad/channel\n",
        ),
        (
            "too-many-columns",
            "alpha\tmny_first_0123456789abcdef0123456789abcdef\tch-1\textra\n",
        ),
    ] {
        refuse_token_file(case, content);
    }
}

fn refuse_token_file(case: &str, content: &str) {
    let dir = tempfile::Builder::new()
        .prefix("auth-invalid-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("temporary directory");
    let token_file = dir.path().join("tokens.conf");
    std::fs::write(&token_file, content).expect("write tokens.conf");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--db")
        .arg(dir.path().join("queue.sqlite3"))
        .arg("--bearer-token-file")
        .arg(&token_file)
        .output()
        .expect("run minymaton-server");
    assert!(
        !output.status.success(),
        "server accepted invalid token file ({case})"
    );
    assert!(
        !dir.path().join("queue.sqlite3").exists(),
        "refused server created database ({case})"
    );
}

/// `tools/mcp_send.py` takes its bearer token from the environment variable
/// `--token-env` names, never from argv, and refuses to start when that
/// variable is unset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_send_reads_its_bearer_token_from_the_named_environment_variable() {
    let tokens = format!("alpha\t{ALPHA_TOKEN}\n");
    let server = Server::start_with_bearer_tokens(&tokens).await;
    let reader = server
        .client_with_token(
            ALPHA_TOKEN,
            Lifecycle::Initialize(ProtocolVersion::V_2025_11_25),
        )
        .await;
    let handle = reader.create_subscription("agents", "reader").await;
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tools")
        .join("mcp_send.py");
    let send = |token_env: &str| {
        let mut child = std::process::Command::new("uv")
            .arg("run")
            .arg(&script)
            .arg("http")
            .arg(server.url())
            .arg("--token-env")
            .arg(token_env)
            .arg("--stream")
            .arg("agents")
            .env("MCP_SEND_TEST_TOKEN", ALPHA_TOKEN)
            .env_remove("MCP_SEND_TEST_UNSET")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("run the real mcp_send.py");
        std::io::Write::write_all(
            child.stdin.as_mut().expect("mcp_send input"),
            b"sent-with-env-token\n",
        )
        .expect("type one message");
        drop(child.stdin.take());
        child
            .wait_with_output()
            .expect("mcp_send exits at end of input")
    };

    let sent = tokio::task::block_in_place(|| send("MCP_SEND_TEST_TOKEN"));
    let stdout = String::from_utf8_lossy(&sent.stdout);
    assert!(sent.status.success(), "{stdout}");
    assert!(!stdout.contains("!!"), "{stdout}");
    let received = reader
        .call("receive", receive_args(&handle, 10, 16_384, 0))
        .await
        .ok();
    assert_eq!(payloads(&received), [json!("sent-with-env-token")]);
    assert_eq!(received["events"][0]["publisher"], "alpha", "{received:#}");

    let refused = tokio::task::block_in_place(|| send("MCP_SEND_TEST_UNSET"));
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&refused.stdout),
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(!refused.status.success(), "{output}");
    assert!(output.contains("MCP_SEND_TEST_UNSET"), "{output}");
    assert!(!output.contains("connected to"), "{output}");
}

const CHARLIE_TOKEN: &str = "mny_charlie_0123456789abcdef0123456789abcdef";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_ownership_and_per_principal_session_and_concurrency_caps() {
    let tokens = format!("alpha\t{ALPHA_TOKEN}\nbravo\t{BRAVO_TOKEN}\n");
    let server = Server::start_with_bearer_tokens_and_options(
        &tokens,
        &[
            "--max-sessions-per-principal",
            "2",
            "--max-concurrent-requests-per-principal",
            "1",
        ],
    )
    .await;
    let http = reqwest::Client::new();

    let open_session = |token: &str| {
        let http = http.clone();
        let url = server.url().to_owned();
        let auth = format!("Bearer {token}");
        async move {
            http.post(&url)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json, text/event-stream")
                .header(AUTHORIZATION, auth)
                .body(initialize_body())
                .send()
                .await
                .expect("POST initialize")
        }
    };

    let resp_1 = open_session(ALPHA_TOKEN).await;
    assert_eq!(resp_1.status(), StatusCode::OK);
    let sid_1 = resp_1
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("sid_1")
        .to_owned();
    let _ = resp_1.text().await;

    let resp_2 = open_session(ALPHA_TOKEN).await;
    assert_eq!(resp_2.status(), StatusCode::OK);
    let sid_2 = resp_2
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("sid_2")
        .to_owned();
    let _ = resp_2.text().await;

    // 3rd session from alpha succeeds and evicts alpha's oldest session (`sid_1`, `Opus RR-01`).
    let resp_3 = open_session(ALPHA_TOKEN).await;
    assert_eq!(resp_3.status(), StatusCode::OK);
    let sid_3 = resp_3
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("sid_3")
        .to_owned();
    let _ = resp_3.text().await;

    let evicted_sid_1 = http
        .post(server.url())
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json, text/event-stream")
        .header(AUTHORIZATION, format!("Bearer {ALPHA_TOKEN}"))
        .header("mcp-session-id", &sid_1)
        .body(json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}).to_string())
        .send()
        .await
        .expect("POST with evicted sid_1");
    assert_eq!(evicted_sid_1.status(), StatusCode::NOT_FOUND);

    for active_sid in [&sid_2, &sid_3] {
        let active = http
            .post(server.url())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .header(AUTHORIZATION, format!("Bearer {ALPHA_TOKEN}"))
            .header("mcp-session-id", active_sid)
            .body(json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}).to_string())
            .send()
            .await
            .expect("POST with active session");
        assert_eq!(active.status(), StatusCode::OK);
        let _ = active.text().await;
    }

    // Bravo is a distinct principal and can still open sessions.
    let resp_bravo = open_session(BRAVO_TOKEN).await;
    assert_eq!(resp_bravo.status(), StatusCode::OK);
    let _ = resp_bravo.text().await;

    // Bravo presenting alpha's `mcp-session-id` on POST, GET, or DELETE gets 403 Forbidden (`PC-04`).
    let post_hijack = http
        .post(server.url())
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json, text/event-stream")
        .header(AUTHORIZATION, format!("Bearer {BRAVO_TOKEN}"))
        .header("mcp-session-id", &sid_2)
        .body(json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}).to_string())
        .send()
        .await
        .expect("POST with hijacked session");
    assert_eq!(post_hijack.status(), StatusCode::FORBIDDEN);

    let get_hijack = http
        .get(server.url())
        .header(ACCEPT, "text/event-stream")
        .header(AUTHORIZATION, format!("Bearer {BRAVO_TOKEN}"))
        .header("mcp-session-id", &sid_2)
        .send()
        .await
        .expect("GET with hijacked session");
    assert_eq!(get_hijack.status(), StatusCode::FORBIDDEN);

    let delete_hijack = http
        .delete(server.url())
        .header(AUTHORIZATION, format!("Bearer {BRAVO_TOKEN}"))
        .header("mcp-session-id", &sid_2)
        .send()
        .await
        .expect("DELETE with hijacked session");
    assert_eq!(delete_hijack.status(), StatusCode::FORBIDDEN);

    // An open GET /mcp SSE stream holds a concurrency permit for its lifetime (`Sol RR-08`).
    let get_stream = http
        .get(server.url())
        .header(ACCEPT, "text/event-stream")
        .header(AUTHORIZATION, format!("Bearer {ALPHA_TOKEN}"))
        .header("mcp-session-id", &sid_2)
        .send()
        .await
        .expect("GET SSE stream");
    assert_eq!(get_stream.status(), StatusCode::OK);

    let blocked_by_get = open_session(ALPHA_TOKEN).await;
    assert_eq!(blocked_by_get.status(), StatusCode::TOO_MANY_REQUESTS);
    drop(get_stream);

    // Alpha deleting sid_2 and sid_3 frees its session slots. Wait briefly if the OS TCP FIN
    // from `drop(get_stream)` has not yet been processed by the server's I/O driver.
    for sid in [&sid_2, &sid_3] {
        let mut status = StatusCode::TOO_MANY_REQUESTS;
        for _ in 0..50 {
            let deleted = http
                .delete(server.url())
            .header(AUTHORIZATION, format!("Bearer {ALPHA_TOKEN}"))
            .header("mcp-session-id", sid)
                .send()
                .await
                .expect("DELETE own session");
            status = deleted.status();
            if status != StatusCode::TOO_MANY_REQUESTS {
                break;
            }
        }
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    // Test per-principal concurrency cap (`--max-concurrent-requests-per-principal 1`).
    let modern = Lifecycle::Discover(ProtocolVersion::V_2026_07_28);
    let alpha_client = server.client_with_token(ALPHA_TOKEN, modern.clone()).await;
    let bravo_client = server.client_with_token(BRAVO_TOKEN, modern).await;
    let handle = alpha_client.create_subscription("agents", "waiter").await;
    let mut parked = alpha_client
        .start("receive", receive_args(&handle, 10, 16_384, 1))
        .await;
    parked.window_open().await;

    // While alpha's receive is in-flight, a second concurrent HTTP request from alpha returns 429.
    let second_in_flight = open_session(ALPHA_TOKEN).await;
    assert_eq!(second_in_flight.status(), StatusCode::TOO_MANY_REQUESTS);

    // Bravo can still publish and wake alpha's parked receive.
    bravo_client.publish("agents", "wake-alpha", json!("go")).await;
    let woken = parked.finish().await.ok();
    assert_eq!(payloads(&woken), vec![json!("go")]);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sighup_enrollment_reload_admits_new_channels_and_revokes_removed_tokens() {
    let send_hup = |pid: u32| {
        let status = std::process::Command::new("kill")
            .args(["-HUP", &pid.to_string()])
            .status()
            .expect("kill -HUP");
        assert!(status.success());
    };

    let initial = format!("alpha\t{ALPHA_TOKEN}\nbravo\t{BRAVO_TOKEN}\tbravo-ch\n");
    let server = Server::start_with_bearer_tokens(&initial).await;
    let modern = Lifecycle::Discover(ProtocolVersion::V_2026_07_28);
    let alpha = server.client_with_token(ALPHA_TOKEN, modern.clone()).await;
    let alpha_pub = server.client_with_token(ALPHA_TOKEN, modern.clone()).await;
    let alpha_handle = alpha.create_subscription("agents", "kept-waiter").await;
    let bravo = server.client_with_token(BRAVO_TOKEN, modern.clone()).await;
    let bravo_handle = bravo.create_subscription("bravo-ch", "desk").await;

    // On SQLite, `--activate-enrollment` is refused while `minymaton-server` holds the DB lock (`Sol RR-05`).
    let locked = server.run_admin(&[
        "--activate-enrollment",
        server.token_file().to_str().expect("utf8"),
    ]);
    assert!(!locked.status.success());

    // Changing an active token's `bound_channel` in place without rotating/removing the token
    // is rejected on SIGHUP (`O-R4-2`), keeping the previous snapshot active.
    std::fs::write(
        server.token_file(),
        format!("alpha\t{ALPHA_TOKEN}\nbravo\t{BRAVO_TOKEN}\tbravo-ch-moved\n"),
    )
    .expect("write invalid in-place channel change");
    send_hup(server.pid());
    let still_bravo = bravo.call("stream", json!({"action": "list"})).await.ok();
    assert_eq!(still_bravo["streams"], json!(["bravo-ch"]));

    // Park waits on both kept `alpha` and soon-to-be-revoked `bravo`, then replace bravo with charlie (`charlie-ch`) and send SIGHUP.
    let mut alpha_parked = alpha
        .start("receive", receive_args(&alpha_handle, 10, 16_384, 1))
        .await;
    alpha_parked.window_open().await;
    let mut parked = bravo
        .start("receive", receive_args(&bravo_handle, 10, 16_384, 1))
        .await;
    parked.window_open().await;

    std::fs::write(
        server.token_file(),
        format!("alpha\t{ALPHA_TOKEN}\ncharlie\t{CHARLIE_TOKEN}\tcharlie-ch\n"),
    )
    .expect("write updated tokens.conf");
    send_hup(server.pid());

    // Bravo's in-flight parked receive is terminated by token revocation (`O-R4-2`).
    let terminated = parked.finish_result().await;
    assert!(
        terminated.is_err(),
        "revoked token's in-flight wait must be aborted, got {terminated:?}"
    );

    // On SQLite, SIGHUP atomically updates the store, reader, and authenticator (`Opus RR4-01`, `Sol RR-12`),
    // so kept `alpha`'s reader calls, writer calls, and parked receive all survive reload.
    let alpha_streams = alpha_pub
        .call("stream", json!({"action": "list"}))
        .await
        .ok();
    assert_eq!(alpha_streams["streams"], json!(["agents"]));
    alpha_pub
        .publish("agents", "after-reload", json!("alpha-kept"))
        .await;
    let alpha_woken = alpha_parked.finish().await.ok();
    assert_eq!(payloads(&alpha_woken), vec![json!("alpha-kept")]);

    // Bravo's subsequent HTTP requests are rejected with 401 Unauthorized.
    let http = reqwest::Client::new();
    let bravo_after = http
        .post(server.url())
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json, text/event-stream")
        .header(AUTHORIZATION, format!("Bearer {BRAVO_TOKEN}"))
        .body(initialize_body())
        .send()
        .await
        .expect("POST with revoked bravo token");
    assert_eq!(bravo_after.status(), StatusCode::UNAUTHORIZED);

    // Charlie's newly enrolled token and bound channel `charlie-ch` are immediately active.
    let charlie = server.client_with_token(CHARLIE_TOKEN, modern).await;
    let listed = charlie.call("stream", json!({"action": "list"})).await.ok();
    assert_eq!(listed["streams"], json!(["charlie-ch"]));
    let charlie_sub = charlie.create_subscription("charlie-ch", "desk").await;
    charlie
        .publish("charlie-ch", "c1", json!("hello charlie"))
        .await;
    let received = charlie
        .call("receive", receive_args(&charlie_sub, 10, 16_384, 0))
        .await
        .ok();
    assert_eq!(payloads(&received), vec![json!("hello charlie")]);
}
