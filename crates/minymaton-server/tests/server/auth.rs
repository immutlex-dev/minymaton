//! Enrolled bearer-token authentication on `/mcp` and `/mcp/agent/{agent}`.

use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use rmcp::model::{ClientCapabilities, ClientInfo, Implementation, ProtocolVersion};
use serde_json::json;

use crate::common::Server;

const ALPHA_TOKEN: &str = "mny_alpha_0123456789abcdef0123456789abcdef";
const BRAVO_TOKEN: &str = "mny_bravo_fedcba9876543210fedcba9876543210";

fn initialize_body() -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": ClientInfo::new(
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
