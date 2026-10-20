//! The request log: one JSON record per incoming JSON-RPC message, saying what
//! each client called and what it declared, never what it sent.

use std::collections::HashSet;

use reqwest::StatusCode;
use rmcp::model::{ClientCapabilities, ProtocolVersion};
use serde_json::{Value, json};

use crate::common::{Lifecycle, Server, inbox_uri};

/// The name the harness's rmcp clients give in their client information.
const CLIENT: &str = "minymaton-live-arrival-tests";

fn with_method<'a>(records: &'a [Value], method: &str) -> Vec<&'a Value> {
    records
        .iter()
        .filter(|record| record["method"] == method)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_messages_are_logged_with_the_capabilities_declared_at_initialize() {
    let server = Server::start_with_request_log().await;
    let client = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
        .await;
    client.create_stream("orders-never-logged").await;
    let handle = client
        .create_subscription("orders-never-logged", "reader-never-logged")
        .await;
    client
        .publish(
            "orders-never-logged",
            "key-never-logged",
            json!("payload never logged"),
        )
        .await;
    let uri = inbox_uri(&handle);
    client.read_inbox(&uri).await.expect("resources/read");

    let records = server.requests();
    let initialize = with_method(&records, "initialize");
    let [initialize] = initialize.as_slice() else {
        panic!("expected one initialize: {records:#?}");
    };
    assert_eq!(initialize["kind"], "request");
    assert_eq!(initialize["revision"], "2025-11-25");
    assert_eq!(initialize["client"]["name"], CLIENT);
    assert_eq!(initialize["client_capabilities"], json!({}));
    assert!(
        initialize.get("session").is_none(),
        "initialize has no session yet: {initialize:#}"
    );

    let later: Vec<&Value> = records
        .iter()
        .filter(|record| record["method"] != "initialize")
        .collect();
    let sessions: HashSet<&str> = later
        .iter()
        .map(|record| {
            record["session"]
                .as_str()
                .unwrap_or_else(|| panic!("no session on {record:#}"))
        })
        .collect();
    assert_eq!(sessions.len(), 1, "one client, one session: {later:#?}");
    for record in &later {
        assert_eq!(record["revision"], "2025-11-25", "{record:#}");
        assert!(
            record.get("client_capabilities").is_none(),
            "the 2025 revisions declare capabilities only at initialize: {record:#}"
        );
    }
    let initialized = with_method(&records, "notifications/initialized");
    assert_eq!(initialized.len(), 1, "{records:#?}");
    assert_eq!(initialized[0]["kind"], "notification");
    let tools: Vec<&str> = with_method(&records, "tools/call")
        .iter()
        .map(|record| record["tool"].as_str().expect("the tool called"))
        .collect();
    assert_eq!(tools, ["stream", "subscription", "publish"]);
    let reads = with_method(&records, "resources/read");
    assert_eq!(reads.len(), 1, "{records:#?}");
    assert_eq!(reads[0]["resource"], uri);

    let text = server.request_log_text();
    for argument in [
        "orders-never-logged",
        "reader-never-logged",
        "key-never-logged",
        "payload never logged",
    ] {
        assert!(
            !text.contains(argument),
            "the request log contains the argument {argument:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn modern_messages_are_logged_with_their_per_request_capabilities() {
    let server = Server::start_with_request_log().await;
    let tasks = ClientCapabilities::builder().enable_tasks().build();
    let declared = serde_json::to_value(&tasks).expect("serialize the capabilities");
    assert!(
        declared
            .to_string()
            .contains("io.modelcontextprotocol/tasks"),
        "{declared}"
    );
    let client = server
        .client_declaring(Lifecycle::Discover(ProtocolVersion::V_2026_07_28), tasks)
        .await;
    client.create_stream("modern-orders-never-logged").await;

    let records = server.requests();
    let methods: Vec<&str> = records
        .iter()
        .map(|record| record["method"].as_str().expect("a method"))
        .collect();
    assert_eq!(
        methods,
        ["server/discover", "tools/list", "tools/call"],
        "{records:#?}"
    );
    assert_eq!(records[2]["tool"], "stream");
    for record in &records {
        assert_eq!(record["kind"], "request", "{record:#}");
        assert_eq!(record["revision"], "2026-07-28", "{record:#}");
        assert!(
            record.get("session").is_none(),
            "2026-07-28 has no sessions: {record:#}"
        );
        assert_eq!(record["client"]["name"], CLIENT, "{record:#}");
        assert_eq!(record["client_capabilities"], declared, "{record:#}");
    }
    assert!(
        !server
            .request_log_text()
            .contains("modern-orders-never-logged"),
        "the request log contains a tool argument"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn responses_from_the_client_are_logged_as_responses() {
    let server = Server::start_with_request_log().await;
    let raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;

    let status = raw
        .post_message(
            json!({"jsonrpc": "2.0", "id": "reply-7", "result": {"secret": "never logged"}}),
        )
        .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    let responses: Vec<Value> = server
        .requests()
        .into_iter()
        .filter(|record| record["kind"] == "response")
        .collect();
    assert_eq!(responses.len(), 1, "{responses:#?}");
    assert_eq!(responses[0]["id"], "reply-7");
    assert!(responses[0].get("method").is_none(), "{:#}", responses[0]);
    assert!(!server.request_log_text().contains("never logged"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_logged_unless_asked() {
    let server = Server::start().await;
    let client = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_11_25))
        .await;
    client.create_stream("orders").await;

    let files = server.files();
    assert!(
        files.iter().all(|name| name.starts_with("queue.sqlite3")),
        "unexpected files: {files:?}"
    );
}
