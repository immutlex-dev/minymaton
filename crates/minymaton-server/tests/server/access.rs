//! Private streams: a stream created with `members` is open only to the
//! callers named by their bearer tokens, and every message records the
//! authenticated caller that published it.

use rmcp::model::ProtocolVersion;
use serde_json::{Value, json};

use crate::common::{Lifecycle, McpClient, Server, inbox_uri, payloads};

const ALICE_TOKEN: &str = "mny_alice_0123456789abcdef0123456789abcdef";
const BOB_TOKEN: &str = "mny_bob_fedcba9876543210fedcba9876543210";

fn tokens() -> String {
    format!("alice\t{ALICE_TOKEN}\nbob\t{BOB_TOKEN}\n")
}

fn modern() -> Lifecycle {
    Lifecycle::Discover(ProtocolVersion::V_2026_07_28)
}

fn receive(handle: &str) -> Value {
    json!({"handle": handle, "max_events": 10, "max_bytes": 16384, "wait": false})
}

async fn forbidden(client: &McpClient, tool: &str, arguments: Value) {
    let error = client.call(tool, arguments.clone()).await.err();
    assert_eq!(
        error["code"], "stream_forbidden",
        "{tool} {arguments} must be refused: {error:#}"
    );
}

fn names(streams: &[&str]) -> Vec<Value> {
    streams.iter().map(|name| json!(name)).collect()
}

async fn listed_streams(client: &McpClient) -> Vec<Value> {
    client.call("stream", json!({"action": "list"})).await.ok()["streams"]
        .as_array()
        .expect("stream names")
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_private_stream_admits_only_its_members() {
    let server = Server::start_with_bearer_tokens(&tokens()).await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    let bob = server.client_with_token(BOB_TOKEN, modern()).await;

    alice.create_stream("open").await;
    let created = alice
        .call(
            "stream",
            json!({"action": "create", "name": "secrets", "members": []}),
        )
        .await
        .ok();
    assert_eq!(created, json!({"status": "created", "stream": "secrets"}));
    let alice_handle = alice.create_subscription("secrets", "alice-desk").await;
    alice
        .publish(
            "secrets",
            "first",
            json!({"kind": "ask", "question": "q3 deck?"}),
        )
        .await;

    assert_eq!(
        listed_streams(&alice).await,
        names(&["agents", "open", "secrets"])
    );
    assert_eq!(listed_streams(&bob).await, names(&["agents", "open"]));

    forbidden(
        &bob,
        "subscription",
        json!({"action": "create", "stream": "secrets", "label": "bob-desk"}),
    )
    .await;
    forbidden(
        &bob,
        "subscription",
        json!({"action": "list", "stream": "secrets"}),
    )
    .await;
    forbidden(
        &bob,
        "subscription",
        json!({"action": "recover", "stream": "secrets", "label": "alice-desk"}),
    )
    .await;
    forbidden(
        &bob,
        "subscription",
        json!({"action": "delete", "stream": "secrets", "label": "alice-desk"}),
    )
    .await;
    forbidden(
        &bob,
        "publish",
        json!({"stream": "secrets", "idempotency_key": "bob-1", "payload": "hi"}),
    )
    .await;
    forbidden(&bob, "receive", receive(&alice_handle)).await;
    forbidden(
        &bob,
        "stream",
        json!({"action": "grant", "name": "secrets", "caller": "bob"}),
    )
    .await;
    let read = bob.read_inbox(&inbox_uri(&alice_handle)).await;
    let error = format!("{:?}", read.expect_err("bob cannot read alice's inbox"));
    assert!(error.contains("stream_forbidden"), "{error}");
    let bob_agent = server
        .agent_client_with_token("bob-agent", BOB_TOKEN, modern())
        .await;
    forbidden(&bob_agent, "follow", json!({"stream": "secrets"})).await;

    let granted = alice
        .call(
            "stream",
            json!({"action": "grant", "name": "secrets", "caller": "bob"}),
        )
        .await
        .ok();
    assert_eq!(
        granted,
        json!({"status": "granted", "stream": "secrets", "caller": "bob"})
    );
    assert_eq!(
        listed_streams(&bob).await,
        names(&["agents", "open", "secrets"])
    );
    let bob_handle = bob.create_subscription("secrets", "bob-desk").await;
    let received = bob.call("receive", receive(&bob_handle)).await.ok();
    assert_eq!(
        payloads(&received),
        vec![json!({"kind": "ask", "question": "q3 deck?"})]
    );
    bob.publish("secrets", "bob-1", json!({"kind": "answer"}))
        .await;
    let followed = bob_agent
        .call("follow", json!({"stream": "secrets"}))
        .await
        .ok();
    assert_eq!(followed["status"], "following");

    let revoked = alice
        .call(
            "stream",
            json!({"action": "revoke", "name": "secrets", "caller": "bob"}),
        )
        .await
        .ok();
    assert_eq!(
        revoked,
        json!({"status": "revoked", "stream": "secrets", "caller": "bob"})
    );
    forbidden(&bob, "receive", receive(&bob_handle)).await;
    forbidden(
        &bob,
        "publish",
        json!({"stream": "secrets", "idempotency_key": "bob-2", "payload": "hi"}),
    )
    .await;
    assert_eq!(listed_streams(&bob).await, names(&["agents", "open"]));

    let last = alice
        .call(
            "stream",
            json!({"action": "revoke", "name": "secrets", "caller": "alice"}),
        )
        .await
        .err();
    assert_eq!(last["code"], "invalid_arguments", "{last:#}");
    let open = alice
        .call(
            "stream",
            json!({"action": "grant", "name": "open", "caller": "bob"}),
        )
        .await
        .err();
    assert_eq!(open["code"], "invalid_arguments", "{open:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn members_named_at_creation_are_admitted_at_once() {
    let server = Server::start_with_bearer_tokens(&tokens()).await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    let bob = server.client_with_token(BOB_TOKEN, modern()).await;
    alice
        .call(
            "stream",
            json!({"action": "create", "name": "pair", "members": ["bob"]}),
        )
        .await
        .ok();
    let handle = bob.create_subscription("pair", "bob-desk").await;
    alice.publish("pair", "hello", json!("hello bob")).await;
    let received = bob.call("receive", receive(&handle)).await.ok();
    assert_eq!(payloads(&received), vec![json!("hello bob")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_private_stream_needs_authenticated_callers() {
    let server = Server::start().await;
    let client = server.client(modern()).await;
    let error = client
        .call(
            "stream",
            json!({"action": "create", "name": "secrets", "members": []}),
        )
        .await
        .err();
    assert_eq!(error["code"], "private_stream_needs_auth", "{error:#}");
    let listed = listed_streams(&client).await;
    assert!(!listed.contains(&json!("secrets")), "{listed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_event_names_the_caller_that_published_it() {
    let server = Server::start_with_bearer_tokens(&tokens()).await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    let bob = server.client_with_token(BOB_TOKEN, modern()).await;
    alice.create_stream("work").await;
    let handle = alice.create_subscription("work", "alice-desk").await;
    // bob signs as someone else; the envelope still names bob.
    bob.call(
        "publish",
        json!({"stream": "work", "idempotency_key": "spoof", "payload": {"kind": "offer"}, "from": "carol"}),
    )
    .await
    .ok();
    let received = alice.call("receive", receive(&handle)).await.ok();
    let events = received["events"].as_array().expect("events");
    assert_eq!(events.len(), 1, "{received:#}");
    assert_eq!(events[0]["publisher"], "bob", "{received:#}");

    let open = Server::start().await;
    let anonymous = open.client(modern()).await;
    anonymous.create_stream("work").await;
    let handle = anonymous.create_subscription("work", "reader").await;
    anonymous.publish("work", "plain", json!("no caller")).await;
    let received = anonymous.call("receive", receive(&handle)).await.ok();
    let event = &received["events"][0];
    assert_eq!(event["payload"], "no caller", "{received:#}");
    assert!(
        event.get("publisher").is_none(),
        "an unauthenticated publish names no publisher: {received:#}"
    );
}
