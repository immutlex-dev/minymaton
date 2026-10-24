//! Standard push (decision 17): each subscription's inbox is an MCP resource.
//! Every commit pushes `notifications/resources/updated` for it, and reading
//! the inbox delivers the pending batch without acknowledging it.

use rmcp::ServiceError;
use rmcp::model::ProtocolVersion;
use serde_json::{Value, json};

use crate::common::{
    Lifecycle, RESULT_LIMIT, RawSession, Server, ack_only, inbox_content, inbox_uri, keys,
    message_ids, next_update, payloads, receive_args,
};

const LIMIT: u64 = RESULT_LIMIT as u64;
const LEGACY: [ProtocolVersion; 2] = [ProtocolVersion::V_2025_06_18, ProtocolVersion::V_2025_11_25];
const TEMPLATE: &str = "minymaton://inbox/{handle}";

fn tools_client() -> Lifecycle {
    Lifecycle::Initialize(ProtocolVersion::V_2025_11_25)
}

fn modern() -> Lifecycle {
    Lifecycle::Discover(ProtocolVersion::V_2026_07_28)
}

async fn raw_read(raw: &mut RawSession, uri: &str) -> Value {
    let reply = raw.rpc("resources/read", json!({"uri": uri})).await;
    inbox_content(uri, &reply["result"])
}

async fn raw_subscribe(raw: &mut RawSession, uri: &str) {
    let subscribed = raw.rpc("resources/subscribe", json!({"uri": uri})).await;
    assert_eq!(subscribed["result"], json!({}), "{subscribed:#}");
}

async fn raw_ack(raw: &mut RawSession, handle: &str, batch_id: &Value) {
    let (_, outcome) = raw.call_tool("receive", ack_only(handle, batch_id)).await;
    assert_eq!(
        outcome.ok()["ack_result"],
        json!({"batch_id": batch_id, "outcome": "committed"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbox_resources_are_advertised_on_every_revision() {
    let server = Server::start().await;
    for version in &LEGACY {
        let mut raw = server.raw_session(version).await;
        assert_eq!(
            raw.capabilities()["resources"]["subscribe"],
            true,
            "{version}"
        );
        let listed = raw.rpc("resources/templates/list", json!({})).await;
        let templates = listed["result"]["resourceTemplates"]
            .as_array()
            .unwrap_or_else(|| panic!("{version}: {listed:#}"));
        assert_eq!(templates.len(), 1, "{version}: {listed:#}");
        assert_eq!(templates[0]["uriTemplate"], TEMPLATE);
        assert_eq!(templates[0]["mimeType"], "application/json");
    }
    let client = server.client(modern()).await;
    assert_eq!(client.capabilities()["resources"]["subscribe"], true);
    let templates = client.resource_templates().await;
    assert_eq!(templates.len(), 1, "{templates:#?}");
    assert_eq!(templates[0]["uriTemplate"], TEMPLATE);
    assert_eq!(templates[0]["mimeType"], "application/json");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_subscription_pushes_every_commit_on_the_event_stream() {
    let server = Server::start().await;
    let tools = server.client(tools_client()).await;
    for (index, version) in LEGACY.iter().enumerate() {
        let stream = format!("push-{index}");
        tools.create_stream(&stream).await;
        let handle = tools.create_subscription(&stream, "reader").await;
        let uri = inbox_uri(&handle);
        let mut raw = server.raw_session(version).await;
        let mut events = raw.events(None).await;
        raw_subscribe(&mut raw, &uri).await;

        // The second round proves the subscription stays live without re-arming.
        for round in 0..2 {
            let body = json!(format!("{version} message {round}"));
            let published = tools
                .publish(&stream, &format!("m{round}"), body.clone())
                .await;
            assert_eq!(events.next_update().await, uri, "{version} round {round}");
            let content = raw_read(&mut raw, &uri).await;
            assert_eq!(
                keys(&content),
                [
                    "ack_after_handling",
                    "batch_id",
                    "delivery_attempt",
                    "events",
                    "has_more",
                    "status"
                ],
                "{content:#}"
            );
            assert_eq!(content["status"], "events");
            assert_eq!(content["delivery_attempt"], 1);
            assert_eq!(content["has_more"], false);
            assert_eq!(content["ack_after_handling"], content["batch_id"]);
            assert_eq!(message_ids(&content), vec![published["message_id"].clone()]);
            assert_eq!(payloads(&content), vec![body]);
            raw_ack(&mut raw, &handle, &content["ack_after_handling"]).await;
        }
        assert!(
            events.comments.is_empty(),
            "keepalive comments: {:?}",
            events.comments
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn modern_listen_pushes_every_commit() {
    let server = Server::start().await;
    let client = server.client(modern()).await;
    let publisher = server.client(modern()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let uri = inbox_uri(&handle);
    let mut listening = client.listen(&[&uri]).await;
    assert_eq!(
        listening.acknowledged().resource_subscriptions,
        Some(vec![uri.clone()])
    );

    for round in 0..2 {
        let body = json!(format!("message {round}"));
        let published = publisher
            .publish("orders", &format!("m{round}"), body.clone())
            .await;
        assert_eq!(next_update(&mut listening).await, uri, "round {round}");
        let result = client.read_inbox(&uri).await.expect("resources/read");
        assert_eq!(
            result["ttlMs"], 0,
            "an inbox must not be cached: {result:#}"
        );
        assert_eq!(result["cacheScope"], "private", "{result:#}");
        let content = inbox_content(&uri, &result);
        assert_eq!(content["delivery_attempt"], 1);
        assert_eq!(message_ids(&content), vec![published["message_id"].clone()]);
        assert_eq!(payloads(&content), vec![body]);
        let acked = client
            .call("receive", ack_only(&handle, &content["ack_after_handling"]))
            .await
            .ok();
        assert_eq!(acked["ack_result"]["outcome"], "committed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_never_acknowledges() {
    let server = Server::start().await;
    let tools = server.client(tools_client()).await;
    tools.create_stream("orders").await;
    let handle = tools.create_subscription("orders", "reader").await;
    let uri = inbox_uri(&handle);
    let published = tools.publish("orders", "m1", json!("handled once")).await;
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;

    let first = raw_read(&mut raw, &uri).await;
    let second = raw_read(&mut raw, &uri).await;
    let received = tools
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();

    assert_eq!(message_ids(&first), vec![published["message_id"].clone()]);
    assert_eq!(first["delivery_attempt"], 1);
    assert_eq!(second["batch_id"], first["batch_id"]);
    assert_eq!(second["events"], first["events"]);
    assert_eq!(second["delivery_attempt"], 2);
    assert_eq!(received["batch_id"], first["batch_id"]);
    assert_eq!(received["delivery_attempt"], 3);
    assert_eq!(received["wait"]["ended_by"], "pending_batch");

    raw_ack(&mut raw, &handle, &first["batch_id"]).await;
    assert_eq!(raw_read(&mut raw, &uri).await, json!({"status": "empty"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribing_to_an_inbox_with_unread_messages_pushes_at_once() {
    let server = Server::start().await;
    let tools = server.client(tools_client()).await;
    tools.create_stream("orders").await;
    let legacy_handle = tools.create_subscription("orders", "legacy-reader").await;
    let modern_handle = tools.create_subscription("orders", "modern-reader").await;
    let published = tools
        .publish("orders", "early", json!("waiting before anyone subscribed"))
        .await;

    let legacy_uri = inbox_uri(&legacy_handle);
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    let mut events = raw.events(None).await;
    raw_subscribe(&mut raw, &legacy_uri).await;
    assert_eq!(events.next_update().await, legacy_uri);
    assert_eq!(
        message_ids(&raw_read(&mut raw, &legacy_uri).await),
        vec![published["message_id"].clone()]
    );

    let modern_uri = inbox_uri(&modern_handle);
    let client = server.client(modern()).await;
    let mut listening = client.listen(&[&modern_uri]).await;
    assert_eq!(next_update(&mut listening).await, modern_uri);
    let result = client
        .read_inbox(&modern_uri)
        .await
        .expect("resources/read");
    assert_eq!(
        message_ids(&inbox_content(&modern_uri, &result)),
        vec![published["message_id"].clone()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_commit_pushes_every_subscribed_session() {
    let server = Server::start().await;
    let tools = server.client(tools_client()).await;
    tools.create_stream("orders").await;
    let handle = tools.create_subscription("orders", "shared").await;
    let uri = inbox_uri(&handle);
    let mut first = server.raw_session(&ProtocolVersion::V_2025_06_18).await;
    let mut second = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    let mut first_events = first.events(None).await;
    let mut second_events = second.events(None).await;
    raw_subscribe(&mut first, &uri).await;
    raw_subscribe(&mut second, &uri).await;
    let client = server.client(modern()).await;
    let mut listening = client.listen(&[&uri]).await;

    tools.publish("orders", "m1", json!("for everyone")).await;

    assert_eq!(first_events.next_update().await, uri);
    assert_eq!(second_events.next_update().await, uri);
    assert_eq!(next_update(&mut listening).await, uri);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reading_the_inbox_does_not_take_over_a_waiting_receive() {
    let server = Server::start().await;
    let receiver = server.client(tools_client()).await;
    let publisher = server.client(tools_client()).await;
    receiver.create_stream("orders").await;
    let handle = receiver.create_subscription("orders", "reader").await;
    let uri = inbox_uri(&handle);
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;

    let mut waiting = receiver
        .start("receive", receive_args(&handle, 10, LIMIT, 25_000))
        .await;
    waiting.window_open().await;
    assert_eq!(raw_read(&mut raw, &uri).await, json!({"status": "empty"}));
    let published = publisher
        .publish("orders", "m1", json!("either path"))
        .await;
    let received = waiting.finish().await.ok();

    assert_eq!(received["wait"]["ended_by"], "commit");
    assert_eq!(
        message_ids(&received),
        vec![published["message_id"].clone()]
    );
    // Both paths deliver the subscription's one pending batch.
    let read = raw_read(&mut raw, &uri).await;
    assert_eq!(read["batch_id"], received["batch_id"]);
    assert_eq!(read["delivery_attempt"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_inboxes_are_refused() {
    let server = Server::start().await;
    let unknown = inbox_uri("sub_00000000000000000000000000000000");
    let foreign = "minymaton://elsewhere";
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    for uri in [unknown.as_str(), foreign] {
        let subscribed = raw.rpc("resources/subscribe", json!({"uri": uri})).await;
        assert_eq!(subscribed["error"]["code"], -32002, "{subscribed:#}");
        let read = raw.rpc("resources/read", json!({"uri": uri})).await;
        assert_eq!(read["error"]["code"], -32002, "{read:#}");
    }

    let client = server.client(modern()).await;
    let listening = client.listen(&[&unknown]).await;
    assert_eq!(
        listening.acknowledged().resource_subscriptions,
        None,
        "an unknown inbox was accepted"
    );
    match client.read_inbox(&unknown).await {
        Err(ServiceError::McpError(error)) => assert_eq!(error.code.0, -32602, "{error:?}"),
        other => panic!("reading an unknown inbox returned {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resuming_the_event_stream_replays_nothing() {
    let server = Server::start().await;
    let tools = server.client(tools_client()).await;
    tools.create_stream("orders").await;
    let handle = tools.create_subscription("orders", "reader").await;
    let uri = inbox_uri(&handle);
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    // With no event stream open yet, the update waits in the session.
    raw_subscribe(&mut raw, &uri).await;
    tools
        .publish("orders", "m1", json!("waiting for the stream"))
        .await;

    let mut resumed = raw.events(Some("0")).await;
    let replayed = resumed.next_event().await;
    assert!(
        replayed.is_none(),
        "a resumed stream delivered {replayed:?}"
    );
    // A plain event stream still delivers the waiting update: refusing to
    // resume lost nothing.
    let mut events = raw.events(None).await;
    assert_eq!(events.next_update().await, uri);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_event_stream_sends_nothing_while_idle() {
    let server = Server::start().await;
    let tools = server.client(tools_client()).await;
    tools.create_stream("orders").await;
    let handle = tools.create_subscription("orders", "reader").await;
    let uri = inbox_uri(&handle);
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    let mut events = raw.events(None).await;
    raw_subscribe(&mut raw, &uri).await;

    let idle = tools.create_subscription("agents", "idle").await;
    let empty = tools
        .call("receive", receive_args(&idle, 1, RESULT_LIMIT as u64, 0))
        .await
        .ok();
    assert_eq!(empty["wait"]["ended_by"], "no_wait");
    tools
        .publish("orders", "m1", json!("after a quiet stretch"))
        .await;

    assert_eq!(events.next_update().await, uri);
    assert!(
        events.comments.is_empty(),
        "keepalive comments: {:?}",
        events.comments
    );
}
