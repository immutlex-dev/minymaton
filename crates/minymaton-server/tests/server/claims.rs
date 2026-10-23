//! Claims: a publish with `claims` takes the named earlier message for its
//! sender, and only the first claim of a message commits.

use rmcp::model::ProtocolVersion;
use serde_json::{Value, json};

use crate::common::{Lifecycle, McpClient, Server};

fn modern() -> Lifecycle {
    Lifecycle::Discover(ProtocolVersion::V_2026_07_28)
}

fn receive(handle: &str) -> Value {
    json!({"handle": handle, "max_events": 10, "max_bytes": 16384, "wait": false})
}

fn claim(key: &str, ask: &str, from: &str) -> Value {
    json!({
        "stream": "jobs",
        "idempotency_key": key,
        "payload": {"kind": "offer", "ask": ask},
        "from": from,
        "claims": ask,
    })
}

async fn ask(client: &McpClient) -> String {
    let asked = client
        .call(
            "publish",
            json!({
                "stream": "jobs",
                "idempotency_key": "ask",
                "payload": {"kind": "ask", "task": "fix the flaky receive test"},
                "from": "lead",
            }),
        )
        .await
        .ok();
    asked["message_id"].as_str().expect("ask id").to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_first_claim_of_a_message_commits() {
    let mut server = Server::start().await;
    let client = server.client(modern()).await;
    client.create_stream("jobs").await;
    client.create_stream("other").await;
    client.create_subscription("jobs", "lead").await;
    client.create_subscription("jobs", "w1").await;
    let w2 = client.create_subscription("jobs", "w2").await;
    let ask_id = ask(&client).await;

    let first = client
        .start("publish", claim("w1-claim", &ask_id, "w1"))
        .await;
    let second = client
        .start("publish", claim("w2-claim", &ask_id, "w2"))
        .await;
    let outcomes = [first.finish().await, second.finish().await];
    let (won, lost): (Vec<_>, Vec<_>) = outcomes
        .into_iter()
        .partition(|outcome| matches!(outcome, crate::common::Outcome::Success(_)));
    assert_eq!((won.len(), lost.len()), (1, 1), "{won:?} {lost:?}");
    let won = won.into_iter().next().unwrap().ok();
    let lost = lost.into_iter().next().unwrap().err();
    assert_eq!(won["status"], "published", "{won:#}");
    assert_eq!(lost["code"], "already_claimed", "{lost:#}");
    assert_eq!(
        lost["details"]["claim_message_id"], won["message_id"],
        "{lost:#}"
    );
    let winner = lost["details"]["claimed_by"]
        .as_str()
        .expect("winner label")
        .to_owned();
    assert!(winner == "w1" || winner == "w2", "{lost:#}");

    // The winner's retry returns the original result.
    let retry_key = format!("{winner}-claim");
    let retried = client
        .call("publish", claim(&retry_key, &ask_id, &winner))
        .await
        .ok();
    assert_eq!(retried["message_id"], won["message_id"], "{retried:#}");
    // A fresh claim by anyone, the winner included, is refused.
    let again = client
        .call("publish", claim("again", &ask_id, &winner))
        .await
        .err();
    assert_eq!(again["code"], "already_claimed", "{again:#}");

    // A losing claim that carries an ack acknowledges nothing.
    let batch = client.call("receive", receive(&w2)).await.ok();
    let batch_id = batch["batch_id"].clone();
    let mut losing = claim("w2-with-ack", &ask_id, "w2");
    losing["ack"] = batch_id.clone();
    let refused = client.call("publish", losing).await.err();
    assert_eq!(refused["code"], "already_claimed", "{refused:#}");
    let redelivered = client.call("receive", receive(&w2)).await.ok();
    assert_eq!(redelivered["batch_id"], batch_id, "{redelivered:#}");
    assert_eq!(redelivered["delivery_attempt"], 2, "{redelivered:#}");

    // A claim names a message on its own stream and needs a sender.
    let missing = client
        .call(
            "publish",
            claim("missing", "msg_00000000000000000000000000000000", "w2"),
        )
        .await
        .err();
    assert_eq!(missing["code"], "claim_target_not_found", "{missing:#}");
    let elsewhere = client
        .call(
            "publish",
            json!({"stream": "other", "idempotency_key": "x", "payload": "x", "from": "w2", "claims": ask_id}),
        )
        .await
        .err();
    assert_eq!(elsewhere["code"], "claim_target_not_found", "{elsewhere:#}");
    let anonymous = client
        .call(
            "publish",
            json!({"stream": "jobs", "idempotency_key": "anon", "payload": "mine", "claims": ask_id}),
        )
        .await
        .err();
    assert_eq!(anonymous["code"], "invalid_arguments", "{anonymous:#}");

    // The claim is durable.
    server.restart().await;
    let client = server.client(modern()).await;
    let after = client
        .call("publish", claim("after-restart", &ask_id, "w2"))
        .await
        .err();
    assert_eq!(after["code"], "already_claimed", "{after:#}");
    assert_eq!(after["details"]["claimed_by"], winner.as_str(), "{after:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_instructions_teach_jobs_and_knowledge_asks() {
    let server = Server::start().await;
    let client = server.client(modern()).await;
    let instructions = client.instructions().expect("server instructions");
    for needle in [
        "Jobs and knowledge asks",
        "\"kind\"",
        "\"ask\"",
        "offer",
        "award",
        "answer",
        "claims",
        "already_claimed",
        "your owner",
        "members",
    ] {
        assert!(
            instructions.contains(needle),
            "instructions lack {needle:?}: {instructions}"
        );
    }
}
