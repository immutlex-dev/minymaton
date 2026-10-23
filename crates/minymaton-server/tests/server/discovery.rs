use crate::common::{Lifecycle, Server, receive_args};
use rmcp::model::ProtocolVersion;
use serde_json::json;

fn lifecycle() -> Lifecycle {
    Lifecycle::Initialize(ProtocolVersion::V_2025_11_25)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn action_schemas_are_flat_and_requests_remain_strict() {
    let server = Server::start().await;
    let client = server.client(lifecycle()).await;
    let listed = client.list_tools().await;
    for (name, actions, fields) in [
        (
            "stream",
            json!(["create", "grant", "revoke", "list"]),
            vec!["action", "name", "members", "caller", "after", "limit"],
        ),
        (
            "subscription",
            json!(["create", "recover", "delete", "list"]),
            vec![
                "action", "stream", "label", "start_at", "metadata", "after", "limit",
            ],
        ),
    ] {
        let tool = listed["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == name)
            .unwrap();
        let schema = &tool["inputSchema"];
        assert_eq!(schema["type"], "object");
        assert!(schema.get("oneOf").is_none(), "{schema:#}");
        assert_eq!(schema["properties"]["action"]["type"], "string");
        assert_eq!(schema["properties"]["action"]["enum"], actions);
        assert_eq!(schema["required"], json!(["action"]));
        for field in fields {
            assert!(
                schema["properties"].get(field).is_some(),
                "{field}: {schema:#}"
            );
        }
        for action in actions.as_array().unwrap() {
            assert!(
                jsonschema::is_valid(schema, &json!({"action": action})),
                "{action}: {schema:#}"
            );
        }
        assert!(!jsonschema::is_valid(
            schema,
            &json!({"action": "list", "unknown": true}),
        ));
        if name == "subscription" {
            let label = schema["properties"]["label"]["description"]
                .as_str()
                .unwrap();
            assert!(label.contains("Required for recover."));
            assert!(label.contains("Required for delete."));
            assert!(label.contains("The subscription label to retire and remove"));
            let metadata = schema["properties"]["metadata"]["description"]
                .as_str()
                .unwrap();
            assert!(metadata.contains("Optional updated identity metadata"));
        }
    }

    for (tool, args, missing) in [
        ("stream", json!({"action": "create"}), "name"),
        ("subscription", json!({"action": "recover"}), "label"),
        ("subscription", json!({"action": "delete"}), "label"),
    ] {
        let error = client.call(tool, args).await.err();
        assert_eq!(error["code"], "invalid_arguments");
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains(&format!("missing field `{missing}`")),
            "{error:#}"
        );
    }
    for (tool, args) in [
        ("stream", json!({"action": "unknown"})),
        ("stream", json!({"action": "list", "name": "irrelevant"})),
        ("subscription", json!({"action": "unknown"})),
        ("subscription", json!({"action": "delete", "label": null})),
        (
            "subscription",
            json!({"action": "list", "label": "irrelevant"}),
        ),
    ] {
        assert_eq!(
            client.call(tool, args).await.err()["code"],
            "invalid_arguments"
        );
    }

    client.create_stream("schema-check").await;
    let streams = client.call("stream", json!({"action": "list"})).await.ok();
    assert!(
        streams["streams"]
            .as_array()
            .unwrap()
            .contains(&json!("schema-check"))
    );
    let create = json!({
        "action": "create",
        "stream": "schema-check",
        "label": "schema-check",
        "start_at": "earliest",
        "metadata": {"harness": "integration-test"}
    });
    let schema = &listed["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "subscription")
        .unwrap()["inputSchema"];
    assert!(jsonschema::is_valid(schema, &create));
    for invalid in [
        json!({"action": "create", "start_at": "unknown"}),
        json!({"action": "create", "metadata": "not-an-object"}),
        json!({"action": "list", "limit": -1}),
    ] {
        assert!(!jsonschema::is_valid(schema, &invalid), "{invalid:#}");
        assert_eq!(
            client.call("subscription", invalid).await.err()["code"],
            "invalid_arguments"
        );
    }
    let created = client.call("subscription", create).await.ok();
    assert_eq!(created["metadata"]["harness"], "integration-test");
    let handle = created["handle"].as_str().unwrap();
    let recovered = client
        .call(
            "subscription",
            json!({"action": "recover", "stream": "schema-check", "label": "schema-check"}),
        )
        .await
        .ok();
    assert_eq!(recovered["handle"], handle);
    let subscriptions = client
        .call(
            "subscription",
            json!({"action": "list", "stream": "schema-check"}),
        )
        .await
        .ok();
    assert_eq!(subscriptions["subscriptions"][0]["handle"], handle);
    let deleted = client
        .call(
            "subscription",
            json!({"action": "delete", "stream": "schema-check", "label": "schema-check"}),
        )
        .await
        .ok();
    assert_eq!(deleted["status"], "deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_discovery_is_paginated_and_survives_restart() {
    let mut server = Server::start().await;
    let client = server.client(lifecycle()).await;
    client.create_stream("reviews").await;
    client.create_stream("work").await;
    let first = client
        .call("stream", json!({"action": "list", "limit": 2}))
        .await
        .ok();
    assert_eq!(
        first,
        json!({"status": "listed", "streams": ["agents", "reviews"], "next_after": "reviews"})
    );
    let second = client
        .call(
            "stream",
            json!({"action": "list", "limit": 2, "after": first["next_after"]}),
        )
        .await
        .ok();
    assert_eq!(
        second,
        json!({"status": "listed", "streams": ["work"], "next_after": null})
    );
    drop(client);
    server.restart().await;
    let client = server.client(lifecycle()).await;
    let restored = client.call("stream", json!({"action": "list"})).await.ok();
    assert_eq!(
        restored,
        json!({"status": "listed", "streams": ["agents", "reviews", "work"], "next_after": null})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_discovery_is_scoped_and_does_not_advance_delivery() {
    let server = Server::start().await;
    let client = server.client(lifecycle()).await;
    client.create_stream("team").await;
    let first = client.create_subscription("team", "agent-1").await;
    let second = client.create_subscription("team", "agent-2").await;
    client.create_stream("reviews").await;
    client.create_subscription("reviews", "reviewer").await;
    client.publish("team", "hello", json!("hello")).await;
    let listed = client
        .call(
            "subscription",
            json!({"action": "list", "stream": "team", "limit": 1}),
        )
        .await
        .ok();
    assert_eq!(listed["status"], "listed");
    assert_eq!(listed["stream"], "team");
    assert_eq!(listed["next_after"], "agent-1");
    assert_eq!(listed["subscriptions"][0]["label"], "agent-1");
    assert_eq!(listed["subscriptions"][0]["handle"], first);
    assert_eq!(listed["subscriptions"][0]["waiting"], false);
    assert!(listed["subscriptions"][0]["last_active_at"].is_string());
    let listed = client
        .call(
            "subscription",
            json!({"action": "list", "stream": "team", "limit": 1, "after": "agent-1"}),
        )
        .await
        .ok();
    assert_eq!(listed["status"], "listed");
    assert_eq!(listed["stream"], "team");
    assert_eq!(listed["next_after"], serde_json::Value::Null);
    assert_eq!(listed["subscriptions"][0]["label"], "agent-2");
    assert_eq!(listed["subscriptions"][0]["handle"], second);
    assert_eq!(listed["subscriptions"][0]["waiting"], false);
    assert!(listed["subscriptions"][0]["last_active_at"].is_string());
    let received = client
        .call("receive", receive_args(&first, 1, 16384, 0))
        .await
        .ok();
    assert_eq!(received["status"], "events");
    assert_eq!(received["events"][0]["payload"], "hello");
    assert_eq!(received["delivery_attempt"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unassigned_agents_claim_distinct_labels_on_the_default_stream() {
    let server = Server::start().await;
    let first = server.client(lifecycle()).await;
    let second = server.client(lifecycle()).await;
    let assigned = first
        .call(
            "subscription",
            json!({"action": "create", "label": "agent-1"}),
        )
        .await
        .ok();
    assert_eq!(assigned["stream"], "agents");
    let (left, right) = tokio::join!(
        first.call("subscription", json!({"action": "create"})),
        second.call("subscription", json!({"action": "create"})),
    );
    let left = left.ok();
    let right = right.ok();
    assert_eq!(left["status"], "created");
    assert_eq!(right["status"], "created");
    assert_eq!(left["stream"], "agents");
    assert_eq!(right["stream"], "agents");
    assert_ne!(left["handle"], right["handle"]);
    let mut labels = [
        left["label"].as_str().unwrap(),
        right["label"].as_str().unwrap(),
    ];
    labels.sort();
    assert_eq!(labels, ["agent-2", "agent-3"]);
    for subscription in [left, right] {
        let health = first
            .call(
                "receive",
                receive_args(subscription["handle"].as_str().unwrap(), 1, 16384, 0),
            )
            .await
            .ok();
        assert_eq!(health["status"], "empty");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_rejects_invalid_limits_and_protects_claimed_labels_across_clients() {
    let server = Server::start_for_agents(Some("tools")).await;
    let first = server.client(lifecycle()).await;
    let second = server.client(lifecycle()).await;
    let agent_endpoint = server
        .agent_client(
            "agent-1",
            Lifecycle::Discover(ProtocolVersion::V_2026_07_28),
        )
        .await;
    let unbound_instructions = first.instructions().expect("unbound instructions");
    let bound_instructions = agent_endpoint.instructions().expect("bound instructions");
    assert!(
        unbound_instructions.contains(
            "registered tools on this endpoint: stream, subscription, publish, receive)."
        )
    );
    assert!(!unbound_instructions.contains("follow"));
    assert!(!unbound_instructions.contains("message_*"));
    assert!(unbound_instructions.contains("smallest free agent-N"));
    assert!(unbound_instructions.contains("wait true"));
    assert!(unbound_instructions.contains("minymaton-wait --handle <handle> --url <mcp_url>"));
    assert!(unbound_instructions.contains("minymaton-wait.py"));
    assert!(unbound_instructions.contains("MINYMATON_URL / MINYMATON_CONFIG"));
    assert!(unbound_instructions.contains("harness-decoupled"));
    assert!(bound_instructions.contains(
        "registered tools on this endpoint: stream, subscription, publish, receive, follow)."
    ));
    assert!(bound_instructions.contains("instead of a receive loop"));
    assert!(bound_instructions.contains("wait false"));
    assert!(bound_instructions.contains("metadata"));
    assert!(!bound_instructions.contains("{name}"));
    assert!(!bound_instructions.contains("Then receive in an event-driven loop"));

    let created = first
        .call(
            "subscription",
            json!({"action":"create", "label":"agent-1"}),
        )
        .await
        .ok();
    let conflict = second
        .call(
            "subscription",
            json!({"action":"create", "label":"agent-1"}),
        )
        .await
        .err();
    assert_eq!(conflict["code"], "subscription_exists");
    assert!(
        conflict["message"]
            .as_str()
            .unwrap()
            .contains("recover it only if this is your own earlier subscription")
    );
    let follow_conflict = agent_endpoint
        .call("follow", json!({"stream": "agents"}))
        .await
        .err();
    assert_eq!(follow_conflict["code"], "subscription_exists");
    assert!(
        follow_conflict["message"]
            .as_str()
            .unwrap()
            .contains("already has an unbound subscription labelled \"agent-1\"")
    );
    let second_claimed = second
        .call("subscription", json!({"action": "create"}))
        .await
        .ok();
    assert_eq!(second_claimed["label"], "agent-2");
    assert_ne!(second_claimed["handle"], created["handle"]);
    let recovered = first
        .call(
            "subscription",
            json!({"action":"recover", "label":"agent-1"}),
        )
        .await
        .ok();
    assert_eq!(created["handle"], recovered["handle"]);
    for tool in ["stream", "subscription"] {
        for limit in [0, 101, u64::MAX] {
            let error = first
                .call(tool, json!({"action":"list", "limit":limit}))
                .await
                .err();
            assert_eq!(error["code"], "invalid_arguments");
        }
        let error = first
            .call(tool, json!({"action":"list", "after":""}))
            .await
            .err();
        assert_eq!(error["code"], "invalid_arguments");
    }
    let error = first
        .call("subscription", json!({"action":"list", "stream":"missing"}))
        .await
        .err();
    assert_eq!(error["code"], "stream_not_found");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn escaping_heavy_discovery_pages_fit_the_actual_wire_limit_without_losing_entries() {
    let server = Server::start().await;
    let client = server.client(lifecycle()).await;
    let mut expected = Vec::new();
    for index in 0..100 {
        let name = format!("{index:03}{}", "\\".repeat(125));
        client.create_stream(&name).await;
        client.create_subscription("agents", &name).await;
        expected.push(name);
    }
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    for tool in ["stream", "subscription"] {
        let mut after = serde_json::Value::Null;
        let mut found = Vec::new();
        loop {
            let (wire, page) = raw
                .call_tool(tool, json!({"action":"list", "limit":100, "after":after}))
                .await;
            assert!(
                wire.events
                    .iter()
                    .map(|event| event.wire_bytes)
                    .sum::<usize>()
                    <= 16384
            );
            let page = page.ok();
            let names: Vec<String> = if tool == "stream" {
                page["streams"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value.as_str().unwrap().to_owned())
                    .collect()
            } else {
                page["subscriptions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| value["label"].as_str().unwrap().to_owned())
                    .collect()
            };
            assert!(!names.is_empty(), "every nonterminal page must advance");
            found.extend(names);
            if page["next_after"].is_null() {
                break;
            }
            assert_ne!(page["next_after"], after);
            after = page["next_after"].clone();
        }
        let mut expected = expected.clone();
        if tool == "stream" {
            expected.push("agents".to_owned());
        }
        assert_eq!(found, expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_delete_frees_agent_label_wakes_waiter_and_reports_waiting_state() {
    let server = Server::start().await;
    let first = server.client(lifecycle()).await;
    let second = server.client(lifecycle()).await;

    let claimed = first
        .call("subscription", json!({"action": "create"}))
        .await
        .ok();
    assert_eq!(claimed["label"], "agent-1");
    let handle = claimed["handle"].as_str().unwrap().to_owned();

    let mut waiting = first
        .start("receive", receive_args(&handle, 10, 16384, 25_000))
        .await;
    waiting.window_open().await;

    let listed = second
        .call("subscription", json!({"action": "list"}))
        .await
        .ok();
    assert_eq!(listed["subscriptions"][0]["label"], "agent-1");
    assert_eq!(listed["subscriptions"][0]["waiting"], true);

    let deleted = second
        .call(
            "subscription",
            json!({"action": "delete", "label": "agent-1"}),
        )
        .await
        .ok();
    assert_eq!(
        deleted,
        json!({"status": "deleted", "stream": "agents", "label": "agent-1"})
    );

    let ended = waiting.finish().await.ok();
    assert_eq!(ended["status"], "superseded");
    assert_eq!(ended["wait"]["ended_by"], "superseded");

    let missing = first
        .call("receive", receive_args(&handle, 10, 16384, 0))
        .await
        .err();
    assert_eq!(missing["code"], "subscription_not_found");

    let reclaimed = second
        .call("subscription", json!({"action": "create"}))
        .await
        .ok();
    assert_eq!(reclaimed["label"], "agent-1");
    assert_ne!(reclaimed["handle"], handle);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn discovery_reads_proceed_without_blocking_on_exclusive_writer_transaction() {
    let server = Server::start().await;
    let client = server.client(lifecycle()).await;
    client.create_stream("team").await;
    let _handle = client.create_subscription("team", "agent-1").await;

    // Hold a BEGIN IMMEDIATE write transaction on a separate SQLite connection
    // to prove stream.list and subscription.list use the read-only WAL reader
    // without blocking on the writer lock.
    let writer = rusqlite::Connection::open(server.db_path()).expect("open db");
    writer
        .execute_batch("PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE;")
        .expect("begin immediate");

    let streams = client.call("stream", json!({"action": "list"})).await.ok();
    assert_eq!(
        streams,
        json!({"status": "listed", "streams": ["agents", "team"], "next_after": null})
    );

    let subs = client
        .call("subscription", json!({"action": "list", "stream": "team"}))
        .await
        .ok();
    assert_eq!(subs["status"], "listed");
    assert_eq!(subs["subscriptions"][0]["label"], "agent-1");

    writer.execute_batch("ROLLBACK;").expect("rollback");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_metadata_persists_across_create_recover_and_list() {
    let server = Server::start().await;
    let client = server.client(lifecycle()).await;

    let created = client
        .call(
            "subscription",
            json!({
                "action": "create",
                "metadata": {"harness": "copilot-cli", "model": "gemini-4-argon"}
            }),
        )
        .await
        .ok();
    assert_eq!(created["label"], "agent-1");
    assert_eq!(
        created["metadata"],
        json!({"harness": "copilot-cli", "model": "gemini-4-argon"})
    );

    let listed = client
        .call("subscription", json!({"action": "list"}))
        .await
        .ok();
    assert_eq!(
        listed["subscriptions"][0]["metadata"],
        json!({"harness": "copilot-cli", "model": "gemini-4-argon"})
    );

    let recovered = client
        .call(
            "subscription",
            json!({
                "action": "recover",
                "label": "agent-1",
                "metadata": {"harness": "copilot-cli", "model": "gemini-4-argon", "role": "coordinator"}
            }),
        )
        .await
        .ok();
    assert_eq!(
        recovered["metadata"],
        json!({"harness": "copilot-cli", "model": "gemini-4-argon", "role": "coordinator"})
    );

    let preserved = client
        .call(
            "subscription",
            json!({"action": "recover", "label": "agent-1"}),
        )
        .await
        .ok();
    assert_eq!(
        preserved["metadata"],
        json!({"harness": "copilot-cli", "model": "gemini-4-argon", "role": "coordinator"})
    );

    let oversized = client
        .call(
            "subscription",
            json!({
                "action": "create",
                "metadata": {"blob": "x".repeat(1100)}
            }),
        )
        .await
        .err();
    assert_eq!(oversized["code"], "invalid_arguments");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactional_publish_ack_commits_atomically_and_rolls_back_on_failed_publish() {
    let server = Server::start().await;
    let coordinator = server.client(lifecycle()).await;
    let worker = server.client(lifecycle()).await;

    let _coord_handle = coordinator
        .call(
            "subscription",
            json!({"action": "create", "label": "agent-1", "start_at": "earliest"}),
        )
        .await
        .ok()["handle"]
        .as_str()
        .unwrap()
        .to_owned();
    let worker_handle = worker
        .call(
            "subscription",
            json!({"action": "create", "label": "agent-2", "start_at": "earliest"}),
        )
        .await
        .ok()["handle"]
        .as_str()
        .unwrap()
        .to_owned();

    // Coordinator sends a task to agent-2.
    coordinator
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "task-1",
                "payload": {"type": "task", "from": "agent-1", "to": "agent-2", "job": "verify"}
            }),
        )
        .await
        .ok();

    let batch = worker
        .call("receive", receive_args(&worker_handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(batch["status"], "events");
    let batch_id = batch["ack_after_handling"].as_str().unwrap().to_owned();

    // Seed an idempotency key so a conflicting publish with `ack` will fail inside the transaction.
    worker
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "dup-key",
                "payload": {"type": "note", "from": "agent-2"}
            }),
        )
        .await
        .ok();

    let failed = worker
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "dup-key",
                "ack": batch_id,
                "payload": {"type": "result", "from": "agent-2", "to": "agent-1", "ok": false}
            }),
        )
        .await
        .err();
    assert_eq!(failed["code"], "idempotency_conflict");

    // Wrong sender label is also rejected without committing the ack.
    let wrong_sender = coordinator
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "spoof-ack",
                "ack": batch_id,
                "payload": {"type": "result", "from": "agent-1", "to": "agent-2"}
            }),
        )
        .await
        .err();
    assert_eq!(wrong_sender["code"], "invalid_ack");

    // Because both failed, the batch must still be pending and redelivered on receive(wait: false).
    let redelivered = worker
        .call("receive", receive_args(&worker_handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(redelivered["status"], "events");
    assert_eq!(redelivered["batch_id"], batch_id);
    assert_eq!(redelivered["delivery_attempt"], 2);

    // Now publish the reply and commit the ack in a single atomic call.
    let published = worker
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "task-1-reply",
                "ack": batch_id,
                "payload": {"type": "result", "from": "agent-2", "to": "agent-1", "ok": true}
            }),
        )
        .await
        .ok();
    assert_eq!(published["status"], "published");
    assert_eq!(
        published["ack_result"],
        json!({"batch_id": batch_id, "outcome": "committed"})
    );

    // Repeating the identical idempotent publish returns already_acked.
    let replayed = worker
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "task-1-reply",
                "ack": batch_id,
                "payload": {"type": "result", "from": "agent-2", "to": "agent-1", "ok": true}
            }),
        )
        .await
        .ok();
    assert_eq!(
        replayed["ack_result"],
        json!({"batch_id": batch_id, "outcome": "already_acked"})
    );

    // And worker's queue is now empty with zero extra ack calls.
    let empty = worker
        .call("receive", receive_args(&worker_handle, 10, 16384, 0))
        .await
        .ok();
    assert_eq!(empty["status"], "empty");
}
