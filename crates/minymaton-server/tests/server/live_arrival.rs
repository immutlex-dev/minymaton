//! Live arrival: a receive waiting on a stream returns as soon as a message is
//! committed to it, on every approved MCP protocol revision.

use crate::common::{
    Lifecycle, RESULT_LIMIT, Server, ack_only, keys, message_ids, payloads, receive_args, with_ack,
};
use rmcp::model::{ClientCapabilities, ProtocolVersion};
use serde_json::{Value, json};

const LIMIT: u64 = RESULT_LIMIT as u64;

fn legacy() -> Lifecycle {
    Lifecycle::Initialize(ProtocolVersion::V_2025_11_25)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backlog_is_delivered_at_once() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let published = client
        .publish("orders", "order-1", json!("first order"))
        .await;

    let received = client
        .call("receive", receive_args(&handle, 10, LIMIT, 25_000))
        .await
        .ok();

    assert_eq!(received["status"], "events");
    assert_eq!(received["wait"]["ended_by"], "backlog");
    assert_eq!(received["delivery_attempt"], 1);
    assert_eq!(received["has_more"], false);
    assert_eq!(
        received["events"],
        json!([{
            "message_id": published["message_id"],
            "sequence": published["sequence"],
            "published_at": published["published_at"],
            "payload": "first order",
        }])
    );
    assert!(
        received["batch_id"].is_string(),
        "no batch ID in {received:#}"
    );
    assert_eq!(received["ack_after_handling"], received["batch_id"]);
    assert_eq!(
        received["next_receive"],
        receive_args(&handle, 10, LIMIT, 25_000)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_receive_is_woken_by_the_commit() {
    let server = Server::start().await;
    let receiver = server.client(legacy()).await;
    let publisher = server.client(legacy()).await;
    receiver.create_stream("orders").await;
    let handle = receiver.create_subscription("orders", "reader").await;

    let mut waiting = receiver
        .start("receive", receive_args(&handle, 10, LIMIT, 25_000))
        .await;
    waiting.window_open().await;
    let published = publisher
        .publish("orders", "order-1", json!({"order": 1}))
        .await;
    let received = waiting.finish().await.ok();

    assert_eq!(received["status"], "events");
    assert_eq!(received["wait"]["ended_by"], "commit");
    assert_eq!(received["delivery_attempt"], 1);
    assert_eq!(
        message_ids(&received),
        vec![published["message_id"].clone()]
    );
    assert_eq!(payloads(&received), vec![json!({"order": 1})]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_channel_delivers_each_message_as_it_arrives() {
    let server = Server::start().await;
    let receiver = server.client(legacy()).await;
    let publisher = server.client(legacy()).await;
    receiver.create_stream("orders").await;
    let handle = receiver.create_subscription("orders", "reader").await;

    let mut previous: Option<Value> = None;
    let mut sequences = Vec::new();
    for index in 0..3 {
        let arguments = receive_args(&handle, 10, LIMIT, 25_000);
        let arguments = match &previous {
            Some(batch_id) => with_ack(arguments, batch_id),
            None => arguments,
        };
        let mut waiting = receiver.start("receive", arguments).await;
        waiting.window_open().await;
        let body = json!(format!("message {index}"));
        let published = publisher
            .publish("orders", &format!("order-{index}"), body.clone())
            .await;
        let received = waiting.finish().await.ok();

        match &previous {
            Some(batch_id) => assert_eq!(
                received["ack_result"],
                json!({"batch_id": batch_id, "outcome": "committed"})
            ),
            None => assert!(received.get("ack_result").is_none(), "{received:#}"),
        }
        assert_eq!(received["wait"]["ended_by"], "commit");
        assert_eq!(received["delivery_attempt"], 1);
        assert_eq!(
            message_ids(&received),
            vec![published["message_id"].clone()]
        );
        assert_eq!(payloads(&received), vec![body]);
        sequences.push(published["sequence"].as_u64().expect("sequence"));
        previous = Some(received["ack_after_handling"].clone());
    }
    assert!(
        sequences.windows(2).all(|pair| pair[0] < pair[1]),
        "sequences must increase: {sequences:?}"
    );

    let last = previous.expect("the last batch");
    let acked = receiver
        .call("receive", ack_only(&handle, &last))
        .await
        .ok();
    assert_eq!(keys(&acked), ["ack_result", "status"]);
    assert_eq!(acked["status"], "acked");
    assert_eq!(
        acked["ack_result"],
        json!({"batch_id": last, "outcome": "committed"})
    );
    let drained = receiver
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(drained["status"], "empty");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_waiting_receive_returns_empty_immediately_and_clamps_effective_bounds() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;

    let received = client
        .call("receive", receive_args(&handle, 10, 100_000, 0))
        .await
        .ok();

    assert_eq!(keys(&received), ["next_receive", "status", "wait"]);
    assert_eq!(received["status"], "empty");
    assert_eq!(received["wait"]["ended_by"], "no_wait");
    // The effective bound, not the 100,000 bytes asked for, is carried forward.
    assert_eq!(
        received["next_receive"],
        receive_args(&handle, 10, LIMIT, 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unacknowledged_batch_is_redelivered_whole() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let first_message = client.publish("orders", "order-1", json!("one")).await;
    let second_message = client.publish("orders", "order-2", json!("two")).await;

    let first = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    let again = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();

    assert_eq!(first["delivery_attempt"], 1);
    assert_eq!(first["wait"]["ended_by"], "backlog");
    assert_eq!(
        message_ids(&first),
        vec![
            first_message["message_id"].clone(),
            second_message["message_id"].clone()
        ]
    );
    assert_eq!(again["batch_id"], first["batch_id"]);
    assert_eq!(again["events"], first["events"]);
    assert_eq!(again["delivery_attempt"], 2);
    assert_eq!(again["wait"]["ended_by"], "pending_batch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arrival_works_on_every_approved_protocol_revision() {
    let server = Server::start().await;
    let revisions = [
        Lifecycle::Initialize(ProtocolVersion::V_2025_06_18),
        Lifecycle::Initialize(ProtocolVersion::V_2025_11_25),
        Lifecycle::Discover(ProtocolVersion::V_2026_07_28),
    ];
    for (index, lifecycle) in revisions.into_iter().enumerate() {
        let receiver = server.client(lifecycle.clone()).await;
        let publisher = server.client(lifecycle.clone()).await;
        assert_eq!(receiver.negotiated(), lifecycle.requested());
        assert_eq!(
            receiver.tool_names(),
            ["publish", "receive", "stream", "subscription"],
            "{lifecycle:?}"
        );
        let stream = format!("revision-{index}");
        receiver.create_stream(&stream).await;
        let handle = receiver.create_subscription(&stream, "reader").await;

        let mut waiting = receiver
            .start("receive", receive_args(&handle, 10, LIMIT, 25_000))
            .await;
        waiting.window_open().await;
        let published = publisher.publish(&stream, "message", json!(stream)).await;
        let received = waiting.finish().await.ok();

        assert_eq!(received["wait"]["ended_by"], "commit", "{lifecycle:?}");
        assert_eq!(
            message_ids(&received),
            vec![published["message_id"].clone()],
            "{lifecycle:?}"
        );
    }

    let unapproved = server
        .client(Lifecycle::Initialize(ProtocolVersion::V_2025_03_26))
        .await;
    assert_eq!(unapproved.negotiated(), ProtocolVersion::V_2025_11_25);
}

/// Copilot CLI 1.0.89 declares the MCP tasks extension on every request but
/// fails any `tools/call` answered with a task, so a receive must wait and
/// answer with its ordinary result whatever the client declares.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_declaring_tasks_still_gets_the_waiting_receive() {
    let server = Server::start().await;
    let revisions = [
        Lifecycle::Discover(ProtocolVersion::V_2026_07_28),
        Lifecycle::Initialize(ProtocolVersion::V_2025_11_25),
        Lifecycle::Initialize(ProtocolVersion::V_2025_06_18),
    ];
    for (index, lifecycle) in revisions.into_iter().enumerate() {
        let declaring_tasks = ClientCapabilities::builder().enable_tasks().build();
        let receiver = server
            .client_declaring(lifecycle.clone(), declaring_tasks)
            .await;
        let publisher = server.client(lifecycle.clone()).await;
        let stream = format!("tasks-declared-{index}");
        receiver.create_stream(&stream).await;
        let handle = receiver.create_subscription(&stream, "reader").await;

        let mut waiting = receiver
            .start("receive", receive_args(&handle, 10, LIMIT, 25_000))
            .await;
        waiting.window_open().await;
        let published = publisher.publish(&stream, "message", json!(stream)).await;
        let received = waiting.finish().await.ok();

        assert_eq!(received["wait"]["ended_by"], "commit", "{lifecycle:?}");
        assert_eq!(
            message_ids(&received),
            vec![published["message_id"].clone()],
            "{lifecycle:?}"
        );
        assert!(
            receiver.capabilities()["extensions"]["io.modelcontextprotocol/tasks"].is_null(),
            "minymaton must not advertise the tasks extension: {:#}",
            receiver.capabilities()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_events_splits_the_backlog_into_batches() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let mut ids = Vec::new();
    for index in 0..3 {
        let published = client
            .publish("orders", &format!("order-{index}"), json!(index))
            .await;
        ids.push(published["message_id"].clone());
    }
    assert!(
        ids[0] != ids[1] && ids[1] != ids[2] && ids[0] != ids[2],
        "message IDs must differ: {ids:?}"
    );

    let first = client
        .call("receive", receive_args(&handle, 2, LIMIT, 0))
        .await
        .ok();
    let second = client
        .call(
            "receive",
            with_ack(receive_args(&handle, 2, LIMIT, 0), &first["batch_id"]),
        )
        .await
        .ok();

    assert_eq!(message_ids(&first), ids[..2].to_vec());
    assert_eq!(payloads(&first), [json!(0), json!(1)]);
    assert_eq!(first["has_more"], true);
    assert_eq!(
        second["ack_result"],
        json!({"batch_id": first["batch_id"], "outcome": "committed"})
    );
    assert_eq!(message_ids(&second), ids[2..].to_vec());
    assert_eq!(second["has_more"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn results_fit_in_16_kib_on_the_wire() {
    let server = Server::start().await;
    let setup = server.client(legacy()).await;
    setup.create_stream("large").await;
    let handle = setup.create_subscription("large", "reader").await;
    // About 6 KB each, carried twice per result: one fits in 16 KiB, two do not.
    let bodies: Vec<String> = (0..3)
        .map(|index| format!("{index}{}", "a".repeat(5_999)))
        .collect();
    for (index, body) in bodies.iter().enumerate() {
        setup
            .publish("large", &format!("large-{index}"), json!(body))
            .await;
    }

    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;
    let mut previous: Option<Value> = None;
    for (index, body) in bodies.iter().enumerate() {
        let arguments = receive_args(&handle, 10, LIMIT, 0);
        let arguments = match &previous {
            Some(batch_id) => with_ack(arguments, batch_id),
            None => arguments,
        };
        let (reply, outcome) = raw.call_tool("receive", arguments).await;
        let received = outcome.ok();

        let [event] = reply.events.as_slice() else {
            panic!("expected one SSE event, got {:?}", reply.events);
        };
        assert!(
            event.wire_bytes <= RESULT_LIMIT,
            "result {index} took {} bytes on the wire",
            event.wire_bytes
        );
        assert_eq!(payloads(&received), [json!(body)]);
        assert_eq!(received["has_more"], index < 2);
        previous = Some(received["ack_after_handling"].clone());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_too_large_for_a_result_is_refused_and_not_stored() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;

    // Carried twice per result, 9,000 bytes cannot fit in 16 KiB.
    let refused = client
        .call(
            "publish",
            json!({"stream": "orders", "idempotency_key": "too-large", "payload": "a".repeat(9_000)}),
        )
        .await
        .err();

    assert_eq!(refused["code"], "message_too_large");
    assert_eq!(refused["details"]["limit_bytes"], LIMIT);
    let needed = refused["details"]["result_bytes"]
        .as_u64()
        .expect("result_bytes");
    assert!(needed > LIMIT, "reported {needed} bytes");
    let received = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(received["status"], "empty");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newer_receive_supersedes_a_waiting_one() {
    let server = Server::start().await;
    let first = server.client(legacy()).await;
    let second = server.client(legacy()).await;
    let publisher = server.client(legacy()).await;
    first.create_stream("orders").await;
    let handle = first.create_subscription("orders", "reader").await;

    let mut older = first
        .start("receive", receive_args(&handle, 10, LIMIT, 25_000))
        .await;
    older.window_open().await;
    let mut newer = second
        .start("receive", receive_args(&handle, 10, LIMIT, 25_000))
        .await;
    let superseded = older.finish().await.ok();

    assert_eq!(keys(&superseded), ["status", "wait"]);
    assert_eq!(superseded["status"], "superseded");
    assert_eq!(superseded["wait"]["ended_by"], "superseded");

    newer.window_open().await;
    let published = publisher
        .publish("orders", "order-1", json!("for the newer receive"))
        .await;
    let received = newer.finish().await.ok();
    assert_eq!(received["wait"]["ended_by"], "commit");
    assert_eq!(received["delivery_attempt"], 1);
    assert_eq!(
        message_ids(&received),
        vec![published["message_id"].clone()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledgements_do_not_steal_a_parked_receives_ownership() {
    for publish_ack in [false, true] {
        let server = Server::start().await;
        let client = server.client(legacy()).await;
        let other = server.client(legacy()).await;
        client.create_stream("orders").await;
        let handle = client.create_subscription("orders", "reader").await;
        client.publish("orders", "first", json!("first")).await;
        let batch = client
            .call("receive", receive_args(&handle, 10, LIMIT, 0))
            .await
            .ok();
        client
            .call("receive", ack_only(&handle, &batch["batch_id"]))
            .await
            .ok();
        let mut waiting = client
            .start("receive", receive_args(&handle, 10, LIMIT, 1))
            .await;
        waiting.window_open().await;
        if publish_ack {
            other
                .call(
                    "publish",
                    json!({
                        "stream": "orders", "idempotency_key": "self-echo",
                        "from": "reader", "payload": "not for reader", "ack": batch["batch_id"],
                    }),
                )
                .await
                .ok();
        } else {
            other
                .call("receive", ack_only(&handle, &batch["batch_id"]))
                .await
                .ok();
        }
        let invalid = other
            .call(
                "receive",
                with_ack(receive_args(&handle, 10, LIMIT, 0), &json!("bat_unknown")),
            )
            .await
            .err();
        assert_eq!(invalid["code"], "invalid_ack");
        let listed = other
            .call("subscription", json!({"action":"list","stream":"orders"}))
            .await
            .ok();
        assert_eq!(listed["subscriptions"][0]["waiting"], true);
        other.publish("orders", "next", json!("next")).await;
        let received = waiting.finish().await.ok();
        assert_eq!(received["status"], "events");
        assert_eq!(received["delivery_attempt"], 1);
        assert_eq!(payloads(&received), [json!("next")]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn superseded_receive_keeps_its_already_committed_acknowledgement() {
    let server = Server::start().await;
    let first = server.client(legacy()).await;
    let second = server.client(legacy()).await;
    first.create_stream("orders").await;
    let handle = first.create_subscription("orders", "reader").await;
    first.publish("orders", "first", json!("handled")).await;
    let batch = first
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    let mut waiting = first
        .start(
            "receive",
            with_ack(receive_args(&handle, 10, LIMIT, 1), &batch["batch_id"]),
        )
        .await;
    waiting.window_open().await;
    second
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    let superseded = waiting.finish().await.ok();
    assert_eq!(superseded["status"], "superseded");
    assert_eq!(
        superseded["ack_result"],
        json!({
            "batch_id": batch["batch_id"], "outcome": "committed",
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_retry_is_idempotent_and_conflicting_reuse_is_refused() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let payload = json!({"order": 17, "note": "ship \"today\"\n\\ fragile"});

    let first = client.publish("orders", "order-17", payload.clone()).await;
    let retry = client.publish("orders", "order-17", payload.clone()).await;
    let conflict = client
        .call(
            "publish",
            json!({"stream": "orders", "idempotency_key": "order-17", "payload": {"order": 18}}),
        )
        .await
        .err();
    let no_key = client
        .call(
            "publish",
            json!({"stream": "orders", "idempotency_key": "", "payload": 1}),
        )
        .await
        .err();

    assert_eq!(retry, first);
    assert_eq!(conflict["code"], "idempotency_conflict");
    assert_eq!(no_key["code"], "invalid_arguments");
    let received = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(message_ids(&received), vec![first["message_id"].clone()]);
    assert_eq!(payloads(&received), vec![payload]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledgements_commit_once_and_unknown_ones_are_refused() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    client.publish("orders", "order-1", json!("one")).await;
    let delivered = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    let batch_id = delivered["batch_id"].clone();

    let unknown_ack = client
        .call(
            "receive",
            with_ack(receive_args(&handle, 10, LIMIT, 0), &json!("no-such-batch")),
        )
        .await
        .err();
    let unknown_handle = client
        .call("receive", receive_args("no-such-handle", 10, LIMIT, 0))
        .await
        .err();
    let redelivered = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    let committed = client
        .call(
            "receive",
            with_ack(receive_args(&handle, 10, LIMIT, 0), &batch_id),
        )
        .await
        .ok();
    let repeated = client
        .call("receive", ack_only(&handle, &batch_id))
        .await
        .ok();

    assert_eq!(unknown_ack["code"], "invalid_ack");
    assert_eq!(unknown_handle["code"], "subscription_not_found");
    // The refused ACK changed nothing: the same batch is still pending.
    assert_eq!(redelivered["batch_id"], batch_id);
    assert_eq!(redelivered["delivery_attempt"], 2);
    assert_eq!(committed["status"], "empty");
    assert_eq!(
        committed["ack_result"],
        json!({"batch_id": batch_id, "outcome": "committed"})
    );
    assert_eq!(repeated["status"], "acked");
    assert_eq!(
        repeated["ack_result"],
        json!({"batch_id": batch_id, "outcome": "already_acked"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_echo_suppression_and_direct_addressing_filter_delivery_and_advance_skipped_cursors() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;

    // Publish historical backlog before unassigned agents join.
    client
        .publish(
            "agents",
            "history-1",
            json!({"from": "agent-0", "type": "hello"}),
        )
        .await;

    // Unassigned subscription.create defaults to start_at = "latest" (skips history-1).
    let a1 = client
        .call("subscription", json!({"action": "create"}))
        .await
        .ok();
    let a2 = client
        .call("subscription", json!({"action": "create"}))
        .await
        .ok();
    let a3 = client
        .call(
            "subscription",
            json!({"action": "create", "start_at": "earliest"}),
        )
        .await
        .ok();
    assert_eq!(a1["label"], "agent-1");
    assert_eq!(a2["label"], "agent-2");
    assert_eq!(a3["label"], "agent-3");
    let h1 = a1["handle"].as_str().unwrap().to_owned();
    let h2 = a2["handle"].as_str().unwrap().to_owned();
    let h3 = a3["handle"].as_str().unwrap().to_owned();

    // agent-1 sees empty backlog (started at latest), while agent-3 requested earliest.
    let r1_initial = client
        .call("receive", receive_args(&h1, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(r1_initial["status"], "empty");
    let r3_initial = client
        .call("receive", receive_args(&h3, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(r3_initial["status"], "events");
    assert_eq!(r3_initial["events"].as_array().unwrap().len(), 1);
    let r3_ack = r3_initial["ack_after_handling"].clone();
    client.call("receive", ack_only(&h3, &r3_ack)).await.ok();

    // agent-1 publishes a broadcast with from="agent-1": agent-1 does NOT receive its own message,
    // while agent-2 and agent-3 both receive it.
    client
        .call(
            "publish",
            json!({
                "stream": "agents",
                "idempotency_key": "bcast-1",
                "from": "agent-1",
                "payload": {"type": "roll-call", "text": "who is here"}
            }),
        )
        .await
        .ok();

    let r1_self = client
        .call("receive", receive_args(&h1, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(r1_self["status"], "empty");

    let r2_bcast = client
        .call("receive", receive_args(&h2, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(r2_bcast["status"], "events");
    assert_eq!(
        r2_bcast["events"][0]["payload"]["text"],
        json!("who is here")
    );
    let r2_ack = r2_bcast["ack_after_handling"].clone();
    client.call("receive", ack_only(&h2, &r2_ack)).await.ok();

    let r3_bcast = client
        .call("receive", receive_args(&h3, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(r3_bcast["status"], "events");
    let r3_ack2 = r3_bcast["ack_after_handling"].clone();
    client.call("receive", ack_only(&h3, &r3_ack2)).await.ok();

    // agent-2 sends a direct message to agent-1 (inferred from payload.from and payload.to).
    // A parked receive on agent-1 wakes immediately; agent-3 does not see it.
    let mut w1 = client
        .start("receive", receive_args(&h1, 10, LIMIT, 25_000))
        .await;
    w1.window_open().await;

    client
        .publish(
            "agents",
            "dm-2-to-1",
            json!({"from": "agent-2", "to": "agent-1", "type": "reply", "text": "only for 1"}),
        )
        .await;

    let r1_dm = w1.finish().await.ok();
    assert_eq!(r1_dm["status"], "events");
    assert_eq!(r1_dm["wait"]["ended_by"], "commit");
    assert_eq!(r1_dm["events"][0]["payload"]["text"], json!("only for 1"));

    let r3_dm = client
        .call("receive", receive_args(&h3, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(r3_dm["status"], "empty");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_post_replies_carry_no_resume_ids_priming_or_keepalive() {
    let server = Server::start().await;
    let setup = server.client(legacy()).await;
    setup.create_stream("orders").await;
    let handle = setup.create_subscription("orders", "reader").await;
    let mut raw = server.raw_session(&ProtocolVersion::V_2025_11_25).await;

    let (reply, outcome) = raw
        .call_tool("receive", receive_args(&handle, 10, LIMIT, 0))
        .await;

    assert!(
        reply.comments.is_empty(),
        "keepalive comments on the wire: {:?}",
        reply.comments
    );
    let [event] = reply.events.as_slice() else {
        panic!("expected exactly one SSE event, got {:?}", reply.events);
    };
    assert_eq!(
        event.field_names(),
        ["data"],
        "priming, resume ID or retry fields on the wire"
    );
    assert!(event.wire_bytes <= RESULT_LIMIT);
    let received = outcome.ok();
    assert_eq!(received["status"], "empty");
    assert_eq!(received["wait"]["ended_by"], "no_wait");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_session_delivers_after_other_subscription_activity() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let published = client
        .publish("orders", "order-1", json!("after other activity"))
        .await;

    let other = server.client(legacy()).await;
    let idle = other.create_subscription("agents", "idle").await;
    let empty = other
        .call("receive", receive_args(&idle, 1, LIMIT, 0))
        .await
        .ok();
    assert_eq!(empty["wait"]["ended_by"], "no_wait");

    let received = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    assert_eq!(
        message_ids(&received),
        vec![published["message_id"].clone()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriptions_start_at_the_retained_floor_and_are_recovered_by_label() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let duplicate = client
        .call("stream", json!({"action": "create", "name": "orders"}))
        .await
        .err();
    let unnamed = client
        .call("stream", json!({"action": "create", "name": ""}))
        .await
        .err();
    let nowhere_publish = client
        .call(
            "publish",
            json!({"stream": "nowhere", "idempotency_key": "k", "payload": 1}),
        )
        .await
        .err();
    let nowhere_subscription = client
        .call(
            "subscription",
            json!({"action": "create", "stream": "nowhere", "label": "reader"}),
        )
        .await
        .err();
    let published = client
        .publish(
            "orders",
            "early",
            json!("published before any subscription"),
        )
        .await;

    let handle = client.create_subscription("orders", "late-reader").await;
    let recovered = client
        .call(
            "subscription",
            json!({"action": "recover", "stream": "orders", "label": "late-reader"}),
        )
        .await
        .ok();
    let taken = client
        .call(
            "subscription",
            json!({"action": "create", "stream": "orders", "label": "late-reader"}),
        )
        .await
        .err();
    let missing = client
        .call(
            "subscription",
            json!({"action": "recover", "stream": "orders", "label": "nobody"}),
        )
        .await
        .err();
    let received = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();

    assert_eq!(duplicate["code"], "stream_exists");
    assert_eq!(unnamed["code"], "invalid_arguments");
    assert_eq!(nowhere_publish["code"], "stream_not_found");
    assert_eq!(nowhere_subscription["code"], "stream_not_found");
    assert_eq!(
        recovered,
        json!({"status": "recovered", "handle": handle, "stream": "orders", "label": "late-reader"})
    );
    assert_eq!(taken["code"], "subscription_exists");
    assert_eq!(missing["code"], "subscription_not_found");
    assert_eq!(received["wait"]["ended_by"], "backlog");
    assert_eq!(
        message_ids(&received),
        vec![published["message_id"].clone()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receive_bounds_are_enforced_without_losing_the_message() {
    let server = Server::start().await;
    let client = server.client(legacy()).await;
    client.create_stream("orders").await;
    let handle = client.create_subscription("orders", "reader").await;
    let published = client
        .publish("orders", "order-1", json!("b".repeat(1_000)))
        .await;

    let too_small = client
        .call("receive", receive_args(&handle, 10, 1_000, 0))
        .await
        .err();
    let no_events = client
        .call("receive", receive_args(&handle, 0, LIMIT, 0))
        .await
        .err();
    let no_bytes = client
        .call("receive", receive_args(&handle, 10, 0, 0))
        .await
        .err();
    let ack_with_wait = client
        .call(
            "receive",
            json!({"handle": handle, "max_events": 0, "ack": "bat_none", "wait": false}),
        )
        .await
        .err();
    let received = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();
    let shrunk = client
        .call("receive", receive_args(&handle, 10, 1_000, 0))
        .await
        .err();
    let redelivered = client
        .call("receive", receive_args(&handle, 10, LIMIT, 0))
        .await
        .ok();

    assert_eq!(too_small["code"], "batch_exceeds_bounds");
    assert_eq!(too_small["details"]["max_bytes"], 1_000);
    let required = too_small["details"]["required_bytes"]
        .as_u64()
        .expect("required_bytes");
    assert!(required > 1_000, "reported {required} bytes");
    for refused in [&no_events, &no_bytes, &ack_with_wait] {
        assert_eq!(refused["code"], "invalid_arguments", "{refused:#}");
    }
    assert_eq!(
        message_ids(&received),
        vec![published["message_id"].clone()]
    );
    assert_eq!(received["delivery_attempt"], 1);
    // The pending batch does not fit the smaller bound either; that refusal is not an attempt.
    assert_eq!(shrunk["code"], "batch_exceeds_bounds");
    assert_eq!(redelivered["batch_id"], received["batch_id"]);
    assert_eq!(redelivered["delivery_attempt"], 2);
}

#[test]
fn listening_off_the_loopback_interface_is_refused() {
    let dir = tempfile::Builder::new()
        .prefix("live-arrival-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("temporary directory");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .arg("--listen")
        .arg("0.0.0.0:0")
        .arg("--db")
        .arg(dir.path().join("queue.sqlite3"))
        .output()
        .expect("run minymaton-server");

    assert!(!output.status.success(), "the server accepted 0.0.0.0");
    assert!(
        output.stdout.is_empty(),
        "the server printed {:?}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("loopback"),
        "unexpected refusal message: {stderr}"
    );
    assert!(
        !dir.path().join("queue.sqlite3").exists(),
        "the refused server created its database"
    );
}
