use std::process::Command;

use crate::common::{Lifecycle, Server, receive_args};
use minymaton_core::{
    OutputFormat, StreamResult, SubscriptionResult, render_stream_to_string,
    render_subscription_to_string,
};
use rmcp::model::ProtocolVersion;
use serde_json::json;

const ALICE_TOKEN: &str = "mny_alice_0123456789abcdef0123456789abcdef";
const BOB_TOKEN: &str = "mny_bob_fedcba9876543210fedcba987654321000";
const TRIAL_TOKEN: &str = "mny_trial_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn modern() -> Lifecycle {
    Lifecycle::Discover(ProtocolVersion::V_2026_07_28)
}

fn mixed_tokens() -> String {
    format!("alice\t{ALICE_TOKEN}\nbob\t{BOB_TOKEN}\ntrial-user\t{TRIAL_TOKEN}\ttrial-a\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_sweep_idle_and_delete_bulk_preserve_waiters_follow_and_pinned() {
    let server = Server::start_with_description("inbox", "desc-sub").await;
    let client = server.client(modern()).await;
    let waiter_client = server.client(modern()).await;

    client.create_stream("ops").await;
    client.create_subscription("ops", "dead-1").await;
    client.create_subscription("ops", "dead-2").await;
    client.create_subscription("ops", "kept-sub").await;
    let parked_handle = client.create_subscription("ops", "parked-waiter").await;

    // Bind a `follow` subscription on `ops` and on `inbox`.
    let agent_client = server.agent_client("follower", modern()).await;
    agent_client
        .call("follow", json!({"stream": "ops"}))
        .await
        .ok();

    // Park a live receive(wait: true) on `parked-waiter` and wait until its guard is active.
    let mut parked = waiter_client
        .start("receive", receive_args(&parked_handle, 10, 16384, 1))
        .await;
    parked.window_open().await;

    // Sweep idle subscriptions on `ops` with `keep: ["kept-sub"]`.
    let swept = client
        .call(
            "subscription",
            json!({"action": "sweep_idle", "stream": "ops", "keep": ["kept-sub"]}),
        )
        .await
        .ok();
    assert_eq!(
        swept,
        json!({
            "status": "swept",
            "stream": "ops",
            "deleted": ["dead-1", "dead-2"],
            "deleted_count": 2,
        })
    );

    let ops_listed = client
        .call("subscription", json!({"action": "list", "stream": "ops"}))
        .await
        .ok();
    let labels: Vec<&str> = ops_listed["subscriptions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["label"].as_str().unwrap())
        .collect();
    assert_eq!(labels, vec!["follower", "kept-sub", "parked-waiter"]);

    // Verify `--description-subscription` (`desc-sub`) on `inbox` is protected from `sweep_idle`
    // and explicit `delete` / `delete_bulk` (with atomic rollback if included alongside another label).
    client.create_subscription("inbox", "inbox-idle").await;
    client.create_subscription("inbox", "inbox-extra").await;
    let failed_bulk = client
        .call(
            "subscription",
            json!({
                "action": "delete_bulk",
                "stream": "inbox",
                "labels": ["inbox-extra", "desc-sub"]
            }),
        )
        .await
        .err();
    assert_eq!(failed_bulk["code"], "invalid_arguments");

    let swept_inbox = client
        .call(
            "subscription",
            json!({"action": "sweep_idle", "stream": "inbox"}),
        )
        .await
        .ok();
    assert_eq!(
        swept_inbox,
        json!({
            "status": "swept",
            "stream": "inbox",
            "deleted": ["inbox-extra", "inbox-idle"],
            "deleted_count": 2,
        })
    );
    let inbox_listed = client
        .call("subscription", json!({"action": "list", "stream": "inbox"}))
        .await
        .ok();
    let inbox_labels: Vec<&str> = inbox_listed["subscriptions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["label"].as_str().unwrap())
        .collect();
    assert_eq!(inbox_labels, vec!["desc-sub"]);

    // Explicit `delete_bulk` skips missing labels, removes `kept-sub` & `parked-waiter`,
    // and wakes the live parked HTTP waiter as `superseded`.
    let bulk_deleted = client
        .call(
            "subscription",
            json!({
                "action": "delete_bulk",
                "stream": "ops",
                "labels": ["parked-waiter", "missing-sub", "kept-sub", "parked-waiter"]
            }),
        )
        .await
        .ok();
    assert_eq!(
        bulk_deleted,
        json!({
            "status": "deleted_bulk",
            "stream": "ops",
            "deleted": ["parked-waiter", "kept-sub"],
            "deleted_count": 2,
        })
    );
    let woken = parked.finish().await.ok();
    assert_eq!(woken["status"], "superseded");
    assert_eq!(woken["wait"]["ended_by"], "superseded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_sweep_delete_and_delete_bulk_enforce_policy_and_atomicity() {
    let server = Server::start_with_bearer_tokens_and_options(
        &mixed_tokens(),
        &[
            "--description-stream",
            "inbox",
            "--description-subscription",
            "desc-sub",
        ],
    )
    .await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    let alice_waiter = server.client_with_token(ALICE_TOKEN, modern()).await;
    let bob = server.client_with_token(BOB_TOKEN, modern()).await;
    let trial = server.client_with_token(TRIAL_TOKEN, modern()).await;

    // Create streams across access categories and activity states.
    alice.create_stream("empty-open").await;
    alice.create_stream("idle-open").await;
    alice.create_subscription("idle-open", "stale-sub").await;
    alice.create_stream("keep-me").await;
    alice.create_stream("active-wait-stream").await;
    let wait_handle = alice
        .create_subscription("active-wait-stream", "watcher")
        .await;
    alice
        .call(
            "stream",
            json!({"action": "create", "name": "alice-private", "members": ["alice"]}),
        )
        .await
        .ok();
    bob.call(
        "stream",
        json!({"action": "create", "name": "bob-only", "members": ["bob"]}),
    )
    .await
    .ok();

    let agent_client = server
        .agent_client_with_token("follow-agent", ALICE_TOKEN, modern())
        .await;
    agent_client
        .call("follow", json!({"stream": "follow-stream"}))
        .await
        .ok();

    let mut parked = alice_waiter
        .start("receive", receive_args(&wait_handle, 10, 16384, 1))
        .await;
    parked.window_open().await;

    // Permission checks on explicit delete & delete_bulk.
    assert_eq!(
        alice
            .call("stream", json!({"action": "delete", "name": "agents"}))
            .await
            .err()["code"],
        "invalid_arguments"
    );
    assert_eq!(
        alice
            .call("stream", json!({"action": "delete", "name": "inbox"}))
            .await
            .err()["code"],
        "invalid_arguments"
    );
    assert_eq!(
        alice
            .call("stream", json!({"action": "delete", "name": "trial-a"}))
            .await
            .err()["code"],
        "invalid_arguments"
    );
    assert_eq!(
        trial
            .call("stream", json!({"action": "delete", "name": "empty-open"}))
            .await
            .err()["code"],
        "stream_forbidden"
    );
    assert_eq!(
        bob.call(
            "stream",
            json!({"action": "delete", "name": "alice-private"})
        )
        .await
        .err()["code"],
        "stream_forbidden"
    );

    // Atomic rollback: `delete_bulk` with an invalid target leaves `keep-me` intact.
    let rollback = alice
        .call(
            "stream",
            json!({"action": "delete_bulk", "names": ["keep-me", "agents"]}),
        )
        .await
        .err();
    assert_eq!(rollback["code"], "invalid_arguments");

    // `sweep_idle` by `alice` deletes accessible idle streams in sorted order while keeping
    // `agents`, `inbox`, `trial-a`, `bob-only`, `keep-me`, `active-wait-stream`, `follow-stream`.
    let swept = alice
        .call(
            "stream",
            json!({"action": "sweep_idle", "keep": ["keep-me"]}),
        )
        .await
        .ok();
    assert_eq!(
        swept,
        json!({
            "status": "swept",
            "deleted": ["alice-private", "empty-open", "idle-open"],
            "deleted_count": 3,
        })
    );

    let alice_streams = alice.call("stream", json!({"action": "list"})).await.ok();
    assert_eq!(
        alice_streams["streams"],
        json!([
            "active-wait-stream",
            "agents",
            "follow-stream",
            "inbox",
            "keep-me"
        ])
    );
    let bob_streams = bob.call("stream", json!({"action": "list"})).await.ok();
    assert!(
        bob_streams["streams"]
            .as_array()
            .unwrap()
            .contains(&json!("bob-only"))
    );

    // `delete` removes `keep-me` explicitly, and `delete_bulk` removes `active-wait-stream`,
    // waking its parked waiter as `superseded`.
    let deleted = alice
        .call("stream", json!({"action": "delete", "name": "keep-me"}))
        .await
        .ok();
    assert_eq!(deleted, json!({"status": "deleted", "stream": "keep-me"}));

    let deleted_bulk = alice
        .call(
            "stream",
            json!({
                "action": "delete_bulk",
                "names": ["active-wait-stream", "absent-stream", "active-wait-stream"]
            }),
        )
        .await
        .ok();
    assert_eq!(
        deleted_bulk,
        json!({
            "status": "deleted_bulk",
            "deleted": ["active-wait-stream"],
            "deleted_count": 1,
        })
    );
    let woken = parked.finish().await.ok();
    assert_eq!(woken["status"], "superseded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_subcommands_match_mcp_results_byte_for_byte_across_url_and_db() {
    let mut server = Server::start().await;
    let client = server.client(modern()).await;
    client.create_stream("alpha").await;
    client.create_stream("beta").await;
    client.create_subscription("alpha", "idle-a").await;
    client.create_subscription("alpha", "idle-b").await;
    client.create_subscription("alpha", "keep-a").await;

    let mcp_streams = client.call("stream", json!({"action": "list"})).await.ok();
    let mcp_streams_parsed: StreamResult = serde_json::from_value(mcp_streams.clone()).unwrap();
    let expected_json = render_stream_to_string(&mcp_streams_parsed, OutputFormat::Json);
    let expected_csv = render_stream_to_string(&mcp_streams_parsed, OutputFormat::Csv);
    let expected_text = render_stream_to_string(&mcp_streams_parsed, OutputFormat::Text);

    // 1. Live `--url` matches MCP output byte-for-byte across json/csv/text.
    for (fmt, expected) in [
        ("json", &expected_json),
        ("csv", &expected_csv),
        ("text", &expected_text),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
            .args(["stream", "list", "--url", server.url(), "--format", fmt])
            .output()
            .expect("run stream list --url");
        assert!(
            out.status.success(),
            "{fmt}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8(out.stdout)
                .unwrap()
                .trim_end_matches('\n'),
            expected
        );
    }

    // 2. `--db` fails cleanly while the live server holds the database lock.
    let db_path = server.db_path();
    let locked = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .args(["stream", "list", "--db", db_path.to_str().unwrap()])
        .output()
        .expect("run stream list --db while locked");
    assert!(!locked.status.success());

    // 3. After stopping the server, `--db` runs offline and renders identically via the same DRY path.
    let sub_swept_out = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .args([
            "subscription",
            "sweep-idle",
            "--stream",
            "alpha",
            "--keep",
            "keep-a",
            "--url",
            server.url(),
            "--format",
            "json",
        ])
        .output()
        .expect("run subscription sweep-idle --url");
    assert!(
        sub_swept_out.status.success(),
        "{}",
        String::from_utf8_lossy(&sub_swept_out.stderr)
    );
    let sub_swept_stdout = String::from_utf8(sub_swept_out.stdout).unwrap();
    let sub_swept_parsed: SubscriptionResult =
        serde_json::from_str(sub_swept_stdout.trim()).unwrap();
    assert_eq!(
        sub_swept_parsed,
        SubscriptionResult::Swept {
            stream: "alpha".to_owned(),
            deleted: vec!["idle-a".to_owned(), "idle-b".to_owned()],
            deleted_count: 2,
        }
    );
    assert_eq!(
        sub_swept_stdout.trim_end_matches('\n'),
        render_subscription_to_string(&sub_swept_parsed, OutputFormat::Json)
    );

    drop(client);
    server.stop();

    let db_listed = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .args([
            "stream",
            "list",
            "--db",
            db_path.to_str().unwrap(),
            "--format",
            "json",
        ])
        .output()
        .expect("run stream list --db offline");
    assert!(
        db_listed.status.success(),
        "{}",
        String::from_utf8_lossy(&db_listed.stderr)
    );
    assert_eq!(
        String::from_utf8(db_listed.stdout)
            .unwrap()
            .trim_end_matches('\n'),
        expected_json
    );

    let db_swept = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .args([
            "stream",
            "sweep-idle",
            "--keep",
            "alpha",
            "--db",
            db_path.to_str().unwrap(),
            "--format",
            "csv",
        ])
        .output()
        .expect("run stream sweep-idle --db offline");
    assert!(
        db_swept.status.success(),
        "{}",
        String::from_utf8_lossy(&db_swept.stderr)
    );
    let expected_sweep_csv = render_stream_to_string(
        &StreamResult::Swept {
            deleted: vec!["beta".to_owned()],
            deleted_count: 1,
        },
        OutputFormat::Csv,
    );
    assert_eq!(
        String::from_utf8(db_swept.stdout)
            .unwrap()
            .trim_end_matches('\n'),
        expected_sweep_csv
    );

    // Check `--db` subscription list and bulk delete.
    let db_sub_delete = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .args([
            "subscription",
            "delete-bulk",
            "--stream",
            "alpha",
            "--label",
            "keep-a",
            "--db",
            db_path.to_str().unwrap(),
            "--format",
            "text",
        ])
        .output()
        .expect("run subscription delete-bulk --db offline");
    assert!(
        db_sub_delete.status.success(),
        "{}",
        String::from_utf8_lossy(&db_sub_delete.stderr)
    );
    assert_eq!(
        String::from_utf8(db_sub_delete.stdout)
            .unwrap()
            .trim_end_matches('\n'),
        "deleted 1 subscriptions on alpha: keep-a"
    );

    // Empty subscription list in CSV prints nothing (no extra blank line).
    let empty_csv = Command::new(env!("CARGO_BIN_EXE_minymaton-server"))
        .args([
            "subscription",
            "list",
            "--stream",
            "alpha",
            "--db",
            db_path.to_str().unwrap(),
            "--format",
            "csv",
        ])
        .output()
        .expect("run empty subscription list --db");
    assert!(empty_csv.status.success());
    assert!(
        empty_csv.stdout.is_empty(),
        "expected empty stdout, got {:?}",
        String::from_utf8_lossy(&empty_csv.stdout)
    );
}
