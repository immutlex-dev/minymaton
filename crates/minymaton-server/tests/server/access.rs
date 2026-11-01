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

const TRIAL_A_TOKEN: &str = "mny_trial_a_0123456789abcdef0123456789abcdef";
const TRIAL_B_TOKEN: &str = "mny_trial_b_0123456789abcdef0123456789abcdef";
const ORG_1_TOKEN: &str = "mny_org_1_0123456789abcdef0123456789abcdef00";
const ORG_2_TOKEN: &str = "mny_org_2_0123456789abcdef0123456789abcdef00";

fn mixed_tokens() -> String {
    format!(
        "alice\t{ALICE_TOKEN}\nbob\t{BOB_TOKEN}\ntrial-a\t{TRIAL_A_TOKEN}\ttrial-a\ntrial-b\t{TRIAL_B_TOKEN}\ttrial-b\norg-1\t{ORG_1_TOKEN}\torg-shared\norg-2\t{ORG_2_TOKEN}\torg-shared\n"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bound_channel_isolates_trial_callers_across_all_tools_and_resources() {
    let server = Server::start_with_bearer_tokens(&mixed_tokens()).await;

    // Verify initialize.instructions names the caller's bound channel (or "agents" when unbound).
    let init_lifecycle = Lifecycle::Initialize(ProtocolVersion::V_2025_11_25);
    let alice_init = server
        .client_with_token(ALICE_TOKEN, init_lifecycle.clone())
        .await;
    let trial_a_init = server
        .client_with_token(TRIAL_A_TOKEN, init_lifecycle)
        .await;
    let alice_instructions = alice_init.instructions().expect("alice instructions");
    let trial_a_instructions = trial_a_init.instructions().expect("trial-a instructions");
    assert!(
        alice_instructions.contains(
            "For agent coordination use the existing agents stream unless the user names another"
        ),
        "{alice_instructions}"
    );
    assert!(
        trial_a_instructions.contains(
            "For agent coordination use the existing trial-a stream unless the user names another"
        ),
        "{trial_a_instructions}"
    );

    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    let trial_a = server.client_with_token(TRIAL_A_TOKEN, modern()).await;
    let trial_b = server.client_with_token(TRIAL_B_TOKEN, modern()).await;
    let org_1 = server.client_with_token(ORG_1_TOKEN, modern()).await;
    let org_2 = server.client_with_token(ORG_2_TOKEN, modern()).await;

    // Stream listing is strictly scoped: trial callers see only their bound channel;
    // unbound team callers never see bound channels in stream(action="list").
    assert_eq!(listed_streams(&trial_a).await, names(&["trial-a"]));
    assert_eq!(listed_streams(&trial_b).await, names(&["trial-b"]));
    assert_eq!(listed_streams(&org_1).await, names(&["org-shared"]));
    assert_eq!(listed_streams(&org_2).await, names(&["org-shared"]));
    assert_eq!(listed_streams(&alice).await, names(&["agents"]));

    // Omitting `stream` resolves to `bound_channel` for bound callers and `"agents"` for unbound.
    let sub_a = trial_a
        .call("subscription", json!({"action": "create", "label": "desk"}))
        .await
        .ok();
    assert_eq!(sub_a["stream"], "trial-a");
    let handle_a = sub_a["handle"].as_str().expect("handle_a").to_owned();

    let recovered_a = trial_a
        .call("subscription", json!({"action": "recover", "label": "desk"}))
        .await
        .ok();
    assert_eq!(recovered_a["handle"], handle_a);

    let listed_subs_a = trial_a
        .call("subscription", json!({"action": "list"}))
        .await
        .ok();
    assert_eq!(listed_subs_a["stream"], "trial-a");
    assert_eq!(
        listed_subs_a["subscriptions"].as_array().expect("subs").len(),
        1
    );

    let pub_a = trial_a
        .call(
            "publish",
            json!({"stream": "trial-a", "idempotency_key": "a-1", "payload": {"msg": "private to trial-a"}}),
        )
        .await
        .ok();
    assert_eq!(pub_a["stream"], "trial-a");

    let alice_sub = alice
        .call(
            "subscription",
            json!({"action": "create", "label": "alice-desk"}),
        )
        .await
        .ok();
    assert_eq!(alice_sub["stream"], "agents");
    let alice_handle = alice_sub["handle"].as_str().expect("alice_handle").to_owned();
    alice
        .call(
            "publish",
            json!({"stream": "agents", "idempotency_key": "team-1", "payload": {"msg": "team only"}}),
        )
        .await
        .ok();

    // Cross-channel and cross-tier calls are refused with stream_forbidden across every tool & resource.
    for (client, is_bound, blocked_stream, blocked_handle) in [
        (&trial_a, true, "agents", &alice_handle),
        (&trial_a, true, "trial-b", &alice_handle),
        (&trial_b, true, "trial-a", &handle_a),
        (&alice, false, "trial-a", &handle_a),
    ] {
        let create_err = client
            .call("stream", json!({"action": "create", "name": blocked_stream}))
            .await
            .err();
        assert_eq!(
            create_err["code"],
            if is_bound {
                "stream_forbidden"
            } else {
                "stream_exists"
            },
            "{create_err:#}"
        );
        forbidden(
            client,
            "stream",
            json!({"action": "grant", "name": blocked_stream, "caller": "bob"}),
        )
        .await;
        forbidden(
            client,
            "stream",
            json!({"action": "revoke", "name": blocked_stream, "caller": "alice"}),
        )
        .await;
        forbidden(
            client,
            "subscription",
            json!({"action": "create", "stream": blocked_stream, "label": "intruder"}),
        )
        .await;
        forbidden(
            client,
            "subscription",
            json!({"action": "list", "stream": blocked_stream}),
        )
        .await;
        forbidden(
            client,
            "subscription",
            json!({"action": "recover", "stream": blocked_stream, "label": "desk"}),
        )
        .await;
        forbidden(
            client,
            "subscription",
            json!({"action": "delete", "stream": blocked_stream, "label": "desk"}),
        )
        .await;
        forbidden(
            client,
            "publish",
            json!({"stream": blocked_stream, "idempotency_key": "x", "payload": "nope"}),
        )
        .await;
        forbidden(client, "receive", receive(blocked_handle)).await;

        let read_err = format!(
            "{:?}",
            client
                .read_inbox(&inbox_uri(blocked_handle))
                .await
                .expect_err("read_inbox must be refused")
        );
        assert!(read_err.contains("stream_forbidden"), "{read_err}");

        let mut sub = client.listen(&[&inbox_uri(blocked_handle)]).await;
        let next = sub.next().await;
        let listen_err = format!("{next:?} {:?}", sub.end());
        assert!(listen_err.contains("stream_forbidden"), "{listen_err}");
    }

    // Bound callers also cannot create new arbitrary streams or grant/revoke on their own channel.
    forbidden(
        &trial_a,
        "stream",
        json!({"action": "create", "name": "another-stream"}),
    )
    .await;
    forbidden(
        &trial_a,
        "stream",
        json!({"action": "grant", "name": "trial-a", "caller": "alice"}),
    )
    .await;

    // Two tokens bound to the same channel (`org-shared`) share that channel and keep distinct publishers.
    let org_handle = org_1.create_subscription("org-shared", "org-desk").await;
    org_2
        .call(
            "publish",
            json!({"stream": "org-shared", "idempotency_key": "org-k1", "payload": "from org-2"}),
        )
        .await
        .ok();
    let org_batch = org_1.call("receive", receive(&org_handle)).await.ok();
    assert_eq!(payloads(&org_batch), vec![json!("from org-2")]);
    assert_eq!(org_batch["events"][0]["publisher"], "org-2");

    // Omitting `stream` on delete works for bound callers.
    let deleted_a = trial_a
        .call("subscription", json!({"action": "delete", "label": "desk"}))
        .await
        .ok();
    assert_eq!(
        deleted_a,
        json!({"status": "deleted", "stream": "trial-a", "label": "desk"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisioning_refuses_to_collide_with_an_existing_open_or_member_stream() {
    let mut server = Server::start_with_bearer_tokens(&tokens()).await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    alice.create_stream("open-clash").await;
    alice
        .call(
            "stream",
            json!({"action": "create", "name": "private-clash", "members": ["bob"]}),
        )
        .await
        .ok();

    let locked = server.run_admin(&[
        "--activate-enrollment",
        server.token_file().to_str().expect("utf8 token file"),
    ]);
    assert!(
        !locked.status.success(),
        "SQLite --activate-enrollment must refuse to run while minymaton-server holds the DB lock"
    );
    let locked_err = String::from_utf8_lossy(&locked.stderr);
    assert!(
        locked_err.contains("is locked by a running server"),
        "{locked_err}"
    );
    server.stop();

    let current = server.run_admin(&[
        "--activate-enrollment",
        server.token_file().to_str().expect("utf8 token file"),
    ]);
    assert!(
        current.status.success(),
        "{}",
        String::from_utf8_lossy(&current.stderr)
    );
    let current_digest = String::from_utf8_lossy(&current.stdout).trim().to_owned();

    for clash in ["open-clash", "private-clash"] {
        let bad_tokens = server.token_file().with_file_name(format!("{clash}.conf"));
        std::fs::write(
            &bad_tokens,
            format!("alice\t{ALICE_TOKEN}\ntrial\t{TRIAL_A_TOKEN}\t{clash}\n"),
        )
        .expect("write bad tokens");
        let output = server.run_admin(&[
            "--activate-enrollment",
            bad_tokens.to_str().expect("utf8 path"),
            "--expected-enrollment-digest",
            &current_digest,
        ]);
        assert!(
            !output.status.success(),
            "provisioning {clash} must fail closed"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(clash), "{stderr}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_channel_budgets_and_retire_channel_reclaim_bound_channel_resources() {
    let mut server = Server::start_with_bearer_tokens_and_options(
        &mixed_tokens(),
        &[
            "--max-retained-events-per-channel",
            "2",
            "--max-subscriptions-per-channel",
            "2",
        ],
    )
    .await;
    let trial_a = server.client_with_token(TRIAL_A_TOKEN, modern()).await;
    let trial_b = server.client_with_token(TRIAL_B_TOKEN, modern()).await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;

    let sub_1 = trial_a.create_subscription("trial-a", "s1").await;
    let _sub_2 = trial_a.create_subscription("trial-a", "s2").await;
    let third_sub = trial_a
        .call(
            "subscription",
            json!({"action": "create", "stream": "trial-a", "label": "s3"}),
        )
        .await
        .err();
    assert_eq!(third_sub["code"], "storage_full", "{third_sub:#}");

    // Another channel and unbound streams are unaffected by trial-a's subscription cap.
    let _sub_b = trial_b.create_subscription("trial-b", "s1").await;
    let _sub_team = alice.create_subscription("agents", "s1").await;

    trial_a.publish("trial-a", "e1", json!("one")).await;
    trial_a.publish("trial-a", "e2", json!("two")).await;
    let third_pub = trial_a
        .call(
            "publish",
            json!({"stream": "trial-a", "idempotency_key": "e3", "payload": "three"}),
        )
        .await
        .err();
    assert_eq!(third_pub["code"], "storage_full", "{third_pub:#}");

    // Other channels and team streams can still publish when trial-a is full.
    trial_b.publish("trial-b", "b1", json!("still open")).await;
    alice.publish("agents", "a1", json!("still open")).await;

    // On SQLite, standalone `--retire-channel` refuses to run while the server holds the DB lock.
    let locked = server.run_admin(&["--retire-channel", "trial-a"]);
    assert!(
        !locked.status.success(),
        "SQLite --retire-channel must refuse to run while minymaton-server holds the DB lock"
    );
    let locked_err = String::from_utf8_lossy(&locked.stderr);
    assert!(
        locked_err.contains("is locked by a running server"),
        "{locked_err}"
    );
    server.stop();

    // `--retire-channel` refuses open streams ("agents") and non-channel private streams.
    let refuse_agents = server.run_admin(&["--retire-channel", "agents"]);
    assert!(!refuse_agents.status.success());

    // `--retire-channel trial-a` deletes the stream and all its subscriptions/events/batches.
    let retired = server.run_admin(&["--retire-channel", "trial-a"]);
    assert!(
        retired.status.success(),
        "{}",
        String::from_utf8_lossy(&retired.stderr)
    );

    // Re-activating enrollment re-provisions `trial-a` with a clean slate.
    let reenrolled = server.run_admin(&[
        "--activate-enrollment",
        server.token_file().to_str().expect("utf8 token file"),
    ]);
    assert!(
        reenrolled.status.success(),
        "{}",
        String::from_utf8_lossy(&reenrolled.stderr)
    );

    server.restart().await;

    let trial_a = server.client_with_token(TRIAL_A_TOKEN, modern()).await;
    let gone = trial_a.call("receive", receive(&sub_1)).await.err();
    assert_eq!(gone["code"], "subscription_not_found", "{gone:#}");

    let fresh_sub = trial_a.create_subscription("trial-a", "s1").await;
    assert_ne!(
        fresh_sub, sub_1,
        "re-created subscription after retirement must not reuse retired subscription ID/handle"
    );
    trial_a.publish("trial-a", "e3", json!("three")).await;
    let received = trial_a.call("receive", receive(&fresh_sub)).await.ok();
    assert_eq!(payloads(&received), vec![json!("three")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_tool_list_watcher_notifies_on_visibility_revoke_regrant_and_delete() {
    use crate::common::next_tool_change;

    let server = Server::start_with_bearer_tokens_and_options(
        &tokens(),
        &["--delivery", "description,tools"],
    )
    .await;
    let alice = server.client_with_token(ALICE_TOKEN, modern()).await;
    let bob = server.client_with_token(BOB_TOKEN, modern()).await;

    alice
        .call(
            "stream",
            json!({"action": "create", "name": "secrets", "members": ["bob"]}),
        )
        .await
        .ok();

    let mut bob_agent = server
        .agent_client_with_token("bob-agent", BOB_TOKEN, modern())
        .await;
    let mut changes = bob_agent.listen_for_tool_changes().await;
    let follow = bob_agent
        .call("follow", json!({"stream": "secrets"}))
        .await
        .ok();
    assert_eq!(follow["stream"], "secrets");
    next_tool_change(&mut changes).await;
    bob_agent.refresh_tools().await;

    // Publish an event to bob-agent on `secrets`; ToolListWatcher emits `notifications/tools/list_changed`.
    alice
        .publish("secrets", "sec-1", json!({"msg": "eyes-only"}))
        .await;
    next_tool_change(&mut changes).await;
    bob_agent.refresh_tools().await;
    let tools_while_member = bob_agent.list_tools().await;
    let receive_desc = tools_while_member["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .find(|t| t["name"] == "receive")
        .and_then(|t| t["description"].as_str())
        .expect("receive description");
    assert!(receive_desc.contains("eyes-only"), "{receive_desc}");

    // Revoking bob from `secrets` notifies `bob-agent` and hides the pending private batch.
    alice
        .call(
            "stream",
            json!({"action": "revoke", "name": "secrets", "caller": "bob"}),
        )
        .await
        .ok();
    next_tool_change(&mut changes).await;
    bob_agent.refresh_tools().await;
    let tools_after_revoke = bob_agent.list_tools().await;
    let receive_desc_revoked = tools_after_revoke["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .find(|t| t["name"] == "receive")
        .and_then(|t| t["description"].as_str())
        .expect("receive description");
    assert!(
        !receive_desc_revoked.contains("eyes-only"),
        "revoked caller's tool description must not leak private stream batch: {receive_desc_revoked}"
    );

    // Re-granting bob on `secrets` notifies `bob-agent` and restores the bound private batch.
    alice
        .call(
            "stream",
            json!({"action": "grant", "name": "secrets", "caller": "bob"}),
        )
        .await
        .ok();
    next_tool_change(&mut changes).await;
    bob_agent.refresh_tools().await;
    let tools_after_regrant = bob_agent.list_tools().await;
    let receive_desc_regranted = tools_after_regrant["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .find(|t| t["name"] == "receive")
        .and_then(|t| t["description"].as_str())
        .expect("receive description");
    assert!(
        receive_desc_regranted.contains("eyes-only"),
        "re-granted caller must see bound stream batch again: {receive_desc_regranted}"
    );

    // Deleting bob-agent's subscription emits `notifications/tools/list_changed` (deletion sentinel == i64::MAX)
    // and keeps the watcher alive so re-following works.
    bob.call(
        "subscription",
        json!({"action": "delete", "stream": "secrets", "label": "bob-agent"}),
    )
    .await
    .ok();
    next_tool_change(&mut changes).await;
    bob_agent.refresh_tools().await;

    bob_agent
        .call("follow", json!({"stream": "secrets"}))
        .await
        .ok();
    next_tool_change(&mut changes).await;
    bob_agent.refresh_tools().await;
    alice
        .publish("secrets", "sec-2", json!({"msg": "after-refollow"}))
        .await;
    next_tool_change(&mut changes).await;
}
