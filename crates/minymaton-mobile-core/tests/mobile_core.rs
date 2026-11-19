//! The mobile core against real `minymaton-server` instances over real
//! sockets: bearer authentication, the roster, direct messages and their
//! acknowledgements, catch-up after a pause, bus switching, a private CA over
//! TLS and a cold restart on the same device database.
//!
//! The core owns its runtime and blocks in `request`, as the app's worker
//! thread does, so these tests are synchronous. Each bus runs on its own
//! runtime, and its peer (the desk) calls it through the same client.

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};

use minymaton_mobile_core::{Endpoint, MobileCore};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const PHONE_TOKEN: &str = "mny_phone_0123456789abcdef0123456789abcdef";
const DESK_TOKEN: &str = "mny_desk_fedcba9876543210fedcba9876543210";

/// A real server on a loopback port, with its own database and tokens.
struct Bus {
    runtime: Runtime,
    url: String,
    addr: std::net::SocketAddr,
    _dir: TempDir,
}

impl Bus {
    fn start() -> Self {
        let dir = tempfile::tempdir().expect("bus directory");
        let tokens = dir.path().join("tokens.tsv");
        std::fs::write(
            &tokens,
            format!("phone\t{PHONE_TOKEN}\ndesk\t{DESK_TOKEN}\n"),
        )
        .expect("token file");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("bus runtime");
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind");
        let addr = listener.local_addr().expect("address");
        let arguments = vec![
            "--listen".to_owned(),
            "127.0.0.1:0".to_owned(),
            "--db".to_owned(),
            dir.path().join("bus.sqlite3").display().to_string(),
            "--bearer-token-file".to_owned(),
            tokens.display().to_string(),
        ];
        let mut server = runtime.spawn(minymaton_server::run_on(listener, arguments));
        let url = format!("http://{addr}/mcp");
        let probe = Endpoint::new(&url, Some(DESK_TOKEN), None).expect("probe endpoint");
        runtime.block_on(async {
            tokio::select! {
                ended = &mut server => panic!("the bus stopped: {ended:?}"),
                listed = probe.call("stream", json!({"action": "list"})) => {
                    listed.expect("the bus answers a stream list");
                }
            }
        });
        Self {
            runtime,
            url,
            addr,
            _dir: dir,
        }
    }

    fn desk(&self) -> Desk<'_> {
        Desk {
            bus: self,
            endpoint: Endpoint::new(&self.url, Some(DESK_TOKEN), None).expect("desk endpoint"),
        }
    }
}

/// The other side of the conversation: an agent on the same bus.
struct Desk<'a> {
    bus: &'a Bus,
    endpoint: Endpoint,
}

impl Desk<'_> {
    fn call(&self, tool: &str, arguments: Value) -> Value {
        self.bus
            .runtime
            .block_on(self.endpoint.call(tool, arguments))
            .unwrap_or_else(|error| panic!("desk {tool}: {error}"))
    }

    fn join(&self, stream: &str) -> String {
        let created = self.call(
            "subscription",
            json!({"action": "create", "stream": stream, "label": "desk", "start_at": "latest"}),
        );
        created["handle"].as_str().expect("desk handle").to_owned()
    }

    fn publish(&self, stream: &str, payload: Value, to: Option<&str>) -> Value {
        let mut arguments = json!({
            "stream": stream,
            "idempotency_key": format!("desk-{}", payload),
            "payload": payload,
            "from": "desk",
        });
        if let Some(to) = to {
            arguments["to"] = json!(to);
        }
        self.call("publish", arguments)
    }

    /// Everything waiting for the desk, acknowledged.
    fn drain(&self, handle: &str) -> Vec<Value> {
        let mut events = Vec::new();
        let mut ack: Option<String> = None;
        loop {
            let mut arguments =
                json!({"handle": handle, "max_events": 16, "max_bytes": 16384, "wait": false});
            if let Some(ack) = &ack {
                arguments["ack"] = json!(ack);
            }
            let result = self.call("receive", arguments);
            match result["status"].as_str() {
                Some("events") => {
                    events.extend(result["events"].as_array().expect("events").clone());
                    ack = Some(
                        result["ack_after_handling"]
                            .as_str()
                            .expect("ack")
                            .to_owned(),
                    );
                }
                Some("empty") => return events,
                other => panic!("unexpected desk receive {other:?}: {result}"),
            }
        }
    }
}

/// A phone: the core with its own device directory and an event channel.
struct Phone {
    core: MobileCore,
    events: Receiver<Value>,
}

impl Phone {
    fn open(dir: &Path) -> Self {
        let (sender, events) = channel();
        let core = MobileCore::open(
            dir,
            Box::new(move |event: String| {
                let _ = sender.send(serde_json::from_str(&event).expect("event JSON"));
            }),
        )
        .expect("open the core");
        Self { core, events }
    }

    fn try_request(&self, command: Value) -> Result<Value, Value> {
        let response: Value =
            serde_json::from_str(&self.core.request(&command.to_string())).expect("JSON reply");
        if response["ok"] == true {
            Ok(response["result"].clone())
        } else {
            Err(response["error"].clone())
        }
    }

    fn request(&self, command: Value) -> Value {
        self.try_request(command.clone())
            .unwrap_or_else(|error| panic!("{command} failed: {error}"))
    }

    fn add_profile(&self, name: &str, url: &str, ca_pem: Option<&str>) -> String {
        let mut command = json!({"cmd": "add_profile", "name": name, "url": url});
        if let Some(ca_pem) = ca_pem {
            command["ca_pem"] = json!(ca_pem);
        }
        self.request(command)["profile"]["id"]
            .as_str()
            .expect("profile id")
            .to_owned()
    }

    fn activate(&self, profile_id: &str) -> Value {
        self.request(json!({"cmd": "activate", "profile_id": profile_id, "token": PHONE_TOKEN}))
    }

    /// The next event matching `wanted`, skipping the others.
    fn next(&self, wanted: impl Fn(&Value) -> bool) -> Value {
        loop {
            let event = self.events.recv().expect("the core is still emitting");
            if wanted(&event) {
                return event;
            }
        }
    }

    fn waiting(&self, profile_id: &str, stream: &str) -> Value {
        self.next(|event| {
            event["event"] == "waiting"
                && event["profile_id"] == profile_id
                && event["stream"] == stream
        })
    }
}

fn create_stream(phone: &Phone, name: &str) {
    phone.request(json!({"cmd": "create_stream", "name": name}));
}

#[test]
fn activation_requires_the_enrolled_token_and_never_echoes_it() {
    let bus = Bus::start();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("local", &bus.url, None);

    let wrong = "mny_wrong_00000000000000000000000000000000";
    let error = phone
        .try_request(json!({"cmd": "activate", "profile_id": profile, "token": wrong}))
        .expect_err("a wrong token is rejected");
    assert_eq!(error["code"], "unauthorized", "{error}");
    assert!(!error.to_string().contains(wrong), "{error}");

    let error = phone
        .try_request(json!({"cmd": "activate", "profile_id": profile}))
        .expect_err("a missing token is rejected");
    assert_eq!(error["code"], "unauthorized", "{error}");

    let status = phone.request(json!({"cmd": "status"}));
    assert_eq!(status["active_profile"], Value::Null, "{status}");

    let activated = phone.activate(&profile);
    assert_eq!(activated["profile"]["id"], profile.as_str());
    assert!(activated["streams"].is_array(), "{activated}");
    assert_eq!(
        phone.request(json!({"cmd": "status"}))["active_profile"],
        profile.as_str()
    );
}

#[test]
fn sync_history_loads_messages_published_before_joining_and_leaves_no_subscription() {
    let bus = Bus::start();
    let desk = bus.desk();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("local", &bus.url, None);
    phone.activate(&profile);
    create_stream(&phone, "ops");
    desk.join("ops");
    let first = desk.publish("ops", json!({"text": "before one"}), None);
    let second = desk.publish("ops", json!({"text": "before two"}), None);

    let synced = phone.request(json!({"cmd": "sync_history", "stream": "ops"}));
    let messages = synced["messages"].as_array().expect("synced messages");
    assert_eq!(messages.len(), 2, "{synced}");
    assert_eq!(messages[0]["message_id"], first["message_id"], "{synced}");
    assert_eq!(messages[1]["message_id"], second["message_id"], "{synced}");
    assert_eq!(messages[0]["payload"]["text"], "before one", "{synced}");

    // The temporary history subscription is gone: only the desk remains.
    let roster = phone.request(json!({"cmd": "roster", "stream": "ops"}));
    let labels: Vec<&str> = roster["subscriptions"]
        .as_array()
        .expect("subscriptions")
        .iter()
        .map(|entry| entry["label"].as_str().expect("label"))
        .collect();
    assert_eq!(labels, ["desk"], "{roster}");

    // A second sync is a no-op over what is already stored.
    let again = phone.request(json!({"cmd": "sync_history", "stream": "ops"}));
    assert_eq!(
        again["messages"].as_array().expect("messages").len(),
        2,
        "{again}"
    );
}

#[test]
fn a_default_join_receives_messages_published_before_it() {
    let bus = Bus::start();
    let desk = bus.desk();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("local", &bus.url, None);
    phone.activate(&profile);
    create_stream(&phone, "ops");
    desk.join("ops");
    let sent = desk.publish("ops", json!({"text": "already here"}), None);

    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    let woke = phone.next(|event| event["event"] == "messages");
    let messages = woke["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 1, "{woke}");
    assert_eq!(messages[0]["message_id"], sent["message_id"], "{woke}");

    let stored = phone.request(json!({"cmd": "messages", "stream": "ops"}));
    assert_eq!(
        stored["messages"][0]["message_id"], sent["message_id"],
        "{stored}"
    );
}

#[test]
fn a_joined_phone_shows_as_waiting_in_the_roster() {
    let bus = Bus::start();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("local", &bus.url, None);
    phone.activate(&profile);
    create_stream(&phone, "ops");

    let joined = phone.request(json!({
        "cmd": "join", "stream": "ops", "label": "phone",
        "metadata": {"harness": "minymaton-mobile"},
    }));
    assert_eq!(joined["status"], "created", "{joined}");
    phone.waiting(&profile, "ops");

    let roster = phone.request(json!({"cmd": "roster", "stream": "ops"}));
    let me = roster["subscriptions"]
        .as_array()
        .expect("subscriptions")
        .iter()
        .find(|entry| entry["label"] == "phone")
        .unwrap_or_else(|| panic!("phone missing from {roster}"));
    assert_eq!(me["waiting"], true, "{roster}");
    assert_eq!(me["metadata"]["harness"], "minymaton-mobile", "{roster}");

    // Joining again recovers the same subscription instead of failing.
    let again = phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    assert_eq!(again["status"], "recovered", "{again}");
}

#[test]
fn a_direct_message_wakes_the_phone_is_stored_then_acknowledged_and_answered() {
    let bus = Bus::start();
    let desk = bus.desk();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("local", &bus.url, None);
    phone.activate(&profile);
    create_stream(&phone, "ops");
    let desk_handle = desk.join("ops");
    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    phone.waiting(&profile, "ops");

    let sent = desk.publish("ops", json!({"text": "deploy done?"}), Some("phone"));
    let woke = phone.next(|event| event["event"] == "messages");
    let messages = woke["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 1, "{woke}");
    assert_eq!(messages[0]["message_id"], sent["message_id"]);
    assert_eq!(messages[0]["payload"]["text"], "deploy done?");
    assert_eq!(messages[0]["publisher"], "desk");
    assert_eq!(messages[0]["direction"], "in");

    // The batch is stored on the device before the next receive acknowledges it.
    let acked = phone.next(|event| event["event"] == "acked");
    assert_eq!(acked["outcome"], "committed", "{acked}");
    let stored = phone.request(json!({"cmd": "messages", "stream": "ops"}));
    assert_eq!(
        stored["messages"][0]["message_id"], sent["message_id"],
        "{stored}"
    );
    phone.waiting(&profile, "ops");

    let bystander_handle = desk.call(
        "subscription",
        json!({"action": "create", "stream": "ops", "label": "bystander", "start_at": "latest"}),
    )["handle"]
        .as_str()
        .expect("bystander handle")
        .to_owned();

    let reply = phone.request(json!({
        "cmd": "publish", "stream": "ops", "payload": {"text": "yes, shipped"}, "to": " @desk ",
    }));
    assert_eq!(
        reply["message"]["payload"],
        json!({"text": "yes, shipped", "from": "phone", "to": "desk"}),
        "{reply}"
    );
    phone.request(json!({"cmd": "nudge", "stream": "ops", "kind": "yes_go", "to": "desk"}));
    phone.request(json!({"cmd": "nudge", "stream": "ops", "kind": "custom", "text": "  ping  "}));

    let received = desk.drain(&desk_handle);
    let payloads: Vec<&Value> = received.iter().map(|event| &event["payload"]).collect();
    assert_eq!(payloads.len(), 3, "{received:?}");
    assert_eq!(received[0]["message_id"], reply["message"]["message_id"]);
    assert_eq!(
        *payloads[0],
        json!({"text": "yes, shipped", "from": "phone", "to": "desk"}),
        "{received:?}"
    );
    assert_eq!(
        *payloads[1],
        json!({
            "from": "phone", "nudge": "yes_go", "text": "yes / go",
            "source": "minymaton-mobile", "to": "desk",
        })
    );
    assert_eq!(
        *payloads[2],
        json!({"from": "phone", "nudge": "custom", "text": "ping", "source": "minymaton-mobile"})
    );
    for event in &received {
        assert_eq!(event["publisher"], "phone");
    }

    let bystander_events = desk.drain(&bystander_handle);
    assert_eq!(bystander_events.len(), 1, "{bystander_events:?}");
    assert_eq!(
        bystander_events[0]["payload"]["text"], "ping",
        "{bystander_events:?}"
    );

    // The phone's own broadcasts are not echoed back to it; its history
    // holds the incoming message and the three it sent, in order.
    let history = phone.request(json!({"cmd": "messages", "stream": "ops"}));
    let directions: Vec<&str> = history["messages"]
        .as_array()
        .expect("history")
        .iter()
        .map(|message| message["direction"].as_str().expect("direction"))
        .collect();
    assert_eq!(directions, ["in", "out", "out", "out"], "{history}");

    let error = phone
        .try_request(json!({"cmd": "nudge", "stream": "ops", "kind": "custom", "text": "   "}))
        .expect_err("an empty custom nudge is rejected");
    assert_eq!(error["code"], "invalid_command", "{error}");
}

#[test]
fn after_a_pause_the_phone_catches_up_in_order_and_acknowledges() {
    let bus = Bus::start();
    let desk = bus.desk();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("local", &bus.url, None);
    phone.activate(&profile);
    create_stream(&phone, "ops");
    desk.join("ops");
    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    phone.waiting(&profile, "ops");

    phone.request(json!({"cmd": "pause"}));
    let paused = phone.next(|event| event["event"] == "paused");
    assert_eq!(paused["stopped"], json!(["ops"]), "{paused}");
    let status = phone.request(json!({"cmd": "status"}));
    assert_eq!(status["paused"], true);
    assert_eq!(status["watching"], json!([]), "{status}");

    let sent: Vec<Value> = (1..=3)
        .map(|n| desk.publish("ops", json!({"n": n}), None))
        .collect();
    let resumed = phone.request(json!({"cmd": "resume"}));
    assert_eq!(resumed["started"], json!(["ops"]), "{resumed}");

    let mut caught = Vec::new();
    while caught.len() < 3 {
        let event = phone.next(|event| event["event"] == "messages");
        caught.extend(event["messages"].as_array().expect("messages").clone());
    }
    let ids: Vec<&Value> = caught
        .iter()
        .map(|message| &message["message_id"])
        .collect();
    let expected: Vec<&Value> = sent.iter().map(|message| &message["message_id"]).collect();
    assert_eq!(ids, expected);
    phone.next(|event| event["event"] == "acked" && event["outcome"] == "committed");
    phone.waiting(&profile, "ops");

    let stored = phone.request(json!({"cmd": "messages", "stream": "ops"}));
    assert_eq!(stored["messages"].as_array().expect("stored").len(), 3);

    // Nothing is left pending for the phone on the bus: a check with the
    // phone's own handle finds the stream empty (and takes over its wait).
    let joined = phone.request(json!({"cmd": "joined"}));
    let handle = joined["subscriptions"][0]["handle"].clone();
    let check = Endpoint::new(&bus.url, Some(PHONE_TOKEN), None).expect("check endpoint");
    let result = bus
        .runtime
        .block_on(check.call(
            "receive",
            json!({"handle": handle, "max_events": 16, "max_bytes": 16384, "wait": false}),
        ))
        .expect("check receive");
    assert_eq!(result["status"], "empty", "{result}");
    let ended = phone.next(|event| event["event"] == "watch_ended");
    assert_eq!(ended["code"], "superseded", "{ended}");
}

#[test]
fn switching_buses_keeps_each_bus_subscriptions_and_catches_up_on_return() {
    let first = Bus::start();
    let second = Bus::start();
    let first_desk = first.desk();
    let second_desk = second.desk();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let a = phone.add_profile("first", &first.url, None);
    let b = phone.add_profile("second", &second.url, None);

    phone.activate(&a);
    create_stream(&phone, "ops");
    first_desk.join("ops");
    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    phone.waiting(&a, "ops");

    phone.activate(&b);
    create_stream(&phone, "lab");
    second_desk.join("lab");
    phone.request(json!({"cmd": "join", "stream": "lab", "label": "phone"}));
    phone.waiting(&b, "lab");
    let joined = phone.request(json!({"cmd": "joined"}));
    assert_eq!(
        joined["subscriptions"].as_array().expect("joined").len(),
        1,
        "{joined}"
    );
    assert_eq!(joined["subscriptions"][0]["stream"], "lab");

    let missed = first_desk.publish("ops", json!({"text": "while away"}), Some("phone"));
    let here = second_desk.publish("lab", json!({"text": "on the second bus"}), Some("phone"));
    let event = phone.next(|event| event["event"] == "messages");
    assert_eq!(event["profile_id"], b.as_str(), "{event}");
    assert_eq!(event["messages"][0]["message_id"], here["message_id"]);

    let back = phone.activate(&a);
    assert_eq!(back["started"], json!(["ops"]), "{back}");
    let event = phone.next(|event| event["event"] == "messages");
    assert_eq!(event["profile_id"], a.as_str(), "{event}");
    assert_eq!(event["stream"], "ops");
    assert_eq!(event["messages"][0]["message_id"], missed["message_id"]);
}

#[test]
fn a_cold_restart_keeps_profiles_history_and_the_owed_subscription() {
    let bus = Bus::start();
    let desk = bus.desk();
    let device = tempfile::tempdir().unwrap();
    let profile;
    let first;
    {
        let phone = Phone::open(device.path());
        profile = phone.add_profile("local", &bus.url, None);
        phone.activate(&profile);
        create_stream(&phone, "ops");
        desk.join("ops");
        phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
        phone.waiting(&profile, "ops");
        first = desk.publish("ops", json!({"text": "before restart"}), Some("phone"));
        phone.next(|event| event["event"] == "messages");
        phone.core.close();
    }
    let second = desk.publish("ops", json!({"text": "after restart"}), Some("phone"));

    let phone = Phone::open(device.path());
    let profiles = phone.request(json!({"cmd": "profiles"}));
    assert_eq!(profiles["active_profile"], profile.as_str(), "{profiles}");
    assert_eq!(profiles["profiles"][0]["url"], bus.url.as_str());
    assert_eq!(
        phone.request(json!({"cmd": "status"}))["active_profile"],
        Value::Null
    );

    // The token lives in the platform keystore, never in the core database.
    let database = std::fs::read(device.path().join("minymaton-mobile.sqlite3")).unwrap();
    assert!(!String::from_utf8_lossy(&database).contains(PHONE_TOKEN));

    let activated = phone.activate(&profile);
    assert_eq!(activated["started"], json!(["ops"]), "{activated}");
    let event = phone.next(|event| event["event"] == "messages");
    let ids: Vec<&Value> = event["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|message| &message["message_id"])
        .collect();
    // A batch redelivered after the restart is not duplicated on the device.
    assert_eq!(ids, [&second["message_id"]], "{event}");
    let stored = phone.request(json!({"cmd": "messages", "stream": "ops"}));
    let stored: Vec<&Value> = stored["messages"]
        .as_array()
        .expect("stored")
        .iter()
        .map(|message| &message["message_id"])
        .collect();
    assert_eq!(stored, [&first["message_id"], &second["message_id"]]);
}

#[test]
fn a_profile_with_a_private_ca_reaches_the_bus_over_tls() {
    let bus = Bus::start();
    let (ca_pem, config) = private_ca();
    let terminator = bus
        .runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("bind TLS");
    let port = terminator.local_addr().expect("TLS address").port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let upstream = bus.addr;
    bus.runtime.spawn(async move {
        loop {
            let (socket, _) = terminator.accept().await.expect("accept TLS");
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(socket).await else {
                    return;
                };
                let mut plain = tokio::net::TcpStream::connect(upstream)
                    .await
                    .expect("reach the bus");
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut plain).await;
            });
        }
    });
    let url = format!("https://localhost:{port}/mcp");

    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let untrusted = phone.add_profile("no ca", &url, None);
    let error = phone
        .try_request(json!({"cmd": "activate", "profile_id": untrusted, "token": PHONE_TOKEN}))
        .expect_err("an unknown CA is refused");
    assert_eq!(error["code"], "transport_failed", "{error}");
    assert!(!error.to_string().contains(PHONE_TOKEN), "{error}");

    let trusted = phone.add_profile("private ca", &url, Some(&ca_pem));
    phone.activate(&trusted);
    create_stream(&phone, "ops");
    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    phone.waiting(&trusted, "ops");
    let desk = bus.desk();
    let sent = desk.publish("ops", json!({"text": "over tls"}), Some("phone"));
    let event = phone.next(|event| event["event"] == "messages");
    assert_eq!(event["messages"][0]["message_id"], sent["message_id"]);
}

fn private_ca() -> (String, tokio_rustls::rustls::ServerConfig) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let ca_key = KeyPair::generate().expect("CA key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("CA certificate");
    let issuer = Issuer::new(ca_params, ca_key);

    let key = KeyPair::generate().expect("server key");
    let params = CertificateParams::new(vec!["localhost".to_owned()]).expect("server params");
    let cert = params.signed_by(&key, &issuer).expect("server certificate");

    let mut config = tokio_rustls::rustls::ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("protocol versions")
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from(cert.der().to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
    )
    .expect("server config");
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    (ca_cert.pem(), config)
}

#[test]
fn sweeps_and_bulk_deletes_idle_streams_and_subscriptions() {
    let bus = Bus::start();
    let device = tempfile::tempdir().unwrap();
    let phone = Phone::open(device.path());
    let profile = phone.add_profile("bus", &bus.url, None);
    phone.activate(&profile);

    let desk = bus.desk();
    desk.call("stream", json!({"action": "create", "name": "ops"}));
    desk.call("stream", json!({"action": "create", "name": "stale-chan"}));
    desk.call("stream", json!({"action": "create", "name": "keep-chan"}));
    desk.call(
        "subscription",
        json!({"action": "create", "stream": "stale-chan", "label": "idle-sub"}),
    );
    desk.call(
        "subscription",
        json!({"action": "create", "stream": "ops", "label": "dead-1"}),
    );
    desk.call(
        "subscription",
        json!({"action": "create", "stream": "ops", "label": "dead-2"}),
    );
    desk.call(
        "subscription",
        json!({"action": "create", "stream": "ops", "label": "keep-sub"}),
    );

    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    phone.waiting(&profile, "ops");
    phone.request(json!({"cmd": "publish", "stream": "ops", "payload": {"text": "hello"}}));

    // Pause so the phone's watch loop is stopped — `sweep_subscriptions` & `sweep_streams`
    // must still protect the phone's own joined label and joined stream!
    phone.request(json!({"cmd": "pause"}));

    let swept_subs = phone.request(json!({
        "cmd": "sweep_subscriptions",
        "stream": "ops",
        "keep": ["keep-sub"]
    }));
    assert_eq!(swept_subs["deleted"], json!(["dead-1", "dead-2"]));
    assert_eq!(swept_subs["deleted_count"], 2);
    let sub_labels: Vec<&str> = swept_subs["subscriptions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["label"].as_str().unwrap())
        .collect();
    assert_eq!(sub_labels, vec!["keep-sub", "phone"]);

    let swept_streams = phone.request(json!({
        "cmd": "sweep_streams",
        "keep": ["keep-chan"]
    }));
    assert_eq!(swept_streams["deleted"], json!(["stale-chan"]));
    assert_eq!(swept_streams["deleted_count"], 1);
    assert_eq!(
        swept_streams["streams"],
        json!(["agents", "keep-chan", "ops"])
    );

    phone.request(json!({"cmd": "resume"}));
    phone.waiting(&profile, "ops");

    // Bulk-delete subscriptions including the phone's own joined label (`phone`) and `keep-sub`.
    let del_subs = phone.request(json!({
        "cmd": "delete_subscriptions",
        "stream": "ops",
        "labels": ["keep-sub", "phone"]
    }));
    assert_eq!(del_subs["deleted"], json!(["keep-sub", "phone"]));
    assert_eq!(del_subs["deleted_count"], 2);
    assert_eq!(del_subs["subscriptions"], json!([]));
    let joined = phone.request(json!({"cmd": "joined"}));
    assert_eq!(joined["subscriptions"], json!([]));

    // Re-join `ops`, then delete `ops` + `keep-chan` via `delete_streams` and confirm local
    // messages and subscriptions are cleaned up.
    phone.request(json!({"cmd": "join", "stream": "ops", "label": "phone"}));
    phone.waiting(&profile, "ops");
    let del_streams = phone.request(json!({
        "cmd": "delete_streams",
        "streams": ["ops", "keep-chan"]
    }));
    assert_eq!(del_streams["deleted"], json!(["ops", "keep-chan"]));
    assert_eq!(del_streams["deleted_count"], 2);
    assert_eq!(del_streams["streams"], json!(["agents"]));
    let joined_after = phone.request(json!({"cmd": "joined"}));
    assert_eq!(joined_after["subscriptions"], json!([]));
    let msgs_after = phone.request(json!({"cmd": "messages", "stream": "ops"}));
    assert_eq!(msgs_after["messages"], json!([]));
}
