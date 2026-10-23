//! Harness for the minymaton-server integration tests.
//!
//! Every test starts the real `minymaton-server` binary on 127.0.0.1 with its own
//! SQLite file and talks to it over real sockets: through the rmcp 3.1.4 client,
//! as rmcp-based hosts do, and through a raw HTTP client where a test has to see
//! the exact bytes on the wire.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use reqwest::StatusCode;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, ClientCapabilities, ClientInfo, ClientRequest,
    Implementation, ProgressNotificationParam, ProgressToken, ProtocolVersion,
    ReadResourceRequestParams, ServerNotification, ServerResult, SubscriptionFilter,
};
use rmcp::service::{NotificationContext, PeerRequestOptions, RunningService, Subscription};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{
    ClientHandler, ClientLifecycleMode, ClientServiceExt, RoleClient, ServiceError, ServiceExt,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Decision 6: every receive result is at most 16 KiB on the wire.
pub const RESULT_LIMIT: usize = 16_384;

/// How a client starts its MCP lifecycle, and the revision it asks for.
#[derive(Clone, Debug)]
pub enum Lifecycle {
    /// The `initialize` handshake of 2025-06-18 and 2025-11-25 (and older).
    Initialize(ProtocolVersion),
    /// The `server/discover` lifecycle of 2026-07-28.
    Discover(ProtocolVersion),
}

impl Lifecycle {
    pub fn requested(&self) -> ProtocolVersion {
        match self {
            Self::Initialize(version) | Self::Discover(version) => version.clone(),
        }
    }
}

/// A running `minymaton-server` child process. Dropping it kills and reaps that
/// exact process before its temporary directory is removed.
pub struct Server {
    child: Child,
    url: String,
    request_log: Option<PathBuf>,
    dir: Arc<TempDir>,
    /// The options after `--listen` and `--db`, kept for a restart.
    options: Vec<String>,
}

impl Server {
    pub async fn start() -> Self {
        Self::launch(false).await
    }

    /// A server started with `--request-log`.
    pub async fn start_with_request_log() -> Self {
        Self::launch(true).await
    }

    async fn launch(log_requests: bool) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("live-arrival-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("create a temporary directory for the queue database");
        Self::launch_in(Arc::new(dir), log_requests, None, None, None).await
    }

    /// A server started with the description options and `--delivery tools`.
    pub async fn start_with_tool_messages(stream: &str, label: &str) -> Self {
        Self::start_with_delivery(stream, label, "tools").await
    }

    /// A server started with the description options and this `--delivery`.
    pub async fn start_with_delivery(stream: &str, label: &str, delivery: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("tool-set-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("create a temporary directory for the queue database");
        Self::launch_in(
            Arc::new(dir),
            true,
            Some((stream, label)),
            Some(delivery),
            None,
        )
        .await
    }

    /// `--delivery tools` with `--batch-bytes`: one message tool may carry a
    /// batch up to that size.
    pub async fn start_with_tool_messages_and_batch_bytes(
        stream: &str,
        label: &str,
        batch_bytes: u64,
    ) -> Self {
        Self::start_with_delivery_and_batch_bytes(stream, label, "tools", batch_bytes).await
    }

    pub async fn start_with_delivery_and_batch_bytes(
        stream: &str,
        label: &str,
        delivery: &str,
        batch_bytes: u64,
    ) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("tool-set-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("create a temporary directory for the queue database");
        Self::launch_in(
            Arc::new(dir),
            true,
            Some((stream, label)),
            Some(delivery),
            Some(batch_bytes),
        )
        .await
    }

    pub async fn start_with_description(stream: &str, label: &str) -> Self {
        let mut server = Self::start_with_request_log().await;
        server.enable_description(stream, label).await;
        server
    }

    /// Reopens the same real database with the description option. Fixtures
    /// are published through MCP before this, never inserted directly in SQL.
    pub async fn enable_description(&mut self, stream: &str, label: &str) {
        self.child
            .kill()
            .expect("stop the fixture server by its PID");
        self.child.wait().expect("reap the fixture server");
        *self = Self::launch_in(
            Arc::clone(&self.dir),
            true,
            Some((stream, label)),
            None,
            None,
        )
        .await;
    }

    /// A server started with no description options, whose agents choose their own
    /// streams with `follow`, and this `--delivery` if any.
    pub async fn start_for_agents(delivery: Option<&str>) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("follow-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("create a temporary directory for the queue database");
        Self::launch_in(Arc::new(dir), true, None, delivery, None).await
    }

    /// A server started with `--bearer-token-file` containing `tokens_file_content`
    /// and `--request-log`.
    pub async fn start_with_bearer_tokens(tokens_file_content: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("auth-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .expect("create a temporary directory for the queue database");
        let token_file = dir.path().join("tokens.conf");
        std::fs::write(&token_file, tokens_file_content).expect("write tokens.conf");
        let request_log = dir.path().join("requests.jsonl");
        let options = vec![
            "--request-log".to_owned(),
            request_log.to_string_lossy().into_owned(),
            "--bearer-token-file".to_owned(),
            token_file.to_string_lossy().into_owned(),
        ];
        Self::spawn(Arc::new(dir), options).await
    }

    /// Stops this server by its PID and starts it again with the same options
    /// on the same database.
    pub async fn restart(&mut self) {
        self.child.kill().expect("stop the server by its PID");
        self.child.wait().expect("reap the server");
        let options = self.options.clone();
        *self = Self::spawn(Arc::clone(&self.dir), options).await;
    }

    async fn launch_in(
        dir: Arc<TempDir>,
        log_requests: bool,
        description: Option<(&str, &str)>,
        delivery: Option<&str>,
        batch_bytes: Option<u64>,
    ) -> Self {
        let mut options = Vec::new();
        if log_requests {
            options.push("--request-log".to_owned());
            options.push(
                dir.path()
                    .join("requests.jsonl")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        if let Some((stream, label)) = description {
            options.extend([
                "--description-stream".to_owned(),
                stream.to_owned(),
                "--description-subscription".to_owned(),
                label.to_owned(),
            ]);
        }
        if let Some(delivery) = delivery {
            options.extend(["--delivery".to_owned(), delivery.to_owned()]);
        }
        if let Some(bytes) = batch_bytes {
            options.extend(["--batch-bytes".to_owned(), bytes.to_string()]);
        }
        Self::spawn(dir, options).await
    }

    async fn spawn(dir: Arc<TempDir>, options: Vec<String>) -> Self {
        Self::spawn_command(
            dir,
            options,
            Command::new(env!("CARGO_BIN_EXE_minymaton-server")),
        )
        .await
    }

    #[cfg(target_os = "linux")]
    pub async fn start_with_file_limit() -> Self {
        let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "trap '' XFSZ; ulimit -f 1024; exec \"$@\"",
                "storage-test",
            ])
            .arg(env!("CARGO_BIN_EXE_minymaton-server"));
        Self::spawn_command(Arc::new(dir), Vec::new(), command).await
    }

    async fn spawn_command(dir: Arc<TempDir>, options: Vec<String>, mut command: Command) -> Self {
        let request_log = options
            .iter()
            .position(|option| option == "--request-log")
            .map(|index| PathBuf::from(&options[index + 1]));
        command
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--db")
            .arg(dir.path().join("queue.sqlite3"))
            .args(&options);
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn minymaton-server");
        let stdout = child.stdout.take().expect("minymaton-server stdout");
        // The server prints its bound address once it is accepting connections.
        let (first_line, reader) = tokio::task::spawn_blocking(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .expect("read minymaton-server stdout");
            (line, reader)
        })
        .await
        .expect("read the listening line");
        if first_line.is_empty() {
            let status = child.wait().expect("minymaton-server exit status");
            panic!("minymaton-server exited ({status}) without printing its listening address");
        }
        let url = first_line
            .trim_end()
            .strip_prefix("listening on ")
            .unwrap_or_else(|| {
                panic!("unexpected first line from minymaton-server: {first_line:?}")
            })
            .to_owned();
        assert!(
            url.starts_with("http://127.0.0.1:") && url.ends_with("/mcp"),
            "minymaton-server must listen on 127.0.0.1 only: {url}"
        );
        std::thread::spawn(move || {
            for line in reader.lines().map_while(Result::ok) {
                eprintln!("minymaton-server stdout: {line}");
            }
        });
        Self {
            child,
            url,
            request_log,
            dir,
            options,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn db_path(&self) -> PathBuf {
        self.dir.path().join("queue.sqlite3")
    }

    /// The endpoint for the agent with this name.
    pub fn agent_url(&self, agent: &str) -> String {
        format!("{}/agent/{agent}", self.url)
    }

    /// A client whose MCP configuration points at this agent's endpoint.
    pub async fn agent_client(&self, agent: &str, lifecycle: Lifecycle) -> McpClient {
        McpClient::connect(
            &self.agent_url(agent),
            lifecycle,
            ClientCapabilities::default(),
        )
        .await
    }

    /// A legacy-revision session on this agent's endpoint.
    pub async fn agent_raw_session(&self, agent: &str, protocol: &ProtocolVersion) -> RawSession {
        RawSession::open(&self.agent_url(agent), protocol).await
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Whether the server process this test started has not exited.
    pub fn running(&mut self) -> bool {
        self.child
            .try_wait()
            .expect("query the server process")
            .is_none()
    }

    pub async fn client(&self, lifecycle: Lifecycle) -> McpClient {
        McpClient::connect(&self.url, lifecycle, ClientCapabilities::default()).await
    }

    /// A client that sends `token` as its bearer token, so the server knows
    /// it as that token's caller.
    pub async fn client_with_token(&self, token: &str, lifecycle: Lifecycle) -> McpClient {
        McpClient::connect_as(
            &self.url,
            Some(token),
            lifecycle,
            ClientCapabilities::default(),
        )
        .await
    }

    /// A `/mcp/agent/{agent}` client that sends `token` as its bearer token.
    pub async fn agent_client_with_token(
        &self,
        agent: &str,
        token: &str,
        lifecycle: Lifecycle,
    ) -> McpClient {
        McpClient::connect_as(
            &self.agent_url(agent),
            Some(token),
            lifecycle,
            ClientCapabilities::default(),
        )
        .await
    }

    /// A client that declares these capabilities to the server.
    pub async fn client_declaring(
        &self,
        lifecycle: Lifecycle,
        capabilities: ClientCapabilities,
    ) -> McpClient {
        McpClient::connect(&self.url, lifecycle, capabilities).await
    }

    pub async fn raw_session(&self, protocol: &ProtocolVersion) -> RawSession {
        RawSession::open(&self.url, protocol).await
    }

    /// The request log, exactly as the server wrote it.
    pub fn request_log_text(&self) -> String {
        let path = self
            .request_log
            .as_ref()
            .expect("this server was started without a request log");
        std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
    }

    /// The request log's records, one JSON object per line.
    pub fn requests(&self) -> Vec<Value> {
        self.request_log_text()
            .lines()
            .map(|line| {
                serde_json::from_str(line).unwrap_or_else(|error| {
                    panic!("a request log line is not JSON ({error}): {line}")
                })
            })
            .collect()
    }

    /// The names of the files in the server's directory.
    pub fn files(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.dir.path())
            .expect("list the server's directory")
            .map(|entry| {
                entry
                    .expect("a directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        names
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Already exited is fine; either way the process is reaped before the
        // database directory is deleted.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Progress notifications received by one client, kept per progress token until
/// the test that owns that token takes them.
#[derive(Default)]
struct ProgressBook {
    seen: Mutex<HashMap<ProgressToken, VecDeque<ProgressNotificationParam>>>,
    arrived: Notify,
}

impl ProgressBook {
    fn record(&self, progress: ProgressNotificationParam) {
        self.seen
            .lock()
            .expect("progress book")
            .entry(progress.progress_token.clone())
            .or_default()
            .push_back(progress);
        self.arrived.notify_waiters();
    }

    async fn next(&self, token: &ProgressToken) -> ProgressNotificationParam {
        loop {
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            let taken = self
                .seen
                .lock()
                .expect("progress book")
                .get_mut(token)
                .and_then(VecDeque::pop_front);
            if let Some(progress) = taken {
                return progress;
            }
            arrived.await;
        }
    }
}

struct ProgressRecorder {
    book: Arc<ProgressBook>,
    info: ClientInfo,
}

impl ClientHandler for ProgressRecorder {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.book.record(params);
    }

    fn get_info(&self) -> ClientInfo {
        self.info.clone()
    }
}

/// A real rmcp 3.1.4 client connected over Streamable HTTP.
pub struct McpClient {
    service: RunningService<RoleClient, ProgressRecorder>,
    book: Arc<ProgressBook>,
    output_schemas: HashMap<String, Value>,
}

impl McpClient {
    async fn connect(url: &str, lifecycle: Lifecycle, capabilities: ClientCapabilities) -> Self {
        Self::connect_as(url, None, lifecycle, capabilities).await
    }

    /// Connects, sending `token` as the bearer token when one is given.
    pub async fn connect_as(
        url: &str,
        token: Option<&str>,
        lifecycle: Lifecycle,
        capabilities: ClientCapabilities,
    ) -> Self {
        let book = Arc::new(ProgressBook::default());
        let recorder = ProgressRecorder {
            book: Arc::clone(&book),
            info: ClientInfo::new(
                capabilities,
                Implementation::new("minymaton-live-arrival-tests", "0"),
            )
            .with_protocol_version(lifecycle.requested()),
        };
        // A lost session must fail the test, not be silently replaced.
        let mut config = StreamableHttpClientTransportConfig::with_uri(url.to_owned())
            .reinit_on_expired_session(false);
        if let Some(token) = token {
            config = config.auth_header(token);
        }
        let transport = StreamableHttpClientTransport::from_config(config);
        let service = match lifecycle {
            Lifecycle::Initialize(_) => recorder.serve(transport).await,
            Lifecycle::Discover(version) => {
                recorder
                    .serve_with_lifecycle(
                        transport,
                        ClientLifecycleMode::Discover {
                            preferred_versions: vec![version],
                        },
                    )
                    .await
            }
        }
        .expect("rmcp client connects to minymaton-server");
        let output_schemas = output_schemas(&service).await;
        Self {
            service,
            book,
            output_schemas,
        }
    }

    pub fn negotiated(&self) -> ProtocolVersion {
        self.service
            .peer_info()
            .expect("negotiated server information")
            .protocol_version
            .clone()
    }

    pub fn tool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.output_schemas.keys().cloned().collect();
        names.sort();
        names
    }

    /// A fresh tools/list over the real rmcp transport.
    pub async fn list_tools(&self) -> Value {
        let result = self.service.list_tools(None).await.expect("tools/list");
        serde_json::to_value(result).expect("serialize tools/list")
    }

    /// Learns the tools a fresh tools/list shows, keeping the ones it no
    /// longer shows, as a host whose catalog lags behind would.
    pub async fn refresh_tools(&mut self) {
        self.output_schemas
            .extend(output_schemas(&self.service).await);
    }

    /// Sends a tool call and returns while it is still running.
    pub async fn start(&self, tool: &str, arguments: Value) -> PendingCall {
        let Value::Object(arguments) = arguments else {
            panic!("{tool} arguments must be a JSON object");
        };
        let schema = self
            .output_schemas
            .get(tool)
            .unwrap_or_else(|| panic!("minymaton-server does not list the tool {tool}"))
            .clone();
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(
            CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments),
        ));
        let handle = self
            .service
            .send_cancellable_request(request, PeerRequestOptions::no_options())
            .await
            .expect("send tools/call");
        PendingCall {
            tool: tool.to_owned(),
            schema,
            token: handle.progress_token.clone(),
            book: Arc::clone(&self.book),
            response: tokio::spawn(handle.await_response()),
        }
    }

    pub async fn call(&self, tool: &str, arguments: Value) -> Outcome {
        self.start(tool, arguments).await.finish().await
    }

    pub async fn create_stream(&self, name: &str) {
        let created = self
            .call("stream", json!({"action": "create", "name": name}))
            .await
            .ok();
        assert_eq!(created, json!({"status": "created", "stream": name}));
    }

    /// Creates a subscription and returns its handle.
    pub async fn create_subscription(&self, stream: &str, label: &str) -> String {
        let created = self
            .call(
                "subscription",
                json!({"action": "create", "stream": stream, "label": label}),
            )
            .await
            .ok();
        assert_eq!(created["status"], "created");
        assert_eq!(created["stream"], stream);
        assert_eq!(created["label"], label);
        created["handle"]
            .as_str()
            .expect("subscription handle")
            .to_owned()
    }

    pub async fn publish(&self, stream: &str, idempotency_key: &str, payload: Value) -> Value {
        let published = self
            .call(
                "publish",
                json!({"stream": stream, "idempotency_key": idempotency_key, "payload": payload}),
            )
            .await
            .ok();
        assert_eq!(published["status"], "published");
        assert_eq!(published["stream"], stream);
        published
    }

    /// The server capabilities negotiated at connection.
    pub fn capabilities(&self) -> Value {
        let info = self
            .service
            .peer_info()
            .expect("negotiated server information");
        serde_json::to_value(&info.capabilities).expect("serialize server capabilities")
    }

    /// The server instructions negotiated at connection.
    pub fn instructions(&self) -> Option<String> {
        self.service
            .peer_info()
            .expect("negotiated server information")
            .instructions
            .clone()
    }

    pub async fn resource_templates(&self) -> Vec<Value> {
        self.service
            .list_all_resource_templates()
            .await
            .expect("resources/templates/list")
            .iter()
            .map(|template| serde_json::to_value(template).expect("serialize a resource template"))
            .collect()
    }

    /// Opens a 2026-07-28 `subscriptions/listen` stream for these resources and
    /// returns once the server has acknowledged it.
    pub async fn listen(&self, uris: &[&str]) -> Subscription {
        let filter = SubscriptionFilter::builder()
            .resource_subscriptions(uris.iter().copied())
            .build();
        self.service
            .peer()
            .listen(filter)
            .await
            .expect("subscriptions/listen is acknowledged")
    }

    /// Opens a 2026-07-28 `subscriptions/listen` stream for tool-list changes,
    /// as Copilot CLI 1.0.91 does for a server that advertises them, and
    /// returns once the server has acknowledged it.
    pub async fn listen_for_tool_changes(&self) -> Subscription {
        let filter = SubscriptionFilter::builder().tools_list_changed().build();
        self.service
            .peer()
            .listen(filter)
            .await
            .expect("subscriptions/listen for tool-list changes is acknowledged")
    }

    /// The complete `resources/read` result for an inbox.
    pub async fn read_inbox(&self, uri: &str) -> Result<Value, ServiceError> {
        let result = self
            .service
            .read_resource(ReadResourceRequestParams::new(uri))
            .await?;
        Ok(serde_json::to_value(&result).expect("serialize ReadResourceResult"))
    }
}

/// Every listed tool's output schema, by tool name.
async fn output_schemas(
    service: &RunningService<RoleClient, ProgressRecorder>,
) -> HashMap<String, Value> {
    service
        .list_all_tools()
        .await
        .expect("tools/list")
        .into_iter()
        .map(|tool| {
            let schema = tool
                .output_schema
                .clone()
                .unwrap_or_else(|| panic!("tool {} declares no output schema", tool.name));
            (
                tool.name.to_string(),
                Value::Object(schema.as_ref().clone()),
            )
        })
        .collect()
}

/// The address of a subscription's inbox resource.
pub fn inbox_uri(handle: &str) -> String {
    format!("minymaton://inbox/{handle}")
}

/// Checks a `resources/read` result for an inbox: exactly one JSON text item
/// for that address. Returns the parsed content.
pub fn inbox_content(uri: &str, result: &Value) -> Value {
    let contents = result["contents"]
        .as_array()
        .unwrap_or_else(|| panic!("resources/read returned no contents: {result:#}"));
    let [item] = contents.as_slice() else {
        panic!("an inbox read must return exactly one item: {result:#}");
    };
    assert_eq!(item["uri"], uri, "the item's address");
    assert_eq!(item["mimeType"], "application/json", "the item's type");
    let text = item["text"]
        .as_str()
        .unwrap_or_else(|| panic!("the inbox item is not text: {item:#}"));
    serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("the inbox item is not JSON ({error}): {text}"))
}

/// The address named by the next resource-update notification on a
/// 2026-07-28 subscription. Any other notification, or the end of the
/// subscription, fails the test.
pub async fn next_update(subscription: &mut Subscription) -> String {
    match subscription.next().await.expect("the subscription stream") {
        Some(ServerNotification::ResourceUpdatedNotification(update)) => update.params.uri,
        Some(other) => panic!("expected a resource update, got {other:?}"),
        None => panic!("the subscription ended: {:?}", subscription.end()),
    }
}

/// Waits for the next notification on a 2026-07-28 subscription and requires
/// it to announce a changed tool list. Any other notification, or the end of
/// the subscription, fails the test.
pub async fn next_tool_change(subscription: &mut Subscription) {
    let next = subscription.next().await;
    match next.expect("the subscription stream") {
        Some(ServerNotification::ToolListChangedNotification(_)) => {}
        Some(other) => panic!("expected a tool-list change, got {other:?}"),
        None => panic!("the subscription ended: {:?}", subscription.end()),
    }
}

/// A tool call still in flight.
pub struct PendingCall {
    tool: String,
    schema: Value,
    token: ProgressToken,
    book: Arc<ProgressBook>,
    response: JoinHandle<Result<ServerResult, ServiceError>>,
}

impl PendingCall {
    /// Waits for the receive's "window open" progress notification, sent once it
    /// is subscribed to commits and has found nothing to deliver. Fails if the
    /// call returns first.
    pub async fn window_open(&mut self) {
        let book = Arc::clone(&self.book);
        let token = self.token.clone();
        tokio::select! {
            progress = book.next(&token) => {
                assert_eq!(
                    progress.message.as_deref(),
                    Some("window open"),
                    "unexpected progress notification {progress:?}"
                );
            }
            finished = &mut self.response => {
                panic!("{} returned before its window opened: {finished:?}", self.tool);
            }
        }
    }

    pub async fn finish(self) -> Outcome {
        let response = self
            .response
            .await
            .expect("tools/call task")
            .expect("tools/call response");
        let ServerResult::CallToolResult(result) = response else {
            panic!("{} was answered with {response:?}", self.tool);
        };
        let result = serde_json::to_value(&result).expect("serialize CallToolResult");
        check_tool_result(&self.tool, &self.schema, &result)
    }
}

/// A tool result that passed the shape checks every result must pass.
#[derive(Debug)]
pub enum Outcome {
    Success(Value),
    Failure(Value),
}

impl Outcome {
    pub fn ok(self) -> Value {
        match self {
            Self::Success(value) => value,
            Self::Failure(error) => panic!("expected success, got the tool error {error:#}"),
        }
    }

    pub fn err(self) -> Value {
        match self {
            Self::Failure(error) => error,
            Self::Success(value) => panic!("expected a tool error, got {value:#}"),
        }
    }
}

/// Checks the result contract: exactly one text block holding one JSON object,
/// the same object as `structuredContent`; successes match the tool's advertised
/// output schema and errors carry a code.
fn check_tool_result(tool: &str, schema: &Value, result: &Value) -> Outcome {
    let content = result["content"]
        .as_array()
        .unwrap_or_else(|| panic!("{tool} result has no content array: {result:#}"));
    assert_eq!(
        content.len(),
        1,
        "{tool} must return exactly one content block: {result:#}"
    );
    assert_eq!(
        content[0]["type"], "text",
        "{tool} content must be text: {result:#}"
    );
    let text = content[0]["text"].as_str().expect("text content");
    let from_text: Value = serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("{tool} text content is not JSON ({error}): {text}"));
    let structured = result
        .get("structuredContent")
        .cloned()
        .unwrap_or_else(|| panic!("{tool} returned no structuredContent: {result:#}"));
    assert_eq!(
        from_text, structured,
        "{tool}: the text content and structuredContent differ"
    );
    if result.get("isError") == Some(&Value::Bool(true)) {
        assert!(
            structured["code"].is_string(),
            "{tool} error without a code: {structured:#}"
        );
        Outcome::Failure(structured)
    } else {
        if let Err(error) = jsonschema::validate(schema, &structured) {
            panic!("{tool} result breaks its advertised output schema: {error}\n{structured:#}");
        }
        Outcome::Success(structured)
    }
}

/// One Server-Sent Event exactly as it crossed the wire.
#[derive(Debug)]
pub struct SseEvent {
    /// Every byte of the event, including its field lines and the blank line
    /// that ends it.
    pub wire_bytes: usize,
    pub fields: Vec<(String, String)>,
}

impl SseEvent {
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|(name, _)| name.as_str()).collect()
    }

    /// The JSON-RPC message this event carries.
    pub fn message(&self) -> Value {
        let data = self
            .data()
            .unwrap_or_else(|| panic!("SSE event without data: {self:?}"));
        serde_json::from_str(&data)
            .unwrap_or_else(|error| panic!("SSE data is not JSON ({error}): {data:?}"))
    }

    fn data(&self) -> Option<String> {
        let lines: Vec<&str> = self
            .fields
            .iter()
            .filter(|(name, _)| name == "data")
            .map(|(_, value)| value.as_str())
            .collect();
        (!lines.is_empty()).then(|| lines.join("\n"))
    }
}

/// A long-lived GET event stream, read event by event as the server sends it.
pub struct EventStream {
    response: reqwest::Response,
    pending: Vec<u8>,
    fields: Vec<(String, String)>,
    event_bytes: usize,
    pub comments: Vec<String>,
}

impl EventStream {
    /// The next complete event, or `None` once the server ends the stream.
    pub async fn next_event(&mut self) -> Option<SseEvent> {
        loop {
            while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
                let raw: Vec<u8> = self.pending.drain(..=end).collect();
                self.event_bytes += raw.len();
                let line = std::str::from_utf8(&raw)
                    .expect("SSE is UTF-8")
                    .trim_end_matches('\n')
                    .trim_end_matches('\r');
                if line.is_empty() {
                    return Some(SseEvent {
                        wire_bytes: std::mem::take(&mut self.event_bytes),
                        fields: std::mem::take(&mut self.fields),
                    });
                }
                if let Some(comment) = line.strip_prefix(':') {
                    self.comments.push(comment.to_owned());
                } else {
                    let (name, value) = line.split_once(':').unwrap_or((line, ""));
                    self.fields.push((
                        name.to_owned(),
                        value.strip_prefix(' ').unwrap_or(value).to_owned(),
                    ));
                }
            }
            match self.response.chunk().await.expect("read the event stream") {
                Some(bytes) => self.pending.extend_from_slice(&bytes),
                None => {
                    assert!(
                        self.pending.is_empty() && self.fields.is_empty(),
                        "the event stream ended inside an event"
                    );
                    return None;
                }
            }
        }
    }

    /// The next event, which must be a resource update; returns its address.
    pub async fn next_update(&mut self) -> String {
        let event = self
            .next_event()
            .await
            .expect("the event stream ended before an update");
        assert_eq!(
            event.field_names(),
            ["data"],
            "priming, resume ID or retry fields on the event stream"
        );
        let message = event.message();
        assert_eq!(
            keys(&message),
            ["jsonrpc", "method", "params"],
            "{message:#}"
        );
        assert_eq!(message["method"], "notifications/resources/updated");
        assert_eq!(keys(&message["params"]), ["uri"], "{message:#}");
        message["params"]["uri"]
            .as_str()
            .expect("the updated address")
            .to_owned()
    }
}

/// A complete HTTP reply from the server.
#[derive(Debug)]
pub struct RawReply {
    pub events: Vec<SseEvent>,
    pub comments: Vec<String>,
    json_body: Option<Value>,
}

impl RawReply {
    pub async fn read(response: reqwest::Response) -> Self {
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response.bytes().await.expect("response body");
        if content_type.starts_with("application/json") {
            return Self {
                events: Vec::new(),
                comments: Vec::new(),
                json_body: Some(serde_json::from_slice(&body).expect("JSON body")),
            };
        }
        assert!(
            content_type.starts_with("text/event-stream"),
            "unexpected content type {content_type:?}"
        );
        let (events, comments) = parse_sse(&body);
        Self {
            events,
            comments,
            json_body: None,
        }
    }

    /// The one JSON-RPC message this reply carries.
    pub fn message(&self) -> Value {
        if let Some(body) = &self.json_body {
            return body.clone();
        }
        let messages: Vec<Value> = self
            .events
            .iter()
            .map(|event| {
                let data = event
                    .data()
                    .unwrap_or_else(|| panic!("SSE event without data: {event:?}"));
                serde_json::from_str(&data)
                    .unwrap_or_else(|error| panic!("SSE data is not JSON ({error}): {data:?}"))
            })
            .collect();
        let [message] = messages.as_slice() else {
            panic!("expected one JSON-RPC message, got {messages:#?}");
        };
        message.clone()
    }
}

fn parse_sse(body: &[u8]) -> (Vec<SseEvent>, Vec<String>) {
    let mut events = Vec::new();
    let mut comments = Vec::new();
    let mut fields = Vec::new();
    let mut wire_bytes = 0;
    let mut rest = body;
    while !rest.is_empty() {
        let end = rest
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(rest.len(), |index| index + 1);
        let (raw_line, tail) = rest.split_at(end);
        rest = tail;
        wire_bytes += raw_line.len();
        let line = std::str::from_utf8(raw_line)
            .expect("SSE is UTF-8")
            .trim_end_matches('\n')
            .trim_end_matches('\r');
        if line.is_empty() {
            events.push(SseEvent {
                wire_bytes,
                fields: std::mem::take(&mut fields),
            });
            wire_bytes = 0;
        } else if let Some(comment) = line.strip_prefix(':') {
            comments.push(comment.to_owned());
        } else {
            let (name, value) = line.split_once(':').unwrap_or((line, ""));
            fields.push((
                name.to_owned(),
                value.strip_prefix(' ').unwrap_or(value).to_owned(),
            ));
        }
    }
    assert!(
        fields.is_empty() && wire_bytes == 0,
        "the SSE body ended inside an event"
    );
    (events, comments)
}

/// A legacy-revision session driven by hand, so tests can inspect the exact
/// bytes the server sends.
pub struct RawSession {
    http: reqwest::Client,
    url: String,
    session_id: String,
    protocol: String,
    next_id: u64,
    capabilities: Value,
    output_schemas: HashMap<String, Value>,
}

impl RawSession {
    pub async fn finish_without_events(&self, events: &mut EventStream) {
        let response = self
            .http
            .delete(&self.url)
            .header("mcp-session-id", &self.session_id)
            .header("mcp-protocol-version", &self.protocol)
            .send()
            .await
            .expect("close the observed MCP session");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        response
            .bytes()
            .await
            .expect("session-close acknowledgement");
        assert!(
            events.next_event().await.is_none(),
            "unexpected notification before stream EOF"
        );
    }

    async fn open(url: &str, protocol: &ProtocolVersion) -> Self {
        let http = reqwest::Client::new();
        let initialize = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": ClientInfo::new(
                ClientCapabilities::default(),
                Implementation::new("minymaton-wire-check", "0"),
            ).with_protocol_version(protocol.clone()),
        });
        let response = http
            .post(url)
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .body(initialize.to_string())
            .send()
            .await
            .expect("POST initialize");
        assert_eq!(response.status(), StatusCode::OK, "initialize status");
        let session_id = response
            .headers()
            .get("mcp-session-id")
            .expect("a legacy initialize reply carries Mcp-Session-Id")
            .to_str()
            .expect("session ID text")
            .to_owned();
        let reply = RawReply::read(response).await.message();
        assert_eq!(reply["id"], 0);
        let negotiated: rmcp::model::InitializeResult =
            serde_json::from_value(reply["result"].clone()).expect("typed MCP initialization");
        assert_eq!(&negotiated.protocol_version, protocol);
        let mut session = Self {
            http,
            url: url.to_owned(),
            session_id,
            protocol: protocol.as_str().to_owned(),
            next_id: 0,
            capabilities: reply["result"]["capabilities"].clone(),
            output_schemas: HashMap::new(),
        };
        let initialized = session
            .post(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await;
        assert_eq!(
            initialized.status(),
            StatusCode::ACCEPTED,
            "initialized status"
        );
        session.list_tools().await;
        session
    }

    /// A tools/list result. Its tools become callable; tools it no longer
    /// shows stay callable, as in a host whose catalog lags behind.
    pub async fn list_tools(&mut self) -> Value {
        let listed = self.request("tools/list", json!({})).await.message();
        self.output_schemas.extend(
            listed["result"]["tools"]
                .as_array()
                .expect("tools/list result")
                .iter()
                .map(|tool| {
                    (
                        tool["name"].as_str().expect("tool name").to_owned(),
                        tool["outputSchema"].clone(),
                    )
                }),
        );
        listed["result"].clone()
    }

    async fn post(&self, message: Value) -> reqwest::Response {
        self.post_via(&self.http, message).await
    }

    async fn post_via(&self, client: &reqwest::Client, message: Value) -> reqwest::Response {
        client
            .post(&self.url)
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .header("mcp-session-id", self.session_id.as_str())
            .header("mcp-protocol-version", self.protocol.as_str())
            .body(message.to_string())
            .send()
            .await
            .expect("POST to minymaton-server")
    }

    async fn request(&mut self, method: &str, params: Value) -> RawReply {
        self.next_id += 1;
        let id = self.next_id;
        let response = self
            .post(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        assert_eq!(response.status(), StatusCode::OK, "{method} status");
        let reply = RawReply::read(response).await;
        assert_eq!(reply.message()["id"], id, "{method} reply ID");
        reply
    }

    pub async fn call_tool(&mut self, tool: &str, arguments: Value) -> (RawReply, Outcome) {
        let reply = self
            .request("tools/call", json!({"name": tool, "arguments": arguments}))
            .await;
        let schema = self
            .output_schemas
            .get(tool)
            .unwrap_or_else(|| panic!("minymaton-server does not list the tool {tool}"))
            .clone();
        let outcome = check_tool_result(tool, &schema, &reply.message()["result"]);
        (reply, outcome)
    }

    /// The server capabilities from the initialize reply.
    pub fn capabilities(&self) -> &Value {
        &self.capabilities
    }

    /// Sends one JSON-RPC request and returns the server's reply message.
    pub async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.request(method, params).await.message()
    }

    /// Posts one JSON-RPC message that expects no reply, such as a response to
    /// a server request, and returns the HTTP status.
    pub async fn post_message(&self, message: Value) -> StatusCode {
        let response = self.post(message).await;
        let status = response.status();
        response.bytes().await.expect("drain the reply");
        status
    }

    /// Opens the session's GET event stream, optionally asking to resume it.
    pub async fn events(&self, last_event_id: Option<&str>) -> EventStream {
        let mut request = self
            .http
            .get(&self.url)
            .header(ACCEPT, "text/event-stream")
            .header("mcp-session-id", self.session_id.as_str())
            .header("mcp-protocol-version", self.protocol.as_str());
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id);
        }
        let response = request.send().await.expect("GET the event stream");
        assert_eq!(response.status(), StatusCode::OK, "event stream status");
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(
            content_type.starts_with("text/event-stream"),
            "unexpected event stream type {content_type:?}"
        );
        EventStream {
            response,
            pending: Vec::new(),
            fields: Vec::new(),
            event_bytes: 0,
            comments: Vec::new(),
        }
    }
}

pub fn receive_args(handle: &str, max_events: u64, max_bytes: u64, max_wait_ms: u64) -> Value {
    json!({
        "handle": handle,
        "max_events": max_events,
        "max_bytes": max_bytes,
        "wait": max_wait_ms != 0,
    })
}

pub fn with_ack(mut arguments: Value, batch_id: &Value) -> Value {
    arguments["ack"] = batch_id.clone();
    arguments
}

pub fn ack_only(handle: &str, batch_id: &Value) -> Value {
    json!({"handle": handle, "max_events": 0, "ack": batch_id})
}

pub fn message_ids(received: &Value) -> Vec<Value> {
    events(received)
        .iter()
        .map(|event| event["message_id"].clone())
        .collect()
}

pub fn payloads(received: &Value) -> Vec<Value> {
    events(received)
        .iter()
        .map(|event| event["payload"].clone())
        .collect()
}

pub fn keys(object: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = object
        .as_object()
        .unwrap_or_else(|| panic!("expected a JSON object: {object:#}"))
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
}

fn events(received: &Value) -> &Vec<Value> {
    received["events"]
        .as_array()
        .unwrap_or_else(|| panic!("no events in {received:#}"))
}
