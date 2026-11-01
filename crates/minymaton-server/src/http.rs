//! Streamable HTTP for minymaton.
//!
//! rmcp serves the MCP exchange with every timer turned off (decision 4b).
//! GET opens a 2025-revision session's event stream, which carries inbox
//! pushes (decision 17). Events carry no resume IDs, and a request to resume a
//! stream replays nothing (decision 12).

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::http::header::WWW_AUTHENTICATE;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post_service;
use futures::{Stream, StreamExt};
use minymaton_core::{
    Access, MAX_CONCURRENT_REQUESTS_PER_PRINCIPAL, MAX_SESSIONS_PER_PRINCIPAL, Queue,
};
use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
use rmcp::transport::WorkerTransport;
use rmcp::transport::streamable_http_server::session::ServerSseMessage;
use rmcp::transport::streamable_http_server::session::local::{
    LocalSessionManager, LocalSessionManagerError, LocalSessionWorker, SessionConfig, SessionError,
};
use rmcp::transport::streamable_http_server::{
    SessionId, SessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::auth::BearerAuthenticator;
use crate::request_log::{Carrier, RequestLog};
use crate::tools::{Agent, DescriptionSpec, Minymaton, Modes};

/// The request log and the body limit rmcp enforces.
struct Logging {
    log: RequestLog,
    limit: usize,
}

/// Authenticated identity and route scope bound to an HTTP request or MCP session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SessionOwner {
    caller: Option<String>,
    channel: Option<String>,
    token_digest: Option<[u8; 32]>,
    agent: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PrincipalKey {
    Anonymous,
    Token([u8; 32]),
    Caller(String),
}

impl SessionOwner {
    fn principal_key(&self) -> PrincipalKey {
        if let Some(digest) = self.token_digest {
            PrincipalKey::Token(digest)
        } else if let Some(caller) = &self.caller {
            PrincipalKey::Caller(caller.clone())
        } else {
            PrincipalKey::Anonymous
        }
    }
}

struct RequestScope {
    owner: SessionOwner,
}

/// Per-token cancellation handle attached to authenticated HTTP requests.
#[derive(Clone)]
pub(crate) struct TokenCancel(pub(crate) CancellationToken);

tokio::task_local! {
    static REQUEST_SCOPE: RequestScope;
}

struct AuthState {
    auth: Option<Arc<BearerAuthenticator>>,
    token_cancels: HashMap<[u8; 32], CancellationToken>,
}

/// Shared authentication and admission state across `/mcp` and `/mcp/agent/{agent}`.
pub(crate) struct AuthController {
    state: Mutex<AuthState>,
    reload_lock: tokio::sync::Mutex<()>,
    principal_sessions: Mutex<HashMap<PrincipalKey, VecDeque<(bool, String)>>>,
    inflight_semaphores: Mutex<HashMap<PrincipalKey, Arc<Semaphore>>>,
    max_sessions_per_principal: usize,
    max_concurrent_requests_per_principal: usize,
    enforce_cluster_enrollment: bool,
    mcp_sessions: OnceSessions,
    agent_sessions: OnceSessions,
}

#[derive(Default)]
struct OnceSessions(Mutex<Option<Arc<NoResumeSessions>>>);

impl OnceSessions {
    fn set(&self, sessions: Arc<NoResumeSessions>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(sessions);
    }

    fn get(&self) -> Option<Arc<NoResumeSessions>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl AuthController {
    pub(crate) fn new(
        auth: Option<Arc<BearerAuthenticator>>,
        max_sessions_per_principal: usize,
        max_concurrent_requests_per_principal: usize,
        enforce_cluster_enrollment: bool,
    ) -> Arc<Self> {
        let mut cancels = HashMap::new();
        if let Some(auth) = &auth {
            for digest in auth.token_digests() {
                cancels.insert(digest, CancellationToken::new());
            }
        }
        Arc::new(Self {
            state: Mutex::new(AuthState {
                auth,
                token_cancels: cancels,
            }),
            reload_lock: tokio::sync::Mutex::new(()),
            principal_sessions: Mutex::new(HashMap::new()),
            inflight_semaphores: Mutex::new(HashMap::new()),
            max_sessions_per_principal,
            max_concurrent_requests_per_principal,
            enforce_cluster_enrollment,
            mcp_sessions: OnceSessions::default(),
            agent_sessions: OnceSessions::default(),
        })
    }

    pub(crate) fn current_auth(&self) -> Option<Arc<BearerAuthenticator>> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .auth
            .clone()
    }

    /// Atomically verifies under a single lock that `base`'s token digest is
    /// still active and uncancelled in the current authenticator, returning
    /// `base` stamped with the current `enrollment_digest` (`Sol RR-01`, `Opus RR2-01`).
    pub(crate) fn refresh_access(
        &self,
        base: &Access,
    ) -> Result<Access, minymaton_core::ToolError> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(auth) = &state.auth else {
            return Ok(base.clone());
        };
        let Some(digest) = base.token_digest else {
            return Err(minymaton_core::ToolError::new(
                minymaton_core::ErrorCode::AuthorityFenced,
                "bearer token or enrollment is no longer active",
            ));
        };
        let Some(cancel) = state.token_cancels.get(&digest) else {
            return Err(minymaton_core::ToolError::new(
                minymaton_core::ErrorCode::AuthorityFenced,
                "bearer token or enrollment is no longer active",
            ));
        };
        if cancel.is_cancelled() {
            return Err(minymaton_core::ToolError::new(
                minymaton_core::ErrorCode::AuthorityFenced,
                "bearer token or enrollment is no longer active",
            ));
        }
        auth.refresh_access(base)
    }

    pub(crate) fn access_refresher(self: &Arc<Self>) -> minymaton_core::AccessRefresher {
        let auth = Arc::clone(self);
        Arc::new(move |base| auth.refresh_access(base))
    }

    fn swap_state(&self, new_auth: &Arc<BearerAuthenticator>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.token_cancels.retain(|digest, token| {
            if new_auth.has_token_digest(digest) {
                true
            } else {
                token.cancel();
                false
            }
        });
        for digest in new_auth.token_digests() {
            match state.token_cancels.get(&digest) {
                Some(existing) if !existing.is_cancelled() => {}
                _ => {
                    state.token_cancels.insert(digest, CancellationToken::new());
                }
            }
        }
        state.auth = Some(Arc::clone(new_auth));
    }

    /// Commits a SQLite enrollment transition and swaps the active
    /// authenticator atomically under the writer store lock and `reload_lock`
    /// so kept tokens never observe a digest mismatch (`Opus RR3-01`, `Sol RR-12`).
    pub(crate) async fn reload_sqlite(
        &self,
        queue: &Queue,
        new_auth: Arc<BearerAuthenticator>,
    ) -> Result<(), String> {
        let _guard = self.reload_lock.lock().await;
        let new_digest = new_auth.enrollment_digest();
        tokio::task::block_in_place(|| {
            queue
                .store_enrollment_with_hook_blocking(
                    new_auth.bound_channels(),
                    new_digest,
                    |stored| {
                        if stored == Some(new_digest) {
                            None
                        } else {
                            self.current_auth().map(|a| a.enrollment_digest())
                        }
                    },
                    || {
                        self.swap_state(&new_auth);
                    },
                )
                .map(|_| ())
                .map_err(|error| error.message)
        })?;
        if let Some(sessions) = self.mcp_sessions.get() {
            sessions.revoke_removed_tokens(&new_auth).await;
        }
        if let Some(sessions) = self.agent_sessions.get() {
            sessions.revoke_removed_tokens(&new_auth).await;
        }
        queue.notify_enrollment(new_digest);
        Ok(())
    }

    /// Fences all active requests, open GET streams, and sessions on this
    /// gateway when cluster enrollment moves away from the gateway's active
    /// authenticator (`O-R4-2`, `Sol RR-10`).
    pub(crate) async fn fence_enrollment_mismatch(&self, queue: &Queue) {
        let _guard = self.reload_lock.lock().await;
        let active = match tokio::task::block_in_place(|| queue.enrollment_digest_blocking()) {
            Ok(Some(digest)) => Some(digest),
            Ok(None) => None,
            Err(_) => queue.active_enrollment(),
        };
        {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let (Some(auth), Some(active)) = (&state.auth, active) else {
                return;
            };
            if auth.enrollment_digest() == active {
                return;
            }
            for token in state.token_cancels.values() {
                token.cancel();
            }
        }
        if let Some(sessions) = self.mcp_sessions.get() {
            sessions.close_all().await;
        }
        if let Some(sessions) = self.agent_sessions.get() {
            sessions.close_all().await;
        }
    }

    fn token_cancel(&self, digest: &[u8; 32]) -> Option<CancellationToken> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .token_cancels
            .get(digest)
            .cloned()
    }

    fn try_acquire_request(&self, key: &PrincipalKey) -> Option<OwnedSemaphorePermit> {
        let sem = {
            let mut map = self
                .inflight_semaphores
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            Arc::clone(map.entry(key.clone()).or_insert_with(|| {
                Arc::new(Semaphore::new(self.max_concurrent_requests_per_principal))
            }))
        };
        sem.try_acquire_owned().ok()
    }

    fn record_session(
        &self,
        key: &PrincipalKey,
        is_agent: bool,
        id: String,
    ) -> Vec<(bool, String)> {
        if self.max_sessions_per_principal == 0 {
            return vec![(is_agent, id)];
        }
        let mut map = self
            .principal_sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let queue = map.entry(key.clone()).or_default();
        queue.push_back((is_agent, id));
        let mut evicted = Vec::new();
        while queue.len() > self.max_sessions_per_principal {
            if let Some(oldest) = queue.pop_front() {
                evicted.push(oldest);
            }
        }
        evicted
    }

    fn forget_session(&self, key: &PrincipalKey, is_agent: bool, id: &str) {
        let mut map = self
            .principal_sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(queue) = map.get_mut(key) {
            queue.retain(|(entry_agent, entry_id)| !(*entry_agent == is_agent && entry_id == id));
            if queue.is_empty() {
                map.remove(key);
            }
        }
    }

    async fn evict_sessions(&self, evicted: Vec<(bool, String)>) {
        for (is_agent, id) in evicted {
            let target = if is_agent {
                self.agent_sessions.get()
            } else {
                self.mcp_sessions.get()
            };
            if let Some(sessions) = target {
                sessions.close_evicted(&id).await;
            }
        }
    }
}

pub(crate) async fn serve(
    listener: TcpListener,
    queue: Queue,
    request_log: Option<RequestLog>,
    description: Option<DescriptionSpec>,
    modes: Modes,
    auth: Arc<AuthController>,
) -> io::Result<()> {
    axum::serve(
        listener,
        router(queue, request_log, description, modes, auth),
    )
    .await
}

fn router(
    queue: Queue,
    request_log: Option<RequestLog>,
    description: Option<DescriptionSpec>,
    modes: Modes,
    auth: Arc<AuthController>,
) -> Router {
    queue.set_access_refresh(auth.access_refresher());
    let handler = Minymaton::new(queue.clone(), description, modes, Arc::clone(&auth));
    let agents = handler.for_agents();
    let config = StreamableHttpServerConfig::default()
        .with_sse_keep_alive(None)
        .with_sse_retry(None);
    let limit = config.max_request_body_bytes;
    let mcp_sessions = Arc::new(sessions(Arc::clone(&auth), false));
    let agent_sessions = Arc::new(sessions(Arc::clone(&auth), true));
    auth.mcp_sessions.set(Arc::clone(&mcp_sessions));
    auth.agent_sessions.set(Arc::clone(&agent_sessions));

    if auth.enforce_cluster_enrollment {
        let mut enrollment_rx = queue.enrollment_signal();
        let _ = enrollment_rx.borrow_and_update();
        let fence_auth = Arc::clone(&auth);
        let fence_queue = queue.clone();
        tokio::spawn(async move {
            while enrollment_rx.changed().await.is_ok() {
                fence_auth.fence_enrollment_mismatch(&fence_queue).await;
            }
        });
    }

    let service = StreamableHttpService::new(
        move || {
            let channel = REQUEST_SCOPE
                .try_with(|scope| scope.owner.channel.clone())
                .ok()
                .flatten();
            Ok(handler.with_channel(channel))
        },
        Arc::clone(&mcp_sessions),
        config.clone(),
    );
    // Its own sessions: a session opened for one agent is not one on `/mcp`.
    let agent_service = StreamableHttpService::new(
        move || {
            let channel = REQUEST_SCOPE
                .try_with(|scope| scope.owner.channel.clone())
                .ok()
                .flatten();
            Ok(agents.with_channel(channel))
        },
        Arc::clone(&agent_sessions),
        config,
    );
    let mcp_gate = GateState {
        auth: Arc::clone(&auth),
        sessions: mcp_sessions,
        queue: Arc::new(queue),
    };
    let agent_gate = GateState {
        auth: Arc::clone(&auth),
        sessions: agent_sessions,
        queue: Arc::clone(&mcp_gate.queue),
    };
    let mut router = Router::new()
        .route(
            "/mcp",
            post_service(service.clone())
                .get_service(service.clone())
                .delete_service(service)
                .route_layer(middleware::from_fn_with_state(mcp_gate, session_gate)),
        )
        .route(
            "/mcp/agent/{agent}",
            post_service(agent_service.clone())
                .get_service(agent_service.clone())
                .delete_service(agent_service)
                .route_layer(middleware::from_fn_with_state(agent_gate, session_gate))
                .route_layer(middleware::from_fn(name_agent)),
        );
    if let Some(log) = request_log {
        router = router.layer(middleware::from_fn_with_state(
            Arc::new(Logging { log, limit }),
            log_messages,
        ));
    }
    router = router.layer(middleware::from_fn_with_state(auth, auth_gate));
    router
}

#[derive(Clone)]
struct GateState {
    auth: Arc<AuthController>,
    sessions: Arc<NoResumeSessions>,
    queue: Arc<Queue>,
}

/// Admits only enrolled bearer tokens when authentication is configured, and
/// hands the handler the authenticated [`Access`] principal.
async fn auth_gate(
    State(controller): State<Arc<AuthController>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(auth) = controller.current_auth() else {
        return next.run(request).await;
    };
    let Some(access) = auth.authenticate(request.headers()) else {
        return (
            StatusCode::UNAUTHORIZED,
            [(WWW_AUTHENTICATE, "Bearer")],
            "missing or unrecognized bearer token",
        )
            .into_response();
    };
    request.extensions_mut().insert(access);
    next.run(request).await
}

/// Enforces cluster enrollment agreement, MCP session ownership (`PC-04`),
/// per-principal session & concurrency limits (`PC-07`), and live token
/// revocation (`O-R4-2`).
async fn session_gate(
    State(gate): State<GateState>,
    mut request: Request,
    next: Next,
) -> Response {
    let current_auth = gate.auth.current_auth();
    if gate.auth.enforce_cluster_enrollment
        && let Some(auth) = &current_auth
        && let Some(active) = gate.queue.active_enrollment()
        && active != auth.enrollment_digest()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway bearer token enrollment does not match active cluster enrollment",
        )
            .into_response();
    }

    let access = request
        .extensions()
        .get::<Access>()
        .cloned()
        .unwrap_or_default();
    let agent = request.extensions().get::<Agent>().map(|a| a.0.clone());
    let owner = SessionOwner {
        caller: access.caller.clone(),
        channel: access.channel.clone(),
        token_digest: access.token_digest,
        agent,
    };

    let token_cancel = if let Some(digest) = &owner.token_digest {
        let Some(cancel) = gate.auth.token_cancel(digest) else {
            return (
                StatusCode::UNAUTHORIZED,
                [(WWW_AUTHENTICATE, "Bearer")],
                "bearer token has been revoked",
            )
                .into_response();
        };
        if cancel.is_cancelled() {
            return (
                StatusCode::UNAUTHORIZED,
                [(WWW_AUTHENTICATE, "Bearer")],
                "bearer token has been revoked",
            )
                .into_response();
        }
        request
            .extensions_mut()
            .insert(TokenCancel(cancel.clone()));
        Some(cancel)
    } else {
        None
    };

    let session_id = request
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if let Some(sid) = &session_id
        && let Some(recorded) = gate.sessions.owner_of(sid)
    {
        if let Some(digest) = &recorded.token_digest
            && current_auth
                .as_ref()
                .is_some_and(|auth| !auth.has_token_digest(digest))
        {
            let _ = gate.sessions.close_session(&Arc::from(sid.as_str())).await;
            return (
                StatusCode::UNAUTHORIZED,
                [(WWW_AUTHENTICATE, "Bearer")],
                "session bearer token has been revoked",
            )
                .into_response();
        }
        if recorded != owner {
            return (
                StatusCode::FORBIDDEN,
                "mcp-session-id belongs to a different principal or agent route",
            )
                .into_response();
        }
    }

    let is_delete = request.method() == http::Method::DELETE;
    let Some(permit) = gate.auth.try_acquire_request(&owner.principal_key()) else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent requests for this principal",
        )
            .into_response();
    };
    let permit = Some(permit);

    let scope = RequestScope { owner };

    let expected_digest = if gate.auth.enforce_cluster_enrollment {
        current_auth.as_ref().map(|a| a.enrollment_digest())
    } else {
        None
    };
    let mut enrollment_rx = gate.queue.enrollment_signal();
    let _ = enrollment_rx.borrow_and_update();

    let run_future = REQUEST_SCOPE.scope(scope, next.run(request));
    let response = if let Some(cancel) = &token_cancel {
        let mut rx = enrollment_rx.clone();
        let queue = Arc::clone(&gate.queue);
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return (
                    StatusCode::UNAUTHORIZED,
                    [(WWW_AUTHENTICATE, "Bearer")],
                    "bearer token has been revoked",
                )
                    .into_response();
            }
            Ok(()) = rx.changed(), if expected_digest.is_some() => {
                if queue.active_enrollment() != expected_digest {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "gateway bearer token enrollment changed",
                    )
                        .into_response();
                }
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "gateway bearer token enrollment changed",
                )
                    .into_response();
            }
            resp = run_future => resp,
        }
    } else {
        run_future.await
    };

    if is_delete && let Some(sid) = &session_id {
        gate.sessions.release_owner(sid);
    }

    let (parts, body) = response.into_parts();
    let body_queue = Arc::clone(&gate.queue);
    let stream = futures::stream::unfold(
        (
            body.into_data_stream(),
            permit,
            token_cancel,
            enrollment_rx,
            expected_digest,
            body_queue,
        ),
        |(mut stream, permit, cancel, mut enrollment_rx, expected_digest, body_queue)| async move {
            let item = if let Some(cancel) = &cancel {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    Ok(()) = enrollment_rx.changed(), if expected_digest.is_some() => {
                        if body_queue.active_enrollment() != expected_digest {
                            None
                        } else {
                            stream.next().await
                        }
                    }
                    next = stream.next() => next,
                }
            } else {
                stream.next().await
            };
            item.map(|chunk| {
                (
                    chunk,
                    (
                        stream,
                        permit,
                        cancel,
                        enrollment_rx,
                        expected_digest,
                        body_queue,
                    ),
                )
            })
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

/// Hands the handler the agent the path names. A name follows the rules for
/// a subscription label, since it becomes one.
async fn name_agent(Path(agent): Path<String>, mut request: Request, next: Next) -> Response {
    if agent.is_empty() || agent.len() > 128 || agent.chars().any(char::is_control) {
        return (
            StatusCode::BAD_REQUEST,
            "an agent name is 1 to 128 bytes, without control characters",
        )
            .into_response();
    }
    request.extensions_mut().insert(Agent(agent));
    next.run(request).await
}

/// Records each POSTed JSON-RPC message, then hands rmcp the same bytes. The
/// record is on disk before the message is handled.
async fn log_messages(
    State(logging): State<Arc<Logging>>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != http::Method::POST {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, logging.limit).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return if error.into_inner().is::<http_body_util::LengthLimitError>() {
                (StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large").into_response()
            } else {
                (
                    StatusCode::BAD_REQUEST,
                    "the request body could not be read",
                )
                    .into_response()
            };
        }
    };
    if let Ok(message) = serde_json::from_slice::<Value>(&bytes) {
        let header = |name: &str| {
            parts
                .headers
                .get(name)
                .and_then(|value| value.to_str().ok())
        };
        let carrier = Carrier {
            session: header("mcp-session-id"),
            revision: header("mcp-protocol-version"),
        };
        if let Err(error) = logging.log.record(&message, &carrier) {
            eprintln!("minymaton-server: cannot write the request log: {error}");
        }
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

fn sessions(auth: Arc<AuthController>, is_agent: bool) -> NoResumeSessions {
    let mut config = SessionConfig::default();
    config.keep_alive = None;
    config.init_timeout = None;
    config.sse_retry = None;
    config.completed_cache_ttl = Duration::ZERO;
    let mut inner = LocalSessionManager::default();
    inner.session_config = config;
    NoResumeSessions {
        inner,
        owners: Mutex::new(HashMap::new()),
        auth,
        is_agent,
    }
}

/// rmcp's in-memory session manager, without the event IDs and retry hints it
/// attaches so that streams can be resumed, and with per-session principal and
/// agent ownership tracking (`PC-04`, `PC-07`).
struct NoResumeSessions {
    inner: LocalSessionManager,
    owners: Mutex<HashMap<String, SessionOwner>>,
    auth: Arc<AuthController>,
    is_agent: bool,
}

impl NoResumeSessions {
    fn owner_of(&self, id: &str) -> Option<SessionOwner> {
        self.owners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    fn release_owner(&self, id: &str) {
        let removed = self
            .owners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
        if let Some(owner) = removed {
            self.auth
                .forget_session(&owner.principal_key(), self.is_agent, id);
        }
    }

    async fn close_evicted(&self, id: &str) {
        self.release_owner(id);
        let _ = self.inner.close_session(&Arc::from(id)).await;
    }

    async fn close_all(&self) {
        let all_ids: Vec<(String, SessionOwner)> = {
            let mut owners = self.owners.lock().unwrap_or_else(PoisonError::into_inner);
            owners.drain().collect()
        };
        for (id, owner) in all_ids {
            self.auth
                .forget_session(&owner.principal_key(), self.is_agent, &id);
            let _ = self.inner.close_session(&Arc::from(id.as_str())).await;
        }
    }

    async fn revoke_removed_tokens(&self, current: &BearerAuthenticator) {
        let revoked_ids: Vec<String> = {
            let owners = self.owners.lock().unwrap_or_else(PoisonError::into_inner);
            owners
                .iter()
                .filter_map(|(id, owner)| {
                    owner
                        .token_digest
                        .filter(|digest| !current.has_token_digest(digest))
                        .map(|_| id.clone())
                })
                .collect()
        };
        for id in revoked_ids {
            self.release_owner(&id);
            let _ = self.inner.close_session(&Arc::from(id.as_str())).await;
        }
    }
}

type Resumeless = fn(ServerSseMessage) -> ServerSseMessage;

fn without_resume_fields(mut message: ServerSseMessage) -> ServerSseMessage {
    message.event_id = None;
    message.retry = None;
    message
}

impl SessionManager for NoResumeSessions {
    type Error = LocalSessionManagerError;
    type Transport = WorkerTransport<LocalSessionWorker>;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let owner = REQUEST_SCOPE
            .try_with(|scope| scope.owner.clone())
            .unwrap_or_default();
        let key = owner.principal_key();
        let (id, transport) = self.inner.create_session().await?;
        self.owners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id.to_string(), owner);
        let evicted = self
            .auth
            .record_session(&key, self.is_agent, id.to_string());
        self.auth.evict_sessions(evicted).await;
        Ok((id, transport))
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.inner.initialize_session(id, message).await
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        self.inner.has_session(id).await
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        self.release_owner(id.as_ref());
        self.inner.close_session(id).await
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self.inner.create_stream(id, message).await?;
        Ok(stream.map(without_resume_fields as Resumeless))
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner.accept_message(id, message).await
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self.inner.create_standalone_stream(id).await?;
        Ok(stream.map(without_resume_fields as Resumeless))
    }

    /// No stream is resumable: no event IDs are issued, so every one a client
    /// sends is invalid. rmcp answers with an empty stream that ends at once.
    async fn resume(
        &self,
        _id: &SessionId,
        _last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        Err::<futures::stream::Empty<ServerSseMessage>, _>(LocalSessionManagerError::SessionError(
            SessionError::InvalidEventId,
        ))
    }
}

pub(crate) fn default_auth_controller() -> Arc<AuthController> {
    AuthController::new(
        None,
        MAX_SESSIONS_PER_PRINCIPAL,
        MAX_CONCURRENT_REQUESTS_PER_PRINCIPAL,
        false,
    )
}
