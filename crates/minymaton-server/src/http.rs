//! Streamable HTTP for minymaton.
//!
//! rmcp serves the MCP exchange with every timer turned off (decision 4b).
//! GET opens a 2025-revision session's event stream, which carries inbox
//! pushes (decision 17). Events carry no resume IDs, and a request to resume a
//! stream replays nothing (decision 12).

use std::io;
use std::sync::Arc;
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
use minymaton_core::Queue;
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

use crate::auth::BearerAuthenticator;
use crate::request_log::{Carrier, RequestLog};
use crate::tools::{Agent, Binding, Minymaton, Modes};

/// The request log and the body limit rmcp enforces.
struct Logging {
    log: RequestLog,
    limit: usize,
}

pub(crate) async fn serve(
    listener: TcpListener,
    queue: Queue,
    request_log: Option<RequestLog>,
    description: Option<Binding>,
    modes: Modes,
    bearer_auth: Option<Arc<BearerAuthenticator>>,
) -> io::Result<()> {
    axum::serve(
        listener,
        router(queue, request_log, description, modes, bearer_auth),
    )
    .await
}

fn router(
    queue: Queue,
    request_log: Option<RequestLog>,
    description: Option<Binding>,
    modes: Modes,
    bearer_auth: Option<Arc<BearerAuthenticator>>,
) -> Router {
    let handler = Minymaton::new(queue, description, modes);
    let agents = handler.for_agents();
    let config = StreamableHttpServerConfig::default()
        .with_sse_keep_alive(None)
        .with_sse_retry(None);
    let limit = config.max_request_body_bytes;
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(sessions()),
        config.clone(),
    );
    // Its own sessions: a session opened for one agent is not one on `/mcp`.
    let agent_service =
        StreamableHttpService::new(move || Ok(agents.clone()), Arc::new(sessions()), config);
    let mut router = Router::new()
        .route(
            "/mcp",
            post_service(service.clone())
                .get_service(service.clone())
                .delete_service(service),
        )
        .route(
            "/mcp/agent/{agent}",
            post_service(agent_service.clone())
                .get_service(agent_service.clone())
                .delete_service(agent_service)
                .route_layer(middleware::from_fn(name_agent)),
        );
    if let Some(log) = request_log {
        router = router.layer(middleware::from_fn_with_state(
            Arc::new(Logging { log, limit }),
            log_messages,
        ));
    }
    if let Some(auth) = bearer_auth {
        router = router.layer(middleware::from_fn_with_state(auth, auth_gate));
    }
    router
}

async fn auth_gate(
    State(auth): State<Arc<BearerAuthenticator>>,
    request: Request,
    next: Next,
) -> Response {
    if auth.authenticate(request.headers()).is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            [(WWW_AUTHENTICATE, "Bearer")],
            "missing or unrecognized bearer token",
        )
            .into_response();
    }
    next.run(request).await
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

fn sessions() -> NoResumeSessions {
    let mut config = SessionConfig::default();
    config.keep_alive = None;
    config.init_timeout = None;
    config.sse_retry = None;
    config.completed_cache_ttl = Duration::ZERO;
    let mut inner = LocalSessionManager::default();
    inner.session_config = config;
    NoResumeSessions { inner }
}

/// rmcp's in-memory session manager, without the event IDs and retry hints it
/// attaches so that streams can be resumed.
struct NoResumeSessions {
    inner: LocalSessionManager,
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
        self.inner.create_session().await
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
