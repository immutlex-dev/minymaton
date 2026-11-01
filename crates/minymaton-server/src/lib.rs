//! minymaton's MCP server: the queue's tools over Streamable HTTP, on the
//! loopback interface only.

mod auth;
mod http;
mod request_log;
mod tools;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use minymaton_core::{
    Access, Limits, MAX_CONCURRENT_REQUESTS_PER_PRINCIPAL, MAX_SESSIONS_PER_PRINCIPAL, Queue,
    RESULT_LIMIT,
};

use crate::auth::BearerAuthenticator;
use crate::http::AuthController;
use crate::request_log::RequestLog;

const USAGE: &str = "usage: minymaton-server --listen 127.0.0.1:PORT --db PATH [--request-log FILE] [--description-stream NAME --description-subscription LABEL] [--delivery description|tools|description,tools] [--batch-bytes N] [--bearer-token-file FILE] [--max-retained-events-per-channel N] [--max-subscriptions-per-channel N] [--max-bound-channels N] [--max-enrolled-tokens N] [--max-sessions-per-principal N] [--max-concurrent-requests-per-principal N]
       minymaton-server --retire-channel CHANNEL --db PATH
       minymaton-server --activate-enrollment FILE [--expected-enrollment-digest HEX] --db PATH";

/// Serves an already opened queue on a bound loopback listener, with the base
/// tools and no authentication or configured delivery binding. The caller owns
/// queue initialization and can observe its event-driven wait lifecycle.
pub async fn serve(listener: tokio::net::TcpListener, queue: Queue) -> std::io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "minymaton requires a loopback listener",
        ));
    }
    http::serve(
        listener,
        queue,
        None,
        None,
        tools::Modes {
            description: false,
            tools: false,
            batch_bytes: RESULT_LIMIT,
        },
        http::default_auth_controller(),
    )
    .await
}

/// Runs the server until it fails. Once it is accepting connections it prints
/// `listening on http://ADDRESS/mcp` as its first line on standard output.
pub async fn run(arguments: impl IntoIterator<Item = String>) -> Result<(), String> {
    let options = match Options::parse(arguments)? {
        Command::Serve(options) => options,
        Command::RetireChannel { channel, db } => {
            return retire_channel(channel, db).await;
        }
        Command::ActivateEnrollment {
            token_file,
            expected_digest,
            limits,
            db,
        } => {
            return activate_enrollment(token_file, expected_digest, limits, db).await;
        }
    };
    if !options.listen.ip().is_loopback() {
        return Err(format!(
            "refusing to listen on {}: this server only listens on the loopback interface (127.0.0.1)",
            options.listen
        ));
    }
    let bearer_auth = options
        .bearer_token_file
        .as_deref()
        .map(|path| {
            BearerAuthenticator::load(
                path,
                options.limits.max_enrolled_tokens,
                options.limits.max_bound_channels,
            )
        })
        .transpose()?
        .map(Arc::new);
    let queue = Queue::open_with_limits(&options.db, options.limits)?;
    let anon = Access::default();
    match queue
        .stream(
            serde_json::json!({"action": "create", "name": minymaton_core::api::DEFAULT_STREAM}),
            &anon,
        )
        .await
    {
        Ok(_) => {}
        Err(error) if error.code == minymaton_core::ErrorCode::StreamExists => {}
        Err(error) => return Err(format!("cannot prepare shared stream: {}", error.message)),
    }
    if let Some(auth) = &bearer_auth {
        let active = queue
            .enrollment_digest_blocking()
            .map_err(|error| format!("cannot read enrollment digest: {}", error.message))?;
        queue
            .activate_enrollment_blocking(
                auth.bound_channels(),
                auth.enrollment_digest(),
                active,
            )
            .map_err(|error| format!("cannot provision bound channels: {}", error.message))?;
    }
    let request_log = options
        .request_log
        .as_deref()
        .map(RequestLog::open)
        .transpose()?;
    let description = match &options.description {
        Some((stream, label)) => {
            let (stream_id, subscription_id, handle) = queue
                .prepare_description(stream, label, &anon)
                .await
                .map_err(|error| {
                    format!("cannot prepare description subscription: {}", error.message)
                })?;
            Some(tools::DescriptionSpec {
                label: label.clone(),
                default_binding: tools::Binding {
                    stream_id,
                    subscription_id,
                    stream: stream.clone(),
                    label: label.clone(),
                    handle,
                },
            })
        }
        None => None,
    };
    let modes = tools::Modes {
        description: options.description_delivery,
        tools: options.tool_messages,
        batch_bytes: options.batch_bytes,
    };
    let auth_controller = AuthController::new(
        bearer_auth,
        options.max_sessions_per_principal,
        options.max_concurrent_requests_per_principal,
        false,
    );
    #[cfg(unix)]
    if let Some(token_path) = options.bearer_token_file.clone() {
        spawn_sighup_reload(
            token_path,
            options.limits,
            queue.clone(),
            Arc::clone(&auth_controller),
        )?;
    }
    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .map_err(|error| format!("cannot listen on {}: {error}", options.listen))?;
    let address = listener
        .local_addr()
        .map_err(|error| format!("cannot read the listening address: {error}"))?;
    println!("listening on http://{address}/mcp");
    http::serve(
        listener,
        queue,
        request_log,
        description,
        modes,
        auth_controller,
    )
    .await
    .map_err(|error| format!("the server stopped: {error}"))
}

#[cfg(unix)]
fn spawn_sighup_reload(
    token_path: PathBuf,
    limits: Limits,
    queue: Queue,
    auth_controller: Arc<AuthController>,
) -> Result<(), String> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut hup = signal(SignalKind::hangup())
        .map_err(|error| format!("cannot register SIGHUP handler: {error}"))?;
    tokio::spawn(async move {
        while hup.recv().await.is_some() {
            let new_auth = match BearerAuthenticator::load(
                &token_path,
                limits.max_enrolled_tokens,
                limits.max_bound_channels,
            ) {
                Ok(auth) => Arc::new(auth),
                Err(error) => {
                    eprintln!("minymaton-server: SIGHUP token reload rejected: {error}");
                    continue;
                }
            };
            if let Some(current) = auth_controller.current_auth()
                && let Err(error) = current.check_reload_transition(&new_auth)
            {
                eprintln!("minymaton-server: SIGHUP token reload rejected: {error}");
                continue;
            }
            match auth_controller.reload_sqlite(&queue, new_auth).await {
                Ok(()) => {
                    eprintln!("minymaton-server: reloaded bearer tokens");
                }
                Err(error) => {
                    eprintln!("minymaton-server: SIGHUP token reload failed: {error}");
                }
            }
        }
    });
    Ok(())
}

async fn retire_channel(channel: String, db: PathBuf) -> Result<(), String> {
    let queue = Queue::open(&db)?;
    queue
        .delete_stream(&channel)
        .await
        .map_err(|error| format!("cannot retire channel {channel:?}: {}", error.message))?;
    println!("retired channel {channel}");
    Ok(())
}

async fn activate_enrollment(
    token_file: PathBuf,
    expected_digest: Option<[u8; 32]>,
    limits: Limits,
    db: PathBuf,
) -> Result<(), String> {
    let auth = BearerAuthenticator::load(
        &token_file,
        limits.max_enrolled_tokens,
        limits.max_bound_channels,
    )?;
    let queue = Queue::open_with_limits(&db, limits)?;
    let anon = Access::default();
    match queue
        .stream(
            serde_json::json!({"action": "create", "name": minymaton_core::api::DEFAULT_STREAM}),
            &anon,
        )
        .await
    {
        Ok(_) => {}
        Err(error) if error.code == minymaton_core::ErrorCode::StreamExists => {}
        Err(error) => return Err(format!("cannot prepare shared stream: {}", error.message)),
    }
    tokio::task::block_in_place(|| {
        queue.activate_enrollment_blocking(
            auth.bound_channels(),
            auth.enrollment_digest(),
            expected_digest,
        )
    })
    .map_err(|error| format!("cannot activate enrollment: {}", error.message))?;
    println!("{}", hex_encode(&auth.enrollment_digest()));
    Ok(())
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn hex_decode_32(hex: &str) -> Result<[u8; 32], String> {
    if hex.len() != 64 {
        return Err(format!(
            "expected a 64-character hex SHA-256 digest, got {} characters",
            hex.len()
        ));
    }
    let mut out = [0u8; 32];
    for (index, slot) in out.iter_mut().enumerate() {
        let pair = &hex[index * 2..index * 2 + 2];
        *slot = u8::from_str_radix(pair, 16)
            .map_err(|_| format!("invalid hex byte {pair:?} in digest"))?;
    }
    Ok(out)
}

/// What the command line asks for.
enum Command {
    Serve(Options),
    /// `--retire-channel CHANNEL`: delete a channel stream and all of its
    /// durable state, then exit.
    RetireChannel {
        channel: String,
        db: PathBuf,
    },
    /// `--activate-enrollment FILE`: validate a bearer token file, provision
    /// its bound channels, and conditionally update the active enrollment
    /// digest, then exit.
    ActivateEnrollment {
        token_file: PathBuf,
        expected_digest: Option<[u8; 32]>,
        limits: Limits,
        db: PathBuf,
    },
}

struct Options {
    listen: SocketAddr,
    db: PathBuf,
    /// Where to record every incoming JSON-RPC message, if anywhere.
    request_log: Option<PathBuf>,
    /// The named stream and subscription whose pending batch the receive
    /// description carries (decision 21).
    description: Option<(String, String)>,
    /// `--delivery description`, the default: the receive description carries
    /// the pending batch and `ack` acknowledges it.
    description_delivery: bool,
    /// `--delivery tools`: each pending batch is its own tool.
    tool_messages: bool,
    /// `--batch-bytes`: the largest batch the description seals. Hosts that
    /// show a new tool only on the next turn get a whole backlog at once.
    batch_bytes: u64,
    /// Optional file of enrolled bearer tokens (`CALLER\tTOKEN[\tCHANNEL]` or `TOKEN`).
    bearer_token_file: Option<PathBuf>,
    limits: Limits,
    max_sessions_per_principal: usize,
    max_concurrent_requests_per_principal: usize,
}

impl Options {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Command, String> {
        let mut listen = None;
        let mut db = None;
        let mut retire_channel = None;
        let mut activate_enrollment = None;
        let mut expected_enrollment_digest = None;
        let mut request_log = None;
        let mut description_stream = None;
        let mut description_subscription = None;
        let mut delivery = None;
        let mut batch_bytes = None;
        let mut bearer_token_file = None;
        let mut limits = Limits::default();
        let mut max_sessions_per_principal = MAX_SESSIONS_PER_PRINCIPAL;
        let mut max_concurrent_requests_per_principal = MAX_CONCURRENT_REQUESTS_PER_PRINCIPAL;
        let mut arguments = arguments.into_iter();
        while let Some(flag) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
            match flag.as_str() {
                "--listen" => {
                    listen = Some(value.parse::<SocketAddr>().map_err(|error| {
                        format!("--listen {value:?} is not an address and port: {error}")
                    })?);
                }
                "--db" => db = Some(PathBuf::from(value)),
                "--retire-channel" => retire_channel = Some(value),
                "--activate-enrollment" => activate_enrollment = Some(PathBuf::from(value)),
                "--expected-enrollment-digest" => {
                    expected_enrollment_digest = Some(hex_decode_32(&value)?);
                }
                "--request-log" => request_log = Some(PathBuf::from(value)),
                "--description-stream" => description_stream = Some(value),
                "--description-subscription" => description_subscription = Some(value),
                "--delivery" => {
                    let mut modes = (false, false);
                    for mode in value.split(',') {
                        let seen = match mode {
                            "description" => &mut modes.0,
                            "tools" => &mut modes.1,
                            other => {
                                return Err(format!(
                                    "--delivery {value:?}: {other:?} is neither description nor tools\n{USAGE}"
                                ));
                            }
                        };
                        if std::mem::replace(seen, true) {
                            return Err(format!(
                                "--delivery {value:?} names {mode} twice\n{USAGE}"
                            ));
                        }
                    }
                    delivery = Some(modes);
                }
                "--batch-bytes" => {
                    let bytes = value.parse::<u64>().map_err(|error| {
                        format!("--batch-bytes {value:?} is not a byte count: {error}")
                    })?;
                    if bytes < RESULT_LIMIT {
                        return Err(format!(
                            "--batch-bytes {bytes} is below {RESULT_LIMIT}, the size one message may reach\n{USAGE}"
                        ));
                    }
                    batch_bytes = Some(bytes);
                }
                "--bearer-token-file" => bearer_token_file = Some(PathBuf::from(value)),
                "--max-retained-events-per-channel" => {
                    let n = value.parse::<u64>().map_err(|error| {
                        format!("--max-retained-events-per-channel {value:?} is invalid: {error}")
                    })?;
                    if n == 0 {
                        return Err("--max-retained-events-per-channel must be at least 1".to_owned());
                    }
                    limits.max_retained_events_per_channel = n;
                }
                "--max-subscriptions-per-channel" => {
                    let n = value.parse::<u64>().map_err(|error| {
                        format!("--max-subscriptions-per-channel {value:?} is invalid: {error}")
                    })?;
                    if n == 0 {
                        return Err("--max-subscriptions-per-channel must be at least 1".to_owned());
                    }
                    limits.max_subscriptions_per_channel = n;
                }
                "--max-bound-channels" => {
                    let n = value.parse::<usize>().map_err(|error| {
                        format!("--max-bound-channels {value:?} is invalid: {error}")
                    })?;
                    if n == 0 {
                        return Err("--max-bound-channels must be at least 1".to_owned());
                    }
                    limits.max_bound_channels = n;
                }
                "--max-enrolled-tokens" => {
                    let n = value.parse::<usize>().map_err(|error| {
                        format!("--max-enrolled-tokens {value:?} is invalid: {error}")
                    })?;
                    if n == 0 {
                        return Err("--max-enrolled-tokens must be at least 1".to_owned());
                    }
                    limits.max_enrolled_tokens = n;
                }
                "--max-sessions-per-principal" => {
                    let n = value.parse::<usize>().map_err(|error| {
                        format!("--max-sessions-per-principal {value:?} is invalid: {error}")
                    })?;
                    if n == 0 {
                        return Err("--max-sessions-per-principal must be at least 1".to_owned());
                    }
                    max_sessions_per_principal = n;
                }
                "--max-concurrent-requests-per-principal" => {
                    let n = value.parse::<usize>().map_err(|error| {
                        format!("--max-concurrent-requests-per-principal {value:?} is invalid: {error}")
                    })?;
                    if n == 0 {
                        return Err("--max-concurrent-requests-per-principal must be at least 1".to_owned());
                    }
                    max_concurrent_requests_per_principal = n;
                }
                other => return Err(format!("unknown argument {other:?}\n{USAGE}")),
            }
        }
        let description = match (description_stream, description_subscription) {
            (None, None) => None,
            (Some(stream), Some(label)) => Some((stream, label)),
            _ => {
                return Err(format!(
                    "description stream and subscription must be supplied together\n{USAGE}"
                ));
            }
        };
        let db = db.ok_or_else(|| format!("--db is required\n{USAGE}"))?;
        if let Some(channel) = retire_channel {
            if listen.is_some()
                || request_log.is_some()
                || description.is_some()
                || delivery.is_some()
                || batch_bytes.is_some()
                || bearer_token_file.is_some()
                || activate_enrollment.is_some()
                || expected_enrollment_digest.is_some()
            {
                return Err(format!(
                    "--retire-channel deletes a channel and exits; it takes only --db\n{USAGE}"
                ));
            }
            return Ok(Command::RetireChannel { channel, db });
        }
        if let Some(token_file) = activate_enrollment {
            if listen.is_some()
                || request_log.is_some()
                || description.is_some()
                || delivery.is_some()
                || batch_bytes.is_some()
                || bearer_token_file.is_some()
            {
                return Err(format!(
                    "--activate-enrollment activates enrollment and exits; it takes only --expected-enrollment-digest and --db\n{USAGE}"
                ));
            }
            return Ok(Command::ActivateEnrollment {
                token_file,
                expected_digest: expected_enrollment_digest,
                limits,
                db,
            });
        }
        if expected_enrollment_digest.is_some() {
            return Err(format!(
                "--expected-enrollment-digest requires --activate-enrollment\n{USAGE}"
            ));
        }
        let (description_delivery, tool_messages) = delivery.unwrap_or((true, false));
        Ok(Command::Serve(Self {
            listen: listen.ok_or_else(|| format!("--listen is required\n{USAGE}"))?,
            db,
            request_log,
            description,
            description_delivery,
            tool_messages,
            batch_bytes: batch_bytes.unwrap_or(RESULT_LIMIT),
            bearer_token_file,
            limits,
            max_sessions_per_principal,
            max_concurrent_requests_per_principal,
        }))
    }
}
