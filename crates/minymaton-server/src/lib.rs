//! minymaton's MCP server: the queue's tools over Streamable HTTP, on the
//! loopback interface only.

mod auth;
mod http;
mod request_log;
mod tools;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use minymaton_core::{Queue, RESULT_LIMIT};

use crate::auth::BearerAuthenticator;
use crate::request_log::RequestLog;

const USAGE: &str = "usage: minymaton-server --listen 127.0.0.1:PORT --db PATH [--request-log FILE] [--description-stream NAME --description-subscription LABEL] [--delivery description|tools|description,tools] [--batch-bytes N] [--bearer-token-file FILE]";

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
        None,
    )
    .await
}

/// Runs the server until it fails. Once it is accepting connections it prints
/// `listening on http://ADDRESS/mcp` as its first line on standard output.
pub async fn run(arguments: impl IntoIterator<Item = String>) -> Result<(), String> {
    let options = Options::parse(arguments)?;
    if !options.listen.ip().is_loopback() {
        return Err(format!(
            "refusing to listen on {}: this server only listens on the loopback interface (127.0.0.1)",
            options.listen
        ));
    }
    let bearer_auth = options
        .bearer_token_file
        .as_deref()
        .map(BearerAuthenticator::load)
        .transpose()?
        .map(Arc::new);
    let queue = Queue::open(&options.db)?;
    match queue
        .stream(
            serde_json::json!({"action": "create", "name": minymaton_core::api::DEFAULT_STREAM}),
        )
        .await
    {
        Ok(_) => {}
        Err(error) if error.code == minymaton_core::ErrorCode::StreamExists => {}
        Err(error) => return Err(format!("cannot prepare shared stream: {}", error.message)),
    }
    let request_log = options
        .request_log
        .as_deref()
        .map(RequestLog::open)
        .transpose()?;
    let description = match &options.description {
        Some((stream, label)) => {
            let handle = queue
                .prepare_description(stream, label)
                .await
                .map_err(|error| {
                    format!("cannot prepare description subscription: {}", error.message)
                })?;
            Some(tools::Binding {
                stream: stream.clone(),
                label: label.clone(),
                handle,
            })
        }
        None => None,
    };
    let modes = tools::Modes {
        description: options.description_delivery,
        tools: options.tool_messages,
        batch_bytes: options.batch_bytes,
    };
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
        bearer_auth,
    )
    .await
    .map_err(|error| format!("the server stopped: {error}"))
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
    /// Optional file of enrolled bearer tokens (`CALLER\tTOKEN` or `TOKEN`).
    bearer_token_file: Option<PathBuf>,
}

impl Options {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut listen = None;
        let mut db = None;
        let mut request_log = None;
        let mut description_stream = None;
        let mut description_subscription = None;
        let mut delivery = None;
        let mut batch_bytes = None;
        let mut bearer_token_file = None;
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
        let (description_delivery, tool_messages) = delivery.unwrap_or((true, false));
        Ok(Self {
            listen: listen.ok_or_else(|| format!("--listen is required\n{USAGE}"))?,
            db: db.ok_or_else(|| format!("--db is required\n{USAGE}"))?,
            request_log,
            description,
            description_delivery,
            tool_messages,
            batch_bytes: batch_bytes.unwrap_or(RESULT_LIMIT),
            bearer_token_file,
        })
    }
}
