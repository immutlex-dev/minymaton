//! DRY CLI subcommands `minymaton stream ...` and `minymaton subscription ...`.
//! Dispatches over `--url` (sessionless 2026-07-28 MCP `tools/call`) or `--db`
//! (local SQLite `Queue`) and renders exclusively through `minymaton_core`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use http::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use minymaton_core::api::StartAt;
use minymaton_core::{
    Access, OutputFormat, Queue, StreamArgs, StreamResult, SubscriptionArgs, SubscriptionResult,
    ToolError, render_stream_to_string, render_subscription_to_string, stream_to_string,
    subscription_to_string,
};
use serde_json::Value;

use crate::mcp_http::{extract_tool_structured, post_tools_call};

const CLI_USAGE: &str =
    "usage: minymaton stream <create|grant|revoke|delete|delete-bulk|sweep-idle|list> [OPTIONS]
       minymaton subscription <create|recover|delete|delete-bulk|sweep-idle|list> [OPTIONS]

Targets (exactly one required, or MINYMATON_URL):
  --url URL            Live MCP endpoint URL
  --db PATH            Local SQLite database (when server is stopped)
  --token-env VAR      Bearer token env var for --url (default: MINYMATON_TOKEN)
  --ca-file PATH       Additional PEM CA bundle for --url

Output:
  --format FORMAT      json (default), text, or csv";

enum Target {
    Url {
        url: String,
        token_env: Option<String>,
        ca_file: Option<PathBuf>,
    },
    Db(PathBuf),
}

struct CommonOptions {
    target: Target,
    format: OutputFormat,
}

pub(crate) async fn run(args: Vec<String>) -> Result<(), String> {
    let mut iter = args.into_iter();
    let Some(group) = iter.next() else {
        return Err(CLI_USAGE.to_owned());
    };
    let rest: Vec<String> = iter.collect();
    if rest.is_empty() {
        return Err(format!(
            "missing action for {group}
{CLI_USAGE}"
        ));
    }
    if rest[0] == "-h" || rest[0] == "--help" {
        println!("{CLI_USAGE}");
        return Ok(());
    }
    let action = rest[0].clone();
    let action_args = &rest[1..];
    if action_args.iter().any(|arg| arg == "-h" || arg == "--help") {
        println!("{CLI_USAGE}");
        return Ok(());
    }

    match group.as_str() {
        "stream" => {
            let (args, common) = parse_stream(&action, action_args)?;
            let out = match common.target {
                Target::Db(db) => {
                    let queue = Queue::open(&db)?;
                    stream_to_string(&queue, args, &Access::default(), common.format)
                        .await
                        .map_err(format_tool_error)?
                }
                Target::Url {
                    url,
                    token_env,
                    ca_file,
                } => {
                    let headers = auth_headers(token_env.as_deref())?;
                    let response = post_tools_call(
                        &url,
                        headers,
                        "stream",
                        "minymaton-cli",
                        &args,
                        ca_file.as_deref(),
                    )
                    .await
                    .map_err(|error| error.message().to_owned())?;
                    let (structured, is_error) = extract_tool_structured(&response, "stream")
                        .map_err(|error| error.message().to_owned())?;
                    if is_error {
                        return Err(format_remote_error(&structured));
                    }
                    let result: StreamResult = serde_json::from_value(structured)
                        .map_err(|error| format!("unexpected stream result: {error}"))?;
                    render_stream_to_string(&result, common.format)
                }
            };
            if !out.is_empty() {
                println!("{out}");
            }
            Ok(())
        }
        "subscription" => {
            let (args, common) = parse_subscription(&action, action_args)?;
            let out = match common.target {
                Target::Db(db) => {
                    let queue = Queue::open(&db)?;
                    subscription_to_string(&queue, args, &Access::default(), common.format)
                        .await
                        .map_err(format_tool_error)?
                }
                Target::Url {
                    url,
                    token_env,
                    ca_file,
                } => {
                    let headers = auth_headers(token_env.as_deref())?;
                    let response = post_tools_call(
                        &url,
                        headers,
                        "subscription",
                        "minymaton-cli",
                        &args,
                        ca_file.as_deref(),
                    )
                    .await
                    .map_err(|error| error.message().to_owned())?;
                    let (structured, is_error) = extract_tool_structured(&response, "subscription")
                        .map_err(|error| error.message().to_owned())?;
                    if is_error {
                        return Err(format_remote_error(&structured));
                    }
                    let result: SubscriptionResult = serde_json::from_value(structured)
                        .map_err(|error| format!("unexpected subscription result: {error}"))?;
                    render_subscription_to_string(&result, common.format)
                }
            };
            if !out.is_empty() {
                println!("{out}");
            }
            Ok(())
        }
        other => Err(format!(
            "unknown subcommand {other:?}
{CLI_USAGE}"
        )),
    }
}

fn format_tool_error(error: ToolError) -> String {
    let code = serde_json::to_value(error.code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "tool_error".to_owned());
    format!("{code}: {}", error.message)
}

fn format_remote_error(structured: &Value) -> String {
    let code = structured["code"].as_str().unwrap_or("tool_error");
    let message = structured["message"].as_str().unwrap_or("request failed");
    format!("{code}: {message}")
}

fn auth_headers(explicit_token_env: Option<&str>) -> Result<HeaderMap, String> {
    let token = match explicit_token_env {
        Some(var_name) => {
            let var_name = var_name.trim();
            if var_name.is_empty() {
                return Err("--token-env cannot be empty".to_owned());
            }
            match std::env::var(var_name) {
                Ok(val) if !val.is_empty() => Some(val),
                Ok(_) | Err(std::env::VarError::NotPresent) => {
                    return Err(format!(
                        "environment variable {var_name} specified by --token-env is not set or is empty"
                    ));
                }
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(format!("{var_name} is not valid Unicode"));
                }
            }
        }
        None => match std::env::var("MINYMATON_TOKEN") {
            Ok(val) if !val.is_empty() => Some(val),
            Ok(_) | Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("MINYMATON_TOKEN is not valid Unicode".to_owned());
            }
        },
    };
    let mut headers = HeaderMap::new();
    if let Some(token) = token {
        let auth = if token.starts_with("Bearer ") {
            token
        } else {
            format!("Bearer {token}")
        };
        let mut value =
            HeaderValue::from_str(&auth).map_err(|_| "invalid Authorization header".to_owned())?;
        value.set_sensitive(true);
        headers.insert(AUTHORIZATION, value);
    }
    Ok(headers)
}

#[derive(Default)]
struct RawFlags {
    url: Option<String>,
    db: Option<PathBuf>,
    token_env: Option<String>,
    ca_file: Option<PathBuf>,
    format: Option<OutputFormat>,
    stream: Option<String>,
    name: Option<String>,
    names: Vec<String>,
    caller: Option<String>,
    members: Vec<String>,
    label: Option<String>,
    labels: Vec<String>,
    keep: Vec<String>,
    after: Option<String>,
    limit: Option<u64>,
    start_at: Option<StartAt>,
    metadata: Option<BTreeMap<String, Value>>,
    positionals: Vec<String>,
}

fn next_value(
    flag: &str,
    inline: Option<String>,
    iter: &mut std::slice::Iter<'_, String>,
) -> Result<String, String> {
    if let Some(value) = inline {
        return Ok(value);
    }
    iter.next()
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_flags(args: &[String]) -> Result<(RawFlags, CommonOptions), String> {
    let mut raw = RawFlags::default();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if !arg.starts_with("--") {
            raw.positionals.push(arg.clone());
            continue;
        }
        let (flag, inline) = match arg.split_once('=') {
            Some((k, v)) => (k, Some(v.to_owned())),
            None => (arg.as_str(), None),
        };
        match flag {
            "--url" => raw.url = Some(next_value(flag, inline, &mut iter)?),
            "--db" => raw.db = Some(PathBuf::from(next_value(flag, inline, &mut iter)?)),
            "--token-env" => raw.token_env = Some(next_value(flag, inline, &mut iter)?),
            "--ca-file" => raw.ca_file = Some(PathBuf::from(next_value(flag, inline, &mut iter)?)),
            "--format" => {
                let value = next_value(flag, inline, &mut iter)?;
                raw.format = Some(value.parse::<OutputFormat>()?);
            }
            "--stream" => raw.stream = Some(next_value(flag, inline, &mut iter)?),
            "--name" => {
                let value = next_value(flag, inline, &mut iter)?;
                if raw.name.is_none() {
                    raw.name = Some(value.clone());
                }
                raw.names.push(value);
            }
            "--caller" => raw.caller = Some(next_value(flag, inline, &mut iter)?),
            "--member" => raw.members.push(next_value(flag, inline, &mut iter)?),
            "--label" => {
                let value = next_value(flag, inline, &mut iter)?;
                if raw.label.is_none() {
                    raw.label = Some(value.clone());
                }
                raw.labels.push(value);
            }
            "--keep" => raw.keep.push(next_value(flag, inline, &mut iter)?),
            "--after" => raw.after = Some(next_value(flag, inline, &mut iter)?),
            "--limit" => {
                let value = next_value(flag, inline, &mut iter)?;
                raw.limit = Some(
                    value
                        .parse::<u64>()
                        .map_err(|error| format!("--limit {value:?} is invalid: {error}"))?,
                );
            }
            "--start-at" => {
                let value = next_value(flag, inline, &mut iter)?;
                raw.start_at = Some(match value.as_str() {
                    "earliest" => StartAt::Earliest,
                    "latest" => StartAt::Latest,
                    _ => {
                        return Err(format!(
                            "--start-at {value:?} is invalid: expected earliest or latest"
                        ));
                    }
                });
            }
            "--metadata" => {
                let value = next_value(flag, inline, &mut iter)?;
                let parsed: BTreeMap<String, Value> = serde_json::from_str(&value)
                    .map_err(|error| format!("--metadata must be a JSON object: {error}"))?;
                raw.metadata = Some(parsed);
            }
            other => return Err(format!("unknown option {other:?}\n{CLI_USAGE}")),
        }
    }

    let format = raw.format.unwrap_or_default();
    let url_from_env = if raw.url.is_none() && raw.db.is_none() {
        std::env::var("MINYMATON_URL")
            .ok()
            .filter(|v| !v.is_empty())
    } else {
        None
    };
    let effective_url = raw.url.take().or(url_from_env);
    let target = match (effective_url, raw.db.take()) {
        (Some(_), Some(_)) => {
            return Err("cannot specify both --url and --db".to_owned());
        }
        (None, None) => {
            return Err("specify --url URL (or MINYMATON_URL) or --db PATH".to_owned());
        }
        (Some(url), None) => Target::Url {
            url,
            token_env: raw.token_env.take(),
            ca_file: raw.ca_file.take(),
        },
        (None, Some(db)) => {
            if raw.token_env.is_some() || raw.ca_file.is_some() {
                return Err("--token-env and --ca-file require --url".to_owned());
            }
            Target::Db(db)
        }
    };
    Ok((raw, CommonOptions { target, format }))
}

fn take_single_name(raw: &mut RawFlags, action: &str) -> Result<String, String> {
    if raw.names.len() > 1 {
        return Err(format!("stream {action} takes a single --name"));
    }
    if let Some(name) = raw.name.take() {
        if !raw.positionals.is_empty() {
            return Err(format!(
                "unexpected positional argument(s) for stream {action}"
            ));
        }
        return Ok(name);
    }
    if raw.positionals.len() == 1 {
        return Ok(raw.positionals.remove(0));
    }
    Err(format!("stream {action} requires --name NAME"))
}

fn reject_unused_stream_flags(raw: &RawFlags, action: &str) -> Result<(), String> {
    if raw.stream.is_some()
        || !raw.labels.is_empty()
        || raw.start_at.is_some()
        || raw.metadata.is_some()
    {
        return Err(format!(
            "subscription flags are not valid for stream {action}"
        ));
    }
    Ok(())
}

fn parse_stream(action: &str, args: &[String]) -> Result<(StreamArgs, CommonOptions), String> {
    let (mut raw, common) = parse_flags(args)?;
    reject_unused_stream_flags(&raw, action)?;
    let parsed = match action {
        "create" => {
            let name = take_single_name(&mut raw, action)?;
            if raw.caller.is_some()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err("unexpected option for stream create".to_owned());
            }
            StreamArgs::Create {
                name,
                members: (!raw.members.is_empty()).then_some(raw.members),
            }
        }
        "grant" | "revoke" => {
            let name = raw
                .name
                .take()
                .or_else(|| {
                    if !raw.positionals.is_empty() {
                        Some(raw.positionals.remove(0))
                    } else {
                        None
                    }
                })
                .ok_or_else(|| format!("stream {action} requires --name NAME"))?;
            let caller = raw
                .caller
                .take()
                .or_else(|| {
                    if !raw.positionals.is_empty() {
                        Some(raw.positionals.remove(0))
                    } else {
                        None
                    }
                })
                .ok_or_else(|| format!("stream {action} requires --caller CALLER"))?;
            if !raw.positionals.is_empty()
                || raw.names.len() > 1
                || !raw.members.is_empty()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err(format!("unexpected option for stream {action}"));
            }
            if action == "grant" {
                StreamArgs::Grant { name, caller }
            } else {
                StreamArgs::Revoke { name, caller }
            }
        }
        "delete" => {
            let name = take_single_name(&mut raw, action)?;
            if raw.caller.is_some()
                || !raw.members.is_empty()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err("unexpected option for stream delete".to_owned());
            }
            StreamArgs::Delete { name }
        }
        "delete-bulk" | "delete_bulk" => {
            if raw.caller.is_some()
                || !raw.members.is_empty()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err("unexpected option for stream delete-bulk".to_owned());
            }
            let mut names = raw.names;
            names.extend(raw.positionals);
            StreamArgs::DeleteBulk { names }
        }
        "sweep-idle" | "sweep_idle" => {
            if raw.name.is_some()
                || raw.caller.is_some()
                || !raw.members.is_empty()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.positionals.is_empty()
            {
                return Err("unexpected option for stream sweep-idle".to_owned());
            }
            StreamArgs::SweepIdle {
                keep: (!raw.keep.is_empty()).then_some(raw.keep),
            }
        }
        "list" => {
            if raw.name.is_some()
                || raw.caller.is_some()
                || !raw.members.is_empty()
                || !raw.keep.is_empty()
                || !raw.positionals.is_empty()
            {
                return Err("unexpected option for stream list".to_owned());
            }
            StreamArgs::List {
                after: raw.after,
                limit: raw.limit,
            }
        }
        other => return Err(format!("unknown stream action {other:?}\n{CLI_USAGE}")),
    };
    Ok((parsed, common))
}

fn take_single_label(raw: &mut RawFlags, action: &str) -> Result<String, String> {
    if raw.labels.len() > 1 {
        return Err(format!("subscription {action} takes a single --label"));
    }
    if let Some(label) = raw.label.take() {
        if !raw.positionals.is_empty() {
            return Err(format!(
                "unexpected positional argument(s) for subscription {action}"
            ));
        }
        return Ok(label);
    }
    if raw.positionals.len() == 1 {
        return Ok(raw.positionals.remove(0));
    }
    Err(format!("subscription {action} requires --label LABEL"))
}

fn parse_subscription(
    action: &str,
    args: &[String],
) -> Result<(SubscriptionArgs, CommonOptions), String> {
    let (mut raw, common) = parse_flags(args)?;
    if raw.name.is_some() || raw.caller.is_some() || !raw.members.is_empty() {
        return Err(format!(
            "stream flags are not valid for subscription {action}"
        ));
    }
    let parsed = match action {
        "create" => {
            if raw.labels.len() > 1
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
                || !raw.positionals.is_empty()
            {
                return Err("unexpected option for subscription create".to_owned());
            }
            SubscriptionArgs::Create {
                stream: raw.stream,
                label: raw.label,
                start_at: raw.start_at,
                metadata: raw.metadata,
            }
        }
        "recover" => {
            let label = take_single_label(&mut raw, action)?;
            if raw.start_at.is_some()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err("unexpected option for subscription recover".to_owned());
            }
            SubscriptionArgs::Recover {
                stream: raw.stream,
                label,
                metadata: raw.metadata,
            }
        }
        "delete" => {
            let label = take_single_label(&mut raw, action)?;
            if raw.start_at.is_some()
                || raw.metadata.is_some()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err("unexpected option for subscription delete".to_owned());
            }
            SubscriptionArgs::Delete {
                stream: raw.stream,
                label,
            }
        }
        "delete-bulk" | "delete_bulk" => {
            if raw.start_at.is_some()
                || raw.metadata.is_some()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.keep.is_empty()
            {
                return Err("unexpected option for subscription delete-bulk".to_owned());
            }
            let mut labels = raw.labels;
            labels.extend(raw.positionals);
            SubscriptionArgs::DeleteBulk {
                stream: raw.stream,
                labels,
            }
        }
        "sweep-idle" | "sweep_idle" => {
            if raw.label.is_some()
                || raw.start_at.is_some()
                || raw.metadata.is_some()
                || raw.after.is_some()
                || raw.limit.is_some()
                || !raw.positionals.is_empty()
            {
                return Err("unexpected option for subscription sweep-idle".to_owned());
            }
            SubscriptionArgs::SweepIdle {
                stream: raw.stream,
                keep: (!raw.keep.is_empty()).then_some(raw.keep),
            }
        }
        "list" => {
            if raw.label.is_some()
                || raw.start_at.is_some()
                || raw.metadata.is_some()
                || !raw.keep.is_empty()
                || !raw.positionals.is_empty()
            {
                return Err("unexpected option for subscription list".to_owned());
            }
            SubscriptionArgs::List {
                stream: raw.stream,
                after: raw.after,
                limit: raw.limit,
            }
        }
        other => {
            return Err(format!(
                "unknown subscription action {other:?}\n{CLI_USAGE}"
            ));
        }
    };
    Ok((parsed, common))
}
