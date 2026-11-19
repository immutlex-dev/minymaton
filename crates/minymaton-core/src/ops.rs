//! Single DRY execution + rendering codepath for `stream` and `subscription`
//! operations across MCP tool calls, `Queue`, and CLI commands.

use std::fmt::Write as _;
use std::str::FromStr;

use crate::api::{StreamArgs, StreamResult, SubscriptionArgs, SubscriptionResult};
use crate::error::ToolError;
use crate::policy::Access;
use crate::queue::Queue;

/// Output representation shared by MCP JSON, `Queue`, and CLI commands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputFormat {
    #[default]
    Json,
    Text,
    Csv,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            "csv" => Ok(Self::Csv),
            _ => Err(format!(
                "unsupported format {value:?}: expected json, text, or csv"
            )),
        }
    }
}

/// Executes a `stream` command and formats its result.
pub async fn stream_to_string(
    queue: &Queue,
    args: StreamArgs,
    access: &Access,
    format: OutputFormat,
) -> Result<String, ToolError> {
    let result = queue.execute_stream(args, access).await?;
    Ok(render_stream_to_string(&result, format))
}

/// Executes a `subscription` command and formats its result.
pub async fn subscription_to_string(
    queue: &Queue,
    args: SubscriptionArgs,
    access: &Access,
    format: OutputFormat,
) -> Result<String, ToolError> {
    let result = queue.execute_subscription(args, access).await?;
    Ok(render_subscription_to_string(&result, format))
}

/// Renders a [`StreamResult`] in the requested [`OutputFormat`].
pub fn render_stream_to_string(result: &StreamResult, format: OutputFormat) -> String {
    match format {
        OutputFormat::Json => {
            serde_json::to_string(result).expect("StreamResult serializes to JSON")
        }
        OutputFormat::Text => render_stream_text(result),
        OutputFormat::Csv => render_stream_csv(result),
    }
}

/// Renders a [`SubscriptionResult`] in the requested [`OutputFormat`].
pub fn render_subscription_to_string(result: &SubscriptionResult, format: OutputFormat) -> String {
    match format {
        OutputFormat::Json => {
            serde_json::to_string(result).expect("SubscriptionResult serializes to JSON")
        }
        OutputFormat::Text => render_subscription_text(result),
        OutputFormat::Csv => render_subscription_csv(result),
    }
}

fn render_stream_text(result: &StreamResult) -> String {
    match result {
        StreamResult::Created { stream } => format!("created stream: {stream}"),
        StreamResult::Granted { stream, caller } => {
            format!("granted {caller} on stream: {stream}")
        }
        StreamResult::Revoked { stream, caller } => {
            format!("revoked {caller} from stream: {stream}")
        }
        StreamResult::Deleted { stream } => format!("deleted stream: {stream}"),
        StreamResult::DeletedBulk {
            deleted,
            deleted_count,
        } => {
            if deleted.is_empty() {
                format!("deleted {deleted_count} streams")
            } else {
                format!("deleted {deleted_count} streams: {}", deleted.join(", "))
            }
        }
        StreamResult::Swept {
            deleted,
            deleted_count,
        } => {
            if deleted.is_empty() {
                format!("swept {deleted_count} idle streams")
            } else {
                format!("swept {deleted_count} idle streams: {}", deleted.join(", "))
            }
        }
        StreamResult::Listed {
            streams,
            next_after,
        } => {
            let mut lines = streams.clone();
            if let Some(next) = next_after {
                lines.push(format!("next_after: {next}"));
            }
            lines.join("\n")
        }
    }
}

fn render_stream_csv(result: &StreamResult) -> String {
    match result {
        StreamResult::Created { stream } => {
            format!("status,stream\ncreated,{}", csv_escape(stream))
        }
        StreamResult::Granted { stream, caller } => format!(
            "status,stream,caller\ngranted,{},{}",
            csv_escape(stream),
            csv_escape(caller)
        ),
        StreamResult::Revoked { stream, caller } => format!(
            "status,stream,caller\nrevoked,{},{}",
            csv_escape(stream),
            csv_escape(caller)
        ),
        StreamResult::Deleted { stream } => {
            format!("status,stream\ndeleted,{}", csv_escape(stream))
        }
        StreamResult::DeletedBulk {
            deleted,
            deleted_count,
        } => format!(
            "status,deleted_count,streams\ndeleted_bulk,{deleted_count},{}",
            csv_escape(&deleted.join(","))
        ),
        StreamResult::Swept {
            deleted,
            deleted_count,
        } => format!(
            "status,deleted_count,streams\nswept,{deleted_count},{}",
            csv_escape(&deleted.join(","))
        ),
        StreamResult::Listed {
            streams,
            next_after,
        } => {
            if streams.is_empty() && next_after.is_none() {
                return String::new();
            }
            let mut out = String::from("stream,next_after");
            for (index, stream) in streams.iter().enumerate() {
                let next = if index + 1 == streams.len() {
                    next_after.as_deref().unwrap_or("")
                } else {
                    ""
                };
                let _ = write!(out, "\n{},{}", csv_escape(stream), csv_escape(next));
            }
            out
        }
    }
}

fn render_subscription_text(result: &SubscriptionResult) -> String {
    match result {
        SubscriptionResult::Created {
            handle,
            stream,
            label,
            ..
        } => format!("created subscription {label} on {stream} ({handle})"),
        SubscriptionResult::Recovered {
            handle,
            stream,
            label,
            ..
        } => format!("recovered subscription {label} on {stream} ({handle})"),
        SubscriptionResult::Deleted { stream, label } => {
            format!("deleted subscription {label} on {stream}")
        }
        SubscriptionResult::DeletedBulk {
            stream,
            deleted,
            deleted_count,
        } => {
            if deleted.is_empty() {
                format!("deleted {deleted_count} subscriptions on {stream}")
            } else {
                format!(
                    "deleted {deleted_count} subscriptions on {stream}: {}",
                    deleted.join(", ")
                )
            }
        }
        SubscriptionResult::Swept {
            stream,
            deleted,
            deleted_count,
        } => {
            if deleted.is_empty() {
                format!("swept {deleted_count} idle subscriptions on {stream}")
            } else {
                format!(
                    "swept {deleted_count} idle subscriptions on {stream}: {}",
                    deleted.join(", ")
                )
            }
        }
        SubscriptionResult::Listed {
            stream,
            subscriptions,
            next_after,
        } => {
            if subscriptions.is_empty() && next_after.is_none() {
                return String::new();
            }
            let mut lines = Vec::with_capacity(subscriptions.len() + 1);
            for entry in subscriptions {
                let state = if entry.waiting { "waiting" } else { "idle" };
                let mut line = format!(
                    "{stream}\t{}\t{state}\t{}\t{}",
                    entry.label, entry.handle, entry.last_active_at
                );
                if let Some(meta) = &entry.metadata {
                    let encoded = serde_json::to_string(meta).unwrap_or_default();
                    let _ = write!(line, "\t{encoded}");
                }
                lines.push(line);
            }
            if let Some(next) = next_after {
                lines.push(format!("next_after: {next}"));
            }
            lines.join("\n")
        }
    }
}

fn render_subscription_csv(result: &SubscriptionResult) -> String {
    match result {
        SubscriptionResult::Created {
            handle,
            stream,
            label,
            metadata,
        } => {
            let meta = metadata
                .as_ref()
                .map(|m| serde_json::to_string(m).unwrap_or_default())
                .unwrap_or_default();
            format!(
                "status,stream,label,handle,metadata\ncreated,{},{},{},{}",
                csv_escape(stream),
                csv_escape(label),
                csv_escape(handle),
                csv_escape(&meta)
            )
        }
        SubscriptionResult::Recovered {
            handle,
            stream,
            label,
            metadata,
        } => {
            let meta = metadata
                .as_ref()
                .map(|m| serde_json::to_string(m).unwrap_or_default())
                .unwrap_or_default();
            format!(
                "status,stream,label,handle,metadata\nrecovered,{},{},{},{}",
                csv_escape(stream),
                csv_escape(label),
                csv_escape(handle),
                csv_escape(&meta)
            )
        }
        SubscriptionResult::Deleted { stream, label } => format!(
            "status,stream,label\ndeleted,{},{}",
            csv_escape(stream),
            csv_escape(label)
        ),
        SubscriptionResult::DeletedBulk {
            stream,
            deleted,
            deleted_count,
        } => format!(
            "status,stream,deleted_count,labels\ndeleted_bulk,{},{deleted_count},{}",
            csv_escape(stream),
            csv_escape(&deleted.join(","))
        ),
        SubscriptionResult::Swept {
            stream,
            deleted,
            deleted_count,
        } => format!(
            "status,stream,deleted_count,labels\nswept,{},{deleted_count},{}",
            csv_escape(stream),
            csv_escape(&deleted.join(","))
        ),
        SubscriptionResult::Listed {
            stream,
            subscriptions,
            next_after,
        } => {
            if subscriptions.is_empty() && next_after.is_none() {
                return String::new();
            }
            let mut out =
                String::from("stream,label,handle,waiting,last_active_at,metadata,next_after");
            for (index, entry) in subscriptions.iter().enumerate() {
                let meta = entry
                    .metadata
                    .as_ref()
                    .map(|m| serde_json::to_string(m).unwrap_or_default())
                    .unwrap_or_default();
                let next = if index + 1 == subscriptions.len() {
                    next_after.as_deref().unwrap_or("")
                } else {
                    ""
                };
                let _ = write!(
                    out,
                    "\n{},{},{},{},{},{},{}",
                    csv_escape(stream),
                    csv_escape(&entry.label),
                    csv_escape(&entry.handle),
                    entry.waiting,
                    csv_escape(&entry.last_active_at),
                    csv_escape(&meta),
                    csv_escape(next)
                );
            }
            out
        }
    }
}

/// RFC-4180 CSV field escaping.
pub fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::SubscriptionEntry;
    use std::collections::BTreeMap;

    #[test]
    fn csv_escapes_commas_quotes_and_newlines() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_escape("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn empty_lists_render_cleanly_across_formats() {
        let empty_streams = StreamResult::Listed {
            streams: vec![],
            next_after: None,
        };
        assert_eq!(
            render_stream_to_string(&empty_streams, OutputFormat::Csv),
            ""
        );
        assert_eq!(
            render_stream_to_string(&empty_streams, OutputFormat::Text),
            ""
        );
        assert_eq!(
            render_stream_to_string(&empty_streams, OutputFormat::Json),
            r#"{"status":"listed","streams":[],"next_after":null}"#
        );

        let empty_subs = SubscriptionResult::Listed {
            stream: "agents".to_owned(),
            subscriptions: vec![],
            next_after: None,
        };
        assert_eq!(
            render_subscription_to_string(&empty_subs, OutputFormat::Csv),
            ""
        );
        assert_eq!(
            render_subscription_to_string(&empty_subs, OutputFormat::Text),
            ""
        );
    }

    #[test]
    fn renders_sweep_and_listed_results_in_text_and_csv() {
        let swept_streams = StreamResult::Swept {
            deleted: vec!["a".to_owned(), "b".to_owned()],
            deleted_count: 2,
        };
        assert_eq!(
            render_stream_to_string(&swept_streams, OutputFormat::Csv),
            "status,deleted_count,streams\nswept,2,\"a,b\""
        );
        assert_eq!(
            render_stream_to_string(&swept_streams, OutputFormat::Text),
            "swept 2 idle streams: a, b"
        );

        let mut meta = BTreeMap::new();
        meta.insert("role".to_owned(), serde_json::json!("ops,lead"));
        let listed = SubscriptionResult::Listed {
            stream: "ops".to_owned(),
            subscriptions: vec![SubscriptionEntry {
                label: "desk".to_owned(),
                handle: "sub_1".to_owned(),
                waiting: true,
                last_active_at: "2026-11-18T00:00:00.000Z".to_owned(),
                metadata: Some(meta),
            }],
            next_after: None,
        };
        let csv = render_subscription_to_string(&listed, OutputFormat::Csv);
        assert!(
            csv.starts_with("stream,label,handle,waiting,last_active_at,metadata,next_after\n")
        );
        assert!(csv.contains("ops,desk,sub_1,true,2026-11-18T00:00:00.000Z,"));
        let text = render_subscription_to_string(&listed, OutputFormat::Text);
        assert!(text.contains("ops\tdesk\twaiting\tsub_1"));
    }
}
