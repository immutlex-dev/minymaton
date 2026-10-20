use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HOST, HeaderMap, HeaderValue};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use rmcp::model::{
    CallToolRequestMethod, ClientCapabilities, Implementation, JsonRpcRequest, ProtocolVersion,
    Request, RequestId, RequestMetaObject,
};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};

const USAGE: &str = "usage: minymaton-wait [-h] --handle HANDLE [--ack ACK] [--max-events MAX_EVENTS] [--max-bytes MAX_BYTES] [--url URL] [--ca-file CA_FILE] [--config CONFIG] [--token-env TOKEN_ENV]";
const MAX_INTERPOLATION_DEPTH: usize = 10;

struct CliOptions {
    handle: Option<String>,
    ack: Option<String>,
    max_events: Option<Box<RawValue>>,
    max_bytes: Option<Box<RawValue>>,
    url: Option<String>,
    ca_file: Option<String>,
    config: Option<PathBuf>,
    token_env: Option<String>,
}

struct Options {
    handle: String,
    ack: Option<String>,
    max_events: Box<RawValue>,
    max_bytes: Box<RawValue>,
    url: String,
    ca_file: Option<PathBuf>,
    headers: HeaderMap,
}

#[derive(Clone, Copy)]
enum Flag {
    Help,
    Handle,
    Ack,
    MaxEvents,
    MaxBytes,
    Url,
    CaFile,
    Config,
    TokenEnv,
}

const LONG_OPTIONS: [(&str, Flag); 9] = [
    ("--help", Flag::Help),
    ("--handle", Flag::Handle),
    ("--ack", Flag::Ack),
    ("--max-events", Flag::MaxEvents),
    ("--max-bytes", Flag::MaxBytes),
    ("--url", Flag::Url),
    ("--ca-file", Flag::CaFile),
    ("--config", Flag::Config),
    ("--token-env", Flag::TokenEnv),
];

enum Argument<'a> {
    Option(&'static str, Flag, Option<&'a str>),
    Value(&'a str),
    Unknown(&'a str),
    End,
}

fn classify<'a>(argument: &'a str, negative_number: &regex::Regex) -> Result<Argument<'a>, String> {
    if argument == "--" {
        return Ok(Argument::End);
    }
    let (name, inline) = match argument.split_once('=') {
        Some((name, value)) => (name, Some(value)),
        None => (argument, None),
    };
    if let Some((name, flag)) = LONG_OPTIONS.iter().find(|(option, _)| *option == name) {
        return Ok(Argument::Option(name, *flag, inline));
    }
    if argument == "-h" {
        return Ok(Argument::Option("-h", Flag::Help, None));
    }
    if argument.starts_with("--") {
        let mut matches = LONG_OPTIONS
            .iter()
            .filter(|(option, _)| option.starts_with(name));
        if let Some((option, flag)) = matches.next() {
            if matches.next().is_some() {
                return Err(format!("ambiguous option: {name}"));
            }
            return Ok(Argument::Option(option, *flag, inline));
        }
    } else if let Some(tail) = argument.strip_prefix("-h") {
        return Ok(Argument::Option("-h", Flag::Help, Some(tail)));
    }
    if !argument.starts_with('-')
        || argument == "-"
        || negative_number.is_match(argument)
        || argument.contains(' ')
    {
        Ok(Argument::Value(argument))
    } else {
        Ok(Argument::Unknown(argument))
    }
}

fn decimal_integer(value: &str, decimal: &regex::Regex) -> Result<Box<RawValue>, String> {
    let text = value.trim();
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text).chars();
    let mut normalized = String::new();
    let mut needs_digit = true;
    for character in digits {
        if character == '_' && !needs_digit {
            needs_digit = true;
            continue;
        }
        let mut buffer = [0; 4];
        if !decimal.is_match(character.encode_utf8(&mut buffer)) {
            return Err("invalid int value".to_owned());
        }
        // Unicode Nd consists of contiguous 0..9 sets, including adjacent styled sets.
        let mut start = u32::from(character);
        while let Some(previous) = start.checked_sub(1).and_then(char::from_u32) {
            if !decimal.is_match(previous.encode_utf8(&mut buffer)) {
                break;
            }
            start -= 1;
        }
        let digit = (u32::from(character) - start) % 10;
        normalized.push(char::from_digit(digit, 10).ok_or("invalid int value")?);
        needs_digit = false;
    }
    if needs_digit {
        return Err("invalid int value".to_owned());
    }
    let digits = normalized.trim_start_matches('0');
    let normalized = if digits.is_empty() {
        "0".to_owned()
    } else {
        format!("{}{digits}", if text.starts_with('-') { "-" } else { "" })
    };
    RawValue::from_string(normalized).map_err(|_| "invalid int value".to_owned())
}

impl CliOptions {
    fn parse() -> Result<Option<Self>, String> {
        let mut options = Self {
            handle: None,
            ack: None,
            max_events: None,
            max_bytes: None,
            url: None,
            ca_file: None,
            config: None,
            token_env: None,
        };
        // argparse's negative-number exception, including Python's final-newline '$' match.
        let negative_number = regex::Regex::new(r"^-(?:\d+|\d*\.\d+)\n?$")
            .map_err(|error| format!("cannot parse command-line integers: {error}"))?;
        let decimal = regex::Regex::new(r"\A\d\z")
            .map_err(|error| format!("cannot parse command-line integers: {error}"))?;
        let arguments = std::env::args_os()
            .skip(1)
            .map(|argument| {
                argument
                    .into_string()
                    .map_err(|_| "arguments must be valid Unicode".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        // argparse detects ambiguous options before executing even an earlier help action.
        let mut ended = false;
        let mut arguments = arguments
            .iter()
            .map(|argument| {
                if ended {
                    return Ok(Argument::Value(argument));
                }
                let classified = classify(argument, &negative_number)?;
                ended = matches!(classified, Argument::End);
                Ok(classified)
            })
            .collect::<Result<Vec<_>, String>>()?
            .into_iter();
        let mut extras = Vec::new();
        while let Some(argument) = arguments.next() {
            let (name, flag, inline) = match argument {
                Argument::Option(name, flag, inline) => (name, flag, inline),
                Argument::Value(value) | Argument::Unknown(value) => {
                    extras.push(value);
                    continue;
                }
                Argument::End => continue,
            };
            if matches!(flag, Flag::Help) {
                if inline.is_some_and(|value| {
                    name != "-h" || value.trim_start_matches('h').starts_with(['=', '-'])
                }) {
                    return Err("argument -h/--help: ignored explicit argument".to_owned());
                }
                return Ok(None);
            }
            let value = match inline {
                Some(value) => value,
                None => match arguments.next() {
                    Some(Argument::Value(value)) => value,
                    _ => return Err(format!("argument {name}: expected one argument")),
                },
            };
            match flag {
                Flag::Handle => options.handle = Some(value.to_owned()),
                Flag::Ack => options.ack = Some(value.to_owned()),
                Flag::Url => options.url = Some(value.to_owned()),
                Flag::CaFile => options.ca_file = Some(value.to_owned()),
                Flag::Config => options.config = Some(PathBuf::from(value)),
                Flag::TokenEnv => options.token_env = Some(value.to_owned()),
                Flag::MaxEvents | Flag::MaxBytes => {
                    let number = decimal_integer(value, &decimal)
                        .map_err(|error| format!("argument {name}: {error}"))?;
                    if matches!(flag, Flag::MaxEvents) {
                        options.max_events = Some(number);
                    } else {
                        options.max_bytes = Some(number);
                    }
                }
                Flag::Help => return Ok(None),
            }
        }
        if options.handle.is_none()
            && options.config.is_none()
            && std::env::var_os("MINYMATON_HANDLE").is_none()
        {
            return Err("the following arguments are required: --handle".to_owned());
        }
        if !extras.is_empty() {
            return Err(format!("unrecognized arguments: {}", extras.join(" ")));
        }
        Ok(Some(options))
    }
}

struct IniConfig {
    defaults: HashMap<String, String>,
    sections: HashMap<String, HashMap<String, String>>,
}

impl IniConfig {
    fn parse(path: &Path, raw_bytes: &[u8]) -> Result<Self, Failure> {
        let raw_bytes = raw_bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(raw_bytes);
        let content = std::str::from_utf8(raw_bytes)
            .map_err(|_| config_error(path, "cannot read INI config: invalid UTF-8"))?;
        let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
        let mut default_lines: HashMap<String, Vec<String>> = HashMap::new();
        let mut section_lines: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        let mut sections_seen: HashSet<String> = HashSet::new();
        let mut options_seen: HashSet<(String, String)> = HashSet::new();
        let mut current_section: Option<String> = None;
        let mut current_option: Option<String> = None;
        let mut indent_level = 0usize;

        for (index, raw_line) in normalized.lines().enumerate() {
            let line_no = index + 1;
            let clean = raw_line.trim();
            let is_comment = clean.starts_with('#') || clean.starts_with(';');
            if clean.is_empty() || is_comment {
                if !is_comment
                    && let (Some(section), Some(option)) = (&current_section, &current_option)
                {
                    let target = if section == "DEFAULT" {
                        default_lines.get_mut(option)
                    } else {
                        section_lines
                            .get_mut(section)
                            .and_then(|map| map.get_mut(option))
                    };
                    if let Some(lines) = target {
                        lines.push(String::new());
                    }
                }
                continue;
            }
            let cur_indent = raw_line.chars().take_while(|ch| ch.is_whitespace()).count();
            if cur_indent > indent_level
                && let (Some(section), Some(option)) = (&current_section, &current_option)
            {
                let target = if section == "DEFAULT" {
                    default_lines.get_mut(option)
                } else {
                    section_lines
                        .get_mut(section)
                        .and_then(|map| map.get_mut(option))
                };
                if let Some(lines) = target {
                    lines.push(clean.to_owned());
                    continue;
                }
            }
            indent_level = cur_indent;
            if clean.starts_with('[')
                && let Some(close_idx) = clean.rfind(']')
                && close_idx > 1
            {
                let header = &clean[1..close_idx];
                if header != "DEFAULT" && !sections_seen.insert(header.to_owned()) {
                    return Err(config_error(
                        path,
                        &format!(
                            "DuplicateSectionError: section {header:?} already exists at line {line_no}"
                        ),
                    ));
                }
                if header != "DEFAULT" {
                    section_lines.entry(header.to_owned()).or_default();
                }
                current_section = Some(header.to_owned());
                current_option = None;
                continue;
            }
            let Some(section) = current_section.as_ref() else {
                return Err(config_error(
                    path,
                    &format!(
                        "MissingSectionHeaderError: file contains no section headers at line {line_no}"
                    ),
                ));
            };
            let Some(delim_idx) = clean.find(['=', ':']) else {
                return Err(config_error(
                    path,
                    &format!("ParsingError: invalid line at {line_no}"),
                ));
            };
            let raw_option = clean[..delim_idx].trim_end();
            if raw_option.is_empty() {
                return Err(config_error(
                    path,
                    &format!("ParsingError: empty option name at line {line_no}"),
                ));
            }
            let option = raw_option.to_lowercase();
            if !options_seen.insert((section.clone(), option.clone())) {
                return Err(config_error(
                    path,
                    &format!(
                        "DuplicateOptionError: option {option:?} in section {section:?} already exists at line {line_no}"
                    ),
                ));
            }
            let value = clean[delim_idx + 1..].trim().to_owned();
            if section == "DEFAULT" {
                default_lines.insert(option.clone(), vec![value]);
            } else if let Some(map) = section_lines.get_mut(section) {
                map.insert(option.clone(), vec![value]);
            }
            current_option = Some(option);
        }

        let defaults: HashMap<String, String> = default_lines
            .into_iter()
            .map(|(k, lines)| (k, lines.join("\n").trim_end().to_owned()))
            .collect();
        let sections: HashMap<String, HashMap<String, String>> = section_lines
            .into_iter()
            .map(|(sec, map)| {
                let joined = map
                    .into_iter()
                    .map(|(k, lines)| (k, lines.join("\n").trim_end().to_owned()))
                    .collect();
                (sec, joined)
            })
            .collect();

        Ok(Self { defaults, sections })
    }

    fn get(&self, section: &str, option: &str, path: &Path) -> Result<Option<String>, Failure> {
        let Some(sec_map) = self.sections.get(section) else {
            return Ok(None);
        };
        let mut merged = self.defaults.clone();
        for (k, v) in sec_map {
            merged.insert(k.clone(), v.clone());
        }
        let key = option.to_lowercase();
        let Some(raw) = merged.get(&key) else {
            return Ok(None);
        };
        let mut accum = String::new();
        Self::interpolate_some(path, section, &key, &mut accum, raw, &merged, 1)?;
        Ok(Some(accum))
    }

    fn interpolate_some(
        path: &Path,
        section: &str,
        option: &str,
        accum: &mut String,
        mut rest: &str,
        map: &HashMap<String, String>,
        depth: usize,
    ) -> Result<(), Failure> {
        if depth > MAX_INTERPOLATION_DEPTH {
            return Err(config_error(
                path,
                &format!(
                    "InterpolationDepthError: recursion limit exceeded in value substitution for option {option:?} in section {section:?}"
                ),
            ));
        }
        while !rest.is_empty() {
            let Some(pos) = rest.find('%') else {
                accum.push_str(rest);
                return Ok(());
            };
            if pos > 0 {
                accum.push_str(&rest[..pos]);
                rest = &rest[pos..];
            }
            let after_pct = &rest[1..];
            if let Some(after_escaped) = after_pct.strip_prefix('%') {
                accum.push('%');
                rest = after_escaped;
            } else if let Some(after_open) = after_pct.strip_prefix('(') {
                let Some(close_idx) = after_open.find(')') else {
                    return Err(config_error(
                        path,
                        &format!(
                            "InterpolationSyntaxError: bad interpolation variable reference {rest:?} in option {option:?} of section {section:?}"
                        ),
                    ));
                };
                let var_raw = &after_open[..close_idx];
                let after_close = &after_open[close_idx + 1..];
                let Some(after_spec) = after_close.strip_prefix('s') else {
                    return Err(config_error(
                        path,
                        &format!(
                            "InterpolationSyntaxError: bad interpolation variable reference {rest:?} in option {option:?} of section {section:?}"
                        ),
                    ));
                };
                if var_raw.is_empty() {
                    return Err(config_error(
                        path,
                        &format!(
                            "InterpolationSyntaxError: empty interpolation variable reference in option {option:?} of section {section:?}"
                        ),
                    ));
                }
                let var = var_raw.to_lowercase();
                rest = after_spec;
                let Some(val) = map.get(&var) else {
                    return Err(config_error(
                        path,
                        &format!(
                            "InterpolationMissingOptionError: option {option:?} in section {section:?} references missing option {var:?}"
                        ),
                    ));
                };
                if val.contains('%') {
                    Self::interpolate_some(path, section, option, accum, val, map, depth + 1)?;
                } else {
                    accum.push_str(val);
                }
            } else {
                return Err(config_error(
                    path,
                    &format!(
                        "InterpolationSyntaxError: '%' must be followed by '%' or '(' in option {option:?} of section {section:?}"
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl Options {
    fn resolve(cli: CliOptions) -> Result<Self, Failure> {
        let decimal = regex::Regex::new(r"\A\d\z")
            .map_err(|error| Failure::new("config_failed", error.to_string(), 1))?;
        let config_path = match cli.config {
            Some(path) => Some(path),
            None => environment("MINYMATON_CONFIG")?
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
        };
        let ini = match &config_path {
            Some(path) => {
                let raw = std::fs::read(path).map_err(|error| {
                    config_error(path, &format!("cannot read INI config: {error}"))
                })?;
                Some((path.as_path(), IniConfig::parse(path, &raw)?))
            }
            None => None,
        };
        let ini_get = |key: &str| -> Result<Option<String>, Failure> {
            match &ini {
                Some((path, cfg)) => Ok(cfg.get("minymaton", key, path)?.filter(|v| !v.is_empty())),
                None => Ok(None),
            }
        };
        let handle = match cli.handle {
            Some(handle) => handle,
            None => match environment("MINYMATON_HANDLE")?.filter(|v| !v.is_empty()) {
                Some(handle) => handle,
                None => match ini_get("handle")? {
                    Some(handle) => handle,
                    None => {
                        return Err(Failure::new(
                            "usage_error",
                            "the following arguments are required: --handle",
                            2,
                        ));
                    }
                },
            },
        };
        let ack = match cli.ack {
            Some(ack) => Some(ack),
            None => match environment("MINYMATON_ACK")?.filter(|v| !v.is_empty()) {
                Some(ack) => Some(ack),
                None => ini_get("ack")?,
            },
        };
        let max_events = match cli.max_events {
            Some(max_events) => max_events,
            None => match environment("MINYMATON_MAX_EVENTS")?.filter(|v| !v.is_empty()) {
                Some(raw) => decimal_integer(&raw, &decimal).map_err(|error| {
                    Failure::new(
                        "config_failed",
                        format!("invalid MINYMATON_MAX_EVENTS: {error}"),
                        1,
                    )
                })?,
                None => match ini_get("max_events")? {
                    Some(raw) => {
                        let path = config_path
                            .as_deref()
                            .unwrap_or_else(|| Path::new("config"));
                        decimal_integer(&raw, &decimal).map_err(|error| {
                            config_error(path, &format!("invalid max_events: {error}"))
                        })?
                    }
                    None => serde_json::value::to_raw_value(&10)
                        .map_err(|error| Failure::new("config_failed", error.to_string(), 1))?,
                },
            },
        };
        let max_bytes = match cli.max_bytes {
            Some(max_bytes) => max_bytes,
            None => match environment("MINYMATON_MAX_BYTES")?.filter(|v| !v.is_empty()) {
                Some(raw) => decimal_integer(&raw, &decimal).map_err(|error| {
                    Failure::new(
                        "config_failed",
                        format!("invalid MINYMATON_MAX_BYTES: {error}"),
                        1,
                    )
                })?,
                None => match ini_get("max_bytes")? {
                    Some(raw) => {
                        let path = config_path
                            .as_deref()
                            .unwrap_or_else(|| Path::new("config"));
                        decimal_integer(&raw, &decimal).map_err(|error| {
                            config_error(path, &format!("invalid max_bytes: {error}"))
                        })?
                    }
                    None => serde_json::value::to_raw_value(&16384)
                        .map_err(|error| Failure::new("config_failed", error.to_string(), 1))?,
                },
            },
        };
        let ca_file = match cli.ca_file {
            Some(ca_file) => Some(PathBuf::from(ca_file)),
            None => match environment("MINYMATON_CA_FILE")?.filter(|v| !v.is_empty()) {
                Some(ca_file) => Some(PathBuf::from(ca_file)),
                None => ini_get("ca_file")?.map(PathBuf::from),
            },
        };
        let url = match cli.url.filter(|v| !v.is_empty()) {
            Some(url) => url,
            None => match environment("MINYMATON_URL")?.filter(|v| !v.is_empty()) {
                Some(url) => url,
                None => match ini_get("url")? {
                    Some(url) => url,
                    None => match &config_path {
                        Some(path) => {
                            return Err(config_error(
                                path,
                                "missing endpoint URL: specify --url, MINYMATON_URL, or url",
                            ));
                        }
                        None => {
                            return Err(Failure::new(
                                "config_failed",
                                "missing endpoint URL: specify --url, MINYMATON_URL, or url in --config / MINYMATON_CONFIG",
                                1,
                            ));
                        }
                    },
                },
            },
        };
        let explicit_token_env = match cli.token_env {
            Some(token_env) => Some(token_env),
            None => match environment("MINYMATON_TOKEN_ENV")? {
                Some(token_env) if !token_env.is_empty() => Some(token_env),
                _ => ini_get("token_env")?,
            },
        };
        let token = match explicit_token_env {
            Some(var_name) => {
                let var_name = var_name.trim();
                if var_name.is_empty() {
                    return Err(Failure::new(
                        "config_failed",
                        "token_env cannot be empty",
                        1,
                    ));
                }
                match environment(var_name)?.filter(|v| !v.is_empty()) {
                    Some(token) => Some(token),
                    None => {
                        return Err(Failure::new(
                            "config_failed",
                            format!(
                                "environment variable {var_name} specified by token_env is not set or is empty"
                            ),
                            1,
                        ));
                    }
                }
            }
            None => environment("MINYMATON_TOKEN")?.filter(|v| !v.is_empty()),
        };
        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            let bearer_prefix = format!("{} ", "Bearer");
            let auth = if token.starts_with(&bearer_prefix) {
                token
            } else {
                format!("{bearer_prefix}{token}")
            };
            let mut value = HeaderValue::from_str(&auth)
                .map_err(|_| Failure::protocol("invalid Authorization header"))?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        Ok(Self {
            handle,
            ack,
            max_events,
            max_bytes,
            url,
            ca_file,
            headers,
        })
    }
}

struct Failure {
    error: Value,
    exit: u8,
}

impl Failure {
    fn new(code: &str, message: impl Into<String>, exit: u8) -> Self {
        Self {
            error: json!({"code": code, "message": message.into()}),
            exit,
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self::new("protocol_failed", message, 1)
    }

    fn transport(stage: &str, error: &(dyn Error + 'static)) -> Self {
        let mut message = format!("{stage}: {error}");
        let mut source = error.source();
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Self::new("transport_failed", message, 7)
    }

    fn redact(mut self, secrets: &[String]) -> Self {
        if let Some(message) = self.error["message"].as_str() {
            let mut message = message.to_owned();
            for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
                message = message.replace(secret, "[redacted]");
            }
            self.error["message"] = json!(message);
        }
        self
    }

    fn io(message: &str, error: io::Error) -> Self {
        Self::new("transport_failed", format!("{message}: {error}"), 7)
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn environment(name: &str) -> Result<Option<String>, Failure> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(Failure::new(
            "config_failed",
            format!("{name} is not valid Unicode"),
            1,
        )),
    }
}

fn config_error(path: &Path, message: &str) -> Failure {
    Failure::new(
        "config_failed",
        format!("{message} in {}", path.display()),
        1,
    )
}

fn response_matches(message: &Value) -> bool {
    message.is_object()
        && message.get("id") == Some(&json!(2))
        && (message.get("result").is_some() || message.get("error").is_some())
}

struct X509CertInfo {
    self_issued: bool,
    eku_allows_server_auth: bool,
}

struct DerTlv<'a> {
    tag: u8,
    body: &'a [u8],
    rest: &'a [u8],
    full_tlv: &'a [u8],
}

fn read_der_tlv(input: &[u8]) -> Option<DerTlv<'_>> {
    let tag = *input.first()?;
    let len_byte = *input.get(1)?;
    let (header_len, content_len) = if len_byte & 0x80 == 0 {
        (2usize, usize::from(len_byte))
    } else {
        let num_octets = usize::from(len_byte & 0x7f);
        if num_octets == 0 || num_octets > 4 || input.len() < 2 + num_octets {
            return None;
        }
        let mut len = 0usize;
        for &byte in &input[2..2 + num_octets] {
            len = (len << 8) | usize::from(byte);
        }
        (2 + num_octets, len)
    };
    let total_len = header_len.checked_add(content_len)?;
    if input.len() < total_len {
        return None;
    }
    let full_tlv = &input[..total_len];
    let body = &input[header_len..total_len];
    let rest = &input[total_len..];
    Some(DerTlv {
        tag,
        body,
        rest,
        full_tlv,
    })
}

fn inspect_x509_der(der: &[u8]) -> Option<X509CertInfo> {
    // Certificate ::= SEQUENCE { tbsCertificate TBSCertificate, ... }
    let cert_tlv = read_der_tlv(der)?;
    if cert_tlv.tag != 0x30 {
        return None;
    }
    let tbs_tlv = read_der_tlv(cert_tlv.body)?;
    if tbs_tlv.tag != 0x30 {
        return None;
    }
    let mut tbs = tbs_tlv.body;
    // Optional [0] EXPLICIT version
    if tbs.first().copied() == Some(0xa0) {
        tbs = read_der_tlv(tbs)?.rest;
    }
    // serialNumber
    let rest = read_der_tlv(tbs)?.rest;
    // signature AlgorithmIdentifier
    let rest = read_der_tlv(rest)?.rest;
    // issuer Name
    let issuer = read_der_tlv(rest)?;
    // validity
    let rest = read_der_tlv(issuer.rest)?.rest;
    // subject Name
    let subject = read_der_tlv(rest)?;
    // subjectPublicKeyInfo
    let mut rest = read_der_tlv(subject.rest)?.rest;

    let self_issued = issuer.full_tlv == subject.full_tlv;
    let mut eku_allows_server_auth = true;

    while !rest.is_empty() {
        let ext_tlv = read_der_tlv(rest)?;
        rest = ext_tlv.rest;
        // extensions [3] EXPLICIT Extensions
        if ext_tlv.tag == 0xa3 {
            let seq_tlv = read_der_tlv(ext_tlv.body)?;
            if seq_tlv.tag != 0x30 {
                return None;
            }
            let mut exts_seq = seq_tlv.body;
            while !exts_seq.is_empty() {
                let item_tlv = read_der_tlv(exts_seq)?;
                exts_seq = item_tlv.rest;
                if item_tlv.tag != 0x30 {
                    continue;
                }
                let oid_tlv = read_der_tlv(item_tlv.body)?;
                if oid_tlv.tag != 0x06 {
                    continue;
                }
                let mut item_rest = oid_tlv.rest;
                // Optional critical BOOLEAN
                if item_rest.first().copied() == Some(0x01) {
                    item_rest = read_der_tlv(item_rest)?.rest;
                }
                let val_tlv = read_der_tlv(item_rest)?;
                if val_tlv.tag != 0x04 {
                    continue;
                }
                // id-ce-extKeyUsage (2.5.29.37) -> [0x55, 0x1d, 0x25]
                if oid_tlv.body == [0x55, 0x1d, 0x25] {
                    let eku_seq_tlv = read_der_tlv(val_tlv.body)?;
                    if eku_seq_tlv.tag != 0x30 {
                        return None;
                    }
                    let mut eku_seq = eku_seq_tlv.body;
                    let mut found_server_auth = false;
                    while !eku_seq.is_empty() {
                        let kp_tlv = read_der_tlv(eku_seq)?;
                        eku_seq = kp_tlv.rest;
                        // id-kp-serverAuth (1.3.6.1.5.5.7.3.1) -> [0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01]
                        // anyExtendedKeyUsage (2.5.29.37.0) -> [0x55, 0x1d, 0x25, 0x00]
                        if kp_tlv.tag == 0x06
                            && (kp_tlv.body == [0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01]
                                || kp_tlv.body == [0x55, 0x1d, 0x25, 0x00])
                        {
                            found_server_auth = true;
                        }
                    }
                    if !found_server_auth {
                        eku_allows_server_auth = false;
                    }
                }
            }
        }
    }

    Some(X509CertInfo {
        self_issued,
        eku_allows_server_auth,
    })
}

fn load_pem_bundle(
    path: &Path,
    explicit: bool,
    roots: &mut rustls::RootCertStore,
    extra_intermediates: &mut Vec<rustls::pki_types::CertificateDer<'static>>,
    seen_der: &mut HashSet<Vec<u8>>,
) -> Result<(), Failure> {
    use rustls::pki_types::pem::PemObject;
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(error) => {
            if explicit {
                return Err(Failure::transport(
                    &format!("cannot read CA bundle {}", path.display()),
                    &error,
                ));
            }
            return Ok(());
        }
    };
    let mut parsed_any = false;
    let mut accepted_any = false;
    for item in rustls::pki_types::CertificateDer::pem_slice_iter(&data) {
        let cert = match item {
            Ok(cert) => cert,
            Err(error) => {
                if explicit {
                    return Err(Failure::transport(
                        &format!("invalid PEM certificate in {}", path.display()),
                        &error,
                    ));
                }
                continue;
            }
        };
        parsed_any = true;
        let Some(info) = inspect_x509_der(cert.as_ref()) else {
            if explicit {
                return Err(Failure::new(
                    "transport_failed",
                    format!("invalid X.509 certificate in {}", path.display()),
                    7,
                ));
            }
            continue;
        };
        if !info.eku_allows_server_auth {
            continue;
        }
        accepted_any = true;
        if seen_der.insert(cert.as_ref().to_vec()) {
            if info.self_issued {
                let _ = roots.add(cert);
            } else {
                extra_intermediates.push(cert);
            }
        }
    }
    if explicit && (!parsed_any || !accepted_any) {
        return Err(Failure::new(
            "transport_failed",
            format!("no valid CA certificates found in {}", path.display()),
            7,
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct ExtraIntermediatesVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    #[cfg(not(windows))]
    platform: Option<rustls_platform_verifier::Verifier>,
    extra_intermediates: Vec<rustls::pki_types::CertificateDer<'static>>,
}

impl rustls::client::danger::ServerCertVerifier for ExtraIntermediatesVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let mut combined = Vec::with_capacity(intermediates.len() + self.extra_intermediates.len());
        combined.extend_from_slice(intermediates);
        combined.extend(self.extra_intermediates.iter().cloned());
        match self
            .inner
            .verify_server_cert(end_entity, &combined, server_name, ocsp_response, now)
        {
            Ok(verified) => Ok(verified),
            Err(error) => {
                #[cfg(not(windows))]
                if let Some(platform) = &self.platform {
                    return platform.verify_server_cert(
                        end_entity,
                        &combined,
                        server_name,
                        ocsp_response,
                        now,
                    );
                }
                Err(error)
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn build_tls_config(ca_file: Option<&Path>) -> Result<rustls::ClientConfig, Failure> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    let mut extra_intermediates: Vec<rustls::pki_types::CertificateDer<'static>> = Vec::new();
    let mut seen_der: HashSet<Vec<u8>> = HashSet::new();

    #[cfg(windows)]
    {
        for (store_name, is_root_store) in [("ROOT", true), ("CA", false)] {
            for open_result in [
                schannel::cert_store::CertStore::open_current_user(store_name),
                schannel::cert_store::CertStore::open_local_machine(store_name),
            ] {
                let Ok(store) = open_result else {
                    continue;
                };
                for cert in store.certs() {
                    let valid_uses_ok = match cert.valid_uses() {
                        Ok(schannel::cert_context::ValidUses::All) => true,
                        Ok(schannel::cert_context::ValidUses::Oids(oids)) => oids
                            .iter()
                            .any(|oid| oid == "1.3.6.1.5.5.7.3.1" || oid == "2.5.29.37.0"),
                        Err(_) => false,
                    };
                    if !valid_uses_ok {
                        continue;
                    }
                    let der = cert.to_der();
                    let Some(info) = inspect_x509_der(der) else {
                        continue;
                    };
                    if !info.eku_allows_server_auth {
                        continue;
                    }
                    if !seen_der.insert(der.to_vec()) {
                        continue;
                    }
                    let cert_der = rustls::pki_types::CertificateDer::from(der.to_vec());
                    if is_root_store || info.self_issued {
                        let _ = roots.add(cert_der);
                    } else {
                        extra_intermediates.push(cert_der);
                    }
                }
            }
        }
    }

    match environment("SSL_CERT_FILE")? {
        Some(path) if !path.is_empty() => {
            let path = Path::new(&path);
            if path.is_file() {
                load_pem_bundle(
                    path,
                    false,
                    &mut roots,
                    &mut extra_intermediates,
                    &mut seen_der,
                )?;
            }
        }
        _ => {
            #[cfg(windows)]
            let default_files: &[&str] = &[r"C:\Program Files\Common Files\SSL\cert.pem"];
            #[cfg(not(windows))]
            let default_files: &[&str] = &[
                "/etc/ssl/cert.pem",
                "/etc/ssl/certs/ca-certificates.crt",
                "/etc/pki/tls/cert.pem",
            ];
            for candidate in default_files {
                let path = Path::new(candidate);
                if path.is_file() {
                    let _ = load_pem_bundle(
                        path,
                        false,
                        &mut roots,
                        &mut extra_intermediates,
                        &mut seen_der,
                    );
                    break;
                }
            }
        }
    }

    let cert_dirs: Vec<PathBuf> = match std::env::var_os("SSL_CERT_DIR") {
        Some(raw) if !raw.is_empty() => std::env::split_paths(&raw).collect(),
        _ => {
            #[cfg(windows)]
            {
                vec![PathBuf::from(r"C:\Program Files\Common Files\SSL\certs")]
            }
            #[cfg(not(windows))]
            {
                Vec::new()
            }
        }
    };
    for dir in cert_dirs {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            let mut paths: Vec<PathBuf> = entries
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|path| path.is_file())
                .collect();
            paths.sort();
            for path in paths {
                let _ = load_pem_bundle(
                    &path,
                    false,
                    &mut roots,
                    &mut extra_intermediates,
                    &mut seen_der,
                );
            }
        }
    }

    if let Some(path) = ca_file {
        load_pem_bundle(
            path,
            true,
            &mut roots,
            &mut extra_intermediates,
            &mut seen_der,
        )?;
    }

    #[cfg(windows)]
    let verifier: Arc<dyn rustls::client::danger::ServerCertVerifier> = {
        if roots.is_empty() {
            return Err(Failure::new(
                "transport_failed",
                "TLS trust store failed: no trusted root certificates available",
                7,
            ));
        }
        let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|error| Failure::transport("TLS trust store failed", &error))?;
        Arc::new(ExtraIntermediatesVerifier {
            inner,
            extra_intermediates,
        })
    };

    #[cfg(not(windows))]
    let verifier: Arc<dyn rustls::client::danger::ServerCertVerifier> = {
        let platform = rustls_platform_verifier::Verifier::new(Arc::clone(&provider))
            .map_err(|error| Failure::transport("TLS trust store failed", &error))?;
        if roots.is_empty() {
            Arc::new(platform)
        } else {
            let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots),
                Arc::clone(&provider),
            )
            .build()
            .map_err(|error| Failure::transport("TLS trust store failed", &error))?;
            Arc::new(ExtraIntermediatesVerifier {
                inner,
                platform: Some(platform),
                extra_intermediates,
            })
        }
    };

    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| Failure::transport("TLS configuration failed", &error))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

#[derive(Serialize)]
struct ReceiveArguments {
    handle: String,
    max_events: Box<RawValue>,
    max_bytes: Box<RawValue>,
    wait: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    ack: Option<String>,
}

#[derive(Serialize)]
struct ReceiveCall {
    name: &'static str,
    arguments: ReceiveArguments,
    #[serde(rename = "_meta")]
    meta: RequestMetaObject,
}

async fn post_jsonrpc(
    url: &str,
    headers: HeaderMap,
    arguments: ReceiveArguments,
    ca_file: Option<&Path>,
) -> Result<Value, Failure> {
    let has_path = url.split_once("://").is_some_and(|(_, rest)| {
        rest.split(['?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('/'))
    });
    let original_url = url.to_owned();
    let mut url = url::Url::parse(url)
        .map_err(|_| Failure::protocol("MCP endpoint must be an absolute HTTP or HTTPS URL"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(Failure::protocol(
            "MCP endpoint must be an absolute HTTP or HTTPS URL",
        ));
    }
    if !has_path {
        url.set_path("/mcp");
    }
    let mut secrets = vec![
        original_url,
        url.username().to_owned(),
        url.password().unwrap_or_default().to_owned(),
        url.query().unwrap_or_default().to_owned(),
    ];
    secrets.extend(url.query_pairs().map(|(_, value)| value.into_owned()));
    if let Some(auth) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        secrets.push(auth.to_owned());
        secrets.push(auth.strip_prefix("Bearer ").unwrap_or(auth).to_owned());
    }
    // http.client did not interpret URL userinfo as credentials.
    url.set_username("")
        .map_err(|_| Failure::protocol("invalid MCP endpoint"))?;
    url.set_password(None)
        .map_err(|_| Failure::protocol("invalid MCP endpoint"))?;
    let revision = ProtocolVersion::V_2026_07_28;
    let payload = JsonRpcRequest::new(
        RequestId::Number(2),
        Request::<CallToolRequestMethod, _>::new(ReceiveCall {
            name: "receive",
            arguments,
            meta: RequestMetaObject::with_client_context(
                revision.clone(),
                Implementation::new("minymaton-wait", env!("CARGO_PKG_VERSION")),
                ClientCapabilities::default(),
            ),
        }),
    );
    let body = serde_json::to_vec(&payload)
        .map_err(|_| Failure::protocol("cannot encode receive request"))?;
    let request = http::Request::builder()
        .method("POST")
        .uri(&url[url::Position::BeforePath..url::Position::AfterQuery])
        .header(
            HOST,
            &url[url::Position::BeforeHost..url::Position::AfterPort],
        )
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json, text/event-stream")
        .header("MCP-Protocol-Version", revision.as_str())
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "receive")
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| Failure::transport("cannot encode HTTP request", &error))?;
    let mut request = request;
    request.headers_mut().extend(headers);
    let result = async {
        let host = url
            .host_str()
            .ok_or_else(|| Failure::protocol("invalid MCP endpoint"))?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let port = url
            .port_or_known_default()
            .ok_or_else(|| Failure::protocol("invalid MCP port"))?;
        let tls_config = if url.scheme() == "https" {
            Some(build_tls_config(ca_file)?)
        } else {
            None
        };
        // Tokio tries every resolved IPv4/IPv6 address sequentially, without a fallback timer.
        let stream = TcpStream::connect((host, port))
            .await
            .map_err(|error| Failure::transport("TCP connection failed", &error))?;
        if let Some(config) = tls_config {
            let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                .map_err(|error| Failure::transport("invalid TLS server name", &error))?;
            let stream = TlsConnector::from(Arc::new(config))
                .connect(name, stream)
                .await
                .map_err(|error| Failure::transport("TLS handshake failed", &error))?;
            exchange(stream, request).await
        } else {
            exchange(stream, request).await
        }
    }
    .await;
    result.map_err(|error| error.redact(&secrets))
}

async fn exchange<T>(stream: T, request: http::Request<Full<Bytes>>) -> Result<Value, Failure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| Failure::transport("HTTP connection failed", &error))?;
    let response = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|error| Failure::transport("HTTP request failed", &error))?;
        read_response(response).await
    };
    tokio::pin!(connection, response);
    // Keep ownership of the connection here so completion or cancellation closes the socket.
    tokio::select! {
        biased;
        result = &mut response => result,
        result = &mut connection => {
            result.map_err(|error| Failure::transport("HTTP connection failed", &error))?;
            response.await
        }
    }
}

async fn read_response(response: http::Response<Incoming>) -> Result<Value, Failure> {
    if response.status().as_u16() >= 400 {
        return Err(Failure::new(
            "transport_failed",
            format!("HTTP {}", response.status().as_u16()),
            7,
        ));
    }
    let sse = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"));
    if !sse {
        let raw = response
            .into_body()
            .collect()
            .await
            .map_err(|error| Failure::transport("HTTP response body failed", &error))?
            .to_bytes();
        let message: Value = serde_json::from_slice(&raw)
            .map_err(|_| Failure::protocol("invalid JSON-RPC response JSON"))?;
        if !response_matches(&message) {
            return Err(Failure::protocol("missing or mismatched JSON-RPC response"));
        }
        return Ok(message);
    }
    let mut pending = Vec::new();
    let mut data = Vec::new();
    let mut body = response.into_body();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| Failure::transport("HTTP SSE body failed", &error))?;
        let Some(chunk) = frame.data_ref() else {
            continue;
        };
        pending.extend_from_slice(chunk);
        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
            let raw: Vec<_> = pending.drain(..=end).collect();
            let line = std::str::from_utf8(&raw)
                .map_err(|_| Failure::protocol("invalid UTF-8 in SSE response"))?
                .trim_end_matches(['\r', '\n']);
            if let Some(line) = line.strip_prefix("data:") {
                data.push(line.trim_start_matches(' ').to_owned());
            } else if line.is_empty() && !data.is_empty() {
                let text = data.join("\n");
                data.clear();
                let message: Value = serde_json::from_str(&text)
                    .map_err(|_| Failure::protocol("invalid SSE response JSON"))?;
                if !message.is_object() {
                    return Err(Failure::protocol("JSON-RPC response must be an object"));
                }
                if response_matches(&message) {
                    return Ok(message);
                }
            }
        }
    }
    Err(Failure::new(
        "transport_failed",
        "SSE stream closed before JSON-RPC response arrived",
        7,
    ))
}

async fn run(cli: CliOptions) -> Result<(Value, u8), Failure> {
    let options = Options::resolve(cli)?;
    let arguments = ReceiveArguments {
        handle: options.handle,
        max_events: options.max_events,
        max_bytes: options.max_bytes,
        wait: true,
        ack: options.ack.filter(|ack| !ack.is_empty()),
    };
    let response = post_jsonrpc(
        &options.url,
        options.headers,
        arguments,
        options.ca_file.as_deref(),
    )
    .await?;
    if let Some(error) = response.get("error") {
        return Err(Failure {
            error: json!({"code":"jsonrpc_error","message":"receive request failed","details":error}),
            exit: 1,
        });
    }
    let result = response
        .get("result")
        .filter(|result| result.is_object())
        .ok_or_else(|| Failure::protocol("tools/call result must be an object"))?;
    let mut structured = result
        .get("structuredContent")
        .filter(|value| !value.is_null())
        .cloned();
    if structured.is_none()
        && let Some(block) = result
            .get("content")
            .and_then(Value::as_array)
            .and_then(|content| content.iter().find(|block| block["type"] == "text"))
    {
        let text = block.get("text").filter(|text| truthy(text));
        let text = match text {
            None => "{}",
            Some(Value::String(text)) => text,
            Some(_) => return Err(Failure::protocol("tool text must be a string")),
        };
        structured = Some(
            serde_json::from_str(text)
                .map_err(|_| Failure::protocol("invalid receive result JSON"))?,
        );
    }
    let structured = structured
        .filter(|value| value.is_object())
        .ok_or_else(|| Failure::protocol("receive result must contain a JSON object"))?;
    if result.get("isError").is_some_and(truthy) {
        let code = structured["code"]
            .as_str()
            .ok_or_else(|| Failure::protocol("tool error must contain a code and message"))?;
        if !structured["message"].is_string() {
            return Err(Failure::protocol(
                "tool error must contain a code and message",
            ));
        }
        let exit = match code {
            "batch_no_longer_pending" => 4,
            "subscription_not_found" => 5,
            "batch_exceeds_bounds" => 6,
            _ => 1,
        };
        return Err(Failure {
            error: structured,
            exit,
        });
    }
    match structured["status"].as_str() {
        Some("superseded") => Ok((structured, 3)),
        Some("events") => {
            if !structured["events"]
                .as_array()
                .is_some_and(|events| !events.is_empty())
                || !structured["batch_id"].is_string()
                || structured["ack_after_handling"] != structured["batch_id"]
            {
                return Err(Failure::protocol(
                    "events result is missing a batch or its acknowledgement",
                ));
            }
            Ok((structured, 0))
        }
        _ => Err(Failure {
            error: json!({"code":"unexpected_status","message":"waiting receive returned no batch","details":structured}),
            exit: 1,
        }),
    }
}

fn fail(mut failure: Failure) -> ExitCode {
    if failure.exit == 2 {
        let message = failure.error["message"].as_str().unwrap_or("usage error");
        return stderr(format_args!("{USAGE}\nminymaton-wait: error: {message}"), 2);
    }
    let mut secrets = Vec::new();
    if let Ok(token) = std::env::var("MINYMATON_TOKEN")
        && !token.is_empty()
    {
        secrets.push(token);
    }
    if let Ok(var_name) = std::env::var("MINYMATON_TOKEN_ENV")
        && !var_name.is_empty()
        && let Ok(token) = std::env::var(&var_name)
        && !token.is_empty()
    {
        secrets.push(token);
    }
    failure = failure.redact(&secrets);
    let mut error = failure.error;
    error["status"] = json!("error");
    terminal_json(&error, failure.exit)
}

struct AsciiJson;

impl serde_json::ser::Formatter for AsciiJson {
    fn write_string_fragment<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        for character in fragment.chars() {
            if (' '..='\u{7e}').contains(&character) {
                write!(writer, "{character}")?;
            } else {
                for unit in character.encode_utf16(&mut [0; 2]) {
                    write!(writer, "\\u{unit:04x}")?;
                }
            }
        }
        Ok(())
    }
}

fn terminal_json(value: &Value, exit: u8) -> ExitCode {
    let write = |mut output: &mut dyn Write| -> io::Result<()> {
        value
            .serialize(&mut serde_json::Serializer::with_formatter(
                &mut output,
                AsciiJson,
            ))
            .map_err(io::Error::other)?;
        output.write_all(b"\n")?;
        output.flush()
    };
    let result = if exit == 0 {
        write(&mut io::stdout().lock())
    } else {
        write(&mut io::stderr().lock())
    };
    match result {
        Ok(()) => ExitCode::from(exit),
        Err(error) if exit == 0 => fail(Failure::io("cannot write waiter stdout", error)),
        Err(_) => ExitCode::from(7),
    }
}

fn write_line(mut output: impl Write, value: impl Display) -> io::Result<()> {
    writeln!(output, "{value}")?;
    output.flush()
}

fn stdout(value: impl Display) -> ExitCode {
    match write_line(io::stdout().lock(), value) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => fail(Failure::io("cannot write waiter stdout", error)),
    }
}

fn stderr(value: impl Display, exit: u8) -> ExitCode {
    match write_line(io::stderr().lock(), value) {
        Ok(()) => ExitCode::from(exit),
        // No diagnostic sink remains; stdout must stay reserved for delivered batches.
        Err(_) => ExitCode::from(7),
    }
}

#[cfg(unix)]
fn interrupt_signal() -> Result<impl Future<Output = Result<(), Failure>>, Failure> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let mut sender = Some(sender);
    // This is the executable's first SIGINT registration. exec resets caught
    // handlers, so MultipleHandlers here denotes an inherited ignored disposition.
    let receiver = match ctrlc::try_set_handler(move || {
        if let Some(sender) = sender.take() {
            // A completed request may already have dropped its interrupt receiver.
            let _ = sender.send(());
        }
    }) {
        Ok(()) => Some(receiver),
        Err(ctrlc::Error::MultipleHandlers) => None,
        Err(ctrlc::Error::System(error)) => {
            return Err(Failure::transport("cannot listen for Ctrl+C", &error));
        }
        Err(error) => return Err(Failure::transport("cannot listen for Ctrl+C", &error)),
    };
    Ok(async move {
        match receiver {
            Some(receiver) => receiver
                .await
                .map_err(|error| Failure::transport("Ctrl+C channel closed", &error)),
            None => std::future::pending().await,
        }
    })
}

#[cfg(windows)]
fn interrupt_signal() -> Result<impl Future<Output = Result<(), Failure>>, Failure> {
    Ok(async {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| Failure::transport("cannot listen for Ctrl+C", &error))
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let interrupted = match interrupt_signal() {
        Ok(interrupted) => interrupted,
        Err(error) => return fail(error),
    };
    let options = match CliOptions::parse() {
        Ok(Some(options)) => options,
        Ok(None) => {
            return stdout(format_args!(
                "{USAGE}\n\nWait for the next minymaton batch on a subscription.\n\n-h, --help      Show this help message and exit\n--handle HANDLE   Subscription handle (sub_...)\n--ack ACK         Optional handled batch_id (bat_...) to acknowledge first\n--max-events N   Maximum events to seal (default: 10)\n--max-bytes N    Maximum wire bytes (default: 16384)\n--url URL        Optional MCP endpoint URL override\n--ca-file PATH   Optional PEM CA certificate bundle for HTTPS verification\n--config PATH    Optional INI configuration file\n--token-env VAR  Bearer token environment variable name (default: MINYMATON_TOKEN)\n\nUnambiguous long-option prefixes are accepted. Integer bounds accept decimal Unicode digits, signs, inter-digit underscores and surrounding whitespace."
            ));
        }
        Err(error) => {
            return stderr(format_args!("{USAGE}\nminymaton-wait: error: {error}"), 2);
        }
    };
    let outcome = tokio::select! {
        biased;
        interrupted = interrupted => match interrupted {
            Ok(()) => Err(Failure::new("interrupted", "waiter interrupted", 130)),
            Err(error) => Err(error),
        },
        outcome = run(options) => outcome,
    };
    match outcome {
        Ok((terminal, exit)) => terminal_json(&terminal, exit),
        Err(error) => fail(error),
    }
}
