use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use http::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use minymaton_server::mcp_http::{HttpFailure, extract_tool_structured, post_tools_call};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};

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
        HttpFailure::transport(stage, error).into()
    }

    fn io(message: &str, error: io::Error) -> Self {
        HttpFailure::io(message, error).into()
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

impl From<HttpFailure> for Failure {
    fn from(failure: HttpFailure) -> Self {
        Self {
            error: failure.error,
            exit: failure.exit,
        }
    }
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

async fn run(cli: CliOptions) -> Result<(Value, u8), Failure> {
    let options = Options::resolve(cli)?;
    let arguments = ReceiveArguments {
        handle: options.handle,
        max_events: options.max_events,
        max_bytes: options.max_bytes,
        wait: true,
        ack: options.ack.filter(|ack| !ack.is_empty()),
    };
    let response = post_tools_call(
        &options.url,
        options.headers,
        "receive",
        "minymaton-wait",
        arguments,
        options.ca_file.as_deref(),
    )
    .await?;
    let (structured, is_error) = extract_tool_structured(&response, "receive")?;
    if is_error {
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
