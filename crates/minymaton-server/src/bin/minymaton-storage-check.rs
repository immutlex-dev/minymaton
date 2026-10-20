use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};

const STATE_SLOT_BYTES: usize = 16;

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum State {
    Healthy,
    Low,
}

struct Options {
    path: PathBuf,
    state: PathBuf,
    webhook: PathBuf,
    host: String,
    minimum_free_percent: u8,
    notify_test: bool,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut path = None;
        let mut state = None;
        let mut webhook = None;
        let mut host = None;
        let mut minimum_free_percent = 10;
        let mut notify_test = false;
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            if argument == "--notify-test" {
                notify_test = true;
                continue;
            }
            let value = arguments
                .next()
                .ok_or_else(|| format!("{argument} needs a value"))?;
            match argument.as_str() {
                "--path" => path = Some(PathBuf::from(value)),
                "--state" => state = Some(PathBuf::from(value)),
                "--webhook-file" => webhook = Some(PathBuf::from(value)),
                "--host" if !value.is_empty() && !value.chars().any(char::is_control) => {
                    host = Some(value)
                }
                "--minimum-free-percent" => {
                    minimum_free_percent = value
                        .parse()
                        .map_err(|_| "minimum free percent must be 1 to 100")?;
                    if !(1..=100).contains(&minimum_free_percent) {
                        return Err("minimum free percent must be 1 to 100".to_owned());
                    }
                }
                _ => return Err(format!("unknown or invalid option: {argument}")),
            }
        }
        Ok(Self {
            path: path.ok_or("--path is required")?,
            state: state.ok_or("--state is required")?,
            webhook: webhook.ok_or("--webhook-file is required")?,
            host: host.ok_or("--host is required")?,
            minimum_free_percent,
            notify_test,
        })
    }
}

fn private_file(path: &Path, truncate: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .create(true)
        .read(true)
        .write(true)
        .truncate(truncate);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn deliver(webhook: &Path, text: &str) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(webhook)?.permissions().mode() & 0o077 != 0 {
            return Err("webhook file must be private (mode 0600)".into());
        }
    }
    let endpoint = std::fs::read_to_string(webhook)?.trim().to_owned();
    let uri: http::Uri = endpoint.parse().map_err(|_| "invalid webhook URL")?;
    let local = matches!(uri.host(), Some("127.0.0.1" | "[::1]" | "localhost"));
    if uri.authority().is_none()
        || !(uri.scheme_str() == Some("https") || (uri.scheme_str() == Some("http") && local))
    {
        return Err("webhook requires HTTPS, or loopback HTTP for local delivery".into());
    }
    let mut child = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--fail",
            "--http1.1",
            "--config",
            "-",
            "--header",
            "Content-Type: application/json",
            "--data-binary",
        ])
        .arg(serde_json::json!({"text": text}).to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let configuration = format!("url = {}\n", serde_json::to_string(&endpoint)?);
    let written = child
        .stdin
        .take()
        .ok_or("curl stdin unavailable")?
        .write_all(configuration.as_bytes());
    let output = child.wait_with_output()?;
    written?;
    if !output.status.success() {
        return Err(format!(
            "webhook delivery failed (curl exit {:?}); state was not advanced",
            output.status.code()
        )
        .into());
    }
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse()?;
    let parent = options
        .state
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let lock = private_file(&options.state.with_extension("lock"), false)?;
    lock.lock()?;
    let available = fs2::available_space(&options.path)?;
    let total = fs2::total_space(&options.path)?;
    if total == 0 || available > total {
        return Err("filesystem returned invalid capacity statistics".into());
    }
    let current = if u128::from(available) * 100
        < u128::from(total) * u128::from(options.minimum_free_percent)
    {
        State::Low
    } else {
        State::Healthy
    };
    let mut state_file = private_file(&options.state, false)?;
    let mut existing = Vec::new();
    state_file.read_to_end(&mut existing)?;
    let previous = if existing.iter().all(u8::is_ascii_whitespace) {
        None
    } else {
        Some(serde_json::from_slice::<State>(&existing)?)
    };
    let transition = previous != Some(current) && (current == State::Low || previous.is_some());
    if transition || options.notify_test {
        let status = if options.notify_test {
            "DELIVERY TEST"
        } else if current == State::Low {
            "LOW SPACE"
        } else {
            "RECOVERED"
        };
        let message = format!(
            "minymaton {status} on {}: {} available / {} total bytes ({:.2}% free), threshold {}%, path {}.{}",
            options.host,
            available,
            total,
            available as f64 * 100.0 / total as f64,
            options.minimum_free_percent,
            options.path.display(),
            if current == State::Low {
                " Free space before queue writes fail."
            } else {
                ""
            }
        );
        deliver(&options.webhook, &message)?;
    }
    if previous != Some(current) && !options.notify_test {
        let encoded = format!("{:<STATE_SLOT_BYTES$}\n", serde_json::to_string(&current)?);
        state_file.seek(SeekFrom::Start(0))?;
        state_file.write_all(encoded.as_bytes())?;
        state_file.sync_all()?;
        if previous.is_none() {
            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
        }
    }
    println!(
        "{}: {} / {} bytes available ({:.2}% free), minimum {}%",
        options.host,
        available,
        total,
        available as f64 * 100.0 / total as f64,
        options.minimum_free_percent
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("minymaton-storage-check: {error}");
        std::process::exit(1);
    }
}
