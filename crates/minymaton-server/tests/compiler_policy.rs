use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

fn cargo_dependencies(root: &Path, target_dir: &Path) -> HashMap<String, PathBuf> {
    let output = Command::new(env!("CARGO"))
        .args(["test", "--offline", "--locked", "--manifest-path"])
        .arg(root.join("Cargo.toml"))
        .args([
            "-p",
            "minymaton-server",
            "--lib",
            "--no-run",
            "--message-format=json",
        ])
        .arg("--target-dir")
        .arg(target_dir)
        .output()
        .expect("query Cargo dependency artifacts");
    assert!(
        output.status.success(),
        "cargo test --no-run --message-format=json failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut libraries = HashMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if message["reason"] != "compiler-artifact" {
            continue;
        }
        let Some(name) = message["target"]["name"].as_str() else {
            continue;
        };
        if let Some(rlib) = message["filenames"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str())
            .find(|path| path.ends_with(".rlib"))
        {
            libraries.insert(name.to_owned(), PathBuf::from(rlib));
        }
    }
    libraries
}

#[test]
fn the_real_compiler_distinguishes_request_bounds_from_internal_timers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let executable = std::env::current_exe().unwrap();
    let target_dir = executable
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("Cargo target directory");
    let libraries = cargo_dependencies(&root, target_dir);
    let tokio_rlib = libraries.get("tokio").expect("Cargo tokio rlib");
    let serde_json_rlib = libraries.get("serde_json").expect("Cargo serde_json rlib");
    // The nested Cargo build picks its own profile; resolve every dependency
    // from the directory it put the selected rlibs in, not from this test's.
    let dependencies = tokio_rlib.parent().expect("Cargo dependency directory");
    assert_eq!(
        serde_json_rlib.parent(),
        Some(dependencies),
        "tokio and serde_json come from one Cargo artifact directory"
    );
    let temporary = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let decoding = "let request: serde_json::Value = serde_json::from_value(arguments).unwrap();";
    let budget = "let budget = std::time::Duration::from_millis(request[\"max_wait_ms\"].as_u64().unwrap_or(25000));";
    let cases = [
        ("request_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let deadline = tokio::time::Instant::now() + budget;
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), None),
        ("aliased_input", format!(
            "pub async fn receive(input: serde_json::Value) {{ let arguments = input; {decoding} {budget}
             let deadline = tokio::time::Instant::now() + budget;
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), None),
        ("fixed_timer", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding}
             let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("control_only_branch", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding}
             let duration = if request[\"use_long\"].as_u64().unwrap_or(0) == 1 {{
                 std::time::Duration::from_secs(2)
             }} else {{
                 std::time::Duration::from_secs(1)
             }};
             let deadline = tokio::time::Instant::now() + duration;
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("arithmetic_erased_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let deadline = tokio::time::Instant::now() + (budget * 0 + std::time::Duration::from_secs(30));
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("inflated_max_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let deadline = tokio::time::Instant::now() + budget.max(std::time::Duration::from_secs(3600));
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("shifted_clock_inline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let deadline = (tokio::time::Instant::now() + std::time::Duration::from_secs(3600)) + budget;
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("shifted_clock_local", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let base = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
             let _expiry = tokio::time::sleep_until(base + budget); }}"
        ), Some("fogscrib::timed_wait")),
        ("compound_assigned_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding}
             let mut budget = std::time::Duration::from_millis(request[\"max_wait_ms\"].as_u64().unwrap_or(25000));
             budget += std::time::Duration::from_secs(3600);
             let _expiry = tokio::time::sleep_until(tokio::time::Instant::now() + budget); }}"
        ), Some("fogscrib::timed_wait")),
        ("compound_assigned_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let mut deadline = tokio::time::Instant::now() + budget;
             deadline += std::time::Duration::from_secs(3600);
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("local_method_spoof", format!(
            "trait Spoof {{
                 fn unwrap_or(&self, _default: u64) -> u64;
             }}
             impl Spoof for serde_json::Value {{
                 fn unwrap_or(&self, _default: u64) -> u64 {{ 3_600_000 }}
             }}
             pub async fn receive(arguments: serde_json::Value) {{ {decoding}
             let wait_ms = request[\"max_wait_ms\"].unwrap_or(25000);
             let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("bitwise_not_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding}
             let wait_ms = (!request[\"max_wait_ms\"].as_u64().unwrap_or(0)).min(25000);
             let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("cast_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding}
             let wait_ms = (request[\"max_wait_ms\"].as_u64().unwrap_or(25000) as u8 as u64).min(25000);
             let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("closure_discarded_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let discard = |_budget: std::time::Duration| std::time::Duration::from_secs(30);
             let deadline = tokio::time::Instant::now() + discard(budget);
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("closure_in_loop_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             for _item in [1, 2] {{
                 let make = || tokio::time::sleep_until(tokio::time::Instant::now() + budget);
                 let _expiry = make();
             }} }}"
        ), Some("fogscrib::timed_wait")),
        ("closure_outside_loop_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let arm = || tokio::time::sleep_until(tokio::time::Instant::now() + budget);
             for _item in [1, 2] {{
                 let _expiry = arm();
             }} }}"
        ), Some("fogscrib::timed_wait")),
        ("closure_repeated_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let arm = || tokio::time::sleep_until(tokio::time::Instant::now() + budget);
             let _first = arm();
             let _second = arm(); }}"
        ), Some("fogscrib::timed_wait")),
        ("closure_async_block_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let arm = || async move {{
                 tokio::time::sleep_until(tokio::time::Instant::now() + budget).await;
             }};
             let _future = arm(); }}"
        ), Some("fogscrib::timed_wait")),
        ("async_closure_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let arm = async || {{
                 tokio::time::sleep_until(tokio::time::Instant::now() + budget).await;
             }};
             let _future = arm(); }}"
        ), Some("fogscrib::timed_wait")),
        ("overloaded_deref_budget", format!(
            "struct FakeRef(u64);
             impl std::ops::Deref for FakeRef {{
                 type Target = u64;
                 fn deref(&self) -> &u64 {{ &3_600_000 }}
             }}
             pub async fn receive(arguments: serde_json::Value) {{ {decoding}
                 let mut holder = FakeRef(0);
                 holder.0 = request[\"max_wait_ms\"].as_u64().unwrap_or(25000);
                 let wait_ms = (*holder).min(25000);
                 let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
                 let _expiry = tokio::time::sleep_until(deadline);
             }}"
        ), Some("fogscrib::timed_wait")),
        ("autoderef_method_budget", format!(
            "struct FakeOption(u64);
             impl std::ops::Deref for FakeOption {{
                 type Target = Option<u64>;
                 fn deref(&self) -> &Option<u64> {{ &Some(3_600_000) }}
             }}
             pub async fn receive(arguments: serde_json::Value) {{ {decoding}
                 let mut holder = FakeOption(0);
                 holder.0 = request[\"max_wait_ms\"].as_u64().unwrap_or(25000);
                 let wait_ms = holder.unwrap_or(25000);
                 let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(wait_ms);
                 let _expiry = tokio::time::sleep_until(deadline);
             }}"
        ), Some("fogscrib::timed_wait")),
        ("reset_timer", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             let deadline = tokio::time::Instant::now() + budget;
             let mut expiry = Box::pin(tokio::time::sleep_until(deadline));
             expiry.as_mut().reset(tokio::time::Instant::now() + std::time::Duration::from_secs(5)); }}"
        ), Some("fogscrib::timed_wait")),
        ("interval_at_timer",
            "pub async fn tick() { let _ = tokio::time::interval_at(tokio::time::Instant::now(), std::time::Duration::from_secs(1)); }".to_owned(),
            Some("fogscrib::timed_wait")),
        ("constructed_request", format!(
            "pub async fn receive() {{ let arguments = serde_json::json!({{\"max_wait_ms\": 10}}); {decoding} {budget}
             let deadline = tokio::time::Instant::now() + budget;
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("mutated_request", format!(
            "pub async fn receive(mut arguments: serde_json::Value) {{
             arguments = serde_json::json!({{\"max_wait_ms\": 10}}); {decoding} {budget}
             let deadline = tokio::time::Instant::now() + budget;
             let _expiry = tokio::time::sleep_until(deadline); }}"
        ), Some("fogscrib::timed_wait")),
        ("renewed_deadline", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
             for _item in [1, 2] {{
                let deadline = tokio::time::Instant::now() + budget;
                let _expiry = tokio::time::sleep_until(deadline);
             }} }}"
        ), Some("fogscrib::timed_wait")),
        ("helper_deadline", format!(
            "fn deadline(arguments: serde_json::Value) -> tokio::time::Instant {{ {decoding} {budget}
                tokio::time::Instant::now() + budget
             }}
             pub async fn receive(arguments: serde_json::Value) {{
                let _expiry = tokio::time::sleep_until(deadline(arguments));
             }}"
        ), Some("fogscrib::timed_wait")),
        ("native_sleep", "pub fn wait() { std::thread::sleep(std::time::Duration::from_secs(1)); }".to_owned(),
            Some("fogscrib::timed_wait")),
        ("discarded_budget", format!(
            "fn discard(_request: serde_json::Value) -> std::time::Duration {{
                std::time::Duration::from_secs(1)
             }}
             pub async fn receive(arguments: serde_json::Value) {{ {decoding}
                let deadline = tokio::time::Instant::now() + discard(request);
                let _expiry = tokio::time::sleep_until(deadline);
             }}"
        ), Some("fogscrib::timed_wait")),
        ("replaced_budget", format!(
            "pub async fn receive(arguments: serde_json::Value) {{ {decoding} {budget}
                let mut replaced = budget;
                replaced = std::time::Duration::from_secs(1);
                let deadline = tokio::time::Instant::now() + replaced;
                let _expiry = tokio::time::sleep_until(deadline);
             }}"
        ), Some("fogscrib::timed_wait")),
        ("internal_generation", "pub struct State { pub schema_version: u64 }".to_owned(),
            Some("fogscrib::schema_generation")),
        ("event_wait", "pub async fn wait(mut events: tokio::sync::mpsc::Receiver<()>) { events.recv().await; }".to_owned(),
            None),
    ];
    for (name, source, expected_error) in cases {
        let path = temporary.path().join(format!("{name}.rs"));
        std::fs::write(&path, source).unwrap();
        let output = Command::new(root.join("tools/compiler-lints/bin/fogscrib-rustc"))
            .args(["--edition=2024", "--crate-type=lib", "--emit=metadata"])
            .arg(&path)
            .arg("--out-dir")
            .arg(temporary.path())
            .arg("-L")
            .arg(format!("dependency={}", dependencies.display()))
            .arg("--extern")
            .arg(format!("tokio={}", tokio_rlib.display()))
            .arg("--extern")
            .arg(format!("serde_json={}", serde_json_rlib.display()))
            .output()
            .expect("invoke the real typed compiler gate");
        let diagnostics = String::from_utf8_lossy(&output.stderr);
        let metadata = temporary.path().join(format!("lib{name}.rmeta"));
        match expected_error {
            Some(error) => {
                assert!(!output.status.success(), "{name} unexpectedly compiled");
                assert!(diagnostics.contains(error), "{name}: {diagnostics}");
                assert!(!metadata.exists(), "{name} produced accepted metadata");
            }
            None => {
                assert!(output.status.success(), "{name}: {diagnostics}");
                assert!(metadata.exists(), "{name} produced no metadata");
            }
        }
    }
}
