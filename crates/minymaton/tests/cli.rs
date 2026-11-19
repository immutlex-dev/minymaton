use std::process::Command;

#[test]
fn minymaton_binary_runs_dry_stream_and_subscription_cli_commands() {
    let dir = std::env::temp_dir().join(format!("minymaton-cli-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let db = dir.join("queue.sqlite3");
    let db_str = db.to_str().unwrap();

    for args in [
        vec!["stream", "create", "--name", "ops", "--db", db_str],
        vec!["stream", "create", "--name", "stale", "--db", db_str],
        vec![
            "subscription",
            "create",
            "--stream",
            "ops",
            "--label",
            "idle-1",
            "--db",
            db_str,
        ],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_minymaton"))
            .args(&args)
            .output()
            .expect("run minymaton cli");
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let swept = Command::new(env!("CARGO_BIN_EXE_minymaton"))
        .args([
            "stream",
            "sweep-idle",
            "--keep",
            "ops",
            "--db",
            db_str,
            "--format",
            "json",
        ])
        .output()
        .expect("run minymaton stream sweep-idle");
    assert!(
        swept.status.success(),
        "{}",
        String::from_utf8_lossy(&swept.stderr)
    );
    assert_eq!(
        String::from_utf8(swept.stdout)
            .unwrap()
            .trim_end_matches('\n'),
        r#"{"status":"swept","deleted":["stale"],"deleted_count":1}"#
    );
    let _ = std::fs::remove_dir_all(&dir);
}
