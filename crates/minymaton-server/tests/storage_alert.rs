use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;

use serde_json::Value;

#[test]
fn real_disk_checks_deliver_transitions_and_preserve_state_after_delivery_failure() {
    let directory = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let state = directory.path().join("state.json");
    let webhook = directory.path().join("webhook");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let collector = std::thread::spawn(move || {
        let mut deliveries = Vec::new();
        for stream in listener.incoming() {
            let mut reader = BufReader::new(stream.unwrap());
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" {
                    break;
                }
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let failed = first.contains("/fail ");
            reader
                .get_mut()
                .write_all(if failed {
                    b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                } else {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                })
                .unwrap();
            if first.contains("/finish ") {
                break;
            }
            deliveries.push(serde_json::from_slice::<Value>(&body).unwrap());
        }
        deliveries
    });
    let run = |threshold: &str, test: bool| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_minymaton-storage-check"));
        command
            .arg("--path")
            .arg(directory.path())
            .arg("--state")
            .arg(&state)
            .arg("--webhook-file")
            .arg(&webhook)
            .args(["--host", "integration", "--minimum-free-percent", threshold]);
        if test {
            command.arg("--notify-test");
        }
        command.output().unwrap()
    };
    std::fs::write(&webhook, format!("http://{address}/fail")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&webhook, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let healthy = run("1", false);
    assert!(
        healthy.status.success(),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    let original = std::fs::read(&state).unwrap();
    assert_eq!(original.len(), 17, "fixed-size state slot");
    let failed = run("100", false);
    assert!(
        !failed.status.success(),
        "a rejected delivery must fail the checker"
    );
    assert_eq!(std::fs::read(&state).unwrap(), original);
    std::fs::write(&webhook, format!("http://{address}/alert")).unwrap();
    for threshold in ["100", "100", "1"] {
        let output = run(threshold, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(&state).unwrap().len(),
            17,
            "state slot remains fixed-size across low and healthy transitions"
        );
    }
    let before_test = std::fs::read(&state).unwrap();
    assert_eq!(before_test.len(), 17);
    let test = run("100", true);
    assert!(
        test.status.success(),
        "{}",
        String::from_utf8_lossy(&test.stderr)
    );
    assert_eq!(
        std::fs::read(&state).unwrap(),
        before_test,
        "delivery tests must not consume real capacity transitions"
    );
    let mut finish = TcpStream::connect(address).unwrap();
    finish.write_all(b"POST /finish HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
    finish.read_to_end(&mut Vec::new()).unwrap();
    let deliveries = collector.join().unwrap();
    assert_eq!(
        deliveries.len(),
        4,
        "failed low, retried low, recovery, delivery test"
    );
    assert!(
        deliveries[0]["text"]
            .as_str()
            .unwrap()
            .contains("LOW SPACE")
    );
    assert!(
        deliveries[1]["text"]
            .as_str()
            .unwrap()
            .contains("LOW SPACE")
    );
    assert!(
        deliveries[2]["text"]
            .as_str()
            .unwrap()
            .contains("RECOVERED")
    );
    assert!(
        deliveries[3]["text"]
            .as_str()
            .unwrap()
            .contains("DELIVERY TEST")
    );
}
