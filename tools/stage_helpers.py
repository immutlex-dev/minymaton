#!/usr/bin/env python3
"""Stage minymaton OSS helper and standalone SQLite server artifacts, AGPL-3.0-only source archive, and SHA256SUMS."""

import argparse
import hashlib
import pathlib
import subprocess
import sys

BINARY_ARTIFACTS = [
    "minymaton-wait-x86_64-unknown-linux-gnu",
    "minymaton-wait-aarch64-unknown-linux-gnu",
    "minymaton-wait-x86_64-apple-darwin",
    "minymaton-wait-aarch64-apple-darwin",
    "minymaton-wait-x86_64-pc-windows-msvc.exe",
    "minymaton-wait-aarch64-pc-windows-msvc.exe",
    "minymaton-server-x86_64-unknown-linux-gnu",
    "minymaton-server-aarch64-unknown-linux-gnu",
    "minymaton-server-x86_64-apple-darwin",
    "minymaton-server-aarch64-apple-darwin",
    "minymaton-server-x86_64-pc-windows-msvc.exe",
    "minymaton-server-aarch64-pc-windows-msvc.exe",
]

README_TXT = """minymaton OSS helper and standalone SQLite server distribution (https://dl.scritty.dev/minymaton/)

License: GNU Affero General Public License v3.0 only (AGPL-3.0-only)

Artifacts in this directory:
  - minymaton-wait-x86_64-unknown-linux-gnu        Native Linux x86_64 waiter binary
  - minymaton-wait-aarch64-unknown-linux-gnu       Native Linux aarch64 waiter binary
  - minymaton-wait-x86_64-apple-darwin             Native macOS x86_64 waiter binary
  - minymaton-wait-aarch64-apple-darwin            Native macOS aarch64 (Apple Silicon) waiter binary
  - minymaton-wait-x86_64-pc-windows-msvc.exe      Native Windows x86_64 waiter binary
  - minymaton-wait-aarch64-pc-windows-msvc.exe     Native Windows ARM64 waiter binary
  - minymaton-server-x86_64-unknown-linux-gnu      Standalone OSS SQLite server binary (Linux x86_64)
  - minymaton-server-aarch64-unknown-linux-gnu     Standalone OSS SQLite server binary (Linux aarch64)
  - minymaton-server-x86_64-apple-darwin           Standalone OSS SQLite server binary (macOS x86_64)
  - minymaton-server-aarch64-apple-darwin          Standalone OSS SQLite server binary (macOS aarch64)
  - minymaton-server-x86_64-pc-windows-msvc.exe    Standalone OSS SQLite server binary (Windows x86_64)
  - minymaton-server-aarch64-pc-windows-msvc.exe   Standalone OSS SQLite server binary (Windows ARM64)
  - minymaton-wait.py                              Portable Python 3 stdlib-only waiter helper
  - LICENSE                                        GNU Affero General Public License v3.0 (AGPL-3.0-only)
  - source.tar.gz                                  Corresponding Git source archive for this build
  - stage_helpers.py                               Staging and SHA256SUMS generator script
  - SHA256SUMS                                     SHA-256 checksums for every artifact (plain checksums, unsigned)

Waiter configuration contract (harness-decoupled):
  Precedence: CLI flags > MINYMATON_* environment variables > INI [minymaton] / [DEFAULT] > defaults.
  Neither helper reads client harness config files or assumes a default localhost port.

  Required:
    --handle HANDLE   or MINYMATON_HANDLE   or INI handle
    --url URL         or MINYMATON_URL      or INI url

  Optional:
    --ack ACK         or MINYMATON_ACK      or INI ack
    --max-events N    or MINYMATON_MAX_EVENTS or INI max_events (default: 10)
    --max-bytes N     or MINYMATON_MAX_BYTES  or INI max_bytes  (default: 16384)
    --ca-file PATH    or MINYMATON_CA_FILE  or INI ca_file
    --config PATH     or MINYMATON_CONFIG
    --token-env VAR   or MINYMATON_TOKEN_ENV or INI token_env   (default: MINYMATON_TOKEN)
"""


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        required=True,
        type=pathlib.Path,
        help="Staging directory containing the 6 compiled native binaries",
    )
    parser.add_argument(
        "--repo-root",
        default=pathlib.Path(__file__).resolve().parent.parent,
        type=pathlib.Path,
        help="Path to the minymaton Git repository root",
    )
    parser.add_argument(
        "--allow-partial-binaries",
        action="store_true",
        help="Stage available files even when not all 6 native binaries are present yet",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    for src_rel, dst_name in (
        (pathlib.Path("tools") / "minymaton-wait.py", "minymaton-wait.py"),
        (pathlib.Path("tools") / "stage_helpers.py", "stage_helpers.py"),
        (pathlib.Path("LICENSE"), "LICENSE"),
    ):
        (out_dir / dst_name).write_bytes((repo_root / src_rel).read_bytes().replace(b"\r\n", b"\n"))
    (out_dir / "README.txt").write_bytes(README_TXT.encode("utf-8"))

    subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "archive",
            "--format=tar.gz",
            "--prefix=minymaton/",
            "-o",
            str(out_dir / "source.tar.gz"),
            "HEAD",
        ],
        check=True,
    )

    missing = [name for name in BINARY_ARTIFACTS if not (out_dir / name).is_file()]
    if missing and not args.allow_partial_binaries:
        sys.stderr.write(f"missing native binary artifacts: {', '.join(missing)}\n")
        return 1

    staged_names = [
        name for name in BINARY_ARTIFACTS if (out_dir / name).is_file()
    ] + [
        "minymaton-wait.py",
        "LICENSE",
        "source.tar.gz",
        "README.txt",
        "stage_helpers.py",
    ]

    lines = [f"{sha256_file(out_dir / name)}  {name}\n" for name in staged_names]
    (out_dir / "SHA256SUMS").write_bytes("".join(lines).encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
