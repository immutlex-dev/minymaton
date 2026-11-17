"""Build a native trial waiter from the published, locked public crate."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "aarch64-pc-windows-msvc",
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--target", required=True, choices=TARGETS)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".exe" if os.name == "nt" else ""
    with tempfile.TemporaryDirectory(prefix="minymaton-wait-build-") as directory:
        root = Path(directory)
        subprocess.run(
            [
                "cargo", "install", "--locked", "--version", args.version,
                "--target", args.target, "--bin", "minymaton-wait",
                "--root", str(root / "install"), "minymaton-server",
            ],
            cwd=root,
            check=True,
        )
        built = root / "install" / "bin" / f"minymaton-wait{suffix}"
        subprocess.run([str(built), "--help"], cwd=root, check=True)
        destination = out_dir / f"minymaton-wait-{args.target}{suffix}"
        shutil.copy2(built, destination)
        print(f"Built and smoke-tested {destination.name}", flush=True)


if __name__ == "__main__":
    main()
