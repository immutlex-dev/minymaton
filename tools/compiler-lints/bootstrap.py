"""Build the pinned compiler gate without modifying a Cargo manifest or lockfile."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
RESULTS = HERE / "results"


def run(command: list[str], name: str, env: dict[str, str]) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    (RESULTS / f"{name}.stdout.log").write_text(
        result.stdout, encoding="utf-8"
    )
    (RESULTS / f"{name}.stderr.log").write_text(
        result.stderr, encoding="utf-8"
    )
    (RESULTS / f"{name}.json").write_text(
        json.dumps({"command": command, "exit_code": result.returncode}, indent=2),
        encoding="utf-8",
    )
    if result.returncode:
        raise RuntimeError(f"{name} failed ({result.returncode}):\n{result.stderr}")
    return result.stdout.strip()


def build(directory: Path, env: dict[str, str], toolchain: str, check: bool) -> None:
    sources = {source: source.read_bytes() for source in HERE.glob("*.rs")}
    sources[ROOT / "rust-toolchain.toml"] = (ROOT / "rust-toolchain.toml").read_bytes()
    suffix = ".exe" if os.name == "nt" else ""
    command = [
        "rustc",
        f"+{toolchain}",
        "--edition=2024",
        "-Dwarnings",
        "-Copt-level=2",
        "--crate-name=fogscrib_lint_driver",
        str(HERE / "driver.rs"),
    ]
    if check:
        command.extend(["--emit=metadata", "--out-dir", str(directory)])
    else:
        command.extend(
            [
                "-Cprefer-dynamic",
                "-L",
                f"native={Path(env['FOGSCRIB_LINT_SYSROOT']) / 'lib'}",
                "-o",
                str(directory / f"fogscrib-lint-driver{suffix}"),
            ]
        )
    run(command, "driver-build", env)
    if not check:
        env.pop("RUSTC_BOOTSTRAP")
        run(
            [
                "rustc",
                f"+{toolchain}",
                "--edition=2024",
                "-Dwarnings",
                "-Copt-level=2",
                "--crate-name=fogscrib_rustc",
                str(HERE / "wrapper.rs"),
                "-o",
                str(directory / f"fogscrib-rustc{suffix}"),
            ],
            "wrapper-build",
            env,
        )
        if any(source.read_bytes() != contents for source, contents in sources.items()):
            raise RuntimeError(
                "compiler policy changed during bootstrap; nothing installed"
            )
        for binary in directory.glob(f"*{suffix}" if suffix else "*"):
            if binary.is_file() and (binary.suffix == suffix or not suffix):
                destination = HERE / "bin" / binary.name
                if os.name == "nt" and destination.exists():
                    retired = Path(
                        tempfile.mkdtemp(prefix="retired-", dir=HERE / "bin")
                    )
                    destination.rename(retired / destination.name)
                os.replace(binary, destination)


def main() -> None:
    global RESULTS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="check driver without linking"
    )
    parser.add_argument(
        "--lint",
        action="store_true",
        help="compile both tools through their own gate and Clippy",
    )
    parser.add_argument("--results-dir", type=Path, help="retain complete compiler commands and logs here")
    args = parser.parse_args()
    RESULTS = (args.results_dir or RESULTS).resolve()
    (HERE / "bin").mkdir(exist_ok=True)
    RESULTS.mkdir(parents=True, exist_ok=True)
    toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"][
        "channel"
    ]
    env = os.environ.copy()
    sysroot = run(["rustc", f"+{toolchain}", "--print", "sysroot"], "sysroot", env)
    identity = run(["rustc", f"+{toolchain}", "-vV"], "identity", env)
    host = next(
        line.removeprefix("host: ")
        for line in identity.splitlines()
        if line.startswith("host: ")
    )
    env["FOGSCRIB_LINT_SYSROOT"] = sysroot
    env["FOGSCRIB_LINT_HOST"] = host
    env["FOGSCRIB_LINT_IDENTITY"] = identity
    env["RUSTC_BOOTSTRAP"] = "fogscrib_lint_driver"
    with tempfile.TemporaryDirectory(prefix="build-", dir=HERE / "bin") as temporary:
        build(Path(temporary), env, toolchain, args.check)
    suffix = ".exe" if os.name == "nt" else ""
    if args.lint and not args.check:
        for source in HERE.glob("*.rs"):
            if source.stem == "policy":
                continue
            crate = (
                "fogscrib_lint_driver" if source.stem == "driver" else "fogscrib_rustc"
            )
            env["RUSTC_BOOTSTRAP"] = "fogscrib_lint_driver"
            run(
                [
                    str(HERE / "bin" / f"fogscrib-rustc{suffix}"),
                    "--edition=2024",
                    "-Dwarnings",
                    f"--crate-name={crate}",
                    "--emit=metadata",
                    str(source),
                    "--out-dir",
                    str(HERE / "bin"),
                ],
                f"{source.stem}-self-lint",
                env,
            )
    print(
        f"Compiler gate built with {identity.splitlines()[0]}; full logs: {RESULTS}"
    )


if __name__ == "__main__":
    main()
