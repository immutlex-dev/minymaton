"""Build minymaton-mobile-core for Android and place it in the app's jniLibs.

Each requested ABI is cross-compiled in release mode with the NDK's clang as
the C compiler and linker (the bundled SQLite and ring need it), and the
resulting shared library is copied to
mobile/android/app/src/main/jniLibs/<abi>/libminymaton_mobile_core.so,
where Gradle packages it into the APK.

The NDK is taken from --ndk, ANDROID_NDK_HOME, ANDROID_NDK_ROOT, or the newest
version under <sdk>/ndk, where <sdk> is ANDROID_HOME, ANDROID_SDK_ROOT, or the
platform's default Android Studio location.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LIBRARY = "libminymaton_mobile_core.so"
# Android ABI name -> Rust target triple.
ABIS = {
    "arm64-v8a": "aarch64-linux-android",
    "x86_64": "x86_64-linux-android",
}


def default_sdk() -> Path | None:
    for name in ("ANDROID_HOME", "ANDROID_SDK_ROOT"):
        value = os.environ.get(name)
        if value:
            return Path(value)
    home = Path.home()
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        return Path(local) / "Android" / "Sdk" if local else None
    if sys.platform == "darwin":
        return home / "Library" / "Android" / "sdk"
    return home / "Android" / "Sdk"


def version_key(path: Path) -> tuple[int, ...]:
    return tuple(int(part) for part in path.name.split(".") if part.isdigit())


def find_ndk(explicit: Path | None) -> Path:
    if explicit:
        return explicit
    for name in ("ANDROID_NDK_HOME", "ANDROID_NDK_ROOT"):
        value = os.environ.get(name)
        if value:
            return Path(value)
    sdk = default_sdk()
    installed = sorted((sdk / "ndk").iterdir(), key=version_key) if sdk and (sdk / "ndk").is_dir() else []
    if not installed:
        raise SystemExit("no Android NDK found: pass --ndk or set ANDROID_NDK_HOME")
    return installed[-1]


def toolchain_bin(ndk: Path) -> Path:
    # NDK r23+ ships only x86_64 host toolchains; arm64 hosts run them emulated.
    host = {"win32": "windows", "darwin": "darwin"}.get(sys.platform, "linux")
    bin_dir = ndk / "toolchains" / "llvm" / "prebuilt" / f"{host}-x86_64" / "bin"
    if not bin_dir.is_dir():
        raise SystemExit(f"NDK toolchain not found at {bin_dir}")
    return bin_dir


def build(abi: str, api: int, bin_dir: Path, out_root: Path) -> Path:
    target = ABIS[abi]
    suffix = ".cmd" if sys.platform == "win32" else ""
    exe = ".exe" if sys.platform == "win32" else ""
    clang = bin_dir / f"{target}{api}-clang{suffix}"
    if not clang.is_file():
        raise SystemExit(f"{clang} not found: is API level {api} supported by this NDK?")
    env_target = target.replace("-", "_")
    env = dict(os.environ)
    env[f"CC_{env_target}"] = str(clang)
    env[f"CXX_{env_target}"] = str(bin_dir / f"{target}{api}-clang++{suffix}")
    env[f"AR_{env_target}"] = str(bin_dir / f"llvm-ar{exe}")
    env[f"CARGO_TARGET_{env_target.upper()}_LINKER"] = str(clang)
    subprocess.run(
        [
            "cargo", "build", "--locked", "--release",
            "-p", "minymaton-mobile-core", "--lib", "--target", target,
        ],
        cwd=REPO,
        env=env,
        check=True,
    )
    built = REPO / "target" / target / "release" / LIBRARY
    destination = out_root / abi / LIBRARY
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(built, destination)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--abi", action="append", choices=sorted(ABIS), help="ABI to build; repeatable (default: all)")
    parser.add_argument("--api", type=int, default=24, help="minimum Android API level (default: 24)")
    parser.add_argument("--ndk", type=Path, help="Android NDK directory")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=REPO / "mobile" / "android" / "app" / "src" / "main" / "jniLibs",
        help="jniLibs directory to populate",
    )
    args = parser.parse_args()
    bin_dir = toolchain_bin(find_ndk(args.ndk))
    for abi in args.abi or sorted(ABIS):
        destination = build(abi, args.api, bin_dir, args.out_dir.resolve())
        print(f"{abi}: {destination} ({destination.stat().st_size} bytes) on {platform.machine()}")


if __name__ == "__main__":
    main()
