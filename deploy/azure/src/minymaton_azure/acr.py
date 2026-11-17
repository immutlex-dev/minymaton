"""Container image builder and Entra-authenticated Azure Container Registry pusher.

Ported from `mcp-shelf-broker/build-push.ps1` (lines 25-68) into pure Python:
- Uses `az acr login --expose-token` inside the lane's isolated `AZURE_CONFIG_DIR`
  (`admin_enabled = false`) and logs in with username `00000000-0000-0000-0000-000000000000`.
- Builds `linux/amd64` locally via Podman or Docker (leveraging `Dockerfile`'s native
  `$BUILDPLATFORM` `x86_64-linux-gnu-gcc` cross-compiler on ARM64 hosts + local image cache),
  falling back to `az acr build` if no local container runtime is installed.
- Returns the immutable `<acr>.azurecr.io/minymaton@sha256:<digest>` reference.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from .azure_cli import AzureArmClient
from .config import DeploymentConfig

ACR_TOKEN_USERNAME = "00000000-0000-0000-0000-000000000000"


class AcrBuildError(RuntimeError):
    pass


def _build_fingerprint(cfg: DeploymentConfig) -> str:
    h = hashlib.sha256()
    h.update(f"ver={cfg.minymaton_version}\n".encode("utf-8"))
    h.update(f"git={cfg.minymaton_git_url or ''}@{cfg.minymaton_git_rev or ''}\n".encode("utf-8"))
    for path in sorted(cfg.container_dir.iterdir()):
        if path.is_file():
            h.update(path.name.encode("utf-8") + b"\0")
            raw = path.read_bytes().replace(b"\r\n", b"\n")
            h.update(hashlib.sha256(raw).digest())
    return h.hexdigest()[:16]


def _detect_local_engine() -> tuple[str, str] | None:
    podman = shutil.which("podman.exe") or shutil.which("podman")
    if podman:
        info = subprocess.run([podman, "info"], capture_output=True, text=True)
        if info.returncode != 0:
            subprocess.run([podman, "machine", "start"], capture_output=True, text=True, timeout=180)
            info = subprocess.run([podman, "info"], capture_output=True, text=True)
        if info.returncode == 0:
            return "podman", podman

    docker = shutil.which("docker.exe") or shutil.which("docker")
    if docker:
        info = subprocess.run([docker, "info"], capture_output=True, text=True)
        if info.returncode == 0:
            return "docker", docker

    return None


def _acquire_acr_refresh_token(cfg: DeploymentConfig, arm: AzureArmClient, acr_name: str) -> str:
    raw = arm.run_az(
        [
            "acr",
            "login",
            "--name",
            acr_name,
            "--subscription",
            cfg.lane.subscription_id,
            "--expose-token",
            "--output",
            "json",
        ],
        timeout=90,
    )
    doc = json.loads(raw)
    token = str(doc.get("accessToken") or "").strip()
    if not token:
        raise AcrBuildError(f"az acr login --expose-token returned no accessToken for {acr_name}")
    return token


def _image_exists(engine_bin: str, local_tag: str) -> bool:
    proc = subprocess.run(
        [engine_bin, "image", "inspect", local_tag],
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def build_and_push_image(
    cfg: DeploymentConfig,
    arm: AzureArmClient,
    *,
    acr_name: str,
    acr_login_server: str,
) -> str:
    fp = _build_fingerprint(cfg)
    local_tag = f"localhost/minymaton-aca:{fp}"
    remote_tag = f"{acr_login_server}/minymaton:{cfg.nonce}-{fp}"

    engine = _detect_local_engine()
    if engine is not None:
        engine_kind, engine_bin = engine
        if not _image_exists(engine_bin, local_tag):
            build_cmd = [
                engine_bin,
                "build",
                "--platform",
                "linux/amd64",
                "--build-arg",
                f"MINYMATON_VERSION={cfg.minymaton_version}",
                "--build-arg",
                f"MINYMATON_GIT_URL={cfg.minymaton_git_url or ''}",
                "--build-arg",
                f"MINYMATON_GIT_REV={cfg.minymaton_git_rev or ''}",
                "-t",
                local_tag,
                str(cfg.container_dir),
            ]
            proc = subprocess.run(build_cmd, capture_output=True, text=True, timeout=1800)
            if proc.returncode != 0:
                detail = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
                raise AcrBuildError(f"{engine_kind} build failed:\n{detail[-3000:]}")

        acr_token = _acquire_acr_refresh_token(cfg, arm, acr_name)
        login_proc = subprocess.run(
            [
                engine_bin,
                "login",
                acr_login_server,
                "--username",
                ACR_TOKEN_USERNAME,
                "--password-stdin",
            ],
            input=acr_token,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if login_proc.returncode != 0:
            raise AcrBuildError(f"{engine_kind} login to {acr_login_server} failed: {login_proc.stderr}")

        subprocess.run([engine_bin, "tag", local_tag, remote_tag], check=True, capture_output=True, text=True)

        with tempfile.TemporaryDirectory() as td:
            digest_file = Path(td) / "digest.txt"
            if engine_kind == "podman":
                push_proc = subprocess.run(
                    [engine_bin, "push", "--digestfile", str(digest_file), remote_tag],
                    capture_output=True,
                    text=True,
                    timeout=600,
                )
            else:
                push_proc = subprocess.run(
                    [engine_bin, "push", remote_tag],
                    capture_output=True,
                    text=True,
                    timeout=600,
                )
            if push_proc.returncode != 0:
                detail = ((push_proc.stdout or "") + "\n" + (push_proc.stderr or "")).strip()
                raise AcrBuildError(f"{engine_kind} push failed:\n{detail[-2000:]}")
            if digest_file.is_file():
                digest = digest_file.read_text(encoding="utf-8").strip()
                if digest.startswith("sha256:"):
                    return f"{acr_login_server}/minymaton@{digest}"

        digest_raw = arm.run_az(
            [
                "acr",
                "repository",
                "show",
                "--name",
                acr_name,
                "--image",
                f"minymaton:{cfg.nonce}-{fp}",
                "--subscription",
                cfg.lane.subscription_id,
                "--query",
                "digest",
                "-o",
                "tsv",
            ],
            timeout=60,
        ).strip()
        if not digest_raw.startswith("sha256:"):
            raise AcrBuildError(f"Could not resolve image digest for {remote_tag}: {digest_raw!r}")
        return f"{acr_login_server}/minymaton@{digest_raw}"

    # Fallback when neither Podman nor Docker is installed locally: ACR Tasks remote build
    arm.run_az(
        [
            "acr",
            "build",
            "--registry",
            acr_name,
            "--subscription",
            cfg.lane.subscription_id,
            "--platform",
            "linux/amd64",
            "--build-arg",
            f"MINYMATON_VERSION={cfg.minymaton_version}",
            "--build-arg",
            f"MINYMATON_GIT_URL={cfg.minymaton_git_url or ''}",
            "--build-arg",
            f"MINYMATON_GIT_REV={cfg.minymaton_git_rev or ''}",
            "--image",
            f"minymaton:{cfg.nonce}-{fp}",
            str(cfg.container_dir),
        ],
        timeout=1800,
    )
    digest_raw = arm.run_az(
        [
            "acr",
            "repository",
            "show",
            "--name",
            acr_name,
            "--image",
            f"minymaton:{cfg.nonce}-{fp}",
            "--subscription",
            cfg.lane.subscription_id,
            "--query",
            "digest",
            "-o",
            "tsv",
        ],
        timeout=60,
    ).strip()
    if not digest_raw.startswith("sha256:"):
        raise AcrBuildError(f"Could not resolve ACR build digest: {digest_raw!r}")
    return f"{acr_login_server}/minymaton@{digest_raw}"
