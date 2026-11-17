"""OpenTofu workspace driver for zero-NSG VNet-integrated Azure Container Apps.

Generates an offline `.tofurc` filesystem mirror config for locally mirrored providers
while allowing non-mirrored providers (such as `Azure/azapi`) to download cleanly, and
runs `tofu` in a scrubbed, profile-isolated CLI environment.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .azure_cli import AzureArmClient
from .config import DeploymentConfig, atomic_json, encode_bearer_tokens_b64, save_binding


class TofuError(RuntimeError):
    pass


def _tofu_bin() -> str:
    found = shutil.which("tofu.exe") or shutil.which("tofu")
    if not found:
        raise TofuError("OpenTofu ('tofu') binary not found on PATH")
    return found


def _find_provider_mirror() -> tuple[Path, list[str]] | None:
    candidates = [
        os.environ.get("MINYMATON_PROVIDER_MIRROR"),
        str(Path.home() / ".minymaton-azure" / "provider-mirror"),
        str(Path.home() / ".opentofu" / "provider-mirror"),
    ]
    for raw in candidates:
        if not raw:
            continue
        candidate = Path(raw).expanduser().resolve()
        reg_root = candidate / "registry.opentofu.org"
        if not reg_root.is_dir():
            continue
        mirrored: list[str] = []
        for ns_dir in sorted(reg_root.iterdir()):
            if not ns_dir.is_dir():
                continue
            for name_dir in sorted(ns_dir.iterdir()):
                if name_dir.is_dir() and any(name_dir.iterdir()):
                    mirrored.append(f"registry.opentofu.org/{ns_dir.name}/{name_dir.name}")
        if mirrored:
            return candidate, mirrored
    return None


def _write_cli_config(workspace_dir: Path) -> Path | None:
    found = _find_provider_mirror()
    if found is None:
        return None
    mirror, mirrored = found
    escaped = str(mirror).replace("\\", "/")
    includes = ", ".join(f'"{item}"' for item in mirrored)
    rc_path = workspace_dir / ".tofurc"
    rc_path.write_text(
        "provider_installation {\n"
        "  filesystem_mirror {\n"
        f'    path    = "{escaped}"\n'
        f"    include = [{includes}]\n"
        "  }\n"
        "  direct {\n"
        f"    exclude = [{includes}]\n"
        "  }\n"
        "}\n",
        encoding="utf-8",
        newline="\n",
    )
    return rc_path


class TofuDriver:
    def __init__(self, cfg: DeploymentConfig, arm: AzureArmClient) -> None:
        self.cfg = cfg
        self.arm = arm
        self.tofu_bin = _tofu_bin()

    def sync_workspace(self) -> None:
        ws = self.cfg.workspace_dir
        ws.mkdir(parents=True, exist_ok=True)
        # Remove any stale .tf files left from earlier schema versions
        template_names = {p.name for p in self.cfg.tofu_template_dir.glob("*.tf")}
        for existing_tf in ws.glob("*.tf"):
            if existing_tf.name not in template_names:
                existing_tf.unlink()
        for tf_file in sorted(self.cfg.tofu_template_dir.glob("*.tf")):
            shutil.copy2(tf_file, ws / tf_file.name)
        lock_src = self.cfg.tofu_template_dir / ".terraform.lock.hcl"
        if lock_src.is_file() and not (ws / ".terraform.lock.hcl").is_file():
            shutil.copy2(lock_src, ws / ".terraform.lock.hcl")

    def _env(self, *, require_auth: bool = True) -> dict[str, str]:
        if require_auth:
            self.arm.get_arm_token()
        env = os.environ.copy()
        for key in list(env):
            upper = key.upper()
            if upper.startswith(("ARM_", "TF_", "TOFU_", "AZURE_")):
                env.pop(key, None)
        env["AZURE_CONFIG_DIR"] = str(self.cfg.lane.azure_config_dir)
        env["ARM_USE_CLI"] = "true"
        env["ARM_SUBSCRIPTION_ID"] = self.cfg.lane.subscription_id
        env["ARM_TENANT_ID"] = self.cfg.lane.tenant_id
        env["ARM_SKIP_PROVIDER_REGISTRATION"] = "true"
        env["TF_IN_AUTOMATION"] = "1"
        env["TF_DATA_DIR"] = str(self.cfg.workspace_dir / ".terraform")
        rc_path = _write_cli_config(self.cfg.workspace_dir)
        if rc_path is not None:
            env["TF_CLI_CONFIG_FILE"] = str(rc_path)
        return env

    def _run(
        self,
        args: list[str],
        *,
        timeout: int = 1800,
        require_auth: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(
            [self.tofu_bin, *args],
            cwd=str(self.cfg.workspace_dir),
            env=self._env(require_auth=require_auth),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            detail = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
            raise TofuError(
                f"OpenTofu {' '.join(args[:2])} failed (rc={proc.returncode}):\n{detail[-3000:]}"
            )
        return proc

    def write_tfvars(
        self,
        *,
        deploy_app: bool,
        container_image: str = "",
        bearer_tokens_b64: str = "",
        revision_nonce: str = "v1",
    ) -> Path:
        tfvars = {
            "subscription_id": self.cfg.lane.subscription_id,
            "tenant_id": self.cfg.lane.tenant_id,
            "lane": self.cfg.lane.name,
            "location": self.cfg.location,
            "resource_group_name": self.cfg.resource_group_name,
            "name_prefix": self.cfg.name_prefix,
            "nonce": self.cfg.nonce,
            "expires_at": self.cfg.expires_at,
            "cpu": self.cfg.cpu,
            "memory": self.cfg.memory,
            "deploy_app": deploy_app,
            "container_image": container_image,
            "bearer_tokens_b64": bearer_tokens_b64,
            "revision_nonce": revision_nonce,
        }
        tfvars_path = self.cfg.workspace_dir / "terraform.tfvars.json"
        atomic_json(tfvars_path, tfvars)
        return tfvars_path

    def init(self) -> None:
        self.sync_workspace()
        self._run(["init", "-upgrade", "-input=false", "-no-color"], timeout=300, require_auth=False)

    def validate(self) -> None:
        self.sync_workspace()
        self._run(
            ["init", "-backend=false", "-input=false", "-no-color"],
            timeout=300,
            require_auth=False,
        )
        self._run(["validate", "-no-color"], timeout=120, require_auth=False)

    def plan(self) -> Path:
        tokens_b64 = encode_bearer_tokens_b64(self.cfg.tokens_file)
        self.init()
        self.write_tfvars(deploy_app=False, bearer_tokens_b64=tokens_b64)
        plan_file = self.cfg.workspace_dir / "tfplan.bin"
        self._run(
            ["plan", "-input=false", "-no-color", f"-out={plan_file.name}"],
            timeout=600,
        )
        save_binding(self.cfg, {"phase": "planned"})
        return plan_file

    def apply_foundation(self) -> dict[str, Any]:
        """Phase 1: Provision RG, zero-NSG VNet, Private Endpoint Blob, ACR, and ACA Environment."""
        tokens_b64 = encode_bearer_tokens_b64(self.cfg.tokens_file)
        save_binding(self.cfg, {"phase": "applying_foundation"})
        self.init()
        self.write_tfvars(deploy_app=False, bearer_tokens_b64=tokens_b64)
        self._run(["apply", "-input=false", "-auto-approve", "-no-color"], timeout=1800)
        outputs = self.outputs()
        save_binding(self.cfg, {"outputs": outputs, "phase": "foundation_applied"})
        return outputs

    def apply_app(
        self,
        *,
        container_image: str,
        bearer_tokens_b64: str | None = None,
        revision_nonce: str = "v1",
    ) -> dict[str, Any]:
        """Phase 2: Deploy or update the Container App with an immutable ACR image digest."""
        resolved_b64 = bearer_tokens_b64 or encode_bearer_tokens_b64(self.cfg.tokens_file)
        save_binding(
            self.cfg,
            {
                "container_image": container_image,
                "revision_nonce": revision_nonce,
                "phase": "applying_app",
            },
        )
        self.init()
        self.write_tfvars(
            deploy_app=True,
            container_image=container_image,
            bearer_tokens_b64=resolved_b64,
            revision_nonce=revision_nonce,
        )
        self._run(["apply", "-input=false", "-auto-approve", "-no-color"], timeout=1800)
        outputs = self.outputs()
        save_binding(
            self.cfg,
            {
                "container_image": container_image,
                "revision_nonce": revision_nonce,
                "outputs": outputs,
                "phase": "applied",
            },
        )
        return outputs

    def outputs(self) -> dict[str, Any]:
        proc = self._run(["output", "-json", "-no-color"], timeout=120)
        raw = json.loads(proc.stdout or "{}")
        return {key: val.get("value") for key, val in raw.items() if isinstance(val, dict)}

    def destroy(self) -> None:
        if not self.cfg.workspace_dir.is_dir() or not self.cfg.state_file.is_file():
            return
        saved = (
            json.loads(self.cfg.binding_file.read_text(encoding="utf-8"))
            if self.cfg.binding_file.is_file()
            else {}
        )
        container_image = str(saved.get("container_image") or "")
        revision_nonce = str(saved.get("revision_nonce") or "v1")
        tokens_b64 = (
            encode_bearer_tokens_b64(self.cfg.tokens_file)
            if self.cfg.tokens_file.is_file()
            else ""
        )
        self.init()
        self.write_tfvars(
            deploy_app=bool(container_image),
            container_image=container_image,
            bearer_tokens_b64=tokens_b64,
            revision_nonce=revision_nonce,
        )
        self._run(["destroy", "-input=false", "-auto-approve", "-no-color"], timeout=1800)
