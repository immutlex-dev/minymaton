"""Lane profiles, run state layout, and configuration loading."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class LaneProfile:
    name: str
    subscription_id: str
    tenant_id: str
    azure_config_dir: Path
    default_location: str
    expected_oid: str | None = None


def _discover_from_azure_profile(azure_config_dir: Path) -> tuple[str, str]:
    """Read default subscription_id and tenant_id from an isolated AZURE_CONFIG_DIR."""
    profile_path = azure_config_dir / "azureProfile.json"
    if not profile_path.is_file():
        return "", ""
    try:
        doc = json.loads(profile_path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return "", ""
    subs = doc.get("subscriptions") or []
    if not isinstance(subs, list) or not subs:
        return "", ""
    chosen = next((s for s in subs if isinstance(s, dict) and s.get("isDefault")), subs[0])
    if not isinstance(chosen, dict):
        return "", ""
    sub_id = str(chosen.get("id") or "").strip()
    tenant_id = str(chosen.get("tenantId") or "").strip()
    return sub_id, tenant_id


def built_in_lanes() -> dict[str, LaneProfile]:
    home = Path.home()
    vse_dir = Path(
        os.environ.get("MINYMATON_VSE_AZURE_CONFIG_DIR", str(home / ".azure-personal-vs"))
    ).expanduser()
    mcaps_dir = Path(
        os.environ.get("MINYMATON_MCAPS_AZURE_CONFIG_DIR", str(home / ".azure-mcaps-dbx"))
    ).expanduser()

    vse_sub, vse_tid = _discover_from_azure_profile(vse_dir)
    mcaps_sub, mcaps_tid = _discover_from_azure_profile(mcaps_dir)

    return {
        "vse": LaneProfile(
            name="vse",
            subscription_id=os.environ.get("MINYMATON_VSE_SUBSCRIPTION_ID", vse_sub),
            tenant_id=os.environ.get("MINYMATON_VSE_TENANT_ID", vse_tid),
            azure_config_dir=vse_dir,
            default_location="westus2",
            expected_oid=os.environ.get("MINYMATON_VSE_EXPECTED_OID") or None,
        ),
        "mcaps": LaneProfile(
            name="mcaps",
            subscription_id=os.environ.get("MINYMATON_MCAPS_SUBSCRIPTION_ID", mcaps_sub),
            tenant_id=os.environ.get("MINYMATON_MCAPS_TENANT_ID", mcaps_tid),
            azure_config_dir=mcaps_dir,
            default_location="westus2",
            expected_oid=os.environ.get("MINYMATON_MCAPS_EXPECTED_OID") or None,
        ),
    }


def resolve_lane(
    lane_name: str,
    *,
    subscription_id: str | None = None,
    tenant_id: str | None = None,
    azure_config_dir: str | None = None,
) -> LaneProfile:
    lanes = built_in_lanes()
    if lane_name in lanes:
        base = lanes[lane_name]
        resolved_dir = (
            Path(azure_config_dir).expanduser().resolve()
            if azure_config_dir
            else base.azure_config_dir.expanduser().resolve()
        )
        disc_sub, disc_tid = (
            _discover_from_azure_profile(resolved_dir)
            if azure_config_dir
            else (base.subscription_id, base.tenant_id)
        )
        return LaneProfile(
            name=lane_name,
            subscription_id=subscription_id or disc_sub,
            tenant_id=tenant_id or disc_tid,
            azure_config_dir=resolved_dir,
            default_location=base.default_location,
            expected_oid=None if (subscription_id or azure_config_dir) else base.expected_oid,
        )
    if lane_name != "custom":
        raise ConfigError(f"Unknown lane {lane_name!r}; expected 'vse', 'mcaps', or 'custom'")
    if not azure_config_dir:
        raise ConfigError("Custom lane requires --azure-config-dir")
    resolved_dir = Path(azure_config_dir).expanduser().resolve()
    disc_sub, disc_tid = _discover_from_azure_profile(resolved_dir)
    final_sub = subscription_id or disc_sub
    final_tid = tenant_id or disc_tid
    if not final_sub or not final_tid:
        raise ConfigError(
            "Custom lane requires --subscription-id and --tenant-id when not present in azureProfile.json"
        )
    return LaneProfile(
        name="custom",
        subscription_id=final_sub,
        tenant_id=final_tid,
        azure_config_dir=resolved_dir,
        default_location="westus2",
    )


@dataclass(frozen=True)
class DeploymentConfig:
    lane: LaneProfile
    nonce: str
    location: str
    cpu: float
    memory: str
    minymaton_version: str
    minymaton_git_url: str | None
    minymaton_git_rev: str | None
    state_dir: Path
    tofu_template_dir: Path
    container_dir: Path
    expires_at: str

    @property
    def name_prefix(self) -> str:
        return f"miny-{self.nonce}"

    @property
    def resource_group_name(self) -> str:
        return f"rg-minymaton-{self.lane.name}-{self.nonce}"

    @property
    def workspace_dir(self) -> Path:
        return self.state_dir / "tofu-workspace"

    @property
    def state_file(self) -> Path:
        return self.workspace_dir / "terraform.tfstate"

    @property
    def binding_file(self) -> Path:
        return self.state_dir / "deployment.json"

    @property
    def tokens_file(self) -> Path:
        return self.state_dir / "bearer-tokens.tsv"


def default_tofu_template_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "tofu"


def default_container_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "container"


def default_state_root() -> Path:
    override = os.environ.get("MINYMATON_AZURE_STATE_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return (Path.home() / ".minymaton-azure" / "runs").resolve()


def validate_nonce(nonce: str) -> str:
    cleaned = nonce.strip().lower()
    if not re.fullmatch(r"[a-z0-9]{6,16}", cleaned):
        raise ConfigError(
            f"Invalid nonce {nonce!r}: expected 6-16 lowercase alphanumeric characters"
        )
    return cleaned


def build_config(
    *,
    lane: LaneProfile,
    nonce: str | None = None,
    location: str | None = None,
    cpu: float = 0.25,
    memory: str = "0.5Gi",
    minymaton_version: str = "0.1.6",
    minymaton_git_url: str | None = None,
    minymaton_git_rev: str | None = None,
    state_dir: Path | None = None,
    ttl_hours: int = 8,
) -> DeploymentConfig:
    resolved_nonce = validate_nonce(nonce) if nonce else secrets.token_hex(4)
    resolved_state = (
        state_dir.expanduser().resolve()
        if state_dir
        else (default_state_root() / f"{lane.name}-{resolved_nonce}")
    )
    if cpu <= 0:
        raise ConfigError("cpu must be > 0")
    expires = (datetime.now(UTC) + timedelta(hours=ttl_hours)).replace(microsecond=0)
    return DeploymentConfig(
        lane=lane,
        nonce=resolved_nonce,
        location=(location or lane.default_location).strip().lower(),
        cpu=float(cpu),
        memory=memory.strip(),
        minymaton_version=minymaton_version.strip(),
        minymaton_git_url=minymaton_git_url,
        minymaton_git_rev=minymaton_git_rev,
        state_dir=resolved_state,
        tofu_template_dir=default_tofu_template_dir(),
        container_dir=default_container_dir(),
        expires_at=expires.isoformat(),
    )


def load_saved_config(state_dir: Path) -> tuple[DeploymentConfig, dict[str, Any]]:
    resolved = state_dir.expanduser().resolve()
    binding_path = resolved / "deployment.json"
    if not binding_path.is_file():
        raise ConfigError(f"No deployment.json found in {resolved}")
    data = json.loads(binding_path.read_text(encoding="utf-8"))
    lane_raw = data["lane"]
    lane = LaneProfile(
        name=lane_raw["name"],
        subscription_id=lane_raw["subscription_id"],
        tenant_id=lane_raw["tenant_id"],
        azure_config_dir=Path(lane_raw["azure_config_dir"]),
        default_location=lane_raw["default_location"],
        expected_oid=lane_raw.get("expected_oid"),
    )
    cfg = DeploymentConfig(
        lane=lane,
        nonce=data["nonce"],
        location=data["location"],
        cpu=float(data.get("cpu", 0.25)),
        memory=str(data.get("memory", "0.5Gi")),
        minymaton_version=data["minymaton_version"],
        minymaton_git_url=data.get("minymaton_git_url"),
        minymaton_git_rev=data.get("minymaton_git_rev"),
        state_dir=resolved,
        tofu_template_dir=default_tofu_template_dir(),
        container_dir=default_container_dir(),
        expires_at=data["expires_at"],
    )
    return cfg, data


def save_binding(cfg: DeploymentConfig, extra: dict[str, Any]) -> dict[str, Any]:
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    existing: dict[str, Any] = {}
    if cfg.binding_file.is_file():
        existing = json.loads(cfg.binding_file.read_text(encoding="utf-8"))
    payload = {
        **existing,
        "lane": {
            "name": cfg.lane.name,
            "subscription_id": cfg.lane.subscription_id,
            "tenant_id": cfg.lane.tenant_id,
            "azure_config_dir": str(cfg.lane.azure_config_dir),
            "default_location": cfg.lane.default_location,
            "expected_oid": cfg.lane.expected_oid,
        },
        "nonce": cfg.nonce,
        "location": cfg.location,
        "cpu": cfg.cpu,
        "memory": cfg.memory,
        "minymaton_version": cfg.minymaton_version,
        "minymaton_git_url": cfg.minymaton_git_url,
        "minymaton_git_rev": cfg.minymaton_git_rev,
        "resource_group_name": cfg.resource_group_name,
        "name_prefix": cfg.name_prefix,
        "expires_at": cfg.expires_at,
        "updated_at": datetime.now(UTC).isoformat(),
        **extra,
    }
    atomic_json(cfg.binding_file, payload)
    return payload


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def ensure_bearer_tokens(tokens_file: Path) -> dict[str, str]:
    """Load or generate initial CALLER\\tTOKEN[\\tCHANNEL] entries (>=32-byte URL-safe secrets)."""
    if tokens_file.is_file():
        caller_map: dict[str, str] = {}
        for line in tokens_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split("\t")
            if len(parts) < 2:
                raise ConfigError(f"Malformed bearer-tokens.tsv line: {line!r}")
            caller_map[parts[0]] = parts[1]
        if caller_map:
            return caller_map
    tokens_file.parent.mkdir(parents=True, exist_ok=True)
    generated = {
        "operator": secrets.token_urlsafe(36),
        "peer": secrets.token_urlsafe(36),
    }
    lines = [f"{caller}\t{token}" for caller, token in generated.items()]
    fd = os.open(str(tokens_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")
    return generated


def encode_bearer_tokens_b64(tokens_file: Path) -> str:
    ensure_bearer_tokens(tokens_file)
    raw = tokens_file.read_text(encoding="utf-8").replace("\r\n", "\n").encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
