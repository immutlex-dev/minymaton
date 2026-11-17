"""Dynamic ARM preflight for OpenTofu, Resource Providers, and regional ACA availability."""

from __future__ import annotations

import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Any

from .azure_cli import AzureArmClient, VerifiedIdentity
from .config import DeploymentConfig

REQUIRED_PROVIDERS = (
    "Microsoft.App",
    "Microsoft.ContainerRegistry",
    "Microsoft.ManagedIdentity",
    "Microsoft.Network",
    "Microsoft.Storage",
)


class PreflightError(RuntimeError):
    pass


@dataclass(frozen=True)
class PreflightResult:
    identity: VerifiedIdentity
    tofu_version: str
    providers: dict[str, str]
    aca_region_verified: bool


def check_tofu_binary() -> str:
    tofu = shutil.which("tofu.exe") or shutil.which("tofu")
    if not tofu:
        raise PreflightError("OpenTofu ('tofu') executable was not found on PATH")
    proc = subprocess.run([tofu, "version"], capture_output=True, text=True, check=True)
    first_line = (proc.stdout or "").splitlines()[0].strip()
    match = re.search(r"v(\d+)\.(\d+)\.(\d+)", first_line)
    if not match:
        raise PreflightError(f"Could not parse OpenTofu version from {first_line!r}")
    major, minor = int(match.group(1)), int(match.group(2))
    if major != 1 or minor < 12:
        raise PreflightError(f"OpenTofu >= 1.12.0 required, found {first_line}")
    return first_line


def _norm_location(raw: str) -> str:
    return re.sub(r"[^a-z0-9]", "", raw.lower())


def _ensure_provider_registered(
    arm: AzureArmClient,
    subscription_id: str,
    namespace: str,
    *,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    url = f"/subscriptions/{subscription_id}/providers/{namespace}?api-version=2021-04-01"
    _, _, doc = arm.request("GET", url)
    state = str((doc or {}).get("registrationState") or "")
    if state == "Registered":
        return doc or {}

    # Register only when not already Registered (e.g. fresh Personal VSE subscriptions
    # where Microsoft.App starts NotRegistered; governed MCAPS subscriptions already have
    # these providers Registered so no subscription-scope register call is made).
    arm.request(
        "POST",
        f"/subscriptions/{subscription_id}/providers/{namespace}/register?api-version=2021-04-01",
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        time.sleep(4.0)
        _, _, doc = arm.request("GET", url)
        state = str((doc or {}).get("registrationState") or "")
        if state == "Registered":
            return doc or {}
    raise PreflightError(
        f"Azure resource provider {namespace} did not reach Registered within {timeout_seconds}s "
        f"(last state: {state!r})"
    )


def run_preflight(cfg: DeploymentConfig, arm: AzureArmClient) -> PreflightResult:
    tofu_version = check_tofu_binary()
    identity = arm.verify_lane()

    providers_state: dict[str, str] = {}
    app_doc: dict[str, Any] = {}
    for ns in REQUIRED_PROVIDERS:
        doc = _ensure_provider_registered(arm, cfg.lane.subscription_id, ns)
        providers_state[ns] = str(doc.get("registrationState") or "Registered")
        if ns == "Microsoft.App":
            app_doc = doc

    target_loc = _norm_location(cfg.location)
    aca_locations: set[str] = set()
    for rt in app_doc.get("resourceTypes") or []:
        if isinstance(rt, dict) and str(rt.get("resourceType", "")).lower() in {
            "managedenvironments",
            "containerapps",
        }:
            for loc in rt.get("locations") or []:
                aca_locations.add(_norm_location(str(loc)))

    if aca_locations and target_loc not in aca_locations:
        raise PreflightError(
            f"Microsoft.App Container Apps is not available in region {cfg.location!r}"
        )

    return PreflightResult(
        identity=identity,
        tofu_version=tofu_version,
        providers=providers_state,
        aca_region_verified=True,
    )
