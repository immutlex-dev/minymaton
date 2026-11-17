"""Isolated AZURE_CONFIG_DIR Azure CLI & direct ARM REST client (no Graph gate).

Adapted from the ARM token gate in ~/.copilot/skills/azure-cli-profiles/SKILL.md:
acquires an ARM bearer token from the lane's isolated AZURE_CONFIG_DIR without
invoking `az ad signed-in-user show`, verifies `tid` (and optional `oid`) claims,
and confirms live subscription state via ARM GET /subscriptions/<id>.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import LaneProfile


class AzureAuthError(RuntimeError):
    pass


class ArmApiError(RuntimeError):
    def __init__(self, status: int, url: str, body: str) -> None:
        super().__init__(f"ARM HTTP {status} for {url}: {body[:600]}")
        self.status = status
        self.url = url
        self.body = body


def _discover_az_command() -> list[str]:
    windows_az_py = Path(r"C:\Program Files\Microsoft SDKs\Azure\CLI2\python.exe")
    if os.name == "nt" and windows_az_py.is_file():
        return [str(windows_az_py), "-I", "-B", "-X", "utf8", "-m", "azure.cli"]
    az_bin = shutil.which("az")
    if az_bin:
        return [az_bin]
    raise AzureAuthError("Azure CLI ('az') was not found on this machine")


def decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        raise AzureAuthError("Returned ARM access token is not a valid JWT")
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(padded))


@dataclass
class VerifiedIdentity:
    subscription_id: str
    subscription_name: str
    subscription_state: str
    tenant_id: str
    object_id: str
    upn_or_email: str


class AzureArmClient:
    def __init__(self, lane: LaneProfile) -> None:
        self.lane = lane
        self._az_cmd = _discover_az_command()
        self._cached_token: str | None = None
        self._cached_exp: int = 0

    @property
    def isolated_env(self) -> dict[str, str]:
        if not self.lane.azure_config_dir.is_absolute():
            raise AzureAuthError(
                f"AZURE_CONFIG_DIR must be an absolute path: {self.lane.azure_config_dir}"
            )
        env = os.environ.copy()
        for key in list(env):
            if key.upper().startswith("AZURE_") and key.upper() != "AZURE_CONFIG_DIR":
                env.pop(key, None)
        env["AZURE_CONFIG_DIR"] = str(self.lane.azure_config_dir)
        return env

    def get_arm_token(self, *, force_refresh: bool = False) -> tuple[str, dict[str, Any]]:
        now = int(time.time())
        if (
            not force_refresh
            and self._cached_token is not None
            and self._cached_exp - now > 300
        ):
            return self._cached_token, decode_jwt_claims(self._cached_token)

        if not self.lane.azure_config_dir.is_dir():
            raise AzureAuthError(
                f"Isolated AZURE_CONFIG_DIR does not exist for lane {self.lane.name!r}: "
                f"{self.lane.azure_config_dir}"
            )

        proc = subprocess.run(
            [
                *self._az_cmd,
                "account",
                "get-access-token",
                "--subscription",
                self.lane.subscription_id,
                "--resource",
                "https://management.azure.com/",
                "--output",
                "json",
            ],
            env=self.isolated_env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or proc.stdout or "").strip()
            login_hint = (
                f'$env:AZURE_CONFIG_DIR="{self.lane.azure_config_dir}"; '
                f"az login --tenant {self.lane.tenant_id}"
            )
            raise AzureAuthError(
                f"ARM token acquisition failed for lane {self.lane.name!r} "
                f"(sub {self.lane.subscription_id}): {stderr[:500]}\n"
                f"Re-authenticate that isolated profile with: {login_hint}"
            )

        payload = json.loads(proc.stdout)
        token = str(payload["accessToken"])
        claims = decode_jwt_claims(token)
        tid = str(claims.get("tid", ""))
        oid = str(claims.get("oid", ""))
        if tid.lower() != self.lane.tenant_id.lower():
            raise AzureAuthError(
                f"Token tenant mismatch for lane {self.lane.name!r}: "
                f"expected {self.lane.tenant_id}, got {tid}"
            )
        if self.lane.expected_oid and oid.lower() != self.lane.expected_oid.lower():
            raise AzureAuthError(
                f"Token oid mismatch for lane {self.lane.name!r}: "
                f"expected {self.lane.expected_oid}, got {oid}"
            )
        self._cached_token = token
        self._cached_exp = int(claims.get("exp", now + 600))
        return token, claims

    def run_az(self, args: list[str], *, timeout: int = 120) -> str:
        proc = subprocess.run(
            [*self._az_cmd, *args],
            env=self.isolated_env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"az {' '.join(args[:4])} failed for lane {self.lane.name!r} "
                f"(AZURE_CONFIG_DIR={self.lane.azure_config_dir}): "
                f"{proc.stderr.strip() or proc.stdout.strip()}"
            )
        return proc.stdout

    def request(
        self,
        method: str,
        url_or_path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: int = 60,
    ) -> tuple[int, dict[str, str], dict[str, Any] | None]:
        token, _ = self.get_arm_token()
        url = (
            url_or_path
            if url_or_path.startswith("https://")
            else f"https://management.azure.com{url_or_path}"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        raw_body: bytes | None = None
        if body is not None:
            raw_body = json.dumps(body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=raw_body, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                content = resp.read().decode("utf-8")
                parsed = json.loads(content) if content.strip() else None
                return resp.status, resp_headers, parsed
        except urllib.error.HTTPError as err:
            err_body = err.read().decode("utf-8", errors="replace")
            raise ArmApiError(err.code, url, err_body) from err

    def poll_async_operation(
        self,
        initial_headers: dict[str, str],
        *,
        timeout_seconds: int = 900,
        poll_interval: float = 5.0,
    ) -> dict[str, Any] | None:
        poll_url = initial_headers.get("azure-asyncoperation") or initial_headers.get("location")
        if not poll_url:
            return None
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            status, headers, body = self.request("GET", poll_url, timeout=60)
            if isinstance(body, dict):
                op_status = str(body.get("status", "")).lower()
                if op_status == "succeeded":
                    return body
                if op_status in {"failed", "canceled", "cancelled"}:
                    raise RuntimeError(
                        f"Azure async operation {op_status}: {json.dumps(body)[:800]}"
                    )
                prov = str(body.get("properties", {}).get("provisioningState", "")).lower()
                if prov == "succeeded":
                    return body
                if prov in {"failed", "canceled", "cancelled"}:
                    raise RuntimeError(
                        f"Azure async resource {prov}: {json.dumps(body)[:800]}"
                    )
            elif status == 200:
                return None
            retry_after = headers.get("retry-after")
            sleep_for = float(retry_after) if retry_after and retry_after.isdigit() else poll_interval
            time.sleep(min(max(sleep_for, 2.0), 15.0))
        raise TimeoutError(f"Timed out polling Azure async operation: {poll_url}")

    def verify_lane(self) -> VerifiedIdentity:
        _, claims = self.get_arm_token()
        _, _, sub = self.request(
            "GET",
            f"/subscriptions/{self.lane.subscription_id}?api-version=2022-12-01",
        )
        if not isinstance(sub, dict):
            raise AzureAuthError("Empty ARM subscription response")
        state = str(sub.get("state", ""))
        if state != "Enabled":
            raise AzureAuthError(
                f"Subscription {self.lane.subscription_id} in lane {self.lane.name!r} "
                f"is {state!r}, not 'Enabled'"
            )
        upn = str(
            claims.get("upn")
            or claims.get("unique_name")
            or claims.get("email")
            or claims.get("preferred_username")
            or ""
        )
        return VerifiedIdentity(
            subscription_id=self.lane.subscription_id,
            subscription_name=str(sub.get("displayName", "")),
            subscription_state=state,
            tenant_id=str(claims.get("tid", "")),
            object_id=str(claims.get("oid", "")),
            upn_or_email=upn,
        )
