"""Deterministic teardown and independent zero-survivor verification."""

from __future__ import annotations

from typing import Any

from .azure_cli import ArmApiError, AzureArmClient
from .config import DeploymentConfig, save_binding
from .tofu import TofuDriver


class TeardownError(RuntimeError):
    pass


def destroy_deployment(cfg: DeploymentConfig, arm: AzureArmClient) -> dict[str, Any]:
    tofu = TofuDriver(cfg, arm)
    tofu_err: Exception | None = None
    try:
        tofu.destroy()
    except Exception as exc:  # noqa: BLE001 - fallback to ARM RG delete if Tofu state partial
        tofu_err = exc

    # Ensure the exclusively owned workload resource group is deleted even if OpenTofu
    # failed mid-apply before recording every resource in local state.
    rg_url = f"/subscriptions/{cfg.lane.subscription_id}/resourceGroups/{cfg.resource_group_name}?api-version=2022-12-01"
    rg_deleted_via_arm = False
    try:
        _, _, rg_doc = arm.request("GET", rg_url)
        if rg_doc is not None:
            status, headers, _ = arm.request("DELETE", rg_url, timeout=120)
            if status in {200, 202}:
                arm.poll_async_operation(headers, timeout_seconds=900, poll_interval=6.0)
            rg_deleted_via_arm = True
    except ArmApiError as err:
        if err.status != 404:
            raise

    result = {
        "resource_group_name": cfg.resource_group_name,
        "tofu_destroy_error": str(tofu_err) if tofu_err else None,
        "rg_deleted_via_arm_fallback": rg_deleted_via_arm,
    }
    save_binding(cfg, {"phase": "destroyed", "destroy": result})
    return result


def prove_empty(cfg: DeploymentConfig, arm: AzureArmClient, *, settle_seconds: int = 180) -> dict[str, Any]:
    """Independently verify via ARM REST that the workload RG and all prefixed resources are gone."""
    import time

    rg_url = f"/subscriptions/{cfg.lane.subscription_id}/resourceGroups/{cfg.resource_group_name}?api-version=2022-12-01"
    rg_exists = True
    try:
        arm.request("GET", rg_url)
    except ArmApiError as err:
        if err.status == 404:
            rg_exists = False
        else:
            raise

    if rg_exists:
        raise TeardownError(
            f"Resource group {cfg.resource_group_name!r} still exists in subscription "
            f"{cfg.lane.subscription_id}"
        )

    # Subscription-wide scan for any leaked resource matching our nonce prefix or RG.
    # Azure Container Apps asynchronously garbage-collects its platform-managed `ME_*`
    # infrastructure resource group shortly after the Container App Environment is deleted.
    rg_needle = f"/resourcegroups/{cfg.resource_group_name.lower()}/"
    name_needle = cfg.name_prefix.lower()
    deadline = time.monotonic() + settle_seconds
    survivors: list[str] = []
    while True:
        _, _, res_list = arm.request(
            "GET",
            f"/subscriptions/{cfg.lane.subscription_id}/resources?api-version=2021-04-01",
            timeout=90,
        )
        survivors = []
        for item in (res_list or {}).get("value") or []:
            res_id = str(item.get("id", ""))
            res_name = str(item.get("name", ""))
            if rg_needle in res_id.lower() or name_needle in res_name.lower():
                survivors.append(res_id)

        if not survivors:
            break
        only_managed_aca_rg = all("/resourcegroups/me_" in sid.lower() for sid in survivors)
        if not only_managed_aca_rg or time.monotonic() >= deadline:
            raise TeardownError(
                f"Found surviving Azure resources after teardown: {survivors}"
            )
        time.sleep(6.0)

    proof = {
        "subscription_id": cfg.lane.subscription_id,
        "resource_group_name": cfg.resource_group_name,
        "resource_group_http_status": 404,
        "surviving_resources": [],
    }
    save_binding(cfg, {"phase": "proven_empty", "prove_empty": proof})
    return proof
