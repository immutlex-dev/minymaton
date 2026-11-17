"""Real Azure end-to-end 2x lifecycle test suite for minymaton-azure.

Exercises real ARM preflight, real two-phase zero-NSG OpenTofu apply (VNet-integrated
Workload Profile Azure Container Apps + Private Endpoint Blob Storage + ACR + Managed
Identity), real linux/amd64 container build + ACR image digest push, real Microsoft-managed
WebPKI TLS on `:443`, real >250s parked `receive(wait=true)` over ACA HTTPS `:443` +
`gateway.py` 25s SSE keepalives + `minymaton-server`, real `tools/minymaton-wait.py`
CLI wakeup, real Litestream Private Endpoint Blob SQLite WAL persistence + bearer token
rotation across an ACA revision replacement, real `tofu destroy`, and independent
`prove_empty` verification across 2 full cycles per lane.

Enable live Azure provisioning with:
  MINYMATON_LIVE_E2E=1 MINYMATON_E2E_LANE=mcaps uv run pytest -v -s
"""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path

import pytest

from minymaton_azure.azure_cli import AzureArmClient
from minymaton_azure.cli import deploy_stack
from minymaton_azure.config import (
    build_config,
    ensure_bearer_tokens,
    load_saved_config,
    resolve_lane,
    save_binding,
)
from minymaton_azure.preflight import check_tofu_binary, run_preflight
from minymaton_azure.teardown import destroy_deployment, prove_empty
from minymaton_azure.tofu import TofuDriver
from minymaton_azure.verify import verify_deployment


def test_local_config_profile_discovery_and_tofu_validate(tmp_path: Path) -> None:
    tofu_ver = check_tofu_binary()
    assert "OpenTofu v1." in tofu_ver

    repo_azure_dir = Path(__file__).resolve().parents[1]
    assert list(repo_azure_dir.rglob("*.sh")) == []

    fake_az_dir = tmp_path / ".azure-custom-test"
    fake_az_dir.mkdir(parents=True)
    (fake_az_dir / "azureProfile.json").write_text(
        '{"subscriptions":[{"id":"00000000-0000-0000-0000-000000000001",'
        '"tenantId":"00000000-0000-0000-0000-000000000002","isDefault":true,'
        '"user":{"name":"live.com#operator@example.org","type":"user"}}]}',
        encoding="utf-8",
    )
    custom = resolve_lane("custom", azure_config_dir=str(fake_az_dir))
    assert custom.subscription_id == "00000000-0000-0000-0000-000000000001"
    assert custom.tenant_id == "00000000-0000-0000-0000-000000000002"

    cfg = build_config(lane=custom, nonce="abc12345", state_dir=tmp_path / "run1")
    assert cfg.resource_group_name == "rg-minymaton-custom-abc12345"
    assert cfg.name_prefix == "miny-abc12345"
    assert cfg.cpu == 0.25
    assert cfg.memory == "0.5Gi"

    save_binding(cfg, {"container_image": "example.azurecr.io/minymaton@sha256:00"})
    reloaded, saved_doc = load_saved_config(cfg.state_dir)
    assert reloaded.resource_group_name == cfg.resource_group_name
    assert reloaded.lane.subscription_id == custom.subscription_id
    assert saved_doc["container_image"] == "example.azurecr.io/minymaton@sha256:00"

    tokens = ensure_bearer_tokens(cfg.tokens_file)
    assert set(tokens) == {"operator", "peer"}
    assert all(len(v) >= 32 for v in tokens.values())

    driver = TofuDriver(cfg, AzureArmClient(custom))
    driver.validate()


@pytest.mark.skipif(
    os.environ.get("MINYMATON_LIVE_E2E") != "1",
    reason="Set MINYMATON_LIVE_E2E=1 to run billable 2x live Azure E2E qualification",
)
def test_two_cycle_full_iac_lifecycle(tmp_path: Path) -> None:
    lane_name = os.environ.get("MINYMATON_E2E_LANE", "mcaps")
    cycles = int(os.environ.get("MINYMATON_E2E_CYCLES", "2"))
    long_poll_seconds = float(os.environ.get("MINYMATON_E2E_LONG_POLL_SECONDS", "255"))
    location = os.environ.get("MINYMATON_E2E_LOCATION", "westus2")

    lane = resolve_lane(lane_name)
    arm = AzureArmClient(lane)

    for cycle_idx in range(1, cycles + 1):
        nonce = f"c{cycle_idx}{secrets.token_hex(3)}"
        state_dir = tmp_path / f"{lane_name}-cycle-{cycle_idx}-{nonce}"
        cfg = build_config(
            lane=lane,
            nonce=nonce,
            location=location,
            state_dir=state_dir,
        )
        tofu = TofuDriver(cfg, arm)
        try:
            pre = run_preflight(cfg, arm)
            assert pre.identity.subscription_state == "Enabled"
            assert pre.aca_region_verified is True

            plan_path = tofu.plan()
            assert plan_path.is_file()

            outputs, image_ref = deploy_stack(cfg, arm)
            assert "@sha256:" in image_ref
            assert outputs["mcp_url"].startswith("https://")
            assert outputs["mcp_url"].endswith("/mcp")

            report = verify_deployment(
                cfg,
                arm,
                outputs,
                long_poll_seconds=long_poll_seconds,
            )
            print(
                json.dumps(
                    {"cycle": cycle_idx, "lane": lane_name, "nonce": nonce, "verify": report},
                    sort_keys=True,
                ),
                flush=True,
            )
            assert report["deployer_nsg_count"] == 0
            assert report["storage_public_network_access"] == "disabled"
            assert report["storage_shared_key_enabled"] is False
            assert report["http_80_redirect_verified"] is True
            assert report["https_443_mcp_verified"] is True
            assert report["auth_401_verified"] is True
            assert report["long_poll_elapsed_seconds"] >= long_poll_seconds
            assert report["boot_id_before"] != report["boot_id_after"]
            assert report["tls_cert_persisted_across_restart"] is True
            assert report["sqlite_wal_persisted_across_restart"] is True
            assert report["cli_waiter_verified"] is True
            assert report["token_rotation_verified"] is True
        finally:
            destroy_deployment(cfg, arm)
            proof = prove_empty(cfg, arm)
            print(
                json.dumps(
                    {"cycle": cycle_idx, "lane": lane_name, "nonce": nonce, "prove_empty": proof},
                    sort_keys=True,
                ),
                flush=True,
            )
            assert proof["resource_group_http_status"] == 404
            assert proof["surviving_resources"] == []
