"""Command-line interface for portable zero-NSG Azure Container Apps deployments of minymaton."""

from __future__ import annotations

import argparse
import json
import secrets
from pathlib import Path
from typing import Any

from .acr import build_and_push_image
from .azure_cli import AzureArmClient
from .config import (
    DeploymentConfig,
    build_config,
    encode_bearer_tokens_b64,
    ensure_bearer_tokens,
    load_saved_config,
    resolve_lane,
    save_binding,
)
from .preflight import run_preflight
from .teardown import destroy_deployment, prove_empty
from .tofu import TofuDriver
from .verify import verify_deployment


def _add_target_args(parser: argparse.ArgumentParser, *, allow_new: bool = True) -> None:
    if allow_new:
        parser.add_argument(
            "--lane",
            default="mcaps",
            choices=["vse", "mcaps", "custom"],
            help="Target Azure subscription profile ('vse', 'mcaps', or 'custom')",
        )
        parser.add_argument("--nonce", default=None, help="6-16 char lowercase alphanumeric run nonce")
        parser.add_argument("--location", default=None, help="Azure region (default: westus2)")
        parser.add_argument("--cpu", type=float, default=0.25, help="Container App CPU (default: 0.25)")
        parser.add_argument("--memory", default="0.5Gi", help="Container App memory (default: 0.5Gi)")
        parser.add_argument("--minymaton-version", default="0.1.6", help="crates.io minymaton-server version")
        parser.add_argument("--minymaton-git-url", default=None, help="Optional git repository URL override")
        parser.add_argument("--minymaton-git-rev", default=None, help="Optional git revision override")
        parser.add_argument("--subscription-id", default=None, help="Override Azure subscription ID")
        parser.add_argument("--tenant-id", default=None, help="Override Azure tenant ID")
        parser.add_argument("--azure-config-dir", default=None, help="Override isolated AZURE_CONFIG_DIR")
    parser.add_argument("--state-dir", default=None, help="Explicit deployment state directory")


def _resolve_config(
    args: argparse.Namespace, *, require_existing: bool = False
) -> tuple[DeploymentConfig, dict[str, Any]]:
    if args.state_dir:
        state_path = Path(args.state_dir).expanduser().resolve()
        if (state_path / "deployment.json").is_file():
            return load_saved_config(state_path)
        if require_existing:
            raise SystemExit(f"No existing deployment.json in --state-dir {state_path}")
    elif require_existing:
        raise SystemExit("This command requires --state-dir pointing to an existing deployment")

    lane = resolve_lane(
        args.lane,
        subscription_id=args.subscription_id,
        tenant_id=args.tenant_id,
        azure_config_dir=args.azure_config_dir,
    )
    cfg = build_config(
        lane=lane,
        nonce=args.nonce,
        location=args.location,
        cpu=args.cpu,
        memory=args.memory,
        minymaton_version=args.minymaton_version,
        minymaton_git_url=args.minymaton_git_url,
        minymaton_git_rev=args.minymaton_git_rev,
        state_dir=Path(args.state_dir) if args.state_dir else None,
    )
    saved = {}
    if cfg.binding_file.is_file():
        _, saved = load_saved_config(cfg.state_dir)
    return cfg, saved


def _print_json(data: dict[str, Any]) -> None:
    print(json.dumps(data, indent=2, sort_keys=True), flush=True)


def deploy_stack(cfg: DeploymentConfig, arm: AzureArmClient) -> tuple[dict[str, Any], str]:
    tofu = TofuDriver(cfg, arm)
    foundation = tofu.apply_foundation()
    image_ref = build_and_push_image(
        cfg,
        arm,
        acr_name=str(foundation["acr_name"]),
        acr_login_server=str(foundation["acr_login_server"]),
    )
    outputs = tofu.apply_app(container_image=image_ref, revision_nonce="v1")
    return outputs, image_ref


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="minymaton-azure",
        description="Portable zero-NSG OpenTofu + Azure Container Apps deployer for minymaton-server.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_pre = sub.add_parser("preflight", help="Validate Azure auth, resource providers, and region")
    _add_target_args(p_pre)

    p_plan = sub.add_parser("plan", help="Run preflight and bind an OpenTofu foundation plan")
    _add_target_args(p_plan)

    p_apply = sub.add_parser("apply", help="Provision foundation, build/push ACR image, and deploy ACA")
    _add_target_args(p_apply)

    p_ver = sub.add_parser("verify", help="Run live HTTPS MCP, >250s long-poll, CLI waiter, and WAL checks")
    _add_target_args(p_ver, allow_new=False)
    p_ver.add_argument(
        "--long-poll-seconds",
        type=float,
        default=255.0,
        help="Seconds to hold parked receive(wait=true) open (default: 255s > ACA/LB 240s)",
    )

    p_up = sub.add_parser("up", help="Run preflight -> apply -> verify")
    _add_target_args(p_up)
    p_up.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip the post-deploy verification suite",
    )
    p_up.add_argument(
        "--long-poll-seconds",
        type=float,
        default=255.0,
        help="Seconds to hold parked receive(wait=true) during verification",
    )

    p_rot = sub.add_parser("rotate-tokens", help="Update ACA bearer-tokens secret and roll a new revision")
    _add_target_args(p_rot, allow_new=False)
    p_rot.add_argument("--tokens-file", default=None, help="Optional replacement TSV file")

    p_status = sub.add_parser("status", help="Show local deployment binding and connection URLs")
    _add_target_args(p_status, allow_new=False)

    p_destroy = sub.add_parser("destroy", help="Destroy all Azure resources for a deployment")
    _add_target_args(p_destroy, allow_new=False)

    p_empty = sub.add_parser("prove-empty", help="Independently prove RG=404 and zero surviving resources")
    _add_target_args(p_empty, allow_new=False)

    args = parser.parse_args(argv)
    require_existing = args.command in {"verify", "rotate-tokens", "status", "destroy", "prove-empty"}
    cfg, saved = _resolve_config(args, require_existing=require_existing)

    if args.command == "status":
        _print_json(saved)
        return 0

    arm = AzureArmClient(cfg.lane)

    if args.command == "preflight":
        pre = run_preflight(cfg, arm)
        payload = {
            "lane": cfg.lane.name,
            "location": cfg.location,
            "state_dir": str(cfg.state_dir),
            "subscription_id": pre.identity.subscription_id,
            "subscription_name": pre.identity.subscription_name,
            "tenant_id": pre.identity.tenant_id,
            "object_id": pre.identity.object_id,
            "upn": pre.identity.upn_or_email,
            "tofu_version": pre.tofu_version,
            "providers": pre.providers,
            "aca_region_verified": pre.aca_region_verified,
        }
        save_binding(cfg, {"preflight": payload, "phase": "preflighted"})
        _print_json(payload)
        return 0

    if args.command == "plan":
        run_preflight(cfg, arm)
        tofu = TofuDriver(cfg, arm)
        plan_path = tofu.plan()
        _print_json(
            {
                "state_dir": str(cfg.state_dir),
                "resource_group_name": cfg.resource_group_name,
                "plan_file": str(plan_path),
            }
        )
        return 0

    if args.command == "apply":
        run_preflight(cfg, arm)
        outputs, image_ref = deploy_stack(cfg, arm)
        tokens = ensure_bearer_tokens(cfg.tokens_file)
        _print_json(
            {
                "state_dir": str(cfg.state_dir),
                "container_image": image_ref,
                "callers": sorted(tokens),
                "tokens_file": str(cfg.tokens_file),
                "outputs": outputs,
            }
        )
        return 0

    if args.command == "verify":
        outputs = saved.get("outputs") or TofuDriver(cfg, arm).outputs()
        report = verify_deployment(
            cfg, arm, outputs, long_poll_seconds=float(args.long_poll_seconds)
        )
        _print_json({"state_dir": str(cfg.state_dir), "verification": report})
        return 0

    if args.command == "up":
        run_preflight(cfg, arm)
        outputs, image_ref = deploy_stack(cfg, arm)
        tokens = ensure_bearer_tokens(cfg.tokens_file)
        result: dict[str, Any] = {
            "state_dir": str(cfg.state_dir),
            "container_image": image_ref,
            "callers": sorted(tokens),
            "tokens_file": str(cfg.tokens_file),
            "outputs": outputs,
        }
        if not args.skip_verify:
            result["verification"] = verify_deployment(
                cfg, arm, outputs, long_poll_seconds=float(args.long_poll_seconds)
            )
        _print_json(result)
        return 0

    if args.command == "rotate-tokens":
        src_path = Path(args.tokens_file).expanduser().resolve() if args.tokens_file else cfg.tokens_file
        tsv_text = src_path.read_text(encoding="utf-8").replace("\r\n", "\n")
        if not tsv_text.endswith("\n"):
            tsv_text += "\n"
        if src_path != cfg.tokens_file:
            cfg.tokens_file.write_text(tsv_text, encoding="utf-8", newline="\n")
        container_image = str(saved["container_image"])
        rev_nonce = f"rot-{secrets.token_hex(4)}"
        tofu = TofuDriver(cfg, arm)
        outputs = tofu.apply_app(
            container_image=container_image,
            bearer_tokens_b64=encode_bearer_tokens_b64(cfg.tokens_file),
            revision_nonce=rev_nonce,
        )
        _print_json({"state_dir": str(cfg.state_dir), "rotated": True, "revision_nonce": rev_nonce, "outputs": outputs})
        return 0

    if args.command == "destroy":
        res = destroy_deployment(cfg, arm)
        _print_json(res)
        return 0

    if args.command == "prove-empty":
        proof = prove_empty(cfg, arm)
        _print_json(proof)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
