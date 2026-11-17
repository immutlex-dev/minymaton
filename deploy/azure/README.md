# `minymaton-azure` — Zero-NSG Azure Container Apps Deployer

Deploys [`immutlex-dev/minymaton`](https://github.com/immutlex-dev/minymaton) as a durable Streamable HTTP MCP server on **Azure Container Apps** using `uv` + Python + OpenTofu (`hashicorp/azurerm = 5.3.0` + `Azure/azapi ~> 2.0`), with **zero NSG creation or mutation** and **zero VM public IPs**.

## Architecture (`~$12–$15/mo`, 100% Portable Across Governed Enterprise & Personal Subscriptions)

1. **Zero NSGs & Governance-Immune Networking:**
   - Provisions a VNet (`10.64.0.0/16`) with `snet-aca` (`10.64.0.0/23`, delegated to `Microsoft.App/environments` Workload Profile `Consumption` with `default_outbound_access_enabled = false`) and `snet-pe` (`10.64.2.0/24`, Private Endpoints).
   - Public HTTPS `:443` ingress enters via Azure Container Apps' platform-managed public endpoint rather than the virtual network subnet, and intra-VNet traffic to `snet-pe` uses Azure's default `AllowVnetInBound`/`AllowVnetOutBound` rules — so enterprise governance policies that attach empty subnet NSGs (`securityRules: []`) after deployment never break public ingress or storage replication, and zero NAT Gateway cost is incurred.
2. **Unrestricted Long-Poll Streaming (`mcp_url = https://<fqdn>/mcp`):**
   - ACA terminates Microsoft-managed WebPKI TLS on standard port `:443` and redirects plaintext `:80 -> https://<fqdn>`.
   - ACA's `240s` default HTTP ingress timeout is an **idle** connection timeout; `gateway.py` (`0.0.0.0:8080`) injects `: keepalive` SSE comment frames every `25s` while `receive(wait=true)` or `minymaton-wait` is parked, keeping long-poll MCP connections open across multi-hour waits.
3. **Durable SQLite WAL via Private Endpoint Blob + Litestream:**
   - `minymaton-server` runs against local container disk (`/var/lib/minymaton/minymaton.sqlite3` in `WAL` mode) wrapped by **Litestream** (`1s` continuous WAL replication) over `privatelink.blob.core.windows.net` to a locked-down Azure Storage Account (`public_network_access_enabled = false`, `shared_access_key_enabled = false`, Entra User-Assigned Managed Identity `Storage Blob Data Contributor`).
   - On container revision replacement or restart, `entrypoint.py` automatically restores `/var/lib/minymaton/minymaton.sqlite3` from Private Endpoint Blob before starting `minymaton-server`.

## Quickstart

```powershell
cd deploy/azure
# On managed networks that block pypi.org, set UV_DEFAULT_INDEX or place a local
# git-ignored deploy/azure/uv.toml with:
#   [[index]]
#   url = "https://<your-pypi-mirror>/pypi/simple"
#   default = true
uv sync --extra dev

# Deploy foundation + build/push ACR image + deploy ACA + run >250s long-poll & WAL durability checks:
uv run minymaton-azure up --lane mcaps --nonce demo0001
uv run minymaton-azure up --lane vse   --nonce demo0002

# Update bearer-tokens.tsv (add/revoke callers) and roll a new ACA revision:
uv run minymaton-azure rotate-tokens --state-dir ~/.minymaton-azure/runs/mcaps-demo0001

# Tear down and independently prove zero surviving Azure resources:
uv run minymaton-azure destroy     --state-dir ~/.minymaton-azure/runs/mcaps-demo0001
uv run minymaton-azure prove-empty --state-dir ~/.minymaton-azure/runs/mcaps-demo0001
```
