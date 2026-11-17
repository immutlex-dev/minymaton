# Zero-NSG VNet topology portable across standalone and policy-governed subscriptions
# (ported from skillshanty-shelf/infra/network.tf and mcp-shelf-broker/infra/network.tf):
# - Zero NSGs are created, modified, or referenced. Policy-governed subscriptions may
#   attach empty subnet NSGs out-of-band without drift or ingress breakage: external
#   Workload Profile ACA ingress enters via Microsoft's managed environment endpoint,
#   while intra-VNet traffic from snet-aca to snet-pe uses built-in AllowVnetInBound/OutBound.

locals {
  name_hash = substr(md5("${var.subscription_id}-${var.resource_group_name}"), 0, 15)
  tags = {
    project    = "minymaton"
    lane       = var.lane
    nonce      = var.nonce
    expires_at = var.expires_at
    managed_by = "minymaton-azure"
  }
}

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.tags

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}

resource "azurerm_virtual_network" "main" {
  name                = "${var.name_prefix}-vnet"
  address_space       = [var.vnet_cidr]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}

resource "azurerm_subnet" "aca" {
  name                            = "snet-aca"
  resource_group_name             = azurerm_resource_group.main.name
  virtual_network_name            = azurerm_virtual_network.main.name
  address_prefixes                = [var.aca_subnet_cidr]
  default_outbound_access_enabled = false

  delegation {
    name = "aca-environments"
    service_delegation {
      name    = "Microsoft.App/environments"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "pe" {
  name                              = "snet-pe"
  resource_group_name               = azurerm_resource_group.main.name
  virtual_network_name              = azurerm_virtual_network.main.name
  address_prefixes                  = [var.pe_subnet_cidr]
  default_outbound_access_enabled   = false
  private_endpoint_network_policies = "Disabled"
}
