# Private Endpoint Azure Blob Storage for Litestream SQLite WAL + Caddy ACME state
# (ported from skillshanty-shelf/infra/storage.tf and mcp-shelf-broker/infra/storage.tf):
# - public_network_access_enabled = false, shared_access_key_enabled = false,
#   allow_nested_items_to_be_public = false from creation (100% policy-compliant).
# - Blob container created via ARM control plane (azapi_resource) so creation works
#   from any workstation while storage public network access stays Disabled.

resource "azurerm_user_assigned_identity" "app" {
  name                = "${var.name_prefix}-id"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}

resource "azurerm_storage_account" "wal" {
  name                            = "stminy${local.name_hash}"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  public_network_access_enabled   = false
  tags                            = local.tags

  lifecycle {
    ignore_changes = [
      tags["expires_at"],
      public_network_access_enabled,
      shared_access_key_enabled,
      allow_nested_items_to_be_public,
    ]
  }
}

resource "azapi_resource" "wal_container" {
  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01"
  name      = "wal"
  parent_id = "${azurerm_storage_account.wal.id}/blobServices/default"

  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

resource "azurerm_role_assignment" "wal_blob_contributor" {
  scope                = azapi_resource.wal_container.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.app.principal_id
}

resource "azurerm_private_dns_zone" "blob" {
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}

resource "azurerm_private_dns_zone_virtual_network_link" "blob" {
  name                 = "${var.name_prefix}-blob-dnslink"
  private_dns_zone_id  = azurerm_private_dns_zone.blob.id
  virtual_network_id   = azurerm_virtual_network.main.id
  registration_enabled = false
  tags                 = local.tags

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}

resource "azurerm_private_endpoint" "blob" {
  name                = "${var.name_prefix}-blob-pe"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  subnet_id           = azurerm_subnet.pe.id
  tags                = local.tags

  private_service_connection {
    name                           = "${var.name_prefix}-blob-psc"
    private_connection_resource_id = azurerm_storage_account.wal.id
    is_manual_connection           = false
    subresource_names              = ["blob"]
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [azurerm_private_dns_zone.blob.id]
  }

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}
