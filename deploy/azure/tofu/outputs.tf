output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "acr_name" {
  value = azurerm_container_registry.acr.name
}

output "acr_login_server" {
  value = azurerm_container_registry.acr.login_server
}

output "storage_account_name" {
  value = azurerm_storage_account.wal.name
}

output "container_app_environment_id" {
  value = azurerm_container_app_environment.main.id
}

output "container_app_name" {
  value = var.name_prefix
}

output "container_app_id" {
  value = var.deploy_app ? azapi_resource.app[0].id : ""
}

output "aca_subnet_id" {
  value = azurerm_subnet.aca.id
}

output "pe_subnet_id" {
  value = azurerm_subnet.pe.id
}

output "static_ip" {
  value = azurerm_container_app_environment.main.static_ip_address
}

output "fqdn" {
  value = local.public_fqdn
}

output "mcp_url" {
  value = "https://${local.public_fqdn}/mcp"
}
