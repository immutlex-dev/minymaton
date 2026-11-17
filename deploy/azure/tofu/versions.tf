# Portable AzureRM + AzAPI provider setup for VNet-integrated Azure Container Apps:
# - prevent_deletion_if_contains_resources = false allows clean RG deletion when
#   Azure Policy attaches empty subnet NSGs out-of-band.
# - storage_use_azuread = true uses Entra RBAC (shared_access_key_enabled = false).
# - resource_provider_registrations = "none" avoids subscription-scope RP register 403s
#   in policy-governed subscriptions (preflight registers RPs only when NotRegistered).

terraform {
  required_version = ">= 1.12.0, < 2.0.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "= 5.3.0"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.0"
    }
  }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }

  subscription_id                 = var.subscription_id
  tenant_id                       = var.tenant_id
  storage_use_azuread             = true
  resource_provider_registrations = "none"
}

provider "azapi" {
  subscription_id            = var.subscription_id
  tenant_id                  = var.tenant_id
  use_cli                    = true
  skip_provider_registration = true
}
