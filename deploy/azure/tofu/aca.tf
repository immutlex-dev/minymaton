# ACR + VNet-integrated Workload Profile ACA Environment + Container App
# (ported from mcp-shelf-broker/infra/containerapp.tf and skillshanty-shelf/infra/broker.tf):
# - ACR admin_enabled = false; pulls use User-Assigned Identity with AcrPull.
# - ACA uses standard HTTPS :443 ingress (:80 -> :443 TLS redirect enforced by ACA Envoy ->
#   targetPort 8080), while gateway.py emits 25s SSE comment keepalives so ACA Envoy's
#   240s idle request timeout never drops parked receive(wait=true) or minymaton-wait.

locals {
  public_fqdn = "${var.name_prefix}.${azurerm_container_app_environment.main.default_domain}"
}

resource "azurerm_container_registry" "acr" {
  name                          = "acrminy${local.name_hash}"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = azurerm_resource_group.main.location
  sku                           = "Basic"
  admin_enabled                 = false
  public_network_access_enabled = true
  tags                          = local.tags

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}

resource "azurerm_role_assignment" "acr_pull" {
  scope                = azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.app.principal_id
}

resource "azurerm_container_app_environment" "main" {
  name                           = "${var.name_prefix}-cae"
  resource_group_name            = azurerm_resource_group.main.name
  location                       = azurerm_resource_group.main.location
  infrastructure_subnet_id       = azurerm_subnet.aca.id
  internal_load_balancer_enabled = false
  tags                           = local.tags

  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }

  lifecycle {
    ignore_changes = [
      infrastructure_resource_group_name,
      tags["expires_at"],
    ]
  }
}

resource "azapi_resource" "app" {
  count     = var.deploy_app ? 1 : 0
  type      = "Microsoft.App/containerApps@2024-03-01"
  name      = var.name_prefix
  parent_id = azurerm_resource_group.main.id
  location  = azurerm_resource_group.main.location
  tags      = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.app.id]
  }

  body = {
    properties = {
      environmentId       = azurerm_container_app_environment.main.id
      workloadProfileName = "Consumption"
      configuration = {
        activeRevisionsMode = "Single"
        registries = [
          {
            server   = azurerm_container_registry.acr.login_server
            identity = azurerm_user_assigned_identity.app.id
          }
        ]
        ingress = {
          external      = true
          targetPort    = 8080
          transport     = "http"
          allowInsecure = false
          traffic = [
            {
              latestRevision = true
              weight         = 100
            }
          ]
        }
      }
      template = {
        terminationGracePeriodSeconds = 30
        scale = {
          minReplicas = 1
          maxReplicas = 1
        }
        containers = [
          {
            name  = "minymaton"
            image = var.container_image
            resources = {
              cpu    = var.cpu
              memory = var.memory
            }
            env = [
              {
                name  = "PUBLIC_FQDN"
                value = local.public_fqdn
              },
              {
                name  = "STORAGE_ACCOUNT"
                value = azurerm_storage_account.wal.name
              },
              {
                name  = "BLOB_CONTAINER"
                value = "wal"
              },
              {
                name  = "AZURE_CLIENT_ID"
                value = azurerm_user_assigned_identity.app.client_id
              },
              {
                name  = "REVISION_NONCE"
                value = var.revision_nonce
              },
              {
                name      = "BEARER_TOKENS_B64"
                secretRef = "bearer-tokens-b64"
              }
            ]
            probes = [
              {
                type = "Startup"
                httpGet = {
                  path = "/healthz"
                  port = 8080
                }
                initialDelaySeconds = 1
                periodSeconds       = 2
                failureThreshold    = 90
                timeoutSeconds      = 3
              },
              {
                type = "Liveness"
                httpGet = {
                  path = "/healthz"
                  port = 8080
                }
                periodSeconds    = 15
                failureThreshold = 4
                timeoutSeconds   = 5
              }
            ]
          }
        ]
      }
    }
  }

  sensitive_body = {
    properties = {
      configuration = {
        secrets = [
          {
            name  = "bearer-tokens-b64"
            value = var.bearer_tokens_b64
          }
        ]
      }
    }
  }

  depends_on = [
    azurerm_role_assignment.acr_pull,
    azurerm_role_assignment.wal_blob_contributor,
    azurerm_private_endpoint.blob,
    azurerm_private_dns_zone_virtual_network_link.blob,
  ]

  lifecycle {
    ignore_changes = [tags["expires_at"]]
  }
}
