variable "lane" {
  description = "Deployment lane profile identifier (vse, mcaps, or custom)."
  type        = string
}

variable "subscription_id" {
  description = "Target Azure subscription UUID."
  type        = string
}

variable "tenant_id" {
  description = "Target Entra tenant UUID."
  type        = string
}

variable "location" {
  description = "Azure region supporting Azure Container Apps, ACR, and Private Link."
  type        = string
}

variable "resource_group_name" {
  description = "Dedicated workload resource group name."
  type        = string
}

variable "name_prefix" {
  description = "Short lowercase alphanumeric prefix for child Azure resources (miny-<nonce>)."
  type        = string
}

variable "nonce" {
  description = "Unique lowercase alphanumeric deployment nonce."
  type        = string
}

variable "expires_at" {
  description = "ISO-8601 UTC timestamp tag for lifecycle hygiene."
  type        = string
}

variable "vnet_cidr" {
  description = "Virtual network CIDR block."
  type        = string
  default     = "10.64.0.0/16"
}

variable "aca_subnet_cidr" {
  description = "Delegated Azure Container Apps infrastructure subnet CIDR (/23)."
  type        = string
  default     = "10.64.0.0/23"
}

variable "pe_subnet_cidr" {
  description = "Private Endpoint subnet CIDR (/24) for private Blob Storage WAL access."
  type        = string
  default     = "10.64.2.0/24"
}

variable "cpu" {
  description = "Container App CPU allocation (Consumption profile)."
  type        = number
  default     = 0.25
}

variable "memory" {
  description = "Container App memory allocation (Consumption profile)."
  type        = string
  default     = "0.5Gi"
}

variable "deploy_app" {
  description = "False during Phase 1 (ACR + VNet + Private Blob + CAE), true during Phase 2 once image is pushed."
  type        = bool
  default     = false
}

variable "container_image" {
  description = "Immutable ACR image reference (<acr>.azurecr.io/minymaton@sha256:...) when deploy_app is true."
  type        = string
  default     = ""
}

variable "bearer_tokens_b64" {
  description = "Base64-encoded CALLER<TAB>TOKEN[\\tCHANNEL] enrollment file injected as an ACA secret."
  type        = string
  sensitive   = true
  default     = ""
}

variable "revision_nonce" {
  description = "Monotonic revision token forcing a clean Container App revision restart on rotation or restart tests."
  type        = string
  default     = "v1"
}
