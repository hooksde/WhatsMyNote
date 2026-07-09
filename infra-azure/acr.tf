# One registry for all three images (ACR doesn't have ECR's per-repo resource --
# repositories are just distinct image names pushed to the same registry, e.g.
# <login_server>/ingest:latest, <login_server>/chord-detection:latest).
resource "azurerm_container_registry" "this" {
  # Must be globally unique across Azure and alphanumeric only.
  name                = replace("${var.project}acr", "-", "")
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "Basic"
  admin_enabled       = false # pulled via the container apps' managed identity instead
}
