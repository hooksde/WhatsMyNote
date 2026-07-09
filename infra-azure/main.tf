resource "azurerm_resource_group" "this" {
  name     = "${var.project}-rg"
  location = var.location
}

resource "azurerm_log_analytics_workspace" "this" {
  name                = "${var.project}-logs"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

# Consumption-only environment: no dedicated VNet to size or manage, closest
# match to ECS Fargate's "no cluster to run" simplicity. Add a
# vnet_configuration block here if you need network isolation later (the AWS
# side uses private subnets for its Fargate tasks + MSK).
resource "azurerm_container_app_environment" "this" {
  name                       = "${var.project}-env"
  location                   = azurerm_resource_group.this.location
  resource_group_name        = azurerm_resource_group.this.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id
}
