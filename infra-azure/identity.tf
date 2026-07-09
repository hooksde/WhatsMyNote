# One identity shared by all three apps, mirroring the AWS task role's scope
# (the AWS infra also uses a single shared task role across all its services).
resource "azurerm_user_assigned_identity" "app" {
  name                = "${var.project}-app-identity"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
}

resource "azurerm_role_assignment" "acr_pull" {
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.app.principal_id
}

resource "azurerm_role_assignment" "keyvault_secrets_user" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.app.principal_id
}

# Cosmos DB data-plane RBAC is a separate mechanism from the control-plane
# azurerm_role_assignment above -- it needs this Cosmos-specific resource.
# Resources are scoped to the whole account for the sketch; scope to the
# database/container path for tighter production access.
resource "azurerm_cosmosdb_sql_role_assignment" "app" {
  resource_group_name = azurerm_resource_group.this.name
  account_name        = azurerm_cosmosdb_account.this.name
  role_definition_id  = "${azurerm_cosmosdb_account.this.id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002" # built-in Data Contributor
  principal_id        = azurerm_user_assigned_identity.app.principal_id
  scope               = azurerm_cosmosdb_account.this.id
}
