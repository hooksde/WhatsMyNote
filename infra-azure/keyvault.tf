data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                       = "${var.project}-kv"
  location                   = azurerm_resource_group.this.location
  resource_group_name        = azurerm_resource_group.this.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  enable_rbac_authorization  = true
}

# The Event Hubs connection string is the one secret this stack can't avoid
# (Event Hubs' Kafka head authenticates via SASL/PLAIN, not a passwordless
# token) -- keeping it in Key Vault and referencing it by identity from the
# Container App (see containerapps.tf) is the closest Azure equivalent to the
# AWS side's zero-plaintext-credential IAM auth.
#
# Note: the principal running `terraform apply` needs "Key Vault Secrets
# Officer" (or equivalent) on this vault to write the secret below -- grant
# that to yourself/your CI identity outside this config; it's not the same
# role as the app's "Key Vault Secrets User" read-only grant in identity.tf.
resource "azurerm_key_vault_secret" "eventhub_connection_string" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_namespace.this.default_primary_connection_string
  key_vault_id = azurerm_key_vault.this.id
}
