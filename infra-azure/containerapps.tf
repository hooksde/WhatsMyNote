resource "azurerm_container_app" "svc" {
  for_each                      = local.services
  name                          = "${var.project}-${each.key}"
  container_app_environment_id  = azurerm_container_app_environment.this.id
  resource_group_name           = azurerm_resource_group.this.name
  revision_mode                 = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.app.id]
  }

  registry {
    server   = azurerm_container_registry.this.login_server
    identity = azurerm_user_assigned_identity.app.id
  }

  secret {
    name                = "eventhub-connection-string"
    key_vault_secret_id = azurerm_key_vault_secret.eventhub_connection_string.id
    identity            = azurerm_user_assigned_identity.app.id
  }

  template {
    min_replicas = 1
    max_replicas = 1

    container {
      name   = each.key
      image  = "${azurerm_container_registry.this.login_server}/${each.key}:${var.image_tag}"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "SPRING_KAFKA_BOOTSTRAP_SERVERS"
        value = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
      }
      env {
        name        = "EVENTHUB_CONNECTION_STRING"
        secret_name = "eventhub-connection-string"
      }
      env {
        name  = "SPRING_PROFILES_ACTIVE"
        value = each.value.profiles
      }
      env {
        name  = "COSMOS_ENDPOINT"
        value = azurerm_cosmosdb_account.this.endpoint
      }
    }
  }

  # Only ingest gets a public ingress; the internal consumers stay unreachable
  # from outside the Container Apps environment, matching the AWS ALB wiring
  # (only ingest is attached to the target group there).
  dynamic "ingress" {
    for_each = each.value.external ? [1] : []
    content {
      external_enabled = true
      target_port      = each.value.target_port
      transport        = "auto"

      traffic_weight {
        latest_revision = true
        percentage      = 100
      }
    }
  }

  depends_on = [
    azurerm_role_assignment.acr_pull,
    azurerm_role_assignment.keyvault_secrets_user,
    azurerm_cosmosdb_sql_role_assignment.app,
  ]
}
