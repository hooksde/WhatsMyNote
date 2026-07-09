output "ingest_url" {
  description = "Point the MIDI capture app's INGEST_URL here"
  value       = "https://${azurerm_container_app.svc["ingest"].ingress[0].fqdn}/api/notes"
}

output "eventhub_bootstrap" {
  value = "${azurerm_eventhub_namespace.this.name}.servicebus.windows.net:9093"
}

output "acr_login_server" {
  description = "Tag and push your images here before the apps can start"
  value       = azurerm_container_registry.this.login_server
}
