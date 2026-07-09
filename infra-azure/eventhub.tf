# Standard tier is required for the Kafka-compatible endpoint (Basic doesn't
# support it) -- this is the one setting that turns Event Hubs into a Kafka
# broker as far as spring-kafka is concerned; no other config needed.
resource "azurerm_eventhub_namespace" "this" {
  name                = "${var.project}-ehns"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  sku                 = "Standard"
  capacity            = 1
}

# Event Hubs = Kafka topics once the namespace is Standard+.
resource "azurerm_eventhub" "note_events" {
  name                = "note-events"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  partition_count     = 3
  message_retention   = 1
}

resource "azurerm_eventhub" "chord_events" {
  name                = "chord-events"
  namespace_name      = azurerm_eventhub_namespace.this.name
  resource_group_name = azurerm_resource_group.this.name
  partition_count     = 3
  message_retention   = 1
}
