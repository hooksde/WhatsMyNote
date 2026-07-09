# Serverless billing (pay per request) is the closest match to the DynamoDB
# tables' PAY_PER_REQUEST mode on the AWS side -- no capacity to plan for a demo.
resource "azurerm_cosmosdb_account" "this" {
  name                = "${var.project}-cosmos"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  consistency_policy {
    consistency_level = "Session"
  }

  capabilities {
    name = "EnableServerless"
  }

  geo_location {
    location          = azurerm_resource_group.this.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "this" {
  name                = "piano"
  resource_group_name = azurerm_resource_group.this.name
  account_name        = azurerm_cosmosdb_account.this.name
}

# Partition key matches the DynamoDB sink's hash key (sourceId), so both sinks
# shard identically and CosmosDbEventSink/DynamoDbEventSink stay interchangeable.
resource "azurerm_cosmosdb_sql_container" "note_events" {
  name                = "note-events"
  resource_group_name = azurerm_resource_group.this.name
  account_name        = azurerm_cosmosdb_account.this.name
  database_name       = azurerm_cosmosdb_sql_database.this.name
  partition_key_paths = ["/sourceId"]
}

resource "azurerm_cosmosdb_sql_container" "chord_events" {
  name                = "chord-events"
  resource_group_name = azurerm_resource_group.this.name
  account_name        = azurerm_cosmosdb_account.this.name
  database_name       = azurerm_cosmosdb_sql_database.this.name
  partition_key_paths = ["/sourceId"]
}
