variable "location" {
  type    = string
  default = "eastus"
}

variable "project" {
  type    = string
  default = "whatsmynote"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

# The three Container Apps. Only ingest takes inbound HTTP (from the edge app);
# the other two are internal consumers with no public ingress.
# "azure" activates the Event Hubs Kafka auth properties (application-azure.yml
# in each module); it's combined with each app's own profile (topology / sink
# choice), mirroring the AWS infra's "cloud" profile pattern.
locals {
  services = {
    ingest          = { external = true, target_port = 8080, profiles = "azure" }
    chord-detection = { external = false, target_port = 0, profiles = "session,azure" }
    sink            = { external = false, target_port = 0, profiles = "cosmosdb,azure" }
  }
}
