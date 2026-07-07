variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "piano-stream"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

# The three Fargate services. Only ingest takes inbound HTTP (from the edge app);
# the other two are internal consumers with no public port.
# "cloud" activates the MSK IAM auth properties (application-cloud.yml in each
# module); it's combined with each app's own profile (topology / sink choice).
locals {
  services = {
    ingest          = { port = 8080, public = true, profiles = "cloud" }
    chord-detection = { port = 0, public = false, profiles = "session,cloud" }
    sink            = { port = 0, public = false, profiles = "dynamodb,cloud" }
  }
}
