output "ingest_url" {
  description = "Point the MIDI capture app's INGEST_URL here"
  value       = "http://${aws_lb.ingest.dns_name}/api/notes"
}

output "msk_bootstrap" {
  value = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}

output "ecr_repos" {
  description = "Tag and push your images to these before the services can start"
  value       = { for k, r in aws_ecr_repository.svc : k => r.repository_url }
}
