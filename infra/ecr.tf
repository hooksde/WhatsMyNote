resource "aws_ecr_repository" "svc" {
  for_each = local.services
  name     = "${var.project}/${each.key}"

  image_scanning_configuration {
    scan_on_push = true
  }
}
