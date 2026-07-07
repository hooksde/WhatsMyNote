resource "aws_security_group" "msk" {
  name_prefix = "${var.project}-msk-"
  vpc_id      = module.vpc.vpc_id
}

# Let the Fargate services reach the broker over the IAM (SASL) port.
resource "aws_security_group_rule" "msk_from_services" {
  type                     = "ingress"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = aws_security_group.service.id
}

# Serverless MSK keeps the demo cheap and removes broker sizing/ops.
# Authentication is IAM, so the app authenticates with its task role — no
# passwords. Swap to a provisioned cluster if you want to learn broker tuning.
resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${var.project}-msk"

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}
