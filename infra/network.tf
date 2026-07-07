data "aws_availability_zones" "available" {
  state = "available"
}

# Two AZs: public subnets for the ALB, private subnets for Fargate + MSK.
# A single NAT gateway gives the private subnets outbound access (ECR pulls,
# AWS API calls). Fine for dev; use one NAT per AZ for production HA.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project}-vpc"
  cidr = "10.0.0.0/16"

  azs             = slice(data.aws_availability_zones.available.names, 0, 2)
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway = true
  single_nat_gateway = true
}
