resource "aws_ecs_cluster" "this" {
  name = var.project
}

resource "aws_cloudwatch_log_group" "svc" {
  for_each          = local.services
  name              = "/ecs/${var.project}/${each.key}"
  retention_in_days = 14
}

# ---- Security groups --------------------------------------------------------
resource "aws_security_group" "service" {
  name_prefix = "${var.project}-svc-"
  vpc_id      = module.vpc.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.project}-alb-"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Only the ALB may reach the ingest container port.
resource "aws_security_group_rule" "svc_from_alb" {
  type                     = "ingress"
  from_port                = 8080
  to_port                  = 8080
  protocol                 = "tcp"
  security_group_id        = aws_security_group.service.id
  source_security_group_id = aws_security_group.alb.id
}

# ---- Public ALB in front of the ingest service ------------------------------
# HTTP only for the sketch; add an ACM cert + HTTPS listener before exposing it.
resource "aws_lb" "ingest" {
  name               = "${var.project}-ingest"
  load_balancer_type = "application"
  subnets            = module.vpc.public_subnets
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "ingest" {
  name        = "${var.project}-ingest"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = module.vpc.vpc_id
  target_type = "ip"

  health_check {
    path    = "/actuator/health" # requires spring-boot-starter-actuator
    matcher = "200"
  }
}

resource "aws_lb_listener" "ingest" {
  load_balancer_arn = aws_lb.ingest.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.ingest.arn
  }
}

# ---- Task definitions (one per service) -------------------------------------
resource "aws_ecs_task_definition" "svc" {
  for_each                 = local.services
  family                   = "${var.project}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.task_exec.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name         = each.key
      image        = "${aws_ecr_repository.svc[each.key].repository_url}:${var.image_tag}"
      essential    = true
      portMappings = each.value.port > 0 ? [{ containerPort = each.value.port }] : []
      environment = [
        # IAM-authenticated bootstrap endpoint for MSK Serverless
        { name = "SPRING_KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam },
        { name = "AWS_REGION", value = var.region },
        { name = "SPRING_PROFILES_ACTIVE", value = each.value.profiles },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.svc[each.key].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = each.key
        }
      }
    }
  ])
}

# ---- Services ---------------------------------------------------------------
resource "aws_ecs_service" "svc" {
  for_each        = local.services
  name            = each.key
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.svc[each.key].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.private_subnets
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = false
  }

  # Wire only the public service (ingest) to the ALB target group.
  dynamic "load_balancer" {
    for_each = each.value.public ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.ingest.arn
      container_name   = each.key
      container_port   = each.value.port
    }
  }

  depends_on = [aws_lb_listener.ingest]
}
