locals {
  roles = toset(["api", "worker"])
  image = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"
}

resource "aws_ecr_repository" "app" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration { scan_on_push = true }
}
resource "aws_ecs_cluster" "main" {
  name = var.name
}
resource "aws_cloudwatch_log_group" "service" {
  for_each          = local.roles
  name              = "/ecs/${var.name}/${each.key}"
  retention_in_days = 14
}
resource "aws_kinesis_stream" "operations" {
  name                = "${var.name}-operations"
  shard_count         = 1
  retention_period    = 24
  encryption_type     = "KMS"
  kms_key_id          = "alias/aws/kinesis"
  shard_level_metrics = ["IteratorAgeMilliseconds"]
  stream_mode_details { stream_mode = "PROVISIONED" }
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}
resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
resource "aws_iam_role" "task" {
  for_each           = local.roles
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}
resource "aws_iam_role_policy" "api" {
  role = aws_iam_role.task["api"].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["kinesis:PutRecord", "kinesis:PutRecords"], Resource = aws_kinesis_stream.operations.arn }
  ] })
}
resource "aws_iam_role_policy" "worker" {
  role = aws_iam_role.task["worker"].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:GetShardIterator", "kinesis:GetRecords"], Resource = aws_kinesis_stream.operations.arn }
  ] })
}

resource "aws_lb" "api" {
  name               = "${var.name}-api"
  load_balancer_type = "application"
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]
}
resource "aws_lb_target_group" "api" {
  name                 = "${var.name}-api"
  port                 = 8080
  protocol             = "HTTP"
  target_type          = "ip"
  vpc_id               = aws_vpc.main.id
  deregistration_delay = 15
  health_check {
    path                = "/ready"
    interval            = 15
    healthy_threshold   = 2
    unhealthy_threshold = 3
    matcher             = "200"
  }
}
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.api.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = var.certificate_arn == "" ? "forward" : "redirect"
    target_group_arn = var.certificate_arn == "" ? aws_lb_target_group.api.arn : null
    dynamic "redirect" {
      for_each = var.certificate_arn == "" ? [] : [1]
      content {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }
  }
}
resource "aws_lb_listener" "https" {
  count             = var.certificate_arn == "" ? 0 : 1
  load_balancer_arn = aws_lb.api.arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

resource "aws_ecs_task_definition" "service" {
  for_each                 = local.roles
  family                   = "${var.name}-${each.key}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task[each.key].arn
  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }
  container_definitions = jsonencode([{
    name                   = each.key, image = local.image, essential = true,
    readonlyRootFilesystem = true,
    portMappings           = each.key == "api" ? [{ containerPort = 8080, hostPort = 8080, protocol = "tcp" }] : [],
    environment = [
      { name = "SERVICE_ROLE", value = each.key },
      { name = "AWS_REGION", value = var.region },
      { name = "KINESIS_STREAM_NAME", value = aws_kinesis_stream.operations.name },
      { name = "DB_HOST", value = aws_db_instance.main.address },
      { name = "DB_PORT", value = "5432" },
      { name = "DB_NAME", value = aws_db_instance.main.db_name },
      { name = "DB_SSLMODE", value = "verify-full" }
    ],
    secrets = [
      { name = "DB_USER", valueFrom = "${aws_db_instance.main.master_user_secret[0].secret_arn}:username::" },
      { name = "DB_PASSWORD", valueFrom = "${aws_db_instance.main.master_user_secret[0].secret_arn}:password::" },
      { name = "AUTH_TOKEN", valueFrom = aws_secretsmanager_secret.auth.arn }
    ],
    logConfiguration = { logDriver = "awslogs", options = {
      "awslogs-group"  = aws_cloudwatch_log_group.service[each.key].name,
      "awslogs-region" = var.region, "awslogs-stream-prefix" = "ecs"
    } }
  }])
}
resource "aws_ecs_service" "service" {
  for_each                           = var.deploy_services ? local.roles : toset([])
  name                               = "${var.name}-${each.key}"
  cluster                            = aws_ecs_cluster.main.id
  task_definition                    = aws_ecs_task_definition.service[each.key].arn
  desired_count                      = 1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100
  availability_zone_rebalancing      = "DISABLED"
  launch_type                        = "FARGATE"
  health_check_grace_period_seconds  = each.key == "api" ? 30 : null
  wait_for_steady_state              = true
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = aws_subnet.public[*].id
    assign_public_ip = true
    security_groups  = [each.key == "api" ? aws_security_group.api.id : aws_security_group.worker.id]
  }
  dynamic "load_balancer" {
    for_each = each.key == "api" ? [1] : []
    content {
      target_group_arn = aws_lb_target_group.api.arn
      container_name   = "api"
      container_port   = 8080
    }
  }
  depends_on = [aws_lb_listener.http, aws_lb_listener.https, aws_iam_role_policy_attachment.execution, aws_iam_role_policy.secrets, aws_route_table_association.public]
}
