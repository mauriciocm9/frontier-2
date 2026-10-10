output "endpoint" {
  value = "${var.certificate_arn == "" ? "http" : "https"}://${aws_lb.api.dns_name}"
}
output "repository_url" { value = aws_ecr_repository.app.repository_url }
output "region" { value = var.region }
output "cluster_name" { value = aws_ecs_cluster.main.name }
output "stream_name" { value = aws_kinesis_stream.operations.name }
output "database_address" { value = aws_db_instance.main.address }
output "auth_secret_arn" { value = aws_secretsmanager_secret.auth.arn }
