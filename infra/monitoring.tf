# Lag is monitored rather than scaled: this design has exactly one consumer.
resource "aws_cloudwatch_metric_alarm" "worker_lag" {
  alarm_name          = "${var.name}-worker-lag"
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  dimensions          = { StreamName = aws_kinesis_stream.operations.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = 60000
  treat_missing_data  = "notBreaching"
}
