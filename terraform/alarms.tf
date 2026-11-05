# Four alarms, matching the project spec's "demonstrate metric -> threshold
# -> alarm, not a giant monitoring configuration" guidance. Every threshold
# below is a development/demo value, not derived from load testing or a
# real SLA -- see README for that caveat stated plainly, not just here.

resource "aws_sns_topic" "alerts" {
  name = "${var.app_name}-alerts"
  # Subscribe an email/endpoint to this topic yourself, e.g.:
  #   aws sns subscribe --topic-arn <arn> --protocol email --notification-endpoint you@example.com
}

resource "aws_cloudwatch_metric_alarm" "high_cpu" {
  alarm_name          = "${var.app_name}-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 2
  period               = 300
  metric_name          = "cpu_usage_user"
  namespace            = "CWAgent"
  statistic            = "Average"
  # Demo threshold -- a t3.micro doing nothing else; adjust after real load testing.
  threshold            = 80
  dimensions           = { InstanceId = aws_instance.app.id }
  alarm_description    = "CPU user time above 80% for 10 consecutive minutes (demo threshold, not load-tested)."
  alarm_actions        = [aws_sns_topic.alerts.arn]
  treat_missing_data   = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "high_disk" {
  alarm_name          = "${var.app_name}-high-disk"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods   = 1
  period               = 300
  metric_name          = "disk_used_percent"
  namespace            = "CWAgent"
  statistic            = "Average"
  # Demo threshold -- the 20GB root volume this Terraform provisions; adjust for your real volume size.
  threshold            = 85
  dimensions           = { InstanceId = aws_instance.app.id }
  alarm_description    = "Disk usage above 85% (demo threshold for the 20GB volume this config provisions)."
  alarm_actions        = [aws_sns_topic.alerts.arn]
  treat_missing_data   = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "transfer_failures" {
  alarm_name          = "${var.app_name}-transfer-failures"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods   = 1
  period               = 300
  metric_name          = "TransfersFailed"
  namespace            = "ProtobufTransferSystem"
  statistic            = "Sum"
  # Demo threshold -- any single failure triggers this. Tune up for a
  # high-volume deployment where occasional failures are expected/retried.
  threshold            = 1
  alarm_description    = "At least one failed transfer in a 5-minute window (demo threshold: any failure alerts)."
  alarm_actions        = [aws_sns_topic.alerts.arn]
  treat_missing_data   = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "integrity_violations" {
  alarm_name          = "${var.app_name}-integrity-violations"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods   = 1
  period               = 300
  metric_name          = "IntegrityEvents"
  namespace            = "ProtobufTransferSystem"
  dimensions           = { EventType = "FILE_MODIFIED" }
  statistic            = "Sum"
  threshold            = 1
  alarm_description    = "At least one FILE_MODIFIED (integrity violation) event in a 5-minute window."
  alarm_actions        = [aws_sns_topic.alerts.arn]
  treat_missing_data   = "notBreaching"
}
