# Dashboard answers exactly the questions the project's spec lists:
# is the server healthy, are transfers succeeding, how much data moved,
# are recoveries happening, are integrity violations occurring, is the
# host under resource pressure. Nothing decorative.
resource "aws_cloudwatch_dashboard" "app" {
  dashboard_name = "${var.app_name}-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6,
        properties = {
          title  = "Host: CPU / Memory / Disk"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["CWAgent", "cpu_usage_user", "InstanceId", aws_instance.app.id],
            ["CWAgent", "mem_used_percent", "InstanceId", aws_instance.app.id],
            ["CWAgent", "disk_used_percent", "InstanceId", aws_instance.app.id],
          ]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6,
        properties = {
          title  = "Host: Network Throughput"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["CWAgent", "net_bytes_sent", "InstanceId", aws_instance.app.id],
            ["CWAgent", "net_bytes_recv", "InstanceId", aws_instance.app.id],
          ]
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 12, height = 6,
        properties = {
          title  = "Transfers: Started / Completed / Failed"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          metrics = [
            ["ProtobufTransferSystem", "TransfersStarted"],
            ["ProtobufTransferSystem", "TransfersCompleted"],
            ["ProtobufTransferSystem", "TransfersFailed"],
          ]
        }
      },
      {
        type = "metric", x = 12, y = 6, width = 12, height = 6,
        properties = {
          title  = "Bytes Transferred"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          metrics = [
            ["ProtobufTransferSystem", "BytesTransferred"],
          ]
        }
      },
      {
        type = "metric", x = 0, y = 12, width = 8, height = 6,
        properties = {
          title  = "Active Connections"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["ProtobufTransferSystem", "ActiveConnections"],
          ]
        }
      },
      {
        type = "metric", x = 8, y = 12, width = 8, height = 6,
        properties = {
          title  = "Recovered Transfers (per restart)"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          metrics = [
            ["ProtobufTransferSystem", "RecoveredTransfers"],
          ]
        }
      },
      {
        type = "metric", x = 16, y = 12, width = 8, height = 6,
        properties = {
          title  = "Integrity Events"
          view   = "timeSeries"
          region = var.aws_region
          stat   = "Sum"
          metrics = [
            ["ProtobufTransferSystem", "IntegrityEvents"],
          ]
        }
      },
    ]
  })
}
