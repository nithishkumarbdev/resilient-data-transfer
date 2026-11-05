terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Security group: only the ports this specific application actually uses.
# No 0-65535, no 0.0.0.0/0 for SSH.
# ---------------------------------------------------------------------------
resource "aws_security_group" "app" {
  name        = "${var.app_name}-sg"
  description = "Security group for ${var.app_name} -- only the ports the application actually exposes"
  vpc_id      = var.vpc_id != "" ? var.vpc_id : null

  ingress {
    description = "SSH (restricted to the configured source CIDR)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  ingress {
    description = "HTTP management API"
    from_port   = var.http_port
    to_port     = var.http_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Plain TCP file transfer"
    from_port   = var.tcp_port
    to_port     = var.tcp_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "TLS-secured TCP file transfer"
    from_port   = var.tls_port
    to_port     = var.tls_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "UDP file transfer"
    from_port   = var.udp_port
    to_port     = var.udp_port
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Outbound: CloudWatch, package updates, etc."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.app_name}-sg" }
}

# ---------------------------------------------------------------------------
# IAM: least-privilege role for the EC2 instance profile. No long-lived
# credentials are ever placed on the instance -- the application and the
# CloudWatch Agent both use this role's temporary credentials automatically.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "app" {
  name               = "${var.app_name}-ec2-role"
  assume_role_policy = file("${path.module}/../iam/trust-policy.json")
}

resource "aws_iam_role_policy" "app" {
  name = "${var.app_name}-policy"
  role = aws_iam_role.app.id

  # Interpolated with the real account/region -- this is the actual
  # enforced policy. ../iam/policy.json is a readable reference copy
  # with placeholder ARN segments.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishApplicationAndHostMetrics"
        Effect   = "Allow"
        Action   = "cloudwatch:PutMetricData"
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = ["ProtobufTransferSystem", "CWAgent"]
          }
        }
      },
      {
        Sid    = "ShipApplicationLogsToOneLogGroup"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
        ]
        Resource = "${aws_cloudwatch_log_group.app.arn}:*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "app" {
  name = "${var.app_name}-instance-profile"
  role = aws_iam_role.app.name
}

# ---------------------------------------------------------------------------
# CloudWatch: one log group for application logs.
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "app" {
  name              = "/${var.app_name}/application"
  retention_in_days = 14
}

# ---------------------------------------------------------------------------
# EC2 instance. Small, single node, local EBS storage for WAL/file data --
# see README for why S3/EFS were deliberately not introduced.
# ---------------------------------------------------------------------------
resource "aws_instance" "app" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  key_name               = var.key_pair_name
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile    = aws_iam_instance_profile.app.name

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true # AWS-managed encryption at rest for the WAL/file storage volume
  }

  tags = { Name = var.app_name }
}
