variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type. Small on purpose -- this is a functional demo of one server, not a load target."
  type        = string
  default     = "t3.micro"
}

variable "ami_id" {
  description = "AMI id for the EC2 instance. Pin this explicitly (e.g. via `aws ec2 describe-images` for the current Amazon Linux 2023 AMI in your region) rather than using `latest`, so deployments are reproducible."
  type        = string
}

variable "key_pair_name" {
  description = "Existing EC2 key pair name for SSH access. Create one with `aws ec2 create-key-pair` if you don't have one."
  type        = string
}

variable "ssh_allowed_cidr" {
  description = "CIDR block allowed to SSH into the instance. Set this to YOUR IP (e.g. \"203.0.113.4/32\"), never 0.0.0.0/0."
  type        = string
}

variable "vpc_id" {
  description = "VPC to deploy the security group into. Defaults to the account's default VPC if left blank."
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet to launch the EC2 instance into."
  type        = string
}

variable "tcp_port" {
  description = "Plain TCP transfer port."
  type        = number
  default     = 9000
}

variable "tls_port" {
  description = "TLS-secured TCP transfer port."
  type        = number
  default     = 9443
}

variable "udp_port" {
  description = "UDP transfer port."
  type        = number
  default     = 9001
}

variable "http_port" {
  description = "HTTP management API port."
  type        = number
  default     = 8080
}

variable "app_name" {
  description = "Used to name/tag resources and as the CloudWatch log group prefix."
  type        = string
  default     = "protobuf-transfer"
}
