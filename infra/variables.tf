variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, prod)"
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "The ID of the VPC where resources will be deployed"
  type        = string
  default     = "vpc-0abcdef1234567890" # Example placeholder
}

variable "subnet_ids" {
  description = "List of subnet IDs for MSK and MWAA"
  type        = list(string)
  default     = ["subnet-0a1b2c3d", "subnet-0e4f5g6h"] # Example placeholders
}
