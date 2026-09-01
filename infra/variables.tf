variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "vpc_id" {
  description = "The ID of the VPC where resources will be deployed. Get from your AWS account."
  type        = string

  validation {
    condition     = can(regex("^vpc-[a-f0-9]{17}$", var.vpc_id))
    error_message = "VPC ID must be in the format vpc-xxxxxxxxxxxxxxxxx."
  }
}

variable "subnet_ids" {
  description = "List of at least 2 subnet IDs for MSK and MWAA (must be in different AZs)"
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) >= 2
    error_message = "At least 2 subnet IDs required for MSK broker nodes."
  }
}
