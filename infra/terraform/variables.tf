variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "tx-recon"
}

variable "aws_region" {
  description = "AWS region for the lakehouse storage and Glue catalog."
  type        = string
  default     = "ap-south-1"
}

variable "env" {
  description = "Environment name: dev, stage, or prod. Select via -var-file=envs/<env>.tfvars."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "stage", "prod"], var.env)
    error_message = "env must be dev, stage, or prod."
  }
}
