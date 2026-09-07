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
