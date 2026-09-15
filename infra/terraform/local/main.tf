terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Zero-cost local cloud simulation — LocalStack S3 instead of real AWS.
# Run: docker compose -f docker-compose.yml -f docker-compose.localstack.yml up -d
# Then: terraform -chdir=infra/terraform/local init && terraform -chdir=infra/terraform/local plan
# Same code ships to real AWS by changing var below + removing endpoints block.
provider "aws" {
  access_key = "test"
  secret_key = "test"
  region     = var.aws_region
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3  = "http://localhost:4566"
    iam = "http://localhost:4566"
    sts = "http://localhost:4566"
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project}-lakehouse"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_glue_catalog_database" "recon" {
  name = "db"
}
