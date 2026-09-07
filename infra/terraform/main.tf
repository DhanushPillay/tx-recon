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

# Lakehouse storage: MinIO bucket 'lakehouse' -> S3 bucket 'tx-recon-lakehouse'.
resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project}-lakehouse"
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Iceberg catalog: Nessie 'db' -> Glue database. Spark SQL needs no changes;
# TABLE_PREFIX=glue rewrites nessie.db.* to glue.db.* (see src/common/settings.py).
resource "aws_glue_catalog_database" "recon" {
  name = "db"
}
