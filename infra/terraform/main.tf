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
  bucket = "${var.project}-lakehouse-${var.env}"
  tags   = { Environment = var.env }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Fail closed at the storage layer: no public access, SSE-KMS by default,
# short-lived checkpoint churn expires instead of accumulating.
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_kms_key" "lakehouse" {
  description             = "tx-recon lakehouse at-rest encryption (${var.env})"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  tags                    = { Environment = var.env }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    id     = "expire-checkpoints"
    status = "Enabled"
    filter {
      prefix = "checkpoints/"
    }
    expiration {
      days = 14
    }
  }
  rule {
    id     = "expire-tmp"
    status = "Enabled"
    filter {
      prefix = "tmp/"
    }
    expiration {
      days = 7
    }
  }
}

# Iceberg catalog: Nessie 'db' -> Glue database. Spark SQL needs no changes;
# TABLE_PREFIX=glue rewrites nessie.db.* to glue.db.* (see src/common/settings.py).
resource "aws_glue_catalog_database" "recon" {
  name = "db"
}

# Least-privilege skeleton for the Spark/EMR job role: S3 warehouse + Glue
# catalog only. Attach to the EMR EC2 instance profile / IRSA role.
resource "aws_iam_role" "recon_job" {
  name = "${var.project}-recon-job-${var.env}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
  tags = { Environment = var.env }
}

resource "aws_iam_role_policy" "recon_job" {
  name = "${var.project}-recon-job-${var.env}"
  role = aws_iam_role.recon_job.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Effect   = "Allow"
        Resource = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
      },
      {
        Action   = ["glue:Get*", "glue:CreateTable", "glue:UpdateTable"]
        Effect   = "Allow"
        Resource = ["*"]
      },
      {
        Action   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
        Effect   = "Allow"
        Resource = [aws_kms_key.lakehouse.arn]
      },
    ]
  })
}

# Pipeline run log: one group, 30-day retention (bench-day runs set 1 day).
resource "aws_cloudwatch_log_group" "recon" {
  name              = "/tx-recon/${var.env}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.lakehouse.arn
  tags              = { Environment = var.env }
}

# Placeholder for broker/catalog passwords: wire MSK SASL + Nessie DB here,
# never env-vars. Consumed via *_FILE settings (see src/common/settings.py).
resource "aws_secretsmanager_secret" "recon" {
  name                    = "${var.project}/recon/${var.env}"
  recovery_window_in_days = 0 # bench-day stacks delete immediately; prod uses 30
  tags                    = { Environment = var.env }
}
