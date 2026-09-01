terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "s3" {
  #   bucket         = "tx-recon-terraform-state"
  #   key            = "terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-locks"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "tx-recon"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

# ------------------------------------------------------------------------------
# 1. Amazon MSK (Managed Streaming for Kafka) - Replaces Redpanda
# ------------------------------------------------------------------------------
resource "aws_msk_cluster" "kafka_cluster" {
  cluster_name           = "tx-recon-${var.environment}"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.kafka_sg.id]

    storage_info {
      ebs_storage_info {
        volume_size = 1000
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = aws_kms_key.msk_key.arn
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_logs.name
      }
      s3_logs {
        enabled = true
        bucket  = aws_s3_bucket.msk_logs.id
        prefix  = "msk-logs"
      }
    }
  }
}

resource "aws_kms_key" "msk_key" {
  description             = "MSK cluster encryption key"
  deletion_window_in_days = 10
}

resource "aws_cloudwatch_log_group" "msk_logs" {
  name              = "/msk/tx-recon-${var.environment}"
  retention_in_days = 30
}

resource "aws_s3_bucket" "msk_logs" {
  bucket = "tx-recon-msk-logs-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_security_group" "kafka_sg" {
  name        = "tx-recon-kafka-sg-${var.environment}"
  description = "Security group for MSK cluster"
  vpc_id      = var.vpc_id
}

resource "aws_security_group_rule" "kafka_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.kafka_sg.id
}

# ------------------------------------------------------------------------------
# 2. Amazon S3 - Replaces MinIO (Data Lake Storage)
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "datalake" {
  bucket = "tx-recon-datalake-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "datalake_versioning" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake_encryption" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "datalake_public_access" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# ------------------------------------------------------------------------------
# 3. AWS Glue Catalog - Replaces Nessie (Iceberg Catalog)
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "iceberg_db" {
  name          = "tx_recon_iceberg_db_${var.environment}"
  description   = "Iceberg catalog for tx-recon reconciliation data"
  location_uri  = "s3://${aws_s3_bucket.datalake.id}/iceberg/"
}

resource "aws_glue_catalog_table" "reconciliation_table" {
  name          = "reconciliation"
  database_name = aws_glue_catalog_database.iceberg_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "iceberg"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.datalake.id}/iceberg/reconciliation/"

    columns {
      name = "transaction_id"
      type = "string"
    }

    columns {
      name = "amount_paise"
      type = "bigint"
    }

    columns {
      name = "gateway_fee_paise"
      type = "bigint"
    }

    columns {
      name = "bank_fee_paise"
      type = "bigint"
    }

    columns {
      name = "merchant_id"
      type = "string"
    }

    columns {
      name = "gateway_status"
      type = "string"
    }

    columns {
      name = "reconciliation_status"
      type = "string"
    }

    columns {
      name = "bank_ref_id"
      type = "string"
    }

    columns {
      name = "ingested_at"
      type = "timestamp"
    }
  }
}

# ------------------------------------------------------------------------------
# 4. Amazon MWAA - Replaces local Apache Airflow
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "mwaa_dags" {
  bucket = "tx-recon-mwaa-dags-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_mwaa_environment" "airflow" {
  name               = "tx-recon-${var.environment}"
  airflow_version    = "2.11.2"
  environment_class  = "mw1.small"

  execution_role_arn = aws_iam_role.mwaa_role.arn

  source_bucket_arn = aws_s3_bucket.mwaa_dags.arn
  dag_s3_path       = "dags/"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids         = var.subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "WARNING"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
    task_logs {
      enabled   = true
      log_level = "WARNING"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "WARNING"
    }
  }

  depends_on = [aws_security_group_rule.mwaa_egress]
}

resource "aws_security_group" "mwaa_sg" {
  name        = "tx-recon-mwaa-sg-${var.environment}"
  description = "Security group for MWAA"
  vpc_id      = var.vpc_id
}

resource "aws_security_group_rule" "mwaa_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.mwaa_sg.id
}

resource "aws_iam_role" "mwaa_role" {
  name = "tx-recon-mwaa-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_policy" {
  name = "tx-recon-mwaa-policy"
  role = aws_iam_role.mwaa_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject*",
          "s3:ListBucket",
          "s3:PutObject*",
          "s3:DeleteObject*"
        ]
        Resource = [
          aws_s3_bucket.mwaa_dags.arn,
          "${aws_s3_bucket.mwaa_dags.arn}/*",
          aws_s3_bucket.datalake.arn,
          "${aws_s3_bucket.datalake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData"
        ]
        Resource = "*"
      }
    ]
  })
}
