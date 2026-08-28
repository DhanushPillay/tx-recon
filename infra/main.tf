terraform {
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

# ------------------------------------------------------------------------------
# 1. Amazon MSK (Managed Streaming for Kafka) - Replaces Redpanda
# ------------------------------------------------------------------------------
resource "aws_msk_cluster" "kafka_cluster" {
  cluster_name           = "tx-recon-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.large"
    client_subnets = var.subnet_ids
    security_groups = [aws_security_group.kafka_sg.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}

resource "aws_security_group" "kafka_sg" {
  name        = "tx-recon-kafka-sg"
  description = "Security group for MSK cluster"
  vpc_id      = var.vpc_id
}

# ------------------------------------------------------------------------------
# 2. Amazon S3 - Replaces MinIO (Data Lake Storage)
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "datalake" {
  bucket = "tx-recon-datalake-${var.environment}"
}

resource "aws_s3_bucket_versioning" "datalake_versioning" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ------------------------------------------------------------------------------
# 3. AWS Glue Catalog - Replaces Nessie (Iceberg Catalog)
# ------------------------------------------------------------------------------
resource "aws_glue_catalog_database" "iceberg_db" {
  name = "tx_recon_iceberg_db"
}

# ------------------------------------------------------------------------------
# 4. Amazon MWAA - Replaces local Apache Airflow
# ------------------------------------------------------------------------------
resource "aws_s3_bucket" "mwaa_dags" {
  bucket = "tx-recon-mwaa-dags-${var.environment}"
}

resource "aws_mwaa_environment" "airflow" {
  name = "tx-recon-airflow"
  
  airflow_version = "2.6.3"
  environment_class = "mw1.small"
  
  execution_role_arn = aws_iam_role.mwaa_role.arn
  
  source_bucket_arn = aws_s3_bucket.mwaa_dags.arn
  dag_s3_path       = "dags/"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids         = var.subnet_ids
  }
}

resource "aws_security_group" "mwaa_sg" {
  name        = "tx-recon-mwaa-sg"
  description = "Security group for MWAA"
  vpc_id      = var.vpc_id
}

resource "aws_iam_role" "mwaa_role" {
  name = "tx_recon_mwaa_execution_role"

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
