output "lakehouse_bucket" {
  description = "S3 bucket replacing local MinIO 'lakehouse' bucket."
  value       = aws_s3_bucket.lakehouse.id
}

output "glue_database" {
  description = "Glue database replacing the local Nessie namespace."
  value       = aws_glue_catalog_database.recon.name
}
