output "lakehouse_bucket" {
  description = "S3 bucket (LocalStack) replacing MinIO lakehouse."
  value       = aws_s3_bucket.lakehouse.id
}

output "glue_database" {
  description = "Glue database (LocalStack mock) replacing Nessie namespace."
  value       = aws_glue_catalog_database.recon.name
}
