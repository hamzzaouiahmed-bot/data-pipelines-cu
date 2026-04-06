output "minio_console_url" {
  value = "http://localhost:9001"
}

output "minio_api_url" {
  value = "http://localhost:9000"
}

output "postgres_connection" {
  value = "postgresql://${var.postgres_user}:${var.postgres_password}@localhost:5432/${var.postgres_db}"
}

output "bucket_names" {
  value = [for b in module.storage_buckets : b.bucket_name]
}