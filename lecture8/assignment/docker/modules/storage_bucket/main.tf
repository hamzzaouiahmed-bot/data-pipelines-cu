resource "null_resource" "bucket" {
  triggers = {
    bucket_name = var.bucket_name
  }
}