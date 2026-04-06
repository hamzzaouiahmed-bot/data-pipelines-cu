variable "project_name" {
  type    = string
  default = "lecture8-baseline"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "minio_root_user" {
  type    = string
  default = "minioadmin"
}

variable "minio_root_password" {
  type    = string
  default = "minioadmin"
}

variable "postgres_user" {
  type    = string
  default = "postgres"
}

variable "postgres_password" {
  type    = string
  default = "postgres"
}

variable "postgres_db" {
  type    = string
  default = "pipeline_metadata"
}

variable "bucket_names" {
  type    = set(string)
  default = ["raw", "staged"]
}