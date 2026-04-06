terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_image" "minio" {
  name = "minio/minio:latest"
}

resource "docker_image" "postgres" {
  name = "postgres:15"
}

resource "docker_volume" "minio_data" {
  name = "${var.project_name}-minio-data"
}

resource "docker_volume" "postgres_data" {
  name = "${var.project_name}-postgres-data"
}

resource "docker_container" "minio" {
  name  = "${var.project_name}-minio"
  image = docker_image.minio.image_id

  ports {
    internal = 9000
    external = 9000
  }

  ports {
    internal = 9001
    external = 9001
  }

  env = [
    "MINIO_ROOT_USER=${var.minio_root_user}",
    "MINIO_ROOT_PASSWORD=${var.minio_root_password}"
  ]

  command = [
    "server",
    "/data",
    "--console-address",
    ":9001"
  ]

  volumes {
    volume_name    = docker_volume.minio_data.name
    container_path = "/data"
  }
}

resource "docker_container" "postgres" {
  name  = "${var.project_name}-postgres"
  image = docker_image.postgres.image_id

  ports {
    internal = 5432
    external = 5432
  }

  env = [
    "POSTGRES_USER=${var.postgres_user}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
    "POSTGRES_DB=${var.postgres_db}"
  ]

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }
}

module "storage_buckets" {
  source = "./modules/storage_bucket"

  for_each = var.bucket_names

  bucket_name = "${var.project_name}-${var.environment}-${each.value}"
}