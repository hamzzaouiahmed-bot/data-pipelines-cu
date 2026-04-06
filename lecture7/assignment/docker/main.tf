terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }
}

provider "docker" {}
provider "local" {}

resource "local_file" "web_page" {
  filename = "${path.module}/index.html"
  content  = <<-EOT
  <!DOCTYPE html>
  <html>
  <head>
    <title>Lecture 7 - Web Server + n8n</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 40px;
        background: #f4f4f4;
      }
      .card {
        background: white;
        padding: 30px;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        max-width: 700px;
      }
      h1 { color: #222; }
      p { font-size: 18px; }
      code {
        background: #eee;
        padding: 2px 6px;
        border-radius: 4px;
      }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>Lecture 7 Assignment</h1>
      <p>Web server deployed with Terraform and Docker.</p>
      <p>n8n is also running on <code>http://localhost:5678</code>.</p>
      <p>Dependency order is respected: webserver starts before n8n.</p>
    </div>
  </body>
  </html>
  EOT
}

resource "docker_image" "nginx" {
  name = "nginx:alpine"
}

resource "docker_image" "n8n" {
  name = "n8nio/n8n:latest"
}

resource "docker_container" "webserver" {
  name  = "lecture7-webserver"
  image = docker_image.nginx.image_id

  ports {
    internal = 80
    external = 8080
  }

  volumes {
    host_path      = abspath("${path.module}/index.html")
    container_path = "/usr/share/nginx/html/index.html"
  }
}

resource "docker_container" "n8n" {
  name  = "lecture7-n8n"
  image = docker_image.n8n.image_id

  depends_on = [docker_container.webserver]

  ports {
    internal = 5678
    external = 5678
  }

  env = [
    "N8N_HOST=localhost",
    "N8N_PORT=5678",
    "N8N_PROTOCOL=http",
    "NODE_ENV=development"
  ]
}

output "webserver_url" {
  value = "http://localhost:8080"
}

output "n8n_url" {
  value = "http://localhost:5678"
}