terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.1.0"
    }
  }
}

provider "google" {
  project     = var.a_project_id
  region      = var.b_region
}

variable "a_project_id" {
  type        = string
  description = "Project ID (example value: snowflake-pl-project-123)"
}

variable "b_region" {
  type        = string
  description = "Snowflake region (example value: us-east4)"
}

variable "c_zone" {
  type        = string
  description = "Zone within the region (example value: us-east4-b)"
}

variable "d_ssh_cidr" {
  type        = string
  description = "CIDR that will be allowed for SSH connections to the machine, e.g., 203.0.113.45/32"
}

resource "google_compute_network" "snowflake_pl_cn" {
  name                    = "snowflake-pl-network"
  auto_create_subnetworks = true
}


resource "google_compute_subnetwork" "snowflake_pl_subnet" {
  name          = "snowflake-pl-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.b_region
  network       = google_compute_network.snowflake_pl_cn.id
}

resource "google_compute_subnetwork" "snowflake_pl_subnet_psc" {
  name          = "snowflake-pl-subnet-psc"
  ip_cidr_range = "10.0.1.0/24"
  region        = var.b_region
  network       = google_compute_network.snowflake_pl_cn.id
  purpose       = "PRIVATE_SERVICE_CONNECT"
}

resource "google_compute_instance" "snowflake_pl_vm" {
  name         = "snowflake-pl-vm"
  machine_type = "e2-micro"
  zone         = var.c_zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network    = google_compute_network.snowflake_pl_cn.id
    subnetwork = google_compute_subnetwork.snowflake_pl_subnet.id
    access_config {}
  }

  tags = ["snowflake-pl-https-server"]
}

resource "google_compute_firewall" "allow_ssh" {
  name    = "snowflake-pl-allow-ssh"
  network = google_compute_network.snowflake_pl_cn.id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  target_tags   = ["snowflake-pl-https-server"]
  source_ranges = [var.d_ssh_cidr]
}

resource "google_compute_firewall" "snowflake_pl_allow_https" {
  name    = "snowflake-pl-allow-https"
  network = google_compute_network.snowflake_pl_cn.id

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  target_tags = ["snowflake-pl-https-server"]
  source_ranges = ["10.0.1.0/24"] # Only Private Service Connect subnet's CIDR range allowed
}

resource "google_compute_instance_group" "snowflake_pl_ig" {
  name        = "snowflake-pl-instance-group"
  zone        = var.c_zone
  instances   = [google_compute_instance.snowflake_pl_vm.self_link]
  named_port {
    name = "https"
    port = 443
  }
}

resource "google_compute_forwarding_rule" "snowflake_pl_lb" {
  name                  = "snowflake-pl-lb"
  region                = var.b_region
  load_balancing_scheme = "INTERNAL"
  ip_protocol           = "TCP"
  ports                 = ["443"]
  network               = google_compute_network.snowflake_pl_cn.id
  backend_service       = google_compute_region_backend_service.snowflake_pl_lb_backend.id
}

resource "google_compute_region_backend_service" "snowflake_pl_lb_backend" {
  name                    = "snowflake-pl-backend-service"
  region                  = var.b_region
  protocol                = "TCP"
  load_balancing_scheme   = "INTERNAL"
  timeout_sec             = 10

  backend {
    group = google_compute_instance_group.snowflake_pl_ig.self_link
    balancing_mode = "CONNECTION"
  }

  health_checks = [google_compute_health_check.snowflake_pl_health_check.id]
}

resource "google_compute_health_check" "snowflake_pl_health_check" {
  name               = "snowflake-pl-health-check"
  tcp_health_check {
    port = 443
  }
}

resource "google_compute_service_attachment" "snowflake_pl_private_service" {
  name                  = "snowflake-pl-private-service"
  region                = var.b_region
  nat_subnets           = [google_compute_subnetwork.snowflake_pl_subnet_psc.id]
  connection_preference = "ACCEPT_AUTOMATIC" #CHANGEME: If you do not want to automatically accept endpoint connections
  target_service        = google_compute_forwarding_rule.snowflake_pl_lb.id
  enable_proxy_protocol = false
}
