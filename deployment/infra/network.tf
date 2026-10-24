resource "google_compute_network" "vpc" {
  name                    = "observelol-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "observelol-subnet"
  ip_cidr_range            = "10.0.1.0/24"
  region                   = local.region
  network                  = google_compute_network.vpc.id
  private_ip_google_access = true
}

# Reserved static external IP for VM1 (Ingestion entry point)
resource "google_compute_address" "vm1_static_ip" {
  name   = "observelol-vm1-ip"
  region = local.region
}

# Public HTTPS traffic allowed ONLY to VM1 (ingest)
resource "google_compute_firewall" "allow_public_http_vm1" {
  name    = "observelol-allow-public-http-vm1"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["ingest"]
}

# Internal communication between VM1, VM2, and VM3 inside the subnet
resource "google_compute_firewall" "allow_internal" {
  name    = "observelol-allow-internal"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = [google_compute_subnetwork.subnet.ip_cidr_range]
}

# Allow SSH and TCP port-forwarding (e.g. Grafana on VM2) via Google Identity-Aware Proxy (IAP)
resource "google_compute_firewall" "allow_iap_tunnel" {
  name    = "observelol-allow-iap-tunnel"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22", "3000"]
  }

  # Google Cloud IAP TCP forwarding IP range
  source_ranges = ["35.235.240.0/20"]
}
