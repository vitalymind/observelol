# Persistent detachable disk for VM2 (PostgreSQL data and control plane files)
resource "google_compute_disk" "vm2_data" {
  name = local.infra.control_vm.attached_disk.name
  type = local.infra.control_vm.attached_disk.type
  zone = local.zone
  size = local.infra.control_vm.attached_disk.size
}

# VM1: Ingestion (Nginx + Rust Dispatcher + Redis)
resource "google_compute_instance" "vm1_ingest" {
  name         = local.infra.ingest_vm.name
  machine_type = local.infra.ingest_vm.type
  zone         = local.zone
  tags         = ["ingest"]

  boot_disk {
    initialize_params {
      image = local.infra.ingest_vm.image
      size  = local.infra.ingest_vm.boot_disk.size
      type  = local.infra.ingest_vm.boot_disk.type
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id

    # Static public IP for incoming Producer & Admin traffic
    access_config {
      nat_ip = google_compute_address.vm1_static_ip.address
    }
  }

  service_account {
    email  = "${local.vm_sa_name}@${local.project_id}.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  metadata = {
    user-data = templatefile("${path.module}/../vms/vm1-ingest/cloud-init.yml", {
      region = local.region
    })
  }

  desired_status            = "RUNNING"
  allow_stopping_for_update = true
}

# VM2: Control & Data (PostgreSQL + Next.js Control + OTel-LGTM)
resource "google_compute_instance" "vm2_control" {
  name         = local.infra.control_vm.name
  machine_type = local.infra.control_vm.type
  zone         = local.zone
  tags         = ["control"]

  boot_disk {
    initialize_params {
      image = local.infra.control_vm.image
      size  = local.infra.control_vm.boot_disk.size
      type  = local.infra.control_vm.boot_disk.type
    }
  }

  attached_disk {
    source      = google_compute_disk.vm2_data.id
    device_name = local.infra.control_vm.attached_disk.name
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id
    access_config {}
  }

  service_account {
    email  = "${local.vm_sa_name}@${local.project_id}.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  metadata = {
    user-data = templatefile("${path.module}/../vms/vm2-control/cloud-init.yml", {
      region = local.region
    })
  }

  desired_status            = "RUNNING"
  allow_stopping_for_update = true
}

# VM3: Stateless Rust Workers
resource "google_compute_instance" "vm3_worker" {
  name         = local.infra.worker_vm.name
  machine_type = local.infra.worker_vm.type
  zone         = local.zone
  tags         = ["worker"]

  boot_disk {
    initialize_params {
      image = local.infra.worker_vm.image
      size  = local.infra.worker_vm.boot_disk.size
      type  = local.infra.worker_vm.boot_disk.type
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id
    access_config {}
  }

  service_account {
    email  = "${local.vm_sa_name}@${local.project_id}.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }

  metadata = {
    user-data = templatefile("${path.module}/../vms/vm3-worker/cloud-init.yml", {
      region = local.region
    })
  }

  desired_status            = "RUNNING"
  allow_stopping_for_update = true
}
