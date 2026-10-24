output "vm1_public_ip" {
  description = "Static public IP of VM1 (Ingestion)"
  value       = google_compute_address.vm1_static_ip.address
}

output "vm1_internal_ip" {
  description = "Internal VPC IP of VM1 (Ingestion / Redis)"
  value       = google_compute_instance.vm1_ingest.network_interface[0].network_ip
}

output "vm2_internal_ip" {
  description = "Internal VPC IP of VM2 (PostgreSQL / Control / OTel)"
  value       = google_compute_instance.vm2_control.network_interface[0].network_ip
}

output "vm3_internal_ip" {
  description = "Internal VPC IP of VM3 (Workers)"
  value       = google_compute_instance.vm3_worker.network_interface[0].network_ip
}

output "artifact_registry_url" {
  description = "Docker Artifact Registry URL"
  value       = "${local.region}-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}"
}