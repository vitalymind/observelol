# Docker container registry for ObserveLoL services (dispatcher, worker, control, nginx)
resource "google_artifact_registry_repository" "docker_repo" {
  location = local.region
  repository_id = "observelol"
  description = "Docker repository for ObserveLoL containers"
  format = "DOCKER"
}