locals {
  config = yamldecode(file("${path.module}/../../config.yml"))

  # GCP Project & Region
  project_id   = local.config.gcp_project_id
  region       = local.config.gcp_region
  vm_sa_name   = local.config.gcp_vm_sa_name
  state_bucket = local.config.gcp_state_bucket_name

  # Infrastructure definitions
  infra = local.config.infra_definitions
  zone  = "${local.config.gcp_region}-${local.config.infra_definitions.zone}"
}
