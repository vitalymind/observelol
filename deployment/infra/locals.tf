locals {
  config = yamldecode(file("${path.module}/../../config.yml"))

  # GCP Project & Region
  project_id   = local.config.GCP_PROJECT_ID
  region       = local.config.GCP_REGION
  vm_sa_name   = local.config.GCP_VM_SA_NAME
  state_bucket = local.config.GCP_STATE_BUCKET_NAME

  # Infrastructure definitions
  infra = local.config.infra_definitions
  zone  = "${local.config.GCP_REGION}-${local.config.infra_definitions.zone}"
}
