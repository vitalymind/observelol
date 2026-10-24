terraform {
  required_version = ">= 1.13.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.8"
    }
  }

  backend "gcs" {
    bucket = local.infra.state_bucket
    prefix = "terraform/state"
  }
}

provider "google" {
  project = local.project_id
  region  = local.region
  zone    = local.zone
}