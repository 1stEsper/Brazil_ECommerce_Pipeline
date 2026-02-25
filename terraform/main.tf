terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.20.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = file("../credentials/brazil-olist-26-ba5639906dfd.json")
  project     = var.project_id
  region      = var.region
}

# Crete a GCS bucket to serve as the data lake
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  # Add labels for organization 
  labels = {
    env = "dev"
    dep = "data_engineering"
  }
}
