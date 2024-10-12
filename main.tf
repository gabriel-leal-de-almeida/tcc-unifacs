# main.tf

# Configure the GCP provider
provider "google" {
  project = var.project_id
  region  = var.region
}

# Activate necessary APIs
resource "google_project_service" "dataproc" {
  service = "dataproc.googleapis.com"
}

resource "google_project_service" "bigquery" {
  service = "bigquery.googleapis.com"
}

resource "google_project_service" "storage" {
  service = "storage.googleapis.com"
}

# Define resources
# Add your resource definitions here

# Configure the Terraform backend
terraform {
  backend "gcs" {
    bucket  = var.bucket_name
    prefix  = "terraform/state"
  }
}