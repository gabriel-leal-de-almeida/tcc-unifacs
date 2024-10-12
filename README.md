# My Repository GCP Terraform

This repository contains the necessary files to automate the deployment of infrastructure on the Google Cloud Platform (GCP) using Terraform. It also includes workflows for data processing pipelines using GitHub Actions.

## Terraform Configuration

The `main.tf` file defines the Terraform configuration. It includes the following:

- Configuration for the GCP provider.
- Activation of the necessary APIs (Dataproc, BigQuery, Storage).
- Definition of required resources such as Cloud Storage buckets, service accounts, and permissions.
- Configuration of the Terraform backend to use an existing GCS bucket for storing the state (`terraform.tfstate`).

The `variables.tf` file declares the required variables:

```hcl
variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "dataproc_service_account" {
  type = string
}
```

The `versions.tf` file specifies the versions of Terraform and the providers:

```hcl
terraform {
  required_version = ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0.0"
    }
  }
}
```

## GitHub Actions Workflow

The `.github/workflows/terraform.yml` file defines a workflow that automatically applies the Terraform configuration when there are pushes to the main branch. The workflow performs the following steps:

1. Authenticates to GCP using a service account key stored as a GitHub Secret (`GCP_CREDENTIALS`).
2. Initializes Terraform with the backend pointing to the existing bucket.
3. Executes `terraform plan` and `terraform apply`.

## Data Processing Scripts

The `scripts/` directory contains the following scripts:

- `process_data.py`: A PySpark script that reads data from BigQuery and writes it to GCS in various formats (Parquet, ORC, Avro, CSV) with specific compressions.
- `read_data.py`: A PySpark script that reads the files stored in GCS and measures the read performance.

## Automation Scripts

The automation scripts in the `scripts/` directory are used to submit jobs to the Dataproc Serverless:

- `run_jobs.sh`: Submits write jobs for each format and compression.
- `run_read_jobs.sh`: Submits read jobs to evaluate performance.

## Documentation

The `README.md` file provides instructions on how to configure the necessary Secrets in the GitHub repository:

- `GCP_CREDENTIALS`: Content of the JSON file for the service account key.
- `GCP_PROJECT_ID`: ID of the project in GCP.
- `GCP_REGION`: Default region for resources.
- `GCS_BUCKET_NAME`: Name of the existing bucket for Terraform state.

It also explains how to execute the GitHub Actions workflows and how to use the scripts to process data and interpret the results.

Please make sure to replace the sensitive data with appropriate values and configure the necessary permissions and roles for the service accounts.