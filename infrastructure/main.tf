terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.18.0"
    }
  }
}

provider "google" {
  project = "service-requests-chicago"
  region  = "us-central1"
}

resource "google_storage_bucket" "srchi-bucket" {
  name          = "srchicago-2026-raw-bucket"
  location      = "US"
  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = "srchicago_raw_dataset"
  delete_contents_on_destroy = true
}