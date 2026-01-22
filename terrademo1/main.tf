terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = 
  project = "robotic-facet-485016-m1"
  region  = "us-central1"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "robotic-facet-485016-m1-data-lake-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "robotic_facet_485016_m1_bigquery_dataset"
  project    = "robotic-facet-485016-m1"
  location   = "US"
}