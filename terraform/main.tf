terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.40.0"
    }
  }
}

provider "google" {
  project     = var.gcp["project"]
  region      = var.gcp["region"]
  zone        = var.gcp["zone"]
  credentials = var.credentials_file
}

resource "google_storage_bucket" "data-lake" {
  name     = "${var.gcp["project"]}-${var.project}"
  location = var.gcp["region"]

  project                     = var.gcp["project"]
  uniform_bucket_level_access = true
  versioning {
    enabled = false
  }
  # force_destroy = true
}