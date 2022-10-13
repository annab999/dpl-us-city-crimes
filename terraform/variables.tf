variable "credentials_file" {
  description = "path to GCP credentials file for Terraform service account"
  type        = string
}

variable "project" {
  description = "purpose/project of this deployment"
  type        = string
}

variable "gcp" {
  description = "GCP setup details"
  type        = map(string)
}

variable "dataset" {
  description = "topic of dataset"
  type        = map(string)
}