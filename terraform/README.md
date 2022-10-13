# Terraform config
This is a single Terraform module for building GCP resources for the project.

## Components
Here is the component tree:
- GCP
  - GCS bucket: denzoom-project
  - BigQuery dataset: 
  
## Important note on state file
The state file `terraform.tfstate` for this module may actually be stored remotely, e.g., in the GCS bucket for this project. For this project, however, I stored it only locally in my Windows PC and ignored it in this repo. I'll just have to not mess with these files from my Docker host VM running CentOS7. This is to save time from the additional [learning and setup required](https://developer.hashicorp.com/terraform/language/settings/backends/gcs) for remote state files (which would also include proper authentication and credentials storage). I'll do it another day for sure.