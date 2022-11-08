resource "google_bigquery_dataset" "prep" {
  dataset_id = "prep_${var.dataset["name"]}"

  description = var.dataset["desc_prep"]
  project     = var.gcp["project"]
  location    = var.gcp["region"]
  # delete_contents_on_destroy = true

  access {
    role          = "OWNER"
    user_by_email = "terraform-city-crimes@city-crimes.iam.gserviceaccount.com"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  # access {
  #     role          = "OWNER"
  #     user_by_email = "airflow-city-crimes@city-crimes.iam.gserviceaccount.com"
  # }
  access {
    role          = "WRITER"
    user_by_email = "dbt-city-crimes@city-crimes.iam.gserviceaccount.com"
  }
}

resource "google_bigquery_dataset" "sandbox" {
  dataset_id = "sandbox_${var.dataset["name"]}"

  description = var.dataset["desc_sandbox"]
  project     = var.gcp["project"]
  location    = var.gcp["region"]
  # delete_contents_on_destroy = true

  access {
    role          = "OWNER"
    user_by_email = "terraform-city-crimes@city-crimes.iam.gserviceaccount.com"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  # access {
  #     role          = "OWNER"
  #     user_by_email = "airflow-city-crimes@city-crimes.iam.gserviceaccount.com"
  # }
  access {
    role          = "OWNER"
    user_by_email = "dbt-city-crimes@city-crimes.iam.gserviceaccount.com"
  }
}

resource "google_bigquery_dataset" "prod" {
  dataset_id = "prod_${var.dataset["name"]}"

  description = var.dataset["desc_prod"]
  project     = var.gcp["project"]
  location    = var.gcp["region"]
  # delete_contents_on_destroy = true

  access {
    role          = "OWNER"
    user_by_email = "terraform-city-crimes@city-crimes.iam.gserviceaccount.com"
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  # access {
  #     role          = "OWNER"
  #     user_by_email = "airflow-city-crimes@city-crimes.iam.gserviceaccount.com"
  # }
  access {
    role          = "OWNER"
    user_by_email = "dbt-city-crimes@city-crimes.iam.gserviceaccount.com"
  }
}