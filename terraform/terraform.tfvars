### Choose which path to uncomment
credentials_file = "..\\..\\.google\\credentials\\terraform-denzoom-504ff36e2018.json"
# credentials_file = "../../.google/credentials/terraform-denzoom-504ff36e2018.json"

project = "city-crimes"
gcp = {
  project = "city-crimes"
  region  = "us-central1"
  zone    = "us-central1-b"
}
dataset = {
  name         = "us_city_crimes"
  desc_prep    = "datasets from crime reports by a (few) city government(s) in the U.S. (whatever was available) over the last decade"
  desc_sandbox = "processed datasets from crime reports in prep_us_city_crimes"
  desc_prod    = "final output datasets from crime reports in sandbox_us_city_crimes"
}