locals {
  data_lake_bucket = "dtc_data_lake" 
}

variable "project" {
  description = "Your GCP Project ID"
  default = "warm-ring-374916"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west1"
  type = string
}

# not needed for now
variable "buckt_name" {
  description = "The name of the Google Cloud Storage bucket. Must be globally unique."
  default = "prefect-dtc-dez"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "dtc_dez_ny_taxi"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "rides_yellow"
}