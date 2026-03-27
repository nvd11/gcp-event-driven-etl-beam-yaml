variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "jason-hsbc"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west2"
}

variable "subnetwork" {
  description = "Subnetwork to deploy Dataflow"
  type        = string
  default     = "regions/europe-west2/subnetworks/subnet-west2"
}
