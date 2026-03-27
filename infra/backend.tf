terraform {
  backend "gcs" {
    bucket = "jason-hsbc"
    prefix = "terraform/state/gcp-event-driven-etl-beam-yaml"
  }
}
