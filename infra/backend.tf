terraform {
  backend "gcs" {
    bucket = "jason-hsbc"
    prefix = "terraform/state/poc/gcp-event-driven-etl-beam-yaml"
  }
}
