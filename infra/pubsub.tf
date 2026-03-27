resource "google_pubsub_topic" "landing_events" {
  name = "landing-bucket-events-topic-poc"
}

resource "google_pubsub_subscription" "cloudrun_push" {
  name  = "cloudrun-push-sub-poc"
  topic = google_pubsub_topic.landing_events.name

  # Important: Cloud Run needs time to parse YAML and submit to Dataflow.
  # Submitting a job can take 10-30 seconds.
  ack_deadline_seconds = 60

  push_config {
    push_endpoint = "${google_cloud_run_v2_service.orchestrator.uri}/pubsub"
    oidc_token {
      service_account_email = google_service_account.pubsub_invoker.email
    }
  }
}
