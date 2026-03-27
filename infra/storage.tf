resource "google_storage_bucket" "landing" {
  name          = "etl-landing-bucket-poc-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "yaml" {
  name          = "etl-yaml-bucket-poc-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "temp" {
  name          = "etl-temp-bucket-poc-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = true
}

# 1. Google Managed Service Account (Storage service agent)
data "google_storage_project_service_account" "gcs_account" {}

# 2. Grant GCS Service Account permission to publish
resource "google_pubsub_topic_iam_member" "gcs_publisher" {
  topic  = google_pubsub_topic.landing_events.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

# 3. Create Notification
resource "google_storage_notification" "landing_notification" {
  bucket         = google_storage_bucket.landing.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.landing_events.id
  event_types    = ["OBJECT_FINALIZE"]
  depends_on     = [google_pubsub_topic_iam_member.gcs_publisher]
}
