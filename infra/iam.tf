# --- 1. Service Accounts ---

# pubsub-invoker-sa-poc
resource "google_service_account" "pubsub_invoker" {
  account_id   = "pubsub-invoker-sa-poc"
  display_name = "PubSub Push Invoker SA (PoC)"
}

# cloudrun-orchestrator-sa-poc
resource "google_service_account" "cloudrun_orchestrator" {
  account_id   = "cloudrun-orchestrator-sa-poc"
  display_name = "Cloud Run Orchestrator SA (PoC)"
}

# dataflow-worker-sa-poc
resource "google_service_account" "dataflow_worker" {
  account_id   = "dataflow-worker-sa-poc"
  display_name = "Dataflow Worker SA (PoC)"
}

# --- 2. PubSub Invoker Permissions ---
# Grant pubsub invoker permission to invoke Cloud Run
resource "google_cloud_run_service_iam_member" "invoker_run_invoke" {
  project  = google_cloud_run_v2_service.orchestrator.project
  location = google_cloud_run_v2_service.orchestrator.location
  service  = google_cloud_run_v2_service.orchestrator.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}

# --- 3. Cloud Run Orchestrator Permissions ---
# Storage Object Viewer to read YAML
resource "google_project_iam_member" "orchestrator_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.cloudrun_orchestrator.email}"
}

# Dataflow Developer to submit jobs
resource "google_project_iam_member" "orchestrator_dataflow_developer" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${google_service_account.cloudrun_orchestrator.email}"
}

# ActAs Dataflow Worker (allows passing the worker SA to Dataflow runner)
resource "google_service_account_iam_member" "orchestrator_actas_worker" {
  service_account_id = google_service_account.dataflow_worker.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.cloudrun_orchestrator.email}"
}

# --- 4. Dataflow Worker Permissions ---
# Dataflow Worker
resource "google_project_iam_member" "worker_dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Read from GCS (Landing bucket)
resource "google_project_iam_member" "worker_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# Write temp files to GCS
resource "google_project_iam_member" "worker_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# BigQuery Editor
resource "google_project_iam_member" "worker_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

# BigQuery Job User (Needed to create load jobs)
resource "google_project_iam_member" "worker_bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
}
