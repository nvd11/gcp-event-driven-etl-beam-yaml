resource "google_cloud_run_v2_service" "orchestrator" {
  name     = "etl-orchestrator-svc-poc"
  location = var.region
  project  = var.project_id

  template {
    service_account = google_service_account.cloudrun_orchestrator.email
    containers {
      # Use a dummy public image initially so Terraform can create the service.
      # Cloud Build will overwrite this with the real image.
      image = "us-docker.pkg.dev/cloudrun/container/hello"

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "REGION"
        value = var.region
      }
      env {
        name  = "DATAFLOW_WORKER_SA"
        value = google_service_account.dataflow_worker.email
      }
      env {
        name  = "YAML_BUCKET"
        value = google_storage_bucket.yaml.name
      }
      env {
        name  = "TEMP_BUCKET"
        value = google_storage_bucket.temp.name
      }
      env {
        name  = "SUBNETWORK"
        value = var.subnetwork
      }
    }
  }

  lifecycle {
    # Ignore changes to the image because CI/CD will update it
    ignore_changes = [
      template[0].containers[0].image
    ]
  }
}
