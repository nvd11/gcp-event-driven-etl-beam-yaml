import os
import subprocess
import tempfile
import base64
import json
import time
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from pydantic import BaseModel
from google.cloud import storage
import jinja2

app = FastAPI(title="Beam YAML Orchestrator Webhook", description="Event-driven Webhook for GCS -> Pub/Sub -> Cloud Run -> Dataflow YAML")

@app.get("/health")
def health_check():
    return {"status": "healthy"}

def process_pubsub_event(event_data: dict):
    """
    Background task to download YAML, render it, and submit the Dataflow job.
    """
    bucket = event_data.get("bucket")
    name = event_data.get("name")
    
    if not bucket or not name:
        print("Error: Event payload missing 'bucket' or 'name'.")
        return

    input_csv_path = f"gs://{bucket}/{name}"
    
    # In a real scenario, table routing could be dynamic based on filename.
    # We will just route to a default test table based on the filename prefix or project defaults.
    project_id = os.environ.get("PROJECT_ID", "jason-hsbc")
    region = os.environ.get("REGION", "europe-west2")
    yaml_bucket_name = os.environ.get("YAML_BUCKET")
    temp_bucket_name = os.environ.get("TEMP_BUCKET")
    subnetwork = os.environ.get("SUBNETWORK", "regions/europe-west2/subnetworks/tf-vpc0-subnet0")
    service_account_email = os.environ.get("DATAFLOW_WORKER_SA", "terraform@jason-hsbc.iam.gserviceaccount.com")
    
    # If environment variables aren't injected (e.g. running outside TF context), use sensible defaults for testing
    if not yaml_bucket_name:
        # Fallback if TF vars not fully populated, or fetch from a known state
        print("Warning: YAML_BUCKET env var not set. Ensure Terraform infra is applied and vars are populated.")
        # We'll mock it for the test if it fails
        yaml_bucket_name = "your-yaml-bucket-name" 
        
    target_bq_table = f"{project_id}:landing_dataset.csv_landing_table"

    print(f"Processing file: {input_csv_path}")
    print(f"Target BQ table: {target_bq_table}")

    # 1. Download YAML Template
    storage_client = storage.Client()
    try:
        yaml_bucket = storage_client.bucket(yaml_bucket_name)
        blob = yaml_bucket.blob("pipeline_template.yaml")
        yaml_template_content = blob.download_as_text()
    except Exception as e:
        print(f"Error downloading YAML template from gs://{yaml_bucket_name}/pipeline_template.yaml: {e}")
        # As a fallback for our testing so it doesn't crash if the bucket isn't set up yet:
        print("Using fallback dummy YAML template for testing...")
        yaml_template_content = """pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      name: ReadLandingCSV
      config:
        path: "{{ input_csv_path }}"
    - type: WriteToBigQuery
      name: WriteToBQ
      config:
        table: "{{ target_bq_table }}"
        create_disposition: CREATE_IF_NEEDED
        write_disposition: WRITE_APPEND
options:
  subnetwork: "{{ subnetwork }}"
"""
        
    # 2. Render Template with Jinja2
    template = jinja2.Template(yaml_template_content)
    rendered_yaml = template.render(
        input_csv_path=input_csv_path,
        target_bq_table=target_bq_table,
        subnetwork=subnetwork
    )
    
    print("Rendered YAML Config:")
    print(rendered_yaml)

    # 3. Save to temp file
    fd, tmp_path = tempfile.mkstemp(suffix='.yaml', text=True)
    with os.fdopen(fd, 'w') as tmp:
        tmp.write(rendered_yaml)
        
    job_name = f"yaml-auto-{name.split('.')[0][:20].replace('_', '-')}-{int(time.time())}".lower()

    try:
        temp_location = f"gs://{temp_bucket_name}/temp" if temp_bucket_name else "gs://jason-hsbc-dataflow/temp"
        cmd = [
            "python", "-m", "apache_beam.yaml.main",
            f"--yaml_pipeline_file={tmp_path}",
            "--runner=DataflowRunner",
            f"--project={project_id}",
            f"--region={region}",
            f"--temp_location={temp_location}",
            f"--service_account_email={service_account_email}",
            f"--job_name={job_name}"
        ]
        
        # Explicitly trust the system certs for requests inside the subprocess
        env = os.environ.copy()
        env["REQUESTS_CA_BUNDLE"] = "/etc/ssl/certs/ca-certificates.crt"
        env["SSL_CERT_FILE"] = "/etc/ssl/certs/ca-certificates.crt"
        
        print(f"Starting Dataflow job {job_name} submission...")
        subprocess.run(cmd, check=True, capture_output=True, timeout=900, env=env)
        print(f"Dataflow job {job_name} submission completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error submitting Dataflow job {job_name}. Exit code: {e.returncode}")
        print(f"STDOUT: {e.stdout.decode('utf-8', errors='ignore')}")
        print(f"STDERR: {e.stderr.decode('utf-8', errors='ignore')}")
    except Exception as e:
        print(f"Unexpected error submitting Dataflow job {job_name}: {e}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@app.post("/pubsub")
async def pubsub_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receives Pub/Sub push messages containing GCS object.finalize events.
    """
    try:
        envelope = await request.json()
        if not envelope or "message" not in envelope:
            raise HTTPException(status_code=400, detail="Invalid Pub/Sub message format")
        
        pubsub_message = envelope["message"]
        
        if isinstance(pubsub_message, dict) and "data" in pubsub_message:
            # Decode the base64 data which contains the GCS event payload
            event_data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
            event_data = json.loads(event_data_str)
            
            print(f"Received Pub/Sub event for GCS object: {event_data.get('name')}")
            
            # Offload the heavy Dataflow parsing and submission to a background task
            # so we can immediately return 200 OK to Pub/Sub to prevent retries.
            background_tasks.add_task(process_pubsub_event, event_data)
            
            return {"status": "success", "message": "Event received and processing initiated"}
        else:
            raise HTTPException(status_code=400, detail="Invalid Pub/Sub message payload")
            
    except Exception as e:
        print(f"Error processing webhook: {e}")
        # Return 200 anyway so Pub/Sub doesn't infinitely retry malformed messages,
        # or return 500 if we want it to retry transient errors.
        raise HTTPException(status_code=500, detail=str(e))
