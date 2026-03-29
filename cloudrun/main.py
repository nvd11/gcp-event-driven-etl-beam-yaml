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
    
    project_id = os.environ.get("PROJECT_ID", "jason-hsbc")
    region = os.environ.get("REGION", "europe-west2")
    yaml_bucket_name = os.environ.get("YAML_BUCKET")
    temp_bucket_name = os.environ.get("TEMP_BUCKET")
    subnetwork = os.environ.get("SUBNETWORK", "regions/europe-west2/subnetworks/tf-vpc0-subnet0")
    service_account_email = os.environ.get("DATAFLOW_WORKER_SA", "terraform@jason-hsbc.iam.gserviceaccount.com")
    
    if not yaml_bucket_name:
        yaml_bucket_name = "your-yaml-bucket-name" 
        
    target_bq_table = f"{project_id}:etl_poc_dataset.target_users_poc"

    print(f"Processing file: {input_csv_path}")
    print(f"Target BQ table: {target_bq_table}")

    storage_client = storage.Client()
    try:
        yaml_bucket = storage_client.bucket(yaml_bucket_name)
        blob = yaml_bucket.blob("pipeline_template.yaml")
        yaml_template_content = blob.download_as_text()
    except Exception as e:
        print(f"Error downloading YAML template from gs://{yaml_bucket_name}/pipeline_template.yaml: {e}")
        yaml_template_content = """pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config:
        path: "{{ input_csv_path }}"
    - type: WriteToBigQuery
      config:
        table: "{{ target_bq_table }}"
        create_disposition: CREATE_IF_NEEDED
        write_disposition: WRITE_APPEND
options:
  subnetwork: "{{ subnetwork }}"
"""
        
    template = jinja2.Template(yaml_template_content)
    rendered_yaml = template.render(
        input_csv_path=input_csv_path,
        target_bq_table=target_bq_table,
        subnetwork=subnetwork
    )
    
    print("Rendered YAML Config:")
    print(rendered_yaml)

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
    try:
        envelope = await request.json()
        if not envelope or "message" not in envelope:
            raise HTTPException(status_code=400, detail="Invalid Pub/Sub message format")
        pubsub_message = envelope["message"]
        if isinstance(pubsub_message, dict) and "data" in pubsub_message:
            event_data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
            event_data = json.loads(event_data_str)
            print(f"Received Pub/Sub event for GCS object: {event_data.get('name')}")
            background_tasks.add_task(process_pubsub_event, event_data)
            return {"status": "success", "message": "Event received and processing initiated"}
        else:
            raise HTTPException(status_code=400, detail="Invalid Pub/Sub message payload")
    except Exception as e:
        print(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))
