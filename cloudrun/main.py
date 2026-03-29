import os
import subprocess
import tempfile
import base64
import json
import time
from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from google.cloud import storage
import jinja2

app = FastAPI(title="Beam YAML Orchestrator Webhook", description="Event-driven Webhook for GCS -> Pub/Sub -> Cloud Run -> Dataflow YAML")

@app.get("/health")
def health_check():
    return {"status": "healthy"}

class Config:
    """Encapsulates environment variables and global configurations."""
    def __init__(self):
        self.project_id = os.environ.get("PROJECT_ID", "jason-hsbc")
        self.region = os.environ.get("REGION", "europe-west2")
        self.yaml_bucket = os.environ.get("YAML_BUCKET")
        self.temp_bucket = os.environ.get("TEMP_BUCKET")
        self.subnetwork = os.environ.get("SUBNETWORK", "regions/europe-west2/subnetworks/tf-vpc0-subnet0")
        self.service_account = os.environ.get("DATAFLOW_WORKER_SA", "terraform@jason-hsbc.iam.gserviceaccount.com")
        self.target_bq_table = f"{self.project_id}:etl_poc_dataset.target_users_poc"

        if not self.yaml_bucket:
            print("Warning: YAML_BUCKET environment variable is missing. Will fail if not provided.")

def fetch_yaml_template(bucket_name: str, file_name: str = "pipeline_template.yaml") -> str:
    """Downloads the raw YAML template from Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return blob.download_as_text()
    except Exception as e:
        print(f"Failed to fetch {file_name} from gs://{bucket_name}: {e}")
        raise RuntimeError(f"YAML Template fetch failed: {e}")

def render_yaml_template(template_str: str, context: dict) -> str:
    """Renders the Jinja2 YAML template with the dynamic context."""
    template = jinja2.Template(template_str)
    rendered = template.render(**context)
    print("Rendered YAML Config:\n", rendered)
    return rendered

def submit_dataflow_job(yaml_path: str, job_name: str, config: Config):
    """Submits the Apache Beam YAML pipeline to Dataflow Runner via subprocess."""
    temp_location = f"gs://{config.temp_bucket}/temp" if config.temp_bucket else f"gs://{config.project_id}-dataflow/temp"
    
    cmd = [
        "python", "-m", "apache_beam.yaml.main",
        f"--yaml_pipeline_file={yaml_path}",
        "--runner=DataflowRunner",
        f"--project={config.project_id}",
        f"--region={config.region}",
        f"--temp_location={temp_location}",
        f"--service_account_email={config.service_account}",
        f"--job_name={job_name}"
    ]
    
    # Explicitly trust system CA certs to bypass Beam metadata server verification issues in Python slim images
    env = os.environ.copy()
    env["REQUESTS_CA_BUNDLE"] = "/etc/ssl/certs/ca-certificates.crt"
    env["SSL_CERT_FILE"] = "/etc/ssl/certs/ca-certificates.crt"
    
    print(f"Starting Dataflow job submission. Job Name: {job_name}")
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=900, env=env)
        print(f"Successfully submitted Dataflow job: {job_name}")
    except subprocess.CalledProcessError as e:
        print(f"Dataflow submission failed (Exit code: {e.returncode})")
        print(f"STDOUT: {e.stdout.decode('utf-8', errors='ignore')}")
        print(f"STDERR: {e.stderr.decode('utf-8', errors='ignore')}")
        raise

def process_pubsub_event(event_data: dict):
    """
    Core orchestrator task: Extracts event, renders YAML, and deploys to Dataflow.
    """
    bucket = event_data.get("bucket")
    name = event_data.get("name")
    
    if not bucket or not name:
        print("Error: Invalid event payload. Missing 'bucket' or 'name'.")
        return

    input_csv_path = f"gs://{bucket}/{name}"
    job_name = f"yaml-auto-{name.split('.')[0][:20].replace('_', '-')}-{int(time.time())}".lower()
    
    try:
        config = Config()
        print(f"Processing file: {input_csv_path} -> Target Table: {config.target_bq_table}")

        # 1. Fetch Template
        yaml_template_content = fetch_yaml_template(config.yaml_bucket)
        
        # 2. Render Template
        rendered_yaml = render_yaml_template(
            yaml_template_content,
            context={
                "input_csv_path": input_csv_path,
                "target_bq_table": config.target_bq_table,
                "subnetwork": config.subnetwork
            }
        )

        # 3. Save to Temp File & Submit
        fd, tmp_path = tempfile.mkstemp(suffix='.yaml', text=True)
        try:
            with os.fdopen(fd, 'w') as tmp:
                tmp.write(rendered_yaml)
            
            submit_dataflow_job(tmp_path, job_name, config)
            
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    except Exception as e:
        print(f"Failed to process event for {input_csv_path}: {e}")

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
            event_data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
            event_data = json.loads(event_data_str)
            
            print(f"Received Pub/Sub event for GCS object: {event_data.get('name')}")
            
            # Offload processing to background task to quickly ACK Pub/Sub
            background_tasks.add_task(process_pubsub_event, event_data)
            
            return {"status": "success", "message": "Event received and processing initiated"}
        else:
            raise HTTPException(status_code=400, detail="Invalid Pub/Sub message payload")
            
    except Exception as e:
        print(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))
