import os
import subprocess
import tempfile
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Beam YAML Orchestrator", description="Service to trigger Dataflow pipelines from Top-Level YAML.")

class TriggerRequest(BaseModel):
    yaml_config: str
    project: str
    region: str
    temp_location: str
    job_name: str = "yaml-dynamic-job"

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/trigger")
def trigger_pipeline(req: TriggerRequest):
    # Use subprocess but with Popen to NOT block the return
    fd, tmp_path = tempfile.mkstemp(suffix='.yaml', text=True)
    with os.fdopen(fd, 'w') as tmp:
        tmp.write(req.yaml_config)

    try:
        cmd = [
            "python", "-m", "apache_beam.yaml.main",
            f"--yaml_pipeline_file={tmp_path}",
            "--runner=DataflowRunner",
            f"--project={req.project}",
            f"--region={req.region}",
            f"--temp_location={req.temp_location}",
            f"--job_name={req.job_name}"
        ]
        
        # Fire and forget
        # We don't wait for stdout, the job submission will happen in the background
        # Cloud Run needs to know we intend it to outlive the response if it takes a while,
        # but Dataflow submission only takes ~15 seconds. If Cloud Run freezes the CPU,
        # it will thaw on the next request. For a proper async fire-and-forget in FastAPI:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        return {
            "status": "success",
            "message": f"Pipeline submission initiated in background."
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # We deliberately leak the tmp_path here because the background process needs it.
    # A background cron can clean up /tmp.
