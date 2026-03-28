import os
import subprocess
import tempfile
from fastapi import FastAPI, BackgroundTasks, HTTPException
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

def run_beam_sync(yaml_config: str, project: str, region: str, temp_location: str, job_name: str):
    fd, tmp_path = tempfile.mkstemp(suffix='.yaml', text=True)
    with os.fdopen(fd, 'w') as tmp:
        tmp.write(yaml_config)
    try:
        cmd = [
            "python", "-m", "apache_beam.yaml.main",
            f"--yaml_pipeline_file={tmp_path}",
            "--runner=DataflowRunner",
            f"--project={project}",
            f"--region={region}",
            f"--temp_location={temp_location}",
            f"--job_name={job_name}"
        ]
        print(f"Starting Dataflow job {job_name} submission...")
        # Add timeout to ensure it doesn't hang indefinitely (15 minutes max)
        # Use capture_output to avoid filling stdout/stderr buffers leading to hangs
        subprocess.run(cmd, check=True, capture_output=True, timeout=900)
        print(f"Dataflow job {job_name} submission completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error submitting Dataflow job {job_name}. Exit code: {e.returncode}")
        print(f"STDOUT: {e.stdout.decode('utf-8', errors='ignore')}")
        print(f"STDERR: {e.stderr.decode('utf-8', errors='ignore')}")
    except subprocess.TimeoutExpired as e:
        print(f"Timeout submitting Dataflow job {job_name} after {e.timeout} seconds.")
    except Exception as e:
        print(f"Unexpected error submitting Dataflow job {job_name}: {e}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

@app.post("/trigger")
async def trigger_pipeline(req: TriggerRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(
        run_beam_sync, 
        req.yaml_config, 
        req.project, 
        req.region, 
        req.temp_location, 
        req.job_name
    )
    return {
        "status": "success",
        "message": f"Pipeline submission for {req.job_name} initiated in background."
    }
