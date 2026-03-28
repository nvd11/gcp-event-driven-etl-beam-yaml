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
        # DataflowRunner itself does not block until completion by default unless wait_until_finish is True.
        # But wait, apache_beam.yaml.main does not explicitly pass wait_until_finish.
        # Let's run it synchronously so we get the Job ID output, it should finish in 10-30s once DAG is parsed.
        print(f"Starting Dataflow job {job_name} submission...")
        subprocess.run(cmd, check=True)
        print(f"Dataflow job {job_name} submission completed.")
    except Exception as e:
        print(f"Error submitting Dataflow job {job_name}: {e}")
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
