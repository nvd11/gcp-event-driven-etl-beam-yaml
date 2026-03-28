import os
import subprocess
import tempfile
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Beam YAML Orchestrator", description="Service to trigger Dataflow pipelines from Top-Level YAML.")

class TriggerRequest(BaseModel):
    yaml_config: str      # 包含整个 Top-Level YAML 的字符串
    project: str          # GCP Project ID
    region: str           # e.g., us-central1
    temp_location: str    # e.g., gs://my-bucket/temp
    job_name: str = "yaml-dynamic-job" # Dataflow 作业名称

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/trigger")
def trigger_pipeline(req: TriggerRequest):
    """
    接收一个完整的 YAML 字符串参数，写入临时文件并调用 Beam CLI 提交至 Dataflow。
    """
    # 动态将接收到的 yaml 内容写入临时文件
    fd, tmp_path = tempfile.mkstemp(suffix='.yaml', text=True)
    with os.fdopen(fd, 'w') as tmp:
        tmp.write(req.yaml_config)

    try:
        # 组装命令：我们采用 Option B 方案，直接调用 apache_beam.yaml.main
        cmd = [
            "python", "-m", "apache_beam.yaml.main",
            f"--yaml_pipeline_file={tmp_path}",
            "--runner=DataflowRunner",
            f"--project={req.project}",
            f"--region={req.region}",
            f"--temp_location={req.temp_location}",
            f"--job_name={req.job_name}"
        ]
        
        # 提交作业。DataflowRunner 会阻塞直到作业在 GCP 排队成功（即进入 Pending/Running 状态），
        # 而不是等作业执行完，因此适合作为 Cloud Run 的同步响应。
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            # 提交失败，返回 500
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to submit pipeline: {result.stderr}"
            )
            
        return {
            "status": "success",
            "message": f"Pipeline '{req.job_name}' submitted successfully.",
            "stdout": result.stdout
        }
        
    finally:
        # 清理临时文件，防止把 Cloud Run 内存塞满
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
