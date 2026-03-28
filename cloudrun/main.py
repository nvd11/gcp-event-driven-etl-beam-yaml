import os
import tempfile
import apache_beam as beam
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from apache_beam.yaml.main import build_pipeline_components_from_argv

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
    接收一个完整的 YAML 字符串参数，不使用 subprocess 阻塞等待作业完成，
    而是直接通过 Beam SDK 解析并异步提交给 Dataflow。
    """
    fd, tmp_path = tempfile.mkstemp(suffix='.yaml', text=True)
    with os.fdopen(fd, 'w') as tmp:
        tmp.write(req.yaml_config)

    try:
        # 使用最新的 yaml_pipeline_file 参数
        argv = [
            f"--yaml_pipeline_file={tmp_path}",
            "--runner=DataflowRunner",
            f"--project={req.project}",
            f"--region={req.region}",
            f"--temp_location={req.temp_location}",
            f"--job_name={req.job_name}"
        ]
        
        # 提取解析器组件
        options, constructor, display_data = build_pipeline_components_from_argv(argv)
        
        # 构建 Pipeline
        p = beam.Pipeline(options=options, display_data=display_data)
        constructor(p)
        
        # 异步提交！不要使用 'with beam.Pipeline()'，因为 with 会阻塞 Cloud Run 长达数小时，导致 504 Timeout。
        # 调用 p.run() 会返回一个 PipelineResult 对象。
        result = p.run()
        
        job_id = result.job_id() if hasattr(result, 'job_id') else "unknown"

        return {
            "status": "success",
            "message": f"Pipeline '{req.job_name}' submitted successfully.",
            "job_id": job_id
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to submit pipeline: {str(e)}"
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
