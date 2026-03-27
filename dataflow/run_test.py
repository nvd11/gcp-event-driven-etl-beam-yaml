import os
import time
import jinja2
from apache_beam.yaml import main as yaml_main

def run():
    # Parameters
    project_id = "jason-hsbc"
    region = "europe-west2"
    
    landing_bucket = "etl-landing-bucket-poc-e75a1499"
    temp_bucket = "etl-temp-bucket-poc-e75a1499"
    csv_path = f"gs://{landing_bucket}/test.csv"
    target_table = "jason-hsbc:etl_poc_dataset.target_users_poc"
    subnet = "regions/europe-west2/subnetworks/subnet-west2"
    service_account = "dataflow-worker-sa-poc@jason-hsbc.iam.gserviceaccount.com"
    
    # Read the YAML template
    with open("dataflow/pipeline_template.yaml", "r") as f:
        template_str = f.read()
        
    # Render variables
    template = jinja2.Template(template_str)
    rendered_yaml = template.render(
        input_csv_path=csv_path,
        target_bq_table=target_table
    )
    
    print("Rendered top-level YAML:")
    print(rendered_yaml)
    
    # Save the rendered YAML to a temporary file because yaml_main expects a file path
    temp_yaml_path = "/tmp/rendered_pipeline.yaml"
    with open(temp_yaml_path, "w") as f:
        f.write(rendered_yaml)

    job_name = f"native-beam-yaml-test-{int(time.time())}"
    
    # Prepare argv for the native top-level YAML parser
    argv = [
        "--yaml_pipeline_file=" + temp_yaml_path,
        "--runner=DataflowRunner",
        "--project=" + project_id,
        "--region=" + region,
        "--temp_location=gs://" + temp_bucket + "/temp",
        "--service_account_email=" + service_account,
        "--subnetwork=" + subnet,
        "--job_name=" + job_name
    ]
    
    print(f"Submitting job natively: {job_name}")
    print("Argv:", argv)
    
    # Execute the pipeline using the native Beam YAML entrypoint
    yaml_main.run(argv)
    print(f"Successfully submitted job {job_name}!")

if __name__ == "__main__":
    run()
