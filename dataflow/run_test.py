import os
import time
import jinja2
import yaml
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.yaml import yaml_transform

def run():
    project_id = "jason-hsbc"
    region = "europe-west2"
    
    landing_bucket = "etl-landing-bucket-poc-e75a1499"
    temp_bucket = "etl-temp-bucket-poc-e75a1499"
    csv_path = f"gs://{landing_bucket}/test.csv"
    target_table = "jason-hsbc:etl_poc_dataset.target_users_poc"
    subnet = "regions/europe-west2/subnetworks/subnet-west2"
    service_account = "dataflow-worker-sa-poc@jason-hsbc.iam.gserviceaccount.com"
    
    with open("dataflow/pipeline_template.yaml", "r") as f:
        template_str = f.read()
        
    template = jinja2.Template(template_str)
    rendered_yaml = template.render(
        input_csv_path=csv_path,
        target_bq_table=target_table
    )
    
    # Parse the YAML and extract the 'pipeline' block if it exists
    parsed_yaml = yaml.safe_load(rendered_yaml)
    if 'pipeline' in parsed_yaml:
        rendered_yaml = yaml.dump(parsed_yaml['pipeline'])
    
    print("Rendered YAML sent to YamlTransform:")
    print(rendered_yaml)
    
    job_name = f"manual-beam-yaml-test-{int(time.time())}"
    
    options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region=region,
        temp_location=f'gs://{temp_bucket}/temp',
        service_account_email=service_account,
        subnetwork=subnet,
        job_name=job_name
    )
    
    print(f"Submitting job: {job_name}")
    with beam.Pipeline(options=options) as p:
        p | "RunYaml" >> yaml_transform.YamlTransform(rendered_yaml)
        
    print(f"Successfully submitted job {job_name}!")

if __name__ == "__main__":
    run()
