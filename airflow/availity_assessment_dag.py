from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    # EmrServerlessDeleteApplicationOperator,
)

aws_account_id = "117819748843"
region = "us-east-1"
JOB_ROLE_ARN = f"arn:aws:iam::${aws_account_id}:role//lake-freeze-lambda-role"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": f"s3://emr-zone-${aws_account_id}-${region}/logging/"
        }
    },
}

with DAG(
    dag_id="availity_assessment",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_task",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={"name": "availity_assessment"},
    )

    start_job = EmrServerlessStartJobOperator(
        task_id="start_emr_serverless_job",
        application_id=create_app.output,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://deployment-zone-${aws_account_id}/availity_assessment/provider.jar",
                "sparkSubmitParameters": """
                    --driver-memory 4G 
                    --driver-cores 1 
                    --num-executors 1
                    --executor-memory 6G
                    --executor-cores 1
                    --conf spark.dynamicAllocation.enabled=false
                """ 
            }
        },
        enable_application_ui_links=True,
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    create_app >> start_job