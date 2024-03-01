from datetime import datetime
import os

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

aws_account_id = "117819748843"
region = "us-east-1"
os.environ['AWS_DEFAULT_REGION'] = region
JOB_ROLE_ARN = f"arn:aws:iam::{aws_account_id}:role/lake-freeze-lambda-role"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": f"s3://emr-zone-{aws_account_id}-{region}/logging/"
        }
    },
}

with DAG(
    dag_id="availity_assessment",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    
    # create_app = EmrServerlessCreateApplicationOperator(
    #     task_id="create_emr_serverless_task",
    #     release_label="emr-6.6.0",
    #     job_type="SPARK",
    #     # enable_application_ui_links=True,
    #     config={"name": "availity_assessment"},
    # )

    # start_job = EmrServerlessStartJobOperator(
    #     task_id="start_emr_serverless_job",
    #     application_id=create_app.output,
    #     execution_role_arn=JOB_ROLE_ARN,
    #     job_driver={
    #         "sparkSubmit": {
    #             "entryPoint": f"s3://deployment-zone-{aws_account_id}/availity_assessment/provider.jar",
    #             # "sparkSubmitParameters": """--driver-memory 4G 
    #             #     --driver-cores 1 
    #             #     --num-executors 1
    #             #     --executor-memory 6G
    #             #     --executor-cores 1
    #             #     --conf spark.dynamicAllocation.enabled=false
    #             # """ 
    #         }
    #     },
    #     configuration_overrides=DEFAULT_MONITORING_CONFIG,
    # )

    # delete_app = EmrServerlessDeleteApplicationOperator(
    #     task_id="delete_app",
    #     application_id=create_app.output,
    #     trigger_rule="all_done",
    # )

    

    redshift_schema, redshift_table_name = "public", "provider_visits_count_monthly"

    # create_table = RedshiftDataOperator(
    #     task_id='create_table',
    #     database='dev',
    #     workgroup_name="dbt-testgen",
    #     # redshift_conn_id='redshift',
    #     sql=f"""
    #         CREATE TABLE IF NOT EXISTS {redshift_schema}.{redshift_table_name} (
    #             provider_id INTEGER,
    #             visit_month INTEGER,
    #             visit_count INTEGER
    #         );
    #     """,
    # )

    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        schema='public',
        table=redshift_table_name,
        s3_bucket=f'data-zone-{aws_account_id}-{region}',
        s3_key=f'processed/{redshift_table_name}/',
        redshift_conn_id='redshift',
        # aws_conn_id='aws_default',
        copy_options=[
            "FORMAT JSON 'auto'"
        ],
        method='REPLACE'
    )

    # create_app >> start_job >> delete_app >> 
    # create_table >> 
    s3_to_redshift