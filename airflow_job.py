import subprocess
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitHiveJobOperator
)
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 18),
}

dag = DAG(
    'spark_job_on_dataproc',
    default_args=default_args,
    description='A DAG to setup dataproc and run Spark job on that',
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
    tags=['dev'],
)


# Define cluster config
CLUSTER_NAME = '[CLUSTER-NAME]'
PROJECT_ID = '[YOUR-GOOGLE-PROJECT-ID]'
REGION = 'asia-east1'

CLUSTER_CONFIG = {
    'gce_cluster_config': {
        'internal_ip_only': False  # Ensures internal IP only
    },
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 32
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 32
        }
    },
    'software_config': {
        'image_version': '2.2.26-debian12'  # Image version
    }
}

create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)


submit_pyspark_hive_db_table_job  = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_hive_db_table_job_on_dataproc',
    main='gs://c[YOUR-GCP-BUCKET-PATH]/hive_db_table.py',
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    # dataproc_pyspark_properties=spark_job_resources_parm,
    dag=dag,
)


submit_pyspark_sql_to_hive_job  = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_sql_to_hive_job_on_dataproc',
    main='gs://[YOUR-GCP-BUCKET-PATH]/SQL_To_Hive.py',
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    # dataproc_pyspark_properties=spark_job_resources_parm,
    dag=dag,
)

submit_pyspark_reports_generation_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_reports_job_on_dataproc',
    main='gs://[YOUR-GCP-BUCKET-PATH]/reports.py',
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    # dataproc_pyspark_properties=spark_job_resources_parm,
    dag=dag,
)
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster_after',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    dag=dag,
)

create_cluster >> submit_pyspark_hive_db_table_job >> submit_pyspark_sql_to_hive_job >> submit_pyspark_reports_generation_job >> delete_cluster
