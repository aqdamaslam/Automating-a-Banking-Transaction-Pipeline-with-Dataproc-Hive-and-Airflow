from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 17),  # Change Date as per your requirements
}

dag = DAG(
    'adhoc_spark_job_on_dataproc',
    default_args=default_args,
    description='A DAG to setup dataproc and run Spark job on that',
    schedule_interval='0 0 30 * *',  # Runs at midnight on the 30th of every month
    catchup=False,
    tags=['dev'],
)

# Define cluster configuration as per your project requirement. I am setting configuration as per free tier account for learning purpose
CLUSTER_NAME = '[YOUR-GCP-CLUSTER-NAME]'
PROJECT_ID = '[YOUR-GCP-PROJECT-ID]'
REGION = '[REGION-NAME]'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
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

submit_pyspark_sql_to_hivejob = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_SQL_To_Hive_Job_on_dataproc',
    main='gs://[YOUR_GCP_BUCKET-PATH]/[YOUR-SQL-TO-HIVE-PYTHON-FILE]',
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    # dataproc_pyspark_properties=spark_job_resources_parm,
    dag=dag,
)

submit_pyspark_reports_generation_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_reports_job_on_dataproc',
    main='gs://[YOUR_GCP_BUCKET-PATH]/[YOUR-REPORT-GENERATION-PYTHON-FILE]',
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

create_cluster >> submit_pyspark_sql_to_hivejob >> submit_pyspark_reports_generation_job >> delete_cluster
