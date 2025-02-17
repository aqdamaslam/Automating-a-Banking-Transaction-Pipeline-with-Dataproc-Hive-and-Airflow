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


# Define cluster config
CLUSTER_NAME = '[YOUR-DATAPROC-CLUSTER-NAME]'
PROJECT_ID = '[YOUR-PROJECT-ID]'
REGION = '[CLUSTER-REGION]'

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

hive_query = """
    CREATE DATABASE IF NOT EXISTS credit_card;
    USE credit_card;
    CREATE TABLE credit_card.transactions (
        transaction_id INT,
        card_id STRING,
        card_number STRING,
        card_holder_name STRING,
        card_type STRING,
        card_expiry STRING,
        cvv_code STRING,
        issuer_bank_name STRING,
        card_issuer_id INT,
        transaction_amount DOUBLE,
        transaction_date TIMESTAMP,
        merchant_id STRING,
        transaction_status STRING,
        transaction_type STRING,
        payment_method STRING,
        card_country STRING,
        billing_address STRING,
        shipping_address STRING,
        fraud_flag BOOLEAN,
        fraud_alert_sent BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET;
"""

create_hive_table = DataprocSubmitHiveJobOperator(
    task_id='create_hive_database_table',
    query=hive_query,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
)


submit_pyspark_sql_to_hive_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_SQL_To_Hive_Job_on_dataproc',
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

create_cluster >> create_hive_table >> submit_pyspark_sql_to_hive_job >> submit_pyspark_reports_generation_job >> delete_cluster
