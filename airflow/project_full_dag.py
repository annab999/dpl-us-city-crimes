import pendulum as pdl

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.apache.spark import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.dbt.cloud import DbtCloudRunJobOperator

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id = "project_full_dag",
    schedule = '@yearly',
    start_date = pdl.datetime(2001, 1, 1, tz="Asia/Manila"),
    # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
    default_args = def_args,
    max_active_runs = 3,
    tags = ['project']
) as dag:

    upload_data = BashOperator()

    prepare_data = SparkSubmitOperator()

    set_up_bigquery = BigQueryCreateExternalTableOperator()

    process_data = DbtCloudRunJobOperator()

    # task dependencies
    upload_data >> prepare_data >> set_up_bigquery >> process_data