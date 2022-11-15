from airflow import DAG

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import pendulum as pdl
import os

# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "proj_process_once_dag",
    schedule = '@once',
    start_date = pdl.datetime(2022, 11, 1),
    # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = f'{a_home}/include',
    tags = ['project', 'TEST']
) as dag:

    process_data = DbtCloudRunJobOperator(
        task_id = 'process_data',
        dbt_cloud_conn_id = 'dbt_cloud_default',
        job_id = int(os.getenv('PROCESS_DATA_JOB')),
        trigger_reason = 'triggered by Airflow task run',
        trigger_rule = 'none_failed_min_one_success'
    )

    process_data
