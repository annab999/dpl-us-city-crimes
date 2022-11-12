from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import pendulum as pdl
import os

proj = os.getenv('GCP_PROJECT_ID')
# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
gs_bkt = os.getenv('GCP_GCS_BUCKET')  # UPDATE ME IN PROD
earliest_yr = 2001      # Chicago 2001 data, better to be automated
dataset = os.getenv('INIT_DATASET')
fmt = {'in': '.csv', 'out': '.parquet'}
def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "proj_process_data_dag",
    schedule = '@yearly',
    start_date = pdl.datetime(earliest_yr, 1, 1),
    # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = f'{a_home}/include',
    max_active_runs = 3,
    user_defined_macros = {
        'jar_path': os.getenv('JAR_FILE_LOC'),
        'include_dir': f'{a_home}/include'
    },
    tags = ['project', 'TEST']
) as dag:

    # cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
    cities = ['Chicago', 'Los Angeles', 'Austin']
    f_cities = [city.replace(' ', '_').lower() for city in cities]

    with TaskGroup(group_id = 'bq_tg') as tg1:
        for city in f_cities:

            year = '{{ dag_run.logical_date.strftime("%Y") }}'
            gcs_object_exists = GCSObjectExistenceSensor(
                task_id = f'gcs_object_exists_{city}',
                bucket = gs_bkt.replace('gs://', ''),  # UPDATE ME IN PROD
                object = f'clean/{city}/{year}/01/_SUCCESS',
                timeout = 5,
                soft_fail = True,
                retries = 0
            )

            prefix = f'{gs_bkt}/clean/{city}/' + '{{ dag_run.logical_date.strftime("%Y") }}'
            ext_tbl = BigQueryCreateExternalTableOperator(
                task_id = f'ext_tbl_{city}',
                bucket = gs_bkt.replace('gs://', ''),  # UPDATE ME IN PROD
                source_objects = f'{prefix}/*{fmt["out"]}',
                destination_project_dataset_table = f'{dataset}.{city}_{{{{ dag_run.logical_date.strftime("%Y") }}}}_ext',
                source_format = fmt['out'].strip('.').upper(),
                autodetect = True
            )
            
            create_part_query = (
                f"CREATE OR REPLACE TABLE {proj}.{dataset}.{city}_{{{{ dag_run.logical_date.strftime('%Y') }}}}_part"
                f"PARTITION BY DATETIME_TRUNC(timestamp, MONTH)"
                f"AS"
                f"SELECT * FROM {proj}.{dataset}.{city}_{{{{ dag_run.logical_date.strftime('%Y') }}}}_ext;"
            )

            part_tbl = BigQueryInsertJobOperator(
                task_id = f'part_tbl_{city}',
                configuration = {
                    "query": {
                        "query": create_part_query,
                        "useLegacySql": False
                    }
                }
            )

            gcs_object_exists >> ext_tbl >> part_tbl

    for city in f_cities:
        process_data = DbtCloudRunJobOperator(
            task_id = f'process_data_{city}',
            dbt_cloud_conn_id = 'dbt_cloud_default',
            job_id = 0,
            trigger_reason = 'triggered by Airflow task run'
        )

        # task dependencies
        tg1 >> process_data
