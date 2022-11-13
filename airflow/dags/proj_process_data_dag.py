from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator, BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import pendulum as pdl
import os

proj = os.getenv('GCP_PROJECT_ID')
# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
gs_bkt = os.getenv('GCP_GCS_BUCKET')  # UPDATE ME IN PROD
earliest_yr = 2001      # Chicago 2001 data, better to be automated
dset = os.getenv('INIT_DATASET')
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
    year = '{{ dag_run.logical_date.strftime("%Y") }}'

    with TaskGroup(group_id = 'gcp_tg') as tg1:
        for city in f_cities:
            in_prefix = f'{os.getenv("PREFIX_CLEAN")}/{city}/{year}'

            prefix_exists = GCSObjectsWithPrefixExistenceSensor(
                task_id = f'prefix_exists_{city}',
                bucket = gs_bkt.replace('gs://', ''),  # UPDATE ME IN PROD
                prefix = f'{in_prefix}',
                timeout = 5,
                soft_fail = True,
                retries = 0
            )

            del_ext_tbl = BigQueryDeleteTableOperator(
                task_id = f'del_ext_tbl_{city}',
                deletion_dataset_table = f'{proj}.{dset}.{city}_{year}_ext',
                ignore_if_missing = True,
                location = os.getenv('GCP_LOC')
            )

            ext_tbl = BigQueryCreateExternalTableOperator(
                task_id = f'ext_tbl_{city}',
                table_resource = {
                    "tableReference": {
                        "projectId": proj,
                        "datasetId": dset,
                        "tableId": f'{city}_{year}_ext'
                    },
                    "externalDataConfiguration": {
                        "sourceUris": [f'{gs_bkt}/{in_prefix}/*{fmt["out"]}'],
                        "sourceFormat": fmt['out'].strip('.').upper()
                    }
                }
            )
            
            create_part_query = (
                f"CREATE OR REPLACE TABLE {proj}.{dset}.{city}_{year}_part "
                f"PARTITION BY DATETIME_TRUNC(timestamp, MONTH) "
                f"AS "
                f"SELECT * FROM {proj}.{dset}.{city}_{year}_ext;"
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

            prefix_exists >> del_ext_tbl >> ext_tbl >> part_tbl

    process_data = DbtCloudRunJobOperator(
        task_id = 'process_data',
        dbt_cloud_conn_id = 'dbt_cloud_default',
        job_id = 0,
        trigger_reason = 'triggered by Airflow task run',
        trigger_rule = 'none_failed_min_one_success'
    )

    tg1 >> process_data
