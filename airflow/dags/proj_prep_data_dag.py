from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import pendulum as pdl
import os

proj = os.getenv('GCP_PROJECT_ID')
# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
gs_bkt = os.getenv('GCP_GCS_BUCKET')  # UPDATE ME IN PROD
fmt = {'in': '.csv', 'out': '.parquet'}
dataset = 'crime_reports'
def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "proj_prep_data_dag",
    schedule = '@monthly',
    start_date = pdl.datetime(2001, 1, 1, tz="Asia/Manila"),
    # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = f'{a_home}/include',
    max_active_runs = 2,
    user_defined_macros = {
        'jar_path': os.getenv('JAR_FILE_LOC'),
        'include_dir': f'{a_home}/include'
    },
    tags = ['project', 'TEST']
) as dag:

    cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
    f_cities = [city.replace(' ', '_').lower() for city in cities]

    for city in f_cities:
        
        clean_data = SparkSubmitOperator(
            task_id = f'clean_data_{city}',
            application = '{{ include_dir }}/proj_pq_read.py',
            conn_id = 'project_spark',        # not templated
            name = f'clean_data_{city}',
            py_files = '{{ include_dir }}/city_vars.py',
            jars = '{{ jar_path }}',
            driver_memory = '5G',
            executor_memory = '3G',
            max_active_tis_per_dag = 2,
            application_args = [
                cities[f_cities.index(city)],
                '{{ dag_run.logical_date.strftime("%Y") }}',
                '{{ dag_run.logical_date.strftime("%m") }}'],
            verbose = True
        )

    with TaskGroup(group_id = 'bq_tg') as tg1:
        for city in f_cities:

            create_dset = BigQueryCreateEmptyDatasetOperator(
                task_id = f'create_dset_{city}',
                dataset_id = dataset,
                location = 'us-central1',
                dataset_reference = {
                    "friendlyName": f"{dataset.replace('_', ' ').title()}",
                    "description": "Cleaned crime reports from CSV"
                },
                exists_ok = True
            )

            ext_tbl = BigQueryCreateExternalTableOperator(
                task_id = f'ext_tbl_{city}',
                bucket = gs_bkt.replace('gs://', ''),  # UPDATE ME IN PROD
                source_objects = [f'{gs_bkt}/pq/{city}/*'],
                destination_project_dataset_table = f'{dataset}.{city}_ext',
                source_format = fmt['out'].strip('.').upper(),
                autodetect = True
            )
            
            create_part_query = (
                f"CREATE OR REPLACE TABLE {proj}.{dataset}.{city}_part"
                f"PARTITION BY DATE(timestamp)"
                f"AS"
                f"SELECT * FROM {proj}.{dataset}.{city}_ext;"
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

            create_dset >> ext_tbl >> part_tbl

    for city in f_cities:
        process_data = DbtCloudRunJobOperator(
            task_id = f'process_data_{city}',
            dbt_cloud_conn_id = 'dbt_cloud_default',
            job_id = 0,
            trigger_reason = 'triggered by Airflow task run'
        )

        # task dependencies
    clean_data >> tg1 >> process_data
