from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import pendulum as pdl
import os

# proj = os.getenv('GCP_PROJECT_ID')
# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
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
        'gs_bkt': os.getenv('GCP_GCS_BUCKET'),  # UPDATE ME IN PROD
        'jar_path': os.getenv('JAR_FILE_LOC'),
        'include_dir': f'{a_home}/include'
    },
    user_defined_filters = {
        'fmt': (lambda drxn: '.csv' if drxn=='in' else '.parquet'),
        'no_gs': (lambda url: url.replace('gs://', ''))
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
            max_active_tis_per_dag = 2,
            verbose = True,
            application_args = ([
                cities[f_cities.index(city)],
                '{{ dag_run.logical_date.strftime("%Y") }}',
                '{{ dag_run.logical_date.strftime("%m") }}'])
        )

        # list_gcs_pq = [f'{gs_bkt}/clean/{city}/']
        # ext_tbl = BigQueryCreateExternalTableOperator(
        #     task_id="ext_tbl",
        #     table_resource={
        #         "tableReference": {
        #             "projectId": proj,
        #             "datasetId": "<bq_dataset>",
        #             "tableId": f"<ext_table>_ext",
        #         },
        #         "externalDataConfiguration": {
        #             "sourceFormat": fmt['out'].strip('.').upper(),
        #             "sourceUris": list_gcs_pq
        #         },
        #         "autodetect": "True"
        #     },
        # )
        # part_tbl = BigQueryInsertJobOperator()

        # process_data = DbtCloudRunJobOperator()
        # run scripts in production

        # task dependencies
        # clean_data >> ext_tbl >> part_tbl >> process_data
        clean_data
