from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

import pendulum as pdl
import os

# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
gs_bkt = os.getenv('GCP_GCS_BUCKET')
earliest_yr = 2001	# Chicago 2001 data, better to be automated
earliest_mon = 1
def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "proj_prep_data_dag",
    schedule = '@monthly',
    start_date = pdl.datetime(earliest_yr, earliest_mon, 1),
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

    # cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
    cities = ['Chicago', 'Los Angeles', 'Austin']
    f_cities = [city.replace(' ', '_').lower() for city in cities]

    for city in f_cities:
    
        year_mon = '{{ dag_run.logical_date.strftime("%Y") }}/{{ dag_run.logical_date.strftime("%m") }}'
        gcs_object_exists = GCSObjectExistenceSensor(
            task_id = f'gcs_object_exists_{city}',
            bucket = gs_bkt.replace('gs://', ''),  # UPDATE ME IN PROD
            object = f'pq/{city}/{year_mon}/_SUCCESS',
            timeout = 7,
            soft_fail = True,
            retries = 0
        )
    
        clean_data = SparkSubmitOperator(
            task_id = f'clean_data_{city}',
            application = '{{ include_dir }}/proj_pq_clean.py',
            conn_id = 'spark_default',        # not templated
            name = '{{ task.task_id }}',
            py_files = '{{ include_dir }}/city_vars.py',
            jars = '{{ jar_path }}',
            # driver_memory = '5G',
            # executor_memory = '3G',
            application_args = [
                cities[f_cities.index(city)],
                '{{ dag_run.logical_date.strftime("%Y") }}',
                '{{ dag_run.logical_date.strftime("%m") }}'],
            verbose = True
        )

        # task dependencies
        gcs_object_exists >> clean_data
