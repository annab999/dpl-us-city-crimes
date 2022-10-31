import pendulum as pdl
import os

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

from task_functions import parse_py, parse_bash, printer

# proj = os.getenv('GCP_PROJECT_ID')
# gs_bkt = 'gs://' + proj + '-project'
a_home = os.getenv('AIRFLOW_HOME')
def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "test_getdata_dag",
    schedule = '@once',
    start_date = pdl.datetime(2022, 10, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = f'{a_home}/include',
    max_active_runs = 2,
    user_defined_macros = {
        'gs_bkt': os.getenv('GCP_GCS_BUCKET'),  # UPDATE ME IN PROD
        'jar_path': os.getenv('JAR_FILE_LOC'),  # try HTTP URL
        'include_dir': f'{a_home}/include'
    },
    user_defined_filters = {
        'fmt': (lambda drxn: '.csv' if drxn=='in' else '.parquet'),
        'no_gs': (lambda url: url.replace('gs://', ''))
    },
    tags = ['project', 'TEST']
) as dag:

    cities = ['Austin']
    f_cities = [city.replace(' ', '_').lower() for city in cities]
    
    with TaskGroup(group_id = 'files_tg') as tg1:
            
        for city in f_cities:
            parse_link = PythonOperator(
                task_id = f'parse_link_{city}',
                python_callable = parse_py,
                op_kwargs = {
                    'name': city,
                    'ext': "{{ 'in' | fmt }}"
                }
            )
            curls = parse_link.output.map(parse_bash)
            down_up = BashOperator \
                .partial(
                    task_id = f'down_up_{city}',
                    max_active_tis_per_dag = 3,
                    env = {
                        'name': city,
                        'gs': '{{ gs_bkt }}',
                        'ext': "{{ 'in' | fmt }}"
                    },
                    append_env = True) \
                .expand(bash_command = curls)

            parse_link >> down_up
            printer('\n--------after tg1--------\n')
            
    with TaskGroup(group_id = 'data_tg') as tg2:
        for city in f_cities:
            list_fpaths = GCSListObjectsOperator(
                task_id = f'list_fpaths_{city}',
                bucket = '{{ gs_bkt | no_gs }}',  # UPDATE ME IN PROD
                gcp_conn_id = 'google_cloud_test',
                prefix = f'raw/{city}/',
                delimiter = "{{ 'in' | fmt }}"
            )

            prepare_data = SparkSubmitOperator \
                .partial(
                    task_id = f'prepare_data_{city}',
                    application = '{{ include_dir }}/test_file_read.py',
                    conn_id = 'spark_default',
                    name = f'prepare_data_{city}',
                    py_files = '{{ include_dir }}/city_vars.py',
                    jars = '{{ jar_path }}',
                    env_vars = {
                        'city_name': city,
                        'gcs_bkt': '{{ gs_bkt }}'},
                    verbose = True) \
                .expand(application_args = list_fpaths.output)

            list_fpaths >> prepare_data
            printer('\n--------after spark--------\n')

    tg1 >> tg2
    printer('\n--------dag done--------\n')
