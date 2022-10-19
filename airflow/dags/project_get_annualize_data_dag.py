import pendulum as pdl
import os

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from task_functions import parse_py, parse_bash, printer

# proj = os.getenv('GCP_PROJECT_ID')
# gcs_bkt = 'gs://' + proj + '-project'
gcs_bkt = os.getenv('GCP_GCS_BUCKET')
cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
fmt = {'in': '.csv', 'out': '.parquet'}

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "project_get_annualize_data_dag",
    schedule = '@once',
    start_date = pdl.datetime(2022, 10, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = "/opt/airflow/include",
    max_active_runs = 1,
    tags = ['project']
) as dag:

    f_cities = [city.replace(' ', '_').lower() for city in cities]

    with TaskGroup(group_id = 'files_tg') as tg:
        
        for city in f_cities:
            parse_link = PythonOperator(
                task_id = f'parse_link_{city}',
                python_callable = parse_py,
                op_kwargs = {
                    'name': city,
                    'ext': fmt['in'],
                    'gs': gcs_bkt
                }
            )
            curls = parse_link.output.map(parse_bash)
            curl_up = BashOperator \
                .partial(task_id = f'curl_up_{city}') \
                .expand(bash_command = curls)

            parse_link >> curl_up

    printer('\n--------after tg--------\n')

    for city in f_cities:
        # launch Spark app running preprocessing script/s
        # requires that the “spark-submit” binary is in the PATH
        #       or the $SPARK_HOME is set in the extra on the connection.
        prepare_data = SparkSubmitOperator(
            task_id = f'prepare_data_{city}',
            application = '',           # <app.py>
            conf = '',                  # <spark-conf>
            name = "project-airflow-spark",
            application_args = '',      # <list of args>, {city}
            verbose = True              # for debugging
        )

    printer('\n--------after spark--------\n')

    # # task dependencies
    tg >> prepare_data
    printer('\n--------dag done--------\n')
