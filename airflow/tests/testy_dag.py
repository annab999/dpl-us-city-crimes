import pendulum as pdl
import os

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from testy_func import parse_py, parse_bash, printer

# proj = os.getenv('GCP_PROJECT_ID')
# gcs_bkt = 'gs://' + proj + '-project'
gcs_bkt = os.getenv('GCP_GCS_BUCKET')                                              # edited
cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
fmt = {'in': '.csv', 'out': '.parquet'}

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "testy_dag",                                                        # edited
    schedule = '@once',
    start_date = pdl.datetime(2022, 10, 1, tz="Asia/Manila"),
    # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = "/opt/airflow/include",
    max_active_runs = 1,
    tags = ['project', 'TEST']                                                          # edited
) as dag:

    tgs = []
    for grp in ['files', 'bigquery']:
        with TaskGroup(group_id = grp + '_tg') as tg:
            f_cities = [city.replace(' ', '_').lower() for city in cities]
    
            if grp == 'files':
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
                    testpy = PythonOperator(
                        task_id = f'testpy_{city}',
                        python_callable = parse_bash,
                        op_kwargs = {
                            'name': city,
                        }
                    )
                    down_up = BashOperator \
                        .partial(task_id = f'down_up_{city}') \
                        .expand(bash_command = testpy.output)

                    parse_link >> testpy >> down_up

            else:
                printer('\n--------went through else--------\n')                        # edited
                # # update me
                # list_gcs_pq = [f'{gcs_bkt}/prepared/{city}/']
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
                # # choose final columns
                # part_tbl = BigQueryInsertJobOperator()

                # ext_tbl >> part_tbl
        printer('\n--------before appending--------\n')                                 # edited
        tgs.append(tg)
    printer('\n--------out of loop--------\n')                                          # edited
    # prepare_data = SparkSubmitOperator()
    # # launch Spark app running preprocessing script/s
    # process_data = DbtCloudRunJobOperator()
    # # run scripts in production
    # # task dependencies
    # tgs[0] >> prepare_data >> tgs[1] >> process_data
    tgs[0]                                                                              # edited
    printer('\n--------dag done--------\n')
