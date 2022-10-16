import pendulum as pdl

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.apache.spark import SparkSubmitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.dbt.cloud import DbtCloudRunJobOperator
from airflow.utils.task_group import TaskGroup

from task_functions import parse_py, parse_bash, printer

# proj = 'denzoom'
gs_bkt = 'gs://test_data_lake_denzoom/'                                                 # edited
cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
fmt = {'in': '.csv', 'out': '.parquet'}

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3
}

with DAG(
    dag_id = "project_test_dag",                                                        # edited
    schedule = '@yearly',
    start_date = pdl.datetime(2001, 1, 1, tz="Asia/Manila"),
    # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
    default_args = def_args,
    template_searchpath = "../include/",
    max_active_runs = 2,
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
                            'gs': gs_bkt
                        }
                    )
                    curls = parse_link.output.map(parse_bash)
                    down_up = BashOperator \
                        .partial(task_id = f'down_up_{city}') \
                        .expand(bash_command = curls)

                    parse_link >> down_up
            else:
                printer('\n--------went through else--------\n')                        # edited
                # # update me
                # list_gcs_pq = [f'{gs_bkt}/prepared/{city}/']
                # ext_tbl = BigQueryCreateExternalTableOperator(
                #     task_id="ext_tbl",
                #     table_resource={
                #         "tableReference": {
                #             "projectId": proj,
                #             "datasetId": "<bq_dataset>",
                #             "tableId": f"<ext_table>_ext",
                #         },
                #         "externalDataConfiguration": {
                #             "sourceFormat": fmt[1].strip('.').upper(),
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