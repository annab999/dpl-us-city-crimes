import pendulum as pdl
 import os

 from airflow import DAG
 from airflow.utils.task_group import TaskGroup
 from airflow.operators.bash import BashOperator
 from airflow.operators.python import PythonOperator
 from airflow.operators.generic_transfer import GenericTransfer

 from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
 from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
 from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

 from task_functions import parse_py, parse_bash

 proj = os.getenv('GCP_PROJECT_ID')
 gcs_bkt = 'gs://' + proj + '-project'
 cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
 fmt = {'in': '.csv', 'out': '.parquet'}

 def_args = {
     "owner": "airflow",
     "depends_on_past": False,
     "retries": 3
 }

 with DAG(
     dag_id = "project_full_dag",
     schedule = '@yearly',
     start_date = pdl.datetime(2001, 1, 1, tz="Asia/Manila"),
     # end_date = pdl.datetime(2022, 1, 1, tz="Asia/Manila"),
     default_args = def_args,
     template_searchpath = "/home/jbbalasbas_alum_up_edu_ph/dezoomcamp/project/airflow/include",
     max_active_runs = 2,
     tags = ['project']
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
                     curls = parse_link.output.map(parse_bash)
                     down_up = BashOperator \
                         .partial(task_id = f'down_up_{city}') \
                         .expand(bash_command = curls)

                     parse_link >> down_up
             else:
                 # update me
                 list_gcs_pq = [f'{gcs_bkt}/prepared/{city}/']
                 ext_tbl = BigQueryCreateExternalTableOperator(
                     task_id="ext_tbl",
                     table_resource={
                         "tableReference": {
                             "projectId": proj,
                             "datasetId": "<bq_dataset>",
                             "tableId": f"<ext_table>_ext",
                         },
                         "externalDataConfiguration": {
                             "sourceFormat": fmt['out'].strip('.').upper(),
                             "sourceUris": list_gcs_pq
                         },
                         "autodetect": "True"
                     },
                 )
                 # choose final columns
                 part_tbl = BigQueryInsertJobOperator()

                 ext_tbl >> part_tbl
         tgs.append(tg)

     prepare_data = SparkSubmitOperator()
     # launch Spark app running preprocessing script/s
     process_data = DbtCloudRunJobOperator()
     # run scripts in production

     # task dependencies
     tgs[0] >> prepare_data >> tgs[1] >> process_data
