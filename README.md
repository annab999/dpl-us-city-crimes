# End-to-end Pipeline Project
Stuff is still in main [README.md](../README.md)

## Development Issues
Here are the issues I encountered in setting up the project.

### [Airflow] Multiple warnings easily piling up in task instance logs:
```
[2022-10-18, 00:35:45 PST] {taskmixin.py:205} WARNING - Dependency <Task(PythonOperator): files_tg.parse_link_chicago>, files_tg.down_up_chicago already registered for DAG: project_test_dag
[2022-10-18, 00:35:45 PST] {taskmixin.py:205} WARNING - Dependency <Mapped(BashOperator): files_tg.down_up_chicago>, files_tg.parse_link_chicago already registered for DAG: project_test_dag
[2022-10-18, 00:35:45 PST] {taskmixin.py:205} WARNING - Dependency <Task(PythonOperator): files_tg.parse_link_san_francisco>, files_tg.down_up_san_francisco already registered for DAG: project_test_dag
[2022-10-18, 00:35:45 PST] {taskmixin.py:205} WARNING - Dependency <Mapped(BashOperator): files_tg.down_up_san_francisco>, files_tg.parse_link_san_francisco already registered for DAG: project_test_dag
```

- **Observations**: 1 warning logged per task / mapped instance, and there are also others from example DAGs provided by Airflow. So, seems it's not an issue with my code (as I've rechecked already).
  
  ~~(Also, DAG runs)~~ -> no idea what this is for but I'll keep it here in case I remember.

  Found [this](https://github.com/apache/airflow/discussions/20693) confirming it's an unresolved bug. A suggested, unconfirmed fix mentioned (at the bottom) of the thread is to:
  > Get the Xcom instead of XcomArg in Jinjia template or inside task callback function
  
  Again, unconfirmed and, can't really apply this to my BashOp.
  
- **Status**: Have to wait for a patch/workaround.

### [Airflow] Airflow apparently runs from `$AIRFLOW_HOME`, and not `$AIRFLOW_HOME/dags`! >:(
```
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------WE ARE IN /opt/***-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/ are: ['dags', 'logs', 'plugins', 'include', '***.cfg', 'webserver_config.py', '***-worker.pid']-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/include/ are: ['austin.csv', 'chicago.csv', 'los_angeles.csv', 'san_francisco.csv']-------
```
- **Observations**: Tried the following to no avail:
  - use relative, absolute paths
  - with, without trailing slash
  
- **Status**: Just modified code to use path prefix var.

## TODOs:
- dag running per year but parsing is lahat
- need to modify down_up task code due to tiemouts?