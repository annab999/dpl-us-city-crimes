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
  
  ~~(Also, DAG runs)~~ *-> no idea what this is for but I'll keep it here in case I remember.*

  Found [this](https://github.com/apache/airflow/discussions/20693) confirming it's an unresolved bug. A suggested, unconfirmed fix mentioned (at the bottom) of the thread is to:
  > Get the Xcom instead of XcomArg in Jinjia template or inside task callback function
  
  Again, unconfirmed and, can't really apply this to my BashOp.
  
- **Resolution**: Have to wait for a patch/workaround.

### [Airflow] Airflow apparently runs from `$AIRFLOW_HOME`, and not `$AIRFLOW_HOME/dags`! >:(
```
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------WE ARE IN /opt/***-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/ are: ['dags', 'logs', 'plugins', 'include', '***.cfg', 'webserver_config.py', '***-worker.pid']-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/include/ are: ['austin.csv', 'chicago.csv', 'los_angeles.csv', 'san_francisco.csv']-------
```
Above are my own info logs. This had been causing issues with opening the csv files in `parse_link` tasks.
- **Observations**: Tried the following to no avail:
  - with, without trailing slash in `template_searchpath` attribute to DAG, and consequently in `open()` method
  - use relative, absolute paths in `template_searchpath` attribute to DAG
  - use relative path in `open()` method

  Had I tried absolute path in `open()` method immediately, it would work.
  
- **Resolution**: Modify code to use relative path prefix var in `open()` method

### [Airflow] DAGs/tasks sometimes become non-performant/buggy even with fixes
*I wasn't able to take a screenshot, but the boxes for dead tasks are flattened squares instead of the usual. And they're never executed or shaded any color at all.*
- **Observations**: This happens after multiple edits to the DAG file and its tasks. It's like Airflow DB drowns in confusion and doesn't recover, for that DAG.
  
- **Resolution**: Need to restart Airflow, or recreate as a new DAG with a new name. Do every now and then, to avoid false negatives and hours of wasted debugging T_T

### [Airflow] Crosstalk(?) within the .output of an Operator among child task instances
[airflow_mixedoutput_taskinstance_issue.png](docu/airflow_mixedoutput_taskinstance_issue.png?raw=true)
- **Observations**: You could imagine my surprise. Seems this is due to some queueing / race condition with the different values (1 for each city) stored my `<parse_task_var>.output`, wherein I'm unable to specify the task ID.
  
- **Resolution**: Use `ti.xcom_pull(key='<key>', task_ids='<task_id>')` as much as possible to specify value. Don't forget to use **complete ID** (this is not the task var)!

### [Airflow] Terrible Mistake #1: wrong input type (dict of lists) passed to map callable
```
parse_link = PythonOperator(
    task_id = f'parse_link_{city}',
    python_callable = parse_py,
    op_kwargs = {'name': city, 'ext': fmt['in'], 'gs': gcs_bkt}
)
...
def parse_py(name, gs, ext):
...
  return {'name': name, 'fnames': fnames, 'urls': urls, 'gs': gs, 'ext': ext}   # returns a dict
...
curls = parse_link.output.map(parse_bash)                                       # expects a list
```

- **Observations**: Only noticed while diff-ing with trial DAG that worked (using XCom) :( I sincerely thought that the errors were issues with XCom / dynamic task mapping / `.expand()`, because I was using a combo of this in this part. So all my searches were around these lines, and nothing I find could really directly pertain to my issue. Until I played around and diff-ed with a test using ID-ed XCom arg that worked.
  
- **Resolution**: Fix function to return list (of dicts)

### [Airflow] Terrible Mistake #2: wrong input type (list) defined and coded in map callable
```
[2022-10-18T13:02:19.000+0000] {taskinstance.py:1851} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/taskinstance.py", line 1457, in _run_raw_task
    self._execute_task_with_callbacks(context, test_mode)
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/taskinstance.py", line 1576, in _execute_task_with_callbacks
    task_orig = self.render_templates(context=context)
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/taskinstance.py", line 2199, in render_templates
    original_task.render_template_fields(context)
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/mappedoperator.py", line 762, in render_template_fields
    mapped_kwargs, seen_oids = self._expand_mapped_kwargs(context, session)
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/mappedoperator.py", line 539, in _expand_mapped_kwargs
    return self._get_specified_expand_input().resolve(context, session)
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/expandinput.py", line 149, in resolve
    data = {k: self._expand_mapped_field(k, v, context, session=session) for k, v in self.value.items()}
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/expandinput.py", line 149, in <dictcomp>
    data = {k: self._expand_mapped_field(k, v, context, session=session) for k, v in self.value.items()}
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/expandinput.py", line 140, in _expand_mapped_field
    return value[found_index]
  File "/home/airflow/.local/lib/python3.7/site-packages/airflow/models/xcom_arg.py", line 351, in __getitem__
    value = self.value[index]
KeyError: 0
```
My code was:
```
curls = parse_link.output.map(parse_bash)                       # returns a list
...
def parse_bash(url_dict):                                       # thinks it receives a dict
  # entire content of code parses on a dict
```

- **Observations**: Kept getting this error and kept thinking it was again (as the earlier related issue) an Airflow bug with the combo of features I was working on. Only realized the *real* after issue after having fixed the earlier issue with a set of more self-aware eyes.

  
- **Resolution**: Fix function to use arg of each individual item of the mapped list (alternatively, could use combo of `.expand_kwargs()` and a separate function)

## TODOs:
- dag running per year but parsing is lahat
- remove DEBUG logging, example dags
- try: ti.xcom in map func
- `curls = parse_link.output.map(parse_bash)`
- `curls.value[item]`
- set up smooth error handling in webscraping script if no more remaining results to scrape from page