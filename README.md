# End-to-end Pipeline Project
Stuff is still in main [README.md](../README.md)

## Version Matrix
The following are the software (and corresponding versions) used for this project:

| Platform | OS | Software | Version | Notes |
| --- | --- | :---: | :--: | ---: |
| desktop | Windows 10 | gcloud SDK | 407.0.0 | workstation |
| desktop | Windows 10 | Terraform | 1.3.2 | workstation |
| VM | CentOS 7 | gcloud SDK | 407.0.0 |  |
| VM | CentOS 7 | Docker Engine | 20.10.20 |  |
| container | Debian 11 | Python | 3.7.14 | for Airflow |
| container | Debian 11 | gcloud SDK | 407.0.0 | for Airflow |
| container | Debian 11 | Postgres | 13.8 | for Airflow |
| container | Debian 11 | Apache Airflow | 2.4.2 |  |
| container | Debian | Anaconda | 4.12 | for Spark |
| container | Debian | Python | 3.9.12 | for Spark |
| container | Debian | OpenJDK | 17.0.2 | for Spark |
| container | Debian | Apache Spark | 3.3.1 |  |
| cloud | - | BigQuery |  | managed |
| cloud | - | dbt |  | managed |

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

  Found https://github.com/apache/airflow/discussions/20693 confirming it's an unresolved bug. A suggested, unconfirmed fix mentioned (at the bottom) of the thread is to:
  > Get the Xcom instead of XcomArg in Jinjia template or inside task callback function
  
  Again, unconfirmed and, can't really apply this to my BashOp.
  
- **Resolution**: Have to wait for a patch/workaround.

### [Airflow] Airflow apparently runs from `$AIRFLOW_HOME`, and not `$AIRFLOW_HOME/dags`! >:(
Below are my own info logs. This had been causing issues with opening the csv files in `parse_link` tasks.
```
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------WE ARE IN /opt/***-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/ are: ['dags', 'logs', 'plugins', 'include', '***.cfg', 'webserver_config.py', '***-worker.pid']-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/include/ are: ['austin.csv', 'chicago.csv', 'los_angeles.csv', 'san_francisco.csv']-------
```
- **Observations**: Tried the following to no avail:
  - with, without trailing slash in `template_searchpath` attribute to DAG, and consequently in `open()` method
  - use relative, absolute paths in `template_searchpath` attribute to DAG
  - use relative path in `open()` method

  Had I tried absolute path in `open()` method immediately, it would work.
  
- **Resolution**: Modify code to use relative path prefix var in `open()` method

### [Airflow] DAGs/tasks sometimes become non-performant/buggy even with fixes
*I wasn't able to take a screenshot, but the boxes for dead tasks are flattened squares instead of the usual. And they're never executed nor shaded any color at all.*
- **Observations**: This happens after multiple edits to the DAG file and its tasks. It's like Airflow DB drowns in confusion and doesn't recover, for that DAG.
  
- **Resolution**: Need to restart Airflow, or recreate as a new DAG with a new name. Do every now and then, to avoid false negatives and hours of wasted debugging T_T

### [Airflow] Crosstalk(?) within the `<task_var>.output` of an Operator among child task instances
![airflow_mixedoutput_taskinstance_issue.png](docu/airflow_mixedoutput_taskinstance_issue.png?raw=true "Airflow mixed ouput issue with task instances")

- **Observations**: You could imagine my surprise. Seems this is due to some queueing / race condition with the different values (1 for each city) stored in my `<parse_task_var>.output`, wherein I'm unable to specify the task ID.
  
- **Resolution**: Use `ti.xcom_pull(key='<key>', task_ids='<task_id>')` as much as possible to specify value. Don't forget to use **complete ID** (this is not the task var)!

### [Docker] Directory permissions error during startup of Airflow containers
The error when composing the rest of the airflow containers (after `airflow-init`):
```
project-airflow-cli-1        | Unable to load the config, contains a configuration error.
project-airflow-cli-1        | Traceback (most recent call last):
project-airflow-cli-1        |   File "/usr/local/lib/python3.7/pathlib.py", line 1273, in mkdir
project-airflow-cli-1        |     self._accessor.mkdir(self, mode)
project-airflow-cli-1        | FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/logs/scheduler/2022-10-20'
project-airflow-cli-1        |
project-airflow-cli-1        | During handling of the above exception, another exception occurred:
project-airflow-cli-1        |
project-airflow-cli-1        | Traceback (most recent call last):
project-airflow-cli-1        |   File "/usr/local/lib/python3.7/logging/config.py", line 563, in configure
project-airflow-cli-1        |     handler = self.configure_handler(handlers[name])
project-airflow-cli-1        |   File "/usr/local/lib/python3.7/logging/config.py", line 736, in configure_handler
project-airflow-cli-1        |     result = factory(**kwargs)
project-airflow-cli-1        |   File "/home/airflow/.local/lib/python3.7/site-packages/airflow/utils/log/file_processor_handler.py", line 48, in __init__
project-airflow-cli-1        |     Path(self._get_log_directory()).mkdir(parents=True, exist_ok=True)
project-airflow-cli-1        |   File "/usr/local/lib/python3.7/pathlib.py", line 1277, in mkdir
project-airflow-cli-1        |     self.parent.mkdir(parents=True, exist_ok=True)
project-airflow-cli-1        |   File "/usr/local/lib/python3.7/pathlib.py", line 1273, in mkdir
project-airflow-cli-1        |     self._accessor.mkdir(self, mode)
project-airflow-cli-1        | PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'
```

- **Observations**: This came up only after I refactored my `compose.yaml` to include both airflow and spark containers. Made a lot of changes, (code) optimizations, new variables on all compose files because of the merging. But airflow and spark setups had been working separately.

Noticed the permissions on my host airflow directories (generated and existing) also seemed to be typical.

  Tried the following to no avail: *(italicizing items I consequently also used during actual fix)*
  - revert personal `USER ${AIRFLOW_USER}`, which I passed as `ARG` from `compose.yaml`, to original `USER $AIRFLOW_UID`, which should never have gotten passed since only defined in `.env`
    - *also up postgres, redis (I forgot to specify them along with other airflow services)*
      - *also remove `logs/`, `plugins/` folders before composing*
        - *also rebuild with cache cleared*
  - *revert* back *to my personal `USER ${AIRFLOW_USER}` build-time arg*
    - with above sub-bullets also done
  - tried to run standalone airflow `compose.yaml` that worked from before, with the following changes:
    - same .env file as complete compose project (with spark)
    - in a temporary `temp/` folder outside of the original `airflow/` folder
  
  While running the last test above ad comparing preprocessed configs, I noticed that a source for a bind mount was set in `project/` (host *working* directory), when I believe it's supposed to be `project/airflow/` (host *airflow* directory):
  ```
  services:
    airflow-init:
  ...
    volumes:
      - type: bind
        source: /home/j*********ph/dezoomcamp/project
        target: /sources
  ```
  I actually missed that single dot in `- .:sources/` while updating paths and actually grepping volume defs! :<

- **Resolution**: Update host path in bind mount def for `sources/` (used in initialization of Airflow) to actual Airflow directory. Always look closer at volume mappings!

### [Docker] Permissions error during Jupyter startup (in Spark container)
The error when starting up the spark container, which also runs Jupyter, at least until I finish coding the scripts:
```
Traceback (most recent call last):
  File "/opt/conda/bin/jupyter-notebook", line 11, in <module>
    sys.exit(main())
  File "/opt/conda/lib/python3.9/site-packages/jupyter_core/application.py", line 264, in launch_instance
    return super(JupyterApp, cls).launch_instance(argv=argv, **kwargs)
...
  File "/opt/conda/lib/python3.9/site-packages/traitlets/traitlets.py", line 540, in get
    default = obj.trait_defaults(self.name)
  File "/opt/conda/lib/python3.9/site-packages/traitlets/traitlets.py", line 1580, in trait_defaults
    return self._get_trait_default_generator(names[0])(self)
  File "/opt/conda/lib/python3.9/site-packages/jupyter_core/application.py", line 95, in _runtime_dir_default
    ensure_dir_exists(rd, mode=0o700)
  File "/opt/conda/lib/python3.9/site-packages/jupyter_core/utils/__init__.py", line 11, in ensure_dir_exists
    os.makedirs(path, mode=mode)
...
  File "/opt/conda/lib/python3.9/os.py", line 215, in makedirs
    makedirs(head, exist_ok=exist_ok)
  File "/opt/conda/lib/python3.9/os.py", line 225, in makedirs
    mkdir(name, mode)
PermissionError: [Errno 13] Permission denied: '/home/spark_files/.local'
```

- **Observations**: Build is successful. Spark seems to run fine, and error is due to the container default command of opening a Jupyter notebook on start. Won't be a problem in prod once I remove Jupyter from the Spark container, but I'll also solve this now.

  All should be solved by just passing `--allow-root` to the command and using `root` for the entire thing, but that isn't recommended, hence I set up a service user. Tried a lot of stuff here as I thought it was due to my custom user and directories, and in the end realized the error was with the *host side* of the bind mount:
  ```
  Container project-spark-1  Recreated                                                           0.1s
  Attaching to project-spark-1
  project-spark-1  | total 4
  project-spark-1  | drwxrwxr-x. 2 1475897537 1475897537   24 Oct 21 14:17 .
  project-spark-1  | drwxr-xr-x. 1 root       root         25 Oct 21 12:01 ..
  project-spark-1  | -rw-rw-r--. 1 1475897537 1475897537 1801 Oct 21 14:17 Dockerfile
  ```

  I remembered the last step in the Airflow Dockerfile to switch to user with the my host user's UID: `USER ${AIRFLOW_UID}`, where `AIRFLOW_UID=$(id -u)` on the host. When I use this UID in the Dockerfile though, the build gets stuck at `exporting layers` (last step pf everything):
  ```
  ...
  #9 7.849 ++ hash -r
  #9 DONE 8.1s

  #10 exporting to image
  #10 exporting layers
                                        # stalls forever
  ```

  Realized this only happens when I assign UID to my custom user, but not when switching `USER` arg at the last step. But still seems wrong...

  Also finally remembered `chmod` and `chown`. All combos did not work in container directory, then FINALLY (group `root` or gid 0 was sufficient for my case because I added my custom user to that group in the container):
  ```
  $ ls -al spark
  total 4
  drwxrwxr-x. 2 j*********ph j*********ph   24 Oct 21 18:19 .
  drwxrwxr-x. 7 j*********ph j*********ph  178 Oct 21 16:13 ..
  -rw-rw-r--. 1 j*********ph j*********ph 1835 Oct 21 18:00 Dockerfile
  # then, after applying the fix below...
  $ ls -al spark
  total 4
  drwxrwxr-x. 4 j*********ph root                        54 Oct 21 18:25 .
  drwxrwxr-x. 7 j*********ph j*********ph  178 Oct 21 16:13 ..
  -rw-rw-r--. 1 j*********ph root                      1835 Oct 21 18:00 Dockerfile
  drwxr-xr-x. 2                      1000 root                         6 Oct 21 18:25 .ipython
  drwxr-xr-x. 3                      1000 root                        19 Oct 21 18:25 .local
  ```

  - **Resolution**: apply `chown :0 -R <host_dir_of_bind_mount` once on *host side* of the bind mount, as `root` while in container

### [Docker] Weird storage issues with build cache
Did the following upon next day's boot of host (even though I cleaned everything the night before):
```
$ df -h
Filesystem      Size  Used Avail Use% Mounted on
...
/dev/sda2        40G   37G  3.0G  93% /
...

$ docker system df
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          29        6         10.48GB   9.227GB (88%)
Containers      8         0         2.108GB   2.108GB (100%)
Local Volumes   1         1         0B        0B
Build Cache     96        0         3.832TB   3.832TB
$ docker builder prune
WARNING! This will remove all dangling build cache. Are you sure you want to continue? [y/N] y
Deleted build cache objects:
6orvjkx07iy88gy7hvy95hqkl
...
relf2d1e8hiu5t2itbh1kzse5

Total reclaimed space: 3.832TB

$ df -h
Filesystem      Size  Used Avail Use% Mounted on
...
/dev/sda2        40G   28G   13G  69% /
...
```

- **Observations**: Inaccurate size listed for the build cache. Started happening after my build stalls, multiple rebuilds, build aborts. Though I always run `docker builder prune` when I rebuild, seems some data is still cached until system restart.
  
- **Resolution**: Restart docker service after many failed/test builds

### [Airflow] Incomplete stream transfers by BashOp marked by Airflow as successes
So the whole time, I've been using the files from the first successful DAG run and turns out the files were incompletely transferred.

Sample of a false positive run:
```
[2022-10-24, 19:58:04 PST] {subprocess.py:93} INFO -   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
[2022-10-24, 19:58:04 PST] {subprocess.py:93} INFO -                                  Dload  Upload   Total   Spent    Left  Speed
[2022-10-24, 19:59:29 PST] {subprocess.py:93} INFO - 
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0WARNING: Using sequential instead of parallel task execution to transfer from stdin.
[2022-10-24, 19:59:32 PST] {subprocess.py:93} INFO - Copying file://- to gs://test_data_lake_denzoom/raw/austin/2016_Annual_Crime_Data.csv
[2022-10-24, 19:59:33 PST] {subprocess.py:93} INFO - 
[2022-10-24, 19:59:37 PST] {subprocess.py:93} INFO - 
100 81080    0 81080    0     0    875      0 --:--:--  0:01:32 --:--:--   875
100  463k    0  463k    0     0   5105      0 --:--:--  0:01:32 --:--:--  5105
100 1519k    0 1519k    0     0  16712      0 --:--:--  0:01:33 --:--:-- 16712
[2022-10-24, 19:59:37 PST] {subprocess.py:93} INFO - curl: (18) transfer closed with outstanding read data remaining
[2022-10-24, 19:59:38 PST] {subprocess.py:93} INFO - .....................
[2022-10-24, 20:00:00 PST] {subprocess.py:97} INFO - Command exited with return code 0
[2022-10-24, 20:00:02 PST] {taskinstance.py:1406} INFO - Marking task as SUCCESS.
```
Sample of an actual successful run:
```
...
100 59.0M    0 59.0M    0     0   132k      0 --:--:--  0:07:36 --:--:-- 68454
100 59.3M    0 59.3M    0     0   132k      0 --:--:--  0:07:37 --:--:--  116k
[2022-10-24, 20:08:20 PST] {subprocess.py:93} INFO - ......................................................................
[2022-10-24, 20:08:20 PST] {subprocess.py:93} INFO - 
[2022-10-24, 20:08:20 PST] {subprocess.py:93} INFO - Average throughput: 76.4MiB/s
[2022-10-24, 20:08:31 PST] {subprocess.py:97} INFO - Command exited with return code 0
[2022-10-24, 20:08:31 PST] {taskinstance.py:1406} INFO - Marking task as SUCCESS.s
```

- **Observations**: Tried the following, to no avail (I use 300s a lot since it's the default `retry_delay` for Operators):
  - curl timeouts
    - increase `--keepalive-time` (default 60s) to 300
      - decrease to 2 as suggested on web
    - set `--connect-timeout` (no default value) to 300
    - set `--max-time` (no default value) to 300
  - increase `sleep` delay - from initial 10s to up to 45s
  - task param
    - set `max_active_tis_per_dag` (no default value) to 8, 4
  - check tcp
    - `$ cat /proc/sys/net/ipv4/tcp_keepalive_time` gave 7200s, which doesn't seem to apply to this case
  - check `gcloud storage cp` ref - no timeout params
  
  Then, tried running 1 city only (Austin, which had 4 task instances) and worked. So tried the following:
  - remove my `sleep` delay: SEEMS THIS INTRODUCED MORE PROBLEMS THAN SOLVED ANY
    - set even lower `max_active_tis_per_dag`
      - 2: worked 
      - 4: a few false positives, **without `curl (18)` errors**!
      - *(for science)* none: more false positives with  `curl(18)` errors
      - 3: worked

- **Resolution**: Remove my initial sleep delay! Set `max_active_tis_per_dag` to < 4

### [Spark] Repartitioning optimization experiments with 1 year data
Main thing to note:s
  - took >2mins each month for writing to parquet from csv df with minor (time) transformations
  - took 2mins for whole year for writing to parquet from csv df with no transformations
- **Observations**: Tried the following:
  - `df.repartition()` values (best to good location of repartition step)
    - Chicago: none, 2, 4, 6, 12
      - location of repartition step: none, before writing, before filtering
    - San Francisco (best to good):
      - before writing
        - 12, 16, 24, none, 8, 32
      - before parsing years list
        - none, 12, 24, 18
    - Los Angeles (best to good):
      - before writing
        - 24, 12, 20, 10

- **Resolution**: more experiments, but most especially: read more on optimizations (repartition, coalesce, parallelize, cache)

### [Spark-Airflow] Getting remote spark-submit to work remotely
Manual trial from Airflow container:
```
$ spark-submit
/home/airflow/.local/lib/python3.7/site-packages/pyspark/bin/load-spark-env.sh: line 68: ps: command not found
JAVA_HOME is not set
```
When to start Spark master? Must add py files, path, jar file path to Airflow build

- **Observations**: Turns out, as opposed to instructions online to unpack spark tgz onto remote Airflow executor, Spark binaries are already on the machine upon install of `apache-airflow-providers-apache-spark` pip module, and are already callable from `/home/airflow/.local/bin`, which is also already in `PATH`.

- **Resolution**: Need to install `procps` and Spark prerequisite OpenJDK during build of Airflow images

### [Service] Template

- **Observations**: Stuff

- **Resolution**: Stuff

## Terrible Mistakes courtesy of Me
Here is my stupidity in action.

### [Airflow] TM #1: wrong input type (`dict` of `list`s) passed to `map()` callable
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

- **Observations**: Only noticed while diff-ing with trial DAG that worked (using XCom) :( I sincerely thought that the errors were issues with XCom / dynamic task mapping / `.expand()`, because I was using a combo of this in this part.

  So all my searches were around these lines, and nothing I find could really directly pertain to my issue. My snobbish self even thought I was affected by https://github.com/apache/airflow/issues/25061, but of course not.

  Until I played around and diff-ed with a test using ID-ed XCom arg that worked.
  
- **Resolution**: Fix function to return `list` (of `dict`s)

### [Airflow] TM #2: wrong input type (`list`) defined and coded in `map()` callable
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

- **Resolution**: Fix function to use arg of each individual item of the mapped `list` (alternatively, could use combo of `.expand_kwargs()` and a separate function)

## TODOs:
- dag running per year but parsing is lahat
- try: ti.xcom in `map()` func
- `curls = parse_link.output.map(parse_bash)`
- `curls.value[item]`
- set up smooth error handling in webscraping script if no more remaining results to scrape from page
- remove duplicates in records
- read pq, set up cols, add col for each type, union all, sql queries (groupby loc, date/mon, type; sums and avgs), write report
- NTS: always check for NULL values, duplicates before processing
- strip whitespace from austin 'Highest NIBRS/UCR Offense Description', 'GO Location'
  - F.trim('Highest NIBRS/UCR Offense Description')
- remove whitespace from los angeles within Location, Cross Street
- try `deploy-mode - Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client) -> default.`

### Before running prod
- update airflow .env bucket
- remove DEBUG logging, example dags
- upgrade version
  - airflow gcloud 406

### File sizes for reference
```
PS C:\Users\Joanna> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/austin/
   7.22MiB  2022-10-24T18:42:49Z  gs://test_data_lake_denzoom/raw/austin/2016_Annual_Crime_Data.csv
   6.75MiB  2022-10-24T18:43:50Z  gs://test_data_lake_denzoom/raw/austin/2017_Annual_Crime.csv
   3.91MiB  2022-10-24T18:43:53Z  gs://test_data_lake_denzoom/raw/austin/2018_Annual_Crime.csv
   7.52MiB  2022-10-24T18:42:57Z  gs://test_data_lake_denzoom/raw/austin/Annual_Crime_Dataset_2015.csv
TOTAL: 4 objects, 26640531 bytes (25.41MiB)
PS C:\Users\Joanna> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/chicago/
  89.26MiB  2022-10-24T18:42:27Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2001.csv
  90.34MiB  2022-10-24T18:42:36Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2002.csv
  89.83MiB  2022-10-24T18:43:56Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2003.csv
  88.80MiB  2022-10-24T18:43:55Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2004.csv
  85.87MiB  2022-10-24T18:45:00Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2005.csv
  84.89MiB  2022-10-24T18:45:46Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2006.csv
  82.77MiB  2022-10-24T18:45:34Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2007.csv
  80.49MiB  2022-10-24T18:46:31Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2008.csv
  74.12MiB  2022-10-24T18:46:18Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2009.csv
  70.33MiB  2022-10-24T18:46:48Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2010.csv
  69.53MiB  2022-10-24T18:46:59Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2011.csv
  75.99MiB  2022-10-24T18:47:45Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2012.csv
  69.53MiB  2022-10-24T18:47:31Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2013.csv
  62.33MiB  2022-10-24T18:48:04Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2014.csv
  59.76MiB  2022-10-24T18:50:55Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2015.csv
  61.19MiB  2022-10-24T18:48:39Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2016.csv
  60.92MiB  2022-10-24T18:44:34Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2017.csv
  60.80MiB  2022-10-24T18:49:15Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2018.csv
  59.37MiB  2022-10-24T18:49:47Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2019.csv
  48.47MiB  2022-10-24T18:50:54Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2020.csv
  47.42MiB  2022-10-24T18:51:19Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2021.csv
  41.85MiB  2022-10-24T18:51:48Z  gs://test_data_lake_denzoom/raw/chicago/Crimes_-_2022.csv
TOTAL: 22 objects, 1629339498 bytes (1.52GiB)
PS C:\Users\Joanna> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/san_francisco/
 227.24MiB  2022-10-24T18:43:24Z  gs://test_data_lake_denzoom/raw/san_francisco/Police_Department_Incident_Reports_2018_to_Present.csv
 525.42MiB  2022-10-24T18:51:54Z  gs://test_data_lake_denzoom/raw/san_francisco/Police_Department_Incident_Reports_Historical_2003_to_May_2018.csv
TOTAL: 2 objects, 789224651 bytes (752.66MiB)
PS C:\Users\Joanna> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/los_angeles/
 511.52MiB  2022-10-24T18:45:25Z  gs://test_data_lake_denzoom/raw/los_angeles/Crime_Data_from_2010_to_2019.csv
 142.99MiB  2022-10-24T18:43:15Z  gs://test_data_lake_denzoom/raw/los_angeles/Crime_Data_from_2020_to_Present.csv
TOTAL: 2 objects, 686299110 bytes (654.51MiB
```