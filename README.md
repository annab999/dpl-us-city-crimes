# End-to-end Pipeline Project
Stuff is still in main [README.md](../README.md)

## Version Matrices
This project was built and tested on these platforms/software:

| Platform | OS | Software | Version | Notes |
| --- | --- | :---: | :--: | ---: |
| desktop | Windows 10 | gcloud SDK | 407.0.0 | workstation |
| desktop | Windows 10 | Terraform | 1.3.2 | workstation |
| VM | CentOS 7 | gcloud SDK | 407.0.0 | host machine |
| VM | CentOS 7 | Docker Engine | 20.10.20 | host machine |

The apps and software used by the project:

| Platform | OS | Software | Version | Notes |
| --- | --- | :---: | :--: | ---: |
| container | Debian 11 | Python | 3.9.15 | for Airflow |
| container | Debian 11 | gcloud SDK | 407.0.0 | for Airflow |
| container | Debian 11 | Postgres | 13.8 | for Airflow |
| container | Debian 11 | OpenJDK | 17.0.2 | for spark-submit |
| container | Debian 11 | Apache Airflow | 2.4.2 | with CeleryExecutor |
| container | Debian 11 | Anaconda | 4.12 | for Jupyter (dev) |
| container | Debian 11 | Python | 3.9.12 | for Spark |
| container | Debian 11 | OpenJDK | 17.0.2 | for Spark |
| container | Debian 11 | Apache Spark | 3.3.1 |  |
| cloud | - | BigQuery |  | managed |
| cloud | - | dbt |  | managed |

## Deployment Instructions
1. Clone the repo. Only files from this (`project`) folder will be used, however.
2. Set up your GCP (trial) account. Create service accounts, assign corresponding roles, and download JSON keys for the following:
   - Airflow: *Bigquery Admin, Storage Admin, Storage Object Admin, Viewer*
   - Spark: *Storage Admin, Storage Object Admin, Viewer*
   - dbt: *Data Editor, Job User, User, Data Viewer*
3. Set up your Docker host machine / workstation.
   - The following are required:
     - Linux OS
     - at least 15GB of available disk space
     - a user with sufficient rights to run Docker, write stuff
   - Copy the following there:
     - this folder
     - service account JSON keys
4. Edit [.env](./.env) with your GCP details and `CREDS` (JSON) locations. Run `id -u` and use the output as value to `AIRFLOW_UID`.
5. On your host machine, edit and run the following:
   ```
   ### start docker service to be sure
   $ systemctl start docker
   ### replace the placeholder with the path to the project dir
   $ cd </path/to/>project
   ### build the app images
   $ docker compose build
   ### initialize the Airflow DB
   $ docker compose up airflow-init
   ### start up Airflow and Spark services
   $ docker compose up
   ```
6. Access Airflow via `https://<docker-host-address>:8080` with default `airflow/airflow`. Spark master GUI is also accessible at `https://<docker-host-address>:8081`.
7. Trigger `proj_get_data_dag` and wait to finish
8. Trigger `proj_process_data_dag` and wait to finish
9. <GLS page>

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
  > Get the Xcom instead of XcomArg in Jinja template or inside task callback function
  
  Again, unconfirmed and, can't really apply this to my BashOp.
  
- **Resolution**: Have to wait for a patch/workaround

### [Airflow] Apparently runs from `$AIRFLOW_HOME`, not `$AIRFLOW_HOME/dags`! >:(
Below are my own info logs. This had been causing issues with opening the CSV files in `parse_link` tasks.
```
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------WE ARE IN /opt/***-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/ are: ['dags', 'logs', 'plugins', 'include', '***.cfg', 'webserver_config.py', '***-worker.pid']-------
[2022-10-18, 03:59:34 PST] {task_functions.py:8} INFO - ---------stuff in /opt/***/include/ are: ['austin.csv', 'chicago.csv', 'los_angeles.csv', 'san_francisco.csv']-------
```
- **Observations**: Tried the following to no avail:
  - with, without trailing slash in `template_searchpath` attribute to DAG, and consequently in `open()` method
  - use relative, absolute paths in `template_searchpath` attribute to DAG
  - use relative path in `open()` method

  Had I tried absolute path in `open()` method immediately, it would have been working already.
  
- **Resolution**: stop relying on ~that body~ `template_searchpath`, at least in file `open()`

## Terrible Mistakes courtesy of Me
My stupidity in action.

### [Airflow] Wrong input type (`dict` of `list`s) passed to `map()` callable
My code:
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

- **Observations**: Only noticed my mistake while `diff`-ing with test DAG that worked (using ID-ed XCom arg) :( I sincerely thought that the errors were issues with XCom / dynamic task mapping / `.expand()`, because I was using a combo of this sort in this part.

  So all my searches were around these lines, and nothing I find could really directly pertain to my issue. My snobbish self even thought I was affected by https://github.com/apache/airflow/issues/25061, but of course I wasn't.

  Until I played around and `diff`-ed.
  
- **Resolution**: Fix function to return `list` (of `dict`s)

### [Airflow] Wrong input type `list` defined and coded in `map()` callable
Task run error:
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

- **Observations**: Kept getting this error and kept thinking it was, again (as my earlier related mistake), an Airflow bug from the combo of features I was working on. Only realized the *real* issue after having fixed the earlier issue with a set of more self-aware eyes.

- **Resolution**: Fix function to use arg of each item of the mapped `list` (alternatively, could use combo of `.expand_kwargs()` and a separate function)

## Back to Development Issues

### [Airflow] DAGs/tasks sometimes become non-performant/buggy with time
I wasn't able to take a screenshot, but the boxes for dead tasks are flattened squares instead of the usual. And they're never executed nor shaded any color at all.
- **Observations**: This happens after multiple edits to the DAG file and its tasks. It's like Airflow DB drowns in confusion and isn't able to recover, for that DAG.
  
- **Resolution**: Need to restart Airflow, or recreate DAG with a new name. Do every now and then, to avoid false negatives and hours of wasted debugging T_T

### [Airflow] Crosstalk(?) among child tasks within the `<task_var>.output` of an Operator
![airflow_mixedoutput_taskinstance_issue.png](docu/airflow_mixedoutput_taskinstance_issue.png?raw=true "Airflow mixed output issue with task instances")

- **Observations**: You could imagine my surprise. Seems it's due to some queueing / race condition with the different values (1 for each city) stored in my `<parse_task_var>.output`, wherein I'm unable to specify the task ID.
  
- **Resolution**: Use `ti.xcom_pull(key='<key>', task_ids='<task_id>')` to specify value. Don't forget to use **complete task ID** (this is not the task var!)

### [Docker] Directory permissions error during startup of Airflow containers
Error when composing the rest of the airflow containers (after `airflow-init`):
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

- **Observations**: This came up only after I refactored my `compose.yaml` to include both Airflow and Spark containers. Had been making some changes and added variables in all compose files since of the merging, but Airflow and Spark setups were working separately.

  Noticed the permissions on my host Airflow directories (generated and existing) seemed to be typical.

  Tried the following to no avail *(italicizing items I consequently also used during actual fix)*:
    - revert personal `USER ${AIRFLOW_USER}`, which I passed as `ARG` from `compose.yaml`, to original `USER $AIRFLOW_UID`, which should never have been passed since only defined in `.env`
      - *also up Postgres, Redis (I forgot to specify them along with other Airflow services)*
        - *also remove `logs/`, `plugins/` folders before composing*
          - *also rebuild with cache cleared*
    - *revert* back *to my personal `USER ${AIRFLOW_USER}` build-time arg*
      - with above sub-bullets also done
    - run standalone Airflow `compose.yaml` that worked from before, with these changes:
      - same `.env` file as complete compose project (with Spark)
      - in a temporary `temp/` folder outside of the original `airflow/` folder
  
  While running the last test above ad comparing preprocessed configs, I noticed that a source for a bind mount was set in `project/` (host *working* directory), when I believe it's supposed to be `project/airflow/` (host *Airflow* directory):
  ```
  services:
    airflow-init:
  ...
    volumes:
      - type: bind
        source: /home/j*********ph/dezoomcamp/project
        target: /sources
  ```
  I actually missed that single dot in `- .:sources/` while updating paths and actually `grep`-ping volume defs! :<

- **Resolution**: Update host path in bind mount def for `sources/` (used in initialization of Airflow) to actual Airflow directory. Look closer at volume mappings!

### [Docker] Permissions error during Jupyter startup (in Spark container)
Error when starting up the Spark container, which also runs Jupyter, at least for dev:
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

- **Observations**: Build is successful. Spark seems to run fine, and error is due to the container `CMD` opening a Jupyter notebook. Won't be a problem in prod once I remove Jupyter, but I'll also solve this now.

  All should be solved by passing `--allow-root` to the command and using `root` for the entire build, but not recommended, hence I set up a service user. Tried a lot of stuff here as I thought it was due to my custom user and directories, and in the end realized the error was with the *host side* of the bind mount:
  ```
  Container project-spark-1  Recreated                                                           0.1s
  Attaching to project-spark-1
  project-spark-1  | total 4
  project-spark-1  | drwxrwxr-x. 2 1475897537 1475897537   24 Oct 21 14:17 .
  project-spark-1  | drwxr-xr-x. 1 root       root         25 Oct 21 12:01 ..
  project-spark-1  | -rw-rw-r--. 1 1475897537 1475897537 1801 Oct 21 14:17 Dockerfile
  ```

  Remembered the last step in the Airflow Dockerfile to switch to user with the my host user's UID: `USER ${AIRFLOW_UID}`, where `AIRFLOW_UID=$(id -u)` on the host. When I use this UID in the Dockerfile though, the build gets stuck at `exporting layers` (second to last step):
  ```
  ...
  #9 7.849 ++ hash -r
  #9 DONE 8.1s

  #10 exporting to image
  #10 exporting layers
                                        # stalls forever
  ```

  Realized this only happens when I assign UID to my custom user, but not when switching `USER` arg at the last step. But still seems wrong...

  Also finally remembered `chmod` and `chown`. All combos did not work in container directory, then *finally* (group `root` or `gid` 0 was sufficient for my case because I added my custom user to that group in the container):
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

  - **Resolution**: apply `chown :0 -R <host_dir_of_bind_mount>` once on *host side* of the bind mount, as `root` while in container

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

- **Observations**: Inaccurate size listed for the build cache. Started happening after my build stalls, multiple rebuilds, build aborts. Although I always run `docker builder prune` when I rebuild, seems some data is still cached until system restart?
  
- **Resolution**: Restart docker service after many failed/test builds

### [Airflow] False positive incomplete stream transfers by BashOp
So the whole time, I've been using the files from the first successful DAG run, and turns out the files were incompletely transferred.

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
  - tcp params
    - `$ cat /proc/sys/net/ipv4/tcp_keepalive_time` gave 7200s, which doesn't seem to apply to this case
  - `gcloud storage cp` ref has no timeout params
  
  Then, tried running 1 city only and worked. So tried the following together:
  - remove my `sleep` delay: *seems this introduced more problems than solved any*
  - set even lower `max_active_tis_per_dag`
    - 2: worked 
    - 4: a few false positives, **without `curl (18)` errors**!
    - *(for science)* none: more false positives with  `curl(18)` errors
    - 3: worked

- **Resolution**: Remove my initial sleep delay! Set `max_active_tis_per_dag` to < 4

### [Spark] Repartitioning optimization experiments with 1 year data
Main thing to notes: it took >2mins each month for writing to parquet from `df_csv` with minor (time) transformations, while it took 2mins for whole year for writing to parquet from `df_csv` with no transformations.
- **Observations**: Tried the following:
  - `df.repartition()` values (best to good location in script)
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

- **Resolution**: More experiments needed but most especially, read more on optimizations (`.repartition()`, `.coalesce()`, `.parallelize()`, `.cache()`)
### [Spark-Airflow] Getting remote `spark-submit` to work
Manual trial from Airflow container:
```
$ spark-submit
/home/airflow/.local/lib/python3.7/site-packages/pyspark/bin/load-spark-env.sh: line 68: ps: command not found
JAVA_HOME is not set
```
- **Observations**: Turns out that as opposed to multiple instructions online to unpack [Spark tgz](https://spark.apache.org/downloads.html) onto remote Airflow executor, Spark binaries are already in Airflow containers upon install of `pip` module `apache-airflow-providers-apache-spark`, and are callable from `/home/airflow/.local/bin` (already in `PATH`).

- **Resolution**: Need to install `procps` and Spark prerequisite OpenJDK during build of Airflow images

### [Docker] `ENV` step in Dockerfile not interpolating bash command output
In an effort to remove hardcoding of `py4j` lib file in the Spark Dockerfile, I set the following code for updating `PYTHONPATH` env var:
```
ENV PYTHONPATH="${SPARK_HOME}/python/lib/$(ls ${SPARK_HOME}/python/lib/ | grep py4j):${SPARK_HOME}/python:${PYTHONPATH}"
```
However, env var resolution within the container is:
```
$ echo $PYTHONPATH
/opt/spark-3.3.1-bin-hadoop3/python/lib/$(ls /opt/spark-3.3.1-bin-hadoop3/python/lib/ | grep py4j):/opt/spark-3.3.1-bin-hadoop3/python:
```
- **Observations**: Can't just isolate the bash command and save output to an `ARG`, nor does it seem proper to save the output string as another `ENV`. Tried checking for the proper syntax in `ENV` or an implementation via `RUN`, but found https://github.com/moby/moby/issues/29110 (still open).

- **Resolution**: Revert to hardcoded `"${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${SPARK_HOME}/python:${PYTHONPATH}"` and wait for suggested `RUN` feature to be implemented

### [Airflow] `SparkSubmitOperator` parsing of master URL from Airflow *connection* is very weird
When I set `AIRFLOW_CONN_PROJECT_SPARK=spark://project-spark-1:7077`, output is:
```
[2022-10-30, 15:32:11 UTC] {spark_submit.py:334} INFO - Spark-Submit cmd: spark-submit --master project-spark-1:7077 --py-files city_vars.py --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --name prepare_data_austin --verbose test_file_read.py ...
...
[2022-10-30, 15:32:42 UTC] {spark_submit.py:485} INFO - Parsed arguments:
[2022-10-30, 15:32:42 UTC] {spark_submit.py:485} INFO - master                  project-spark-1:7077
```
Or when I set `AIRFLOW_CONN_PROJECT_SPARK=spark://spark://project-spark-1:7077`, output is:
```
[2022-10-31, 08:50:40 UTC] {spark_submit.py:334} INFO - Spark-Submit cmd: spark-submit --master spark --py-files /opt/***/include/city_vars.py --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --name prepare_data_austin --verbose /opt/***/include/test_file_read.py ...
...
[2022-10-31, 08:50:46 UTC] {spark_submit.py:485} INFO - Parsed arguments:
[2022-10-31, 08:50:46 UTC] {spark_submit.py:485} INFO - master                  spark
```
But when I create the connection via GUI and set the `host` field value to be `spark://project-spark-1`, output is desired:
```
[2022-10-30, 18:58:18 UTC] {spark_submit.py:334} INFO - Spark-Submit cmd: spark-submit --master spark://project-spark-1:7077 --py-files city_vars.py --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --name prepare_data_austin --verbose test_file_read.py ...
...
[2022-10-30, 18:58:29 UTC] {spark_submit.py:485} INFO - Parsed arguments:
[2022-10-30, 18:58:29 UTC] {spark_submit.py:485} INFO - master                  spark://project-spark-1:7077
```
- **Observations**: Seems this is the [source code](https://github.com/apache/airflow/blob/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5/airflow/models/connection.py#L58) for parsing from a URI connection:
  ```
  def _parse_netloc_to_hostname(uri_parts):
    """Parse a URI string to get correct Hostname."""
    hostname = unquote(uri_parts.hostname or '')
    if '/' in hostname:
      hostname = uri_parts.netloc
      if "@" in hostname:
        hostname = hostname.rsplit("@", 1)[1]                 # remember this for later
      if ":" in hostname:
        hostname = hostname.split(":", 1)[0]                  # this is where the sorcery happens
      hostname = unquote(hostname)
    return hostname
  ```
  while below is the [source code](https://github.com/apache/airflow/blob/dcdcf3a2b8054fa727efb4cd79d38d2c9c7e1bd5/airflow/models/connection.py#L129) for a GUI connection, which explains why the GUI-registered connection turns out fine:
  ```
  if uri:
    self._parse_from_uri(uri)                 # which leads to the above function
  else:
    self.conn_type = conn_type
    self.host = host                          # this gives the correct master URL
    ...
  ```
  And yet `spark-submit` requires the `spark://` prefix (among other possible prefixes) for it to work. In the 1st scenario above, which I would like to note is exactly how it is formatted in [the docs](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html#howto-connection-spark), following is the error:
  ```
  [2022-10-30, 15:32:42 UTC] {spark_submit.py:485} INFO - Exception in thread "main" org.apache.spark.SparkException: Master must either be yarn or start with spark, mesos, k8s, or local
  [2022-10-30, 15:32:42 UTC] {spark_submit.py:485} INFO - at org.apache.spark.deploy.SparkSubmit.error(SparkSubmit.scala:975)
  [2022-10-30, 15:32:42 UTC] {spark_submit.py:485} INFO - at org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment(SparkSubmit.scala:238)
  ...
  [2022-10-30, 15:32:43 UTC] {taskinstance.py:1851} ERROR - Task failed with exception
  Traceback (most recent call last):
    File "/home/airflow/.local/lib/python3.7/site-packages/airflow/providers/apache/spark/operators/spark_submit.py", line 157, in execute
      self._hook.submit(self._application)
    File "/home/airflow/.local/lib/python3.7/site-packages/airflow/providers/apache/spark/hooks/spark_submit.py", line 417, in submit
      f"Cannot execute: {self._mask_cmd(spark_submit_cmd)}. Error code is: {returncode}."
  airflow.exceptions.AirflowException: Cannot execute: spark-submit --master project-spark-1:7077 --py-files city_vars.py --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --name prepare_data_austin --verbose test_file_read.py ...
  ```
  (Same effect when I set it up as `AIRFLOW_CONN_SPARK_DEFAULT`, and when I set `deploy-mode=cluster`.) But from the source code above, it seems to parse out this same prefix, when connection is registered in URI format.

  Tried running this in preparation for just passing it as an `ENTRYPOINT` to the containers:
  ```
  $ airflow connections add t_spark --conn-json '{"conn_type":"spark","host":"spark://project-spark-1","port": 7077}'
  /home/airflow/.local/lib/python3.7/site-packages/airflow/configuration.py:367: FutureWarning: ...
  Successfully added `conn_id`=t_spark : spark://:@spark://project-spark-1:7077
  ```
  That inserted `@` is weird and familiar (see 1st observed code snippet above). Now, the output is *correct* ?!???!:
  ```
  [2022-10-31, 13:26:50 UTC] {spark_submit.py:334} INFO - Spark-Submit cmd: spark-submit --master spark://project-spark-1:7077 --py-files /opt/***/include/city_vars.py --jars https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --name prepare_data_austin --verbose /opt/***/include/test_file_read.py austin {{ gs_bkt }} raw/austin/2016_Annual_Crime_Data.csv
  [2022-10-31, 13:27:20 UTC] {spark_submit.py:485} INFO - Using properties file: null
  [2022-10-31, 13:27:22 UTC] {spark_submit.py:485} INFO - Parsed arguments:
  [2022-10-31, 13:27:22 UTC] {spark_submit.py:485} INFO - master                  spark://project-spark-1:7077
  ```
  Tried to apply that hidden pre-parsing step by `SparkSubmitOperator` for non-URI connections, as a *workaround* on my URI env var `AIRFLOW_CONN_PROJECT_SPARK: 'spark://:@spark://${SPARK_HOSTNAME}:7077'`, however, it still parsed the master URL as just `spark` (same as 2nd error output above).
  
  Looking back at the code snippet in question, seems it parses the string before the `:` character even *after* parsing with the `@` involved (maybe `if ":" in hostname:` should be an `elif`?)
- **(Ironic) Resolution**: Use a JSON input (instead of URI) to the connection env var, which is **exactly what I've been avoiding** the whole time as [the docs recommended a URI syntax](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html#howto-connection-spark))

### [Service] Template

- **Observations**: Stuff

- **Resolution**: Stuff

## TODOs:
- try: ti.xcom in `map()` func
- `curls = parse_link.output.map(parse_bash)`
- `curls.value[item]`
- set up smooth error handling in webscraping script if no more remaining results to scrape from page
- spark-jupyter: need to apply #docker-permissions-error-during-jupyter-startup-in-spark-container everytime new file is placed in `$FILES_HOME`
- read pq, set up cols, add col for each type, union all, sql queries (groupby loc, date/mon, type; sums and avgs), write report
- NTS: always check for NULL values, duplicates before processing
- strip whitespace from austin 'Highest NIBRS/UCR Offense Description', 'GO Location'
  - F.trim('Highest NIBRS/UCR Offense Description')
- remove whitespace from los angeles within Location, Cross Street
- try spark submit in cluster mode
- try user defined macros for curls func, templates_dict for pythonop
- use task decorators, task branching
- https://stackoverflow.com/questions/7194939/git-change-one-line-in-file-for-the-complete-history
- slim-airflow branch - next step: separate ROOT and AIRFLOW_USER installs (apt-get, pip)

### Before running prod
- update airflow .env bucket
- remove DEBUG logging, example dags
- upgrade version
  - airflow gcloud 406
- when to ${SPARK_HOME}/sbin/stop-master.sh

### File sizes for reference
```
PS> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/austin/
   7.22MiB  2022-10-24T18:42:49Z  gs://test_data_lake_denzoom/raw/austin/2016_Annual_Crime_Data.csv
   6.75MiB  2022-10-24T18:43:50Z  gs://test_data_lake_denzoom/raw/austin/2017_Annual_Crime.csv
   3.91MiB  2022-10-24T18:43:53Z  gs://test_data_lake_denzoom/raw/austin/2018_Annual_Crime.csv
   7.52MiB  2022-10-24T18:42:57Z  gs://test_data_lake_denzoom/raw/austin/Annual_Crime_Dataset_2015.csv
TOTAL: 4 objects, 26640531 bytes (25.41MiB)
PS> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/chicago/
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
PS> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/san_francisco/
 227.24MiB  2022-10-24T18:43:24Z  gs://test_data_lake_denzoom/raw/san_francisco/Police_Department_Incident_Reports_2018_to_Present.csv
 525.42MiB  2022-10-24T18:51:54Z  gs://test_data_lake_denzoom/raw/san_francisco/Police_Department_Incident_Reports_Historical_2003_to_May_2018.csv
TOTAL: 2 objects, 789224651 bytes (752.66MiB)
PS> gcloud storage ls --long --readable-sizes gs://test_data_lake_denzoom/raw/los_angeles/
 511.52MiB  2022-10-24T18:45:25Z  gs://test_data_lake_denzoom/raw/los_angeles/Crime_Data_from_2010_to_2019.csv
 142.99MiB  2022-10-24T18:43:15Z  gs://test_data_lake_denzoom/raw/los_angeles/Crime_Data_from_2020_to_Present.csv
TOTAL: 2 objects, 686299110 bytes (654.51MiB
```
