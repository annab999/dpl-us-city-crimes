# End-to-end Pipeline Project
In this project, I set up a sample end-to-end pipeline consisting of an orchestrator ([Airflow](https://airflow.apache.org/docs/) using CeleryExecutor), a data lake ([Google Cloud Storage](https://cloud.google.com/storage/docs)), a data warehouse ([Google BigQuery](https://cloud.google.com/bigquery/docs)), data processors ([Apache Spark](https://spark.apache.org/docs/latest/), [dbt](https://docs.getdbt.com/reference/dbt_project.yml)), an IaC tool ([Terraform](https://developer.hashicorp.com/terraform/docs)), containers ([Docker](https://docs.docker.com/reference/)), a virtual machine ([Google Compute Engine](https://cloud.google.com/compute/docs)), and a visualization tool ([Google Looker Studio](https://support.google.com/looker-studio/)).

## Data Information
The pipeline sources open data hosted by a (few) city government(s) in the U.S. via its/their (respective) public portal(s). Datasets of interest are annual crime reports per city over the last two decades, or whichever years are available within said period:
- [Chicago crimes 2001-2022](https://data.cityofchicago.org/browse?limitTo=filters&q=Crimes+-+2001&sortBy=alpha)
- [~~San Francisco police incident reports 2003-2022~~](https://data.sfgov.org/browse?limitTo=datasets&q=Police+Department+Incident+Reports&sortBy=alpha&tags=crime+reports)
- [Los Angeles crime data 2010-2022](https://data.lacity.org/browse?limitTo=datasets&q=Crime+Data+from&sortBy=alpha&tags=crime)
- [Austin crime annual crime 2015-2018](https://data.austintexas.gov/browse?limitTo=datasets&q=%22Annual+Crime%22&sortBy=relevance&tags=police) (first 4 results)

Screenshots of currently generated dashboards are below. The first [visualizes Chicago-specific crime data](https://datastudio.google.com/reporting/57f987e6-cf5f-4996-9b3a-ae72ea6f40ac), while the second [collates and compares crime data among Chicago, LA, and Austin](https://datastudio.google.com/reporting/a282c47b-f114-4681-974a-2be9b61ea328).

![chicago-crime-data.png](./docu/chicago-crime-data.png?raw=true "Chicago Crime Data dashboard")
![cross-city-crime-data.png](./docu/cross-city-crime-data.png?raw=true "US Cities Crime Data dashboard")

## Pipeline Workflow:
The rough initial plan for the data pipeline was:
![projplan.png](./docu/projplan.png?raw=true "Project pipeline diagram")

Currently implemented data pipeline is:
*WIP*

with the following Airflow sub-DAGs / tasks in order of execution:
![airflow-dag1.png](./docu/airflow-dag1.png?raw=true "Airflow sub-DAG 1")
![airflow-dag2.png](./docu/airflow-dag2.png?raw=true "Airflow sub-DAG 2")
![airflow-dag3.png](./docu/airflow-dag3.png?raw=true "Airflow sub-DAG 3")
![airflow-dag4.png](./docu/airflow-dag4.png?raw=true "Airflow sub-DAG 4")

and the following dbt sub-DAGs / models:
![dbt-dag1.png](./docu/dbt-dag1.png?raw=true "dbt sub-DAG 1")
![dbt-dag2.png](./docu/dbt-dag2.png?raw=true "dbt sub-DAG 2")
![dbt-dag3.png](./docu/dbt-dag3.png?raw=true "dbt sub-DAG 3")

*Notes in the making of this project are documented in [docu](./docu).*

## Version Matrices
This project was built and tested on these platforms/tools:

| Platform | OS | Software | Version | Notes |
| --- | --- | :---: | :--: | ---: |
| desktop | Windows 10 | gcloud SDK | 407.0.0 | workstation |
| desktop | Windows 10 | Terraform | 1.3.2 | workstation |
| VM | CentOS 7 | gcloud SDK | 407.0.0 | host machine |
| VM | CentOS 7 | Docker Engine | 20.10.20 | host machine |

The apps and software used:

| Platform | OS | Software | Version | Notes |
| --- | --- | :---: | :--: | ---: |
| container | Debian 11 | Python | 3.9.15 | for Airflow |
| container | Debian 11 | gcloud SDK | 407.0.0 | for Airflow |
| container | Debian 11 | Postgres | 13.8 | for Airflow |
| container | Debian 11 | OpenJDK | 17.0.2 | for Airflow spark-submit |
| container | Debian 11 | Apache Airflow | 2.4.2 | with CeleryExecutor |
| container | Debian 11 | Anaconda | 4.12 | for Jupyter (dev) in Spark master |
| container | Debian 11 | Python | 3.9.12 | for Spark master |
| container | Debian 11 | Python | 3.9.15 | for Spark worker |
| container | Debian 11 | OpenJDK | 17.0.2 | for Spark master and worker |
| container | Debian 11 | Apache Spark | 3.3.1 | Standalone mode; for Spark master and worker |
| cloud | - | BigQuery | - | managed |
| cloud | - | dbt Cloud | 1.3.0 | managed |
| cloud | - | Google Looker Studio | - | managed |

## Deployment Instructions (WIP)
Follow the steps below to recreate the pipeline from scratch:
1. Clone the repo.
2. Set up your GCP (trial) account. Create service accounts, assign corresponding roles, and download JSON keys for the following:
   - Airflow: *Bigquery Admin, Storage Admin, Storage Object Admin, Viewer*
   - Spark: *Storage Admin, Storage Object Admin, Viewer*
   - dbt: *BigQuery Data Editor, BigQuery Job User, BigQuery User, BigQuery Data Viewer*
3. Set up your dbt Cloud account. Create a service account API token for Airflow with the role `Job Admin` and the project for this specified.
4. Set up your Docker host machine / workstation.
   - The following are required:
     - Linux OS
     - Docker Engine, Docker Compose plugin
     - at least 15GB of available disk space
     - a user with sufficient rights to run Docker, write stuff
   - Copy the following there:
     - this folder
     - your service account JSON keys
5. Edit [.env](./.env) with your GCP details and `CREDS` (JSON) locations. Run `id -u` and use the output as value to `AIRFLOW_UID`.
6. Connect and log on to your host machine. As this implementation uses gcloud SDK/CLI in some step, ensure that your machine is authenticated with GCP.
   - If using a Compute Engine VM, ensure machine has a service account attached, and the following API scopes enabled (at least): *BigQuery, Cloud Storage*
7. Modify and run the following:
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
8. Access Airflow via `https://<docker-host-address>:8080` with default `airflow/airflow`. Spark master GUI is also accessible at `https://<docker-host-address>:8081`.
9. Trigger `proj_get_data_dag`
10. *dbt*
11. *GLS page*

## Relevant links
- [city-crimes project](https://console.cloud.google.com/home/dashboard?authuser=3&orgonly=true&project=city-crimes) on GCP
- [Looker Studio](https://datastudio.google.com/u/3/)
- dbt Cloud project [city-crimes dev](https://cloud.getdbt.com/next/accounts/119670/projects/180263/develop)
- DTC's [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) (2022 cohort)
- US [Crime Open DB](https://github.com/mpjashby/crime-open-database/tree/master/crime_categories)
- Chicago [IUCR Codes](https://data.cityofchicago.org/widgets/c7ck-438e)
