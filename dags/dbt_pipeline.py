from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import sys
from docker.types import Mount
import os

sys.path.insert(0, '/opt/project')
from extract.ingestion import run_ingest

PROJECT_PATH = os.getenv("PROJECT_PATH")


default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    }

DBT_DIR = '/opt/project/dw_healthcare'
DBT_DOCKER_URL = "unix://var/run/docker.sock"
DBT_NETWORK_MODE = "bridge"
DBT_IMAGE = "fazlan-dbt:1.5.0"

def make_dbt_task(task_id,command):
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        api_version='auto',
        command=command,
        auto_remove=True,
        docker_url=DBT_DOCKER_URL,
        network_mode=DBT_NETWORK_MODE   ,
        working_dir=DBT_DIR,
        mounts=[
            Mount(source=PROJECT_PATH,
                   target='/opt/project', type='bind')
        ])


with DAG(
    dag_id='malaysia_dwh_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025,1,1),
    catchup=False,
) as dag:
    
    ingest = PythonOperator(
        task_id='run_ingest',
        python_callable=run_ingest,
    )
    
    dbt_debug   = make_dbt_task("dbt_debug",   "dbt debug --profiles-dir /opt/project")
    dbt_deps    = make_dbt_task("dbt_deps",    "dbt deps --profiles-dir /opt/project")
    dbt_compile = make_dbt_task("dbt_compile", "dbt compile --profiles-dir /opt/project")
    dbt_snapshot = make_dbt_task("dbt_snapshot", "dbt snapshot --profiles-dir /opt/project")
    dbt_run     = make_dbt_task("dbt_run",     "dbt run --profiles-dir /opt/project")


    ingest >> dbt_debug >> dbt_deps >> dbt_compile >>  dbt_run >> dbt_snapshot

    