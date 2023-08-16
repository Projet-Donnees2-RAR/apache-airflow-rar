from airflow import DAG
from datetime import datetime
from tasks.extract import create_extract_task
from tasks.transform import create_transform_task
from tasks.load import create_load_task

default_args = {
    'owner': 'reichmann',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 0
}

dag = DAG(
    dag_id='RAR-GROUP-DAG',
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily'
)

extract = create_extract_task(dag)
transform = create_transform_task(dag)
load = create_load_task(dag)

extract >> transform >> load