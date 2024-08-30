import sys
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dags.data_config import schedule_dataset
from dags.utils import log_task_start, log_task_end
from extract.scripts.extract_schedule import extract_schedule

sys.path.append('/opt/airflow')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_schedule_dag',
    default_args=default_args,
    description='Extract NBA schedule annually',
    schedule_interval='@yearly',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['extract_schedule_dag']
    )
    extract_schedule_task = PythonOperator(
        task_id='extract_schedule',
        python_callable=extract_schedule,
        outlets=[schedule_dataset]
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['extract_schedule_dag']
    )

    start_log >> extract_schedule_task >> end_log