import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.utils import log_task_start, log_task_end
from datetime import timedelta
from load.scripts.load_schedule import load_schedule

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
    'load_schedule_dag',
    default_args=default_args,
    description='Loads schedule into database.',
    catchup=False
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['load_schedule']
    )
    load_schedule_task= PythonOperator(
        task_id='load_schedule_task',
        python_callable=load_schedule
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['load_schedule'],
    )

    start_log >> load_schedule_task >> end_log