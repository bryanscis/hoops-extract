import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.utils import log_task_start, log_task_end
from dags.load.scripts.load_players import load_players
from data_config import cleaned_current_players_dataset
from datetime import timedelta

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
    'load_players_dag',
    default_args=default_args,
    description='Loads players into database.',
    catchup=False,
    schedule=[cleaned_current_players_dataset]
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['extract_schedule_dag']
    )
    load_players_task = PythonOperator(
        task_id= 'load_players',
        python_callable=load_players,
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['extract_schedule_dag'],
    )

    start_log >> load_players_task >> end_log