from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
from extract.scripts.extract_players import extract_players
from dags.utils import log_task_start, log_task_end

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
    'exrtract_players_dag',
    default_args=default_args,
    description='Extract NBA players and their respective URLs',
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['extract_schedule_dag']
    )
    extract_players_task = PythonOperator(
        task_id='extract_players',
        python_callable=extract_players,
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['extract_schedule_dag']
    )

    start_log >> extract_players_task >> end_log