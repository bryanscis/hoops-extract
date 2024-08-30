import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.data_config import all_players_dataset, all_current_players_dataset
from dags.utils import log_task_start, log_task_end, get_current_season
from datetime import timedelta
from extract.scripts.extract_players import extract_all_players
from scripts.nba_player_list import extract_current_players

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
    'extract_players_dag',
    default_args=default_args,
    description='Extract NBA players',
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['extract_schedule_dag']
    )
    extract_all_players_task = PythonOperator(
        task_id='extract_all_players',
        python_callable=extract_all_players,
        outlets=[all_players_dataset]
    )
    extract_current_players_task = PythonOperator(
        task_id='extract_current_players',
        python_callable=extract_current_players,
        op_args=[str(get_current_season())],
        outlets=[all_current_players_dataset]
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['extract_schedule_dag']
    )

    start_log >> extract_all_players_task >> extract_current_players_task >> end_log