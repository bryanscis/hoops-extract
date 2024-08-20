from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
from dags.utils import log_task_start, log_task_end
from dataset import all_players_dataset, current_players_dataset
from transform.scripts.transform_players import transform_players, update_current_players_file

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
    'transform_players_dag',
    default_args=default_args,
    description='Transform NBA player data',
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['extract_schedule_dag']
    )
    transform_current_players_task = PythonOperator(
        task_id='transform_current_players',
        python_callable= transform_players,
        inlets=[current_players_dataset, all_players_dataset]
    )
    update_current_players_file_task = PythonOperator(
        task_id='updated_current_players_file',
        python_callable=update_current_players_file
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['extract_schedule_dag']
    )

    start_log >> transform_current_players_task >> update_current_players_file_task >> end_log