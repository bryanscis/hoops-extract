import sys
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dags.utils import log_task_start, log_task_end
from dags.data_config import all_players_dataset, all_current_players_dataset, cleaned_current_players_dataset
from datetime import timedelta
from transform.scripts.transform_players import transform_players, update_current_players_file, check_player_changes, complete_verification

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
    schedule=[all_current_players_dataset],
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['transform_players_dag']
    )
    transform_current_players_task = PythonOperator(
        task_id='transform_current_players',
        python_callable= transform_players,
        inlets=[all_current_players_dataset, all_players_dataset],
        provide_context=True
    )
    check_player_changes_task = BranchPythonOperator(
        task_id='check_player_changes',
        python_callable=check_player_changes,
        provide_context=True
    )
    wait_for_verification = TriggerDagRunOperator(
        task_id='wait_for_verification',
        trigger_dag_id='verify_unmatched_players',
        wait_for_completion=True
    )
    update_current_players_file_task = PythonOperator(
        task_id='update_current_players_file',
        python_callable=update_current_players_file
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['transform_players_dag'],
        trigger_rule='none_failed_min_one_success',
        outlets=[cleaned_current_players_dataset]
    )
    start_log >> transform_current_players_task >> check_player_changes_task

    check_player_changes_task >> [wait_for_verification, end_log]

    wait_for_verification >> update_current_players_file_task
    update_current_players_file_task >> end_log

with DAG(
    'verify_unmatched_players',
    description='Manual verification DAG to check if unmatched players have correct names.',
    schedule_interval=None,
) as dag:
    
    complete_verification_task = PythonOperator(
    task_id='complete_verification',
    python_callable=complete_verification
    )

    complete_verification_task