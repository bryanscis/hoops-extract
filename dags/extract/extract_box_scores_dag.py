import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dags.utils import log_task_start, log_task_end
from extract.scripts.extract_box_scores import extract_box_scores
from dataset import schedule_dataset, box_scores_dataset, statistics_dataset

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
    'extract_box_scores_dag',
    default_args=default_args,
    description='Extract NBA box scores',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['extract_box_scores_dag']
    )
    
    extract_box_scores_task = PythonOperator(
        task_id='check_and_extract_box_scores',
        python_callable=extract_box_scores,
        inlets=[schedule_dataset],
        outlets=[box_scores_dataset, statistics_dataset]
    )
    
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['extract_box_scores_dag']
    )

    start_log >> extract_box_scores_task >> end_log
