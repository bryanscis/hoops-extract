import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.utils import log_task_start, log_task_end
from dags.load.scripts.load_box_scores import load_box_scores
from dags.load.scripts.load_statistics import load_statistics
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
    'load_box_scores_dag',
    default_args=default_args,
    description='Loads box scores into database.',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['load_box_scores_dag']
    )
    load_statistics_task = PythonOperator(
        task_id= 'load_statistics',
        python_callable=load_statistics,
    )
    load_box_scores_task = PythonOperator(
        task_id= 'load_box_scores',
        python_callable=load_box_scores,
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['load_box_scores_dag'],
    )

    start_log >> load_statistics_task  >>load_box_scores_task >> end_log