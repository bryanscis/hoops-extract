import sys
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.utils import log_task_start, log_task_end
from dags.transform.scripts.transform_schedule import transform_schedule
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
    'transform_schedule_dag',
    default_args=default_args,
    description='Transform NBA schedule',
    catchup=False,
) as dag:
    
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['transform_schedule_dag']
    )
    transform_schedule_task = PythonOperator(
        task_id='transform_schedule',
        python_callable=transform_schedule
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['transform_schedule_dag']
    )
    start_log >> transform_schedule_task >> end_log