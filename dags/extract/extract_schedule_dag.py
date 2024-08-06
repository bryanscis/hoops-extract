from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
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
    
    extract_task = PythonOperator(
        task_id='extract_schedule',
        python_callable=extract_schedule,
    )
    extract_task