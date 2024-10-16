import sys
from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dags.utils import log_task_start, log_task_end
from datetime import timedelta
from transform.scripts.transform_box_score import transform_box_score
from transform.scripts.transform_statistics import transform_statistics

sys.path.append('/opt/airflow')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transform_box_scores_dag',
    default_args=default_args,
    description='Transform NBA box scores data.',
    catchup=False,
) as dag:
    start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_task_start,
        op_args=['transform_box_scores_dag']
    )
    transform_statistics_task = PythonOperator(
        task_id='transform_statistics',
        python_callable=transform_statistics,
    )
    transform_box_scores_task = PythonOperator(
        task_id='transform_box_score',
        python_callable=transform_box_score,
    )
    end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_task_end,
        op_args=['transform_box_scores_dag'],
    )
    trigger_load_box_scores_dag = TriggerDagRunOperator(
        task_id='load_games',
        trigger_dag_id='load_box_scores_dag',
        conf=XComArg(transform_box_scores_task)
    )
    start_log >> transform_statistics_task >> transform_box_scores_task >> end_log >> trigger_load_box_scores_dag