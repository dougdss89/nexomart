from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'spark_job_example',
    default_args=default_args,
    description='Um exemplo de DAG para rodar jobs Spark',
    schedule_interval='@daily',
)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/airflow/sparkapp/testehadoop.py',
    conn_id='sparkconn',
    dag=dag,
)

spark_submit_task
