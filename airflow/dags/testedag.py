from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator


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
    catchup = False
)
# Início do pipeline
start_load = EmptyOperator(
task_id='start_dag'
)


spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/airflow/apps/shared/novacarga.py',
    conn_id='sparkconn',
    dag=dag,
)

end_load = EmptyOperator(
task_id='end_dag'
)

start_load >> spark_submit_task >> end_load 
