from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'doug',
    'start_date': datetime(2026, 1, 1),
    'retries': 0 # Sem retry para vermos se falha de primeira
}

with DAG(
    dag_id='stress_test_minio_s3a',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['admin', 'test', 'infrastructure']
) as dag:

    # Nós passamos recursos pesados na "mala" do Airflow para exigir da máquina
    run_stress_test = SparkSubmitOperator(
        task_id='run_stress_test_job',
        conn_id='sparkconn',
        application='/dockervol/spark/jobs/landing/stress_test_minio.py',
        name='Stress_Test_MinIO',
        conf={
            'spark.executor.memory': '6g',
            'spark.executor.cores': '4',
            'spark.driver.memory': '2g'
        },
        verbose=True
    )

    run_stress_test