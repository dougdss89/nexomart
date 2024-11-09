from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'iceberg_dag',
    default_args=default_args,
    description='Um exemplo de DAG para rodar jobs Spark',
    schedule_interval='@daily',
    catchup=False
)

# Início do pipeline
start_load = EmptyOperator(
    task_id='start_teste'
)

spark_submit_task = SparkSubmitOperator(
    task_id='submit_iceberg_dag',
    application='/opt/airflow/sparkapps/iceberg.py',
    conn_id='sparkconn',
    packages='org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0',  # Changed hyphen to colon before version
    # conf={
    #     'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    #     # You might also want to add these configurations for Iceberg
    #     'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
    #     'spark.sql.catalog.spark_catalog.type': 'hive'
    # },
    verbose=True,
    dag=dag
)

# Fim do pipeline
end_load = EmptyOperator(
    task_id='end_teste'
)

start_load >> spark_submit_task >> end_load