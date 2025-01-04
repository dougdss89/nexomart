from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'iceberg_dag',
    default_args=default_args,
    description='Um exemplo de DAG para rodar jobs Spark',
    schedule='@daily',
    catchup=False
)

# Início do pipeline
start_load = EmptyOperator(
    task_id='start_teste'
)

spark_submit_task = SparkSubmitOperator(
    task_id='submit_iceberg_dag',
    application='/opt/airflow/dags/iceberg.py',
    name='arrow-spark',
    conn_id='sparkconn',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.executor.extraClassPath': '/opt/spark/jars/*',
        'spark.driver.extraClassPath': '/opt/spark/jars/*'
    },
    packages='org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.5.1',
    verbose=True,
    dag=dag
)

# Fim do pipeline
end_load = EmptyOperator(
    task_id='end_teste'
)

start_load >> spark_submit_task >> end_load
