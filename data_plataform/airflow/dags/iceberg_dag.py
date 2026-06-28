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
    description='Pipeline Iceberg v3 via Unity Catalog OSS (catálogo nexo)',
    schedule='@daily',
    catchup=False
)

start_load = EmptyOperator(task_id='start')

spark_submit_task = SparkSubmitOperator(
    task_id='submit_iceberg_dag',
    application='/opt/airflow/dags/iceberg.py',
    name='nexo-iceberg-pipeline',
    conn_id='sparkconn',
    packages='org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.7.1',
    conf={
        'spark.sql.extensions': (
            'io.delta.sql.DeltaSparkSessionExtension,'
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
        ),
        'spark.sql.catalog.nexo': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.nexo.catalog-impl': 'org.apache.iceberg.rest.RESTCatalog',
        'spark.sql.catalog.nexo.uri': 'http://unitycatalog:8080/api/2.1/unity-catalog/iceberg',
        'spark.sql.catalog.nexo.token': 'not-used',
        'spark.sql.catalog.nexo.warehouse': 'main',
        'spark.sql.catalog.nexo.format-version': '3',
    },
    verbose=True,
    dag=dag
)

end_load = EmptyOperator(task_id='end')

start_load >> spark_submit_task >> end_load
