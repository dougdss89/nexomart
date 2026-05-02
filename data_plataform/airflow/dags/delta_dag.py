from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    "delta_dag",
    default_args=default_args,
    description="Example DAG: Spark job writing Delta Lake tables via Unity Catalog OSS",
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    spark_delta = SparkSubmitOperator(
        task_id="submit_delta_job",
        application="/opt/airflow/spark/delta_sample.py",
        name="delta-unity-catalog",
        conn_id="sparkconn",
        # spark-defaults.conf already configures delta + unity catalogs;
        # only job-specific overrides go here.
        conf={
            "spark.sql.shuffle.partitions": "32",
        },
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_delta >> end
