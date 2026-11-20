from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG

APP = "/dockervol/spark/src/entrypoint/person_customer_app.py"

DEFAULT_ARGS = {
    "owner": "data-engeneering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def layer_task(layer: str, extra_args: list[str] | None = None) -> SparkSubmitOperator:
    
    return SparkSubmitOperator(
        task_id = f"{layer}_person_customer",
        application=APP,
        application_args=["--layer", layer, *(extra_args or [])],
        conn_id= "sparkconn",
        env_vars={"PYTHONPATH": "/dockervol:/dockervol/configs"},
        verbose=True,
    )
    
    
with DAG(
    dag_id = "nexomart_person_customer_dag",
    default_args=DEFAULT_ARGS,
    schedule = None,
    catchup = False,
    max_active_runs=1,
    tags = ["spark", "nexomart", "person"]
) as dag:
    
    start = EmptyOperator(task_id="start")
    bronze = layer_task("bronze")
    silver = layer_task("silver")
    gold = layer_task("gold", ["--reference-date", "{{ ds }}"])
    end = EmptyOperator(task_id="end")
    
    start.set_downstream(bronze)
    bronze.set_downstream(silver)
    silver.set_downstream(gold)
    gold.set_downstream(end)