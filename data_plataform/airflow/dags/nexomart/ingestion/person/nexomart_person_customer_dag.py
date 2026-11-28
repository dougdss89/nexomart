from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from nexomart.common.spark_driver import SparkDriverSubmitOperator

from airflow import DAG

APP = "/dockervol/spark/src/entrypoint/person_customer_app.py"

DEFAULT_ARGS = {
    "owner": "data-engeneering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def layer_task(layer: str, extra_args: list[str] | None = None) -> SparkDriverSubmitOperator:
    
    # The driver runs in the spark-driver container, not in airflow_worker.
    return SparkDriverSubmitOperator(
        task_id = f"{layer}_person_customer",
        application=APP,
        application_args=["--layer", layer, *(extra_args or [])],
        env_vars={"PYTHONPATH": "/dockervol:/dockervol/configs"},
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