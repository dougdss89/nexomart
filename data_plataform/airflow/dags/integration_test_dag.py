"""
integration_test_dag — Teste de fogo end-to-end (PostgreSQL → full Spark pipeline → Iceberg/UC → Trino → dbt)

Flow:
  seed_postgres
      → pg_landing  (PostgreSQL → s3a://landing Parquet)
      → pg_bronze   (Parquet landing → Delta @ s3a://bronze)
      → pg_silver   (Delta bronze → Delta @ s3a://silver)
      → pg_gold     (Delta silver → Iceberg @ s3a://deltadw/iceberglake + registro no UC)
      → trino_validate  (SELECT COUNT(*) FROM nexo.gold.gold_dim_productcategory)
      → dbt_run     (cria view nexo.gold.gold_mart_productcategory sobre a tabela Iceberg)
      → dbt_test
      → end
"""
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime
import logging
import socket

from configconnections_loader import load_config, validate_required_keys

logger = logging.getLogger(__name__)

default_args = {
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

DBT_PROJECT_DIR  = "/dockervol/dbt"
DBT_PROFILES_DIR = "/dockervol/dbt"


def load_folders():
    config = load_config()
    jdf = config.get("job_dag_folder", {})
    validate_required_keys(
        jdf,
        ["airflow_landing_folder", "airflow_rawlayer_folder",
         "airflow_silverlayer_folder", "airflow_goldlayer_folder"],
        "job_dag_folder",
    )
    return (
        jdf["airflow_landing_folder"],
        jdf["airflow_rawlayer_folder"],
        jdf["airflow_silverlayer_folder"],
        jdf["airflow_goldlayer_folder"],
    )


def validate_trino_gold(**context):
    import trino

    conn = trino.dbapi.connect(
        host="trino-coordinator",
        port=8080,
        user="airflow",
        catalog="iceberglake",
        schema="gold",
    )
    cur = conn.cursor()
    cur.execute("SELECT count(*) AS cnt FROM gold_dim_productcategory")
    count = cur.fetchall()[0][0]
    logger.info("gold_dim_productcategory row count via Trino (iceberglake): %s", count)

    if count == 0:
        raise AirflowException("gold_dim_productcategory is empty — pipeline may have failed silently")

    print(f"Trino validation passed: {count} rows in iceberglake.gold.gold_dim_productcategory")


@dag(
    dag_id="integration_e2e_test",
    default_args=default_args,
    description=(
        "End-to-end test: seed PostgreSQL → Landing → Bronze → Silver → "
        "Gold (Iceberg/UC) → Trino → dbt"
    ),
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 12, 26),
    tags=["test", "e2e", "spark", "iceberg", "dbt", "trino", "postgres"],
)
def integration_e2e_test():
    landing_folder, raw_folder, silver_folder, gold_folder = load_folders()

    driver_ip = socket.gethostbyname(socket.gethostname())
    spark_env = {
        "PROJECT_BASE_DIR": "/dockervol/configurations",
        "PYTHONPATH": "/dockervol/configurations",
    }
    spark_conf = {
        "spark.driver.host": driver_ip,
        "spark.driver.bindAddress": "0.0.0.0",
    }

    start = EmptyOperator(task_id="start")

    # ── Step 0: Seed PostgreSQL ───────────────────────────────────────────────
    seed_postgres = BashOperator(
        task_id="seed_postgres",
        bash_command=(
            "uv run python /dockervol/dbapi/api/seeder/product/product_category.py "
            "--min-count 500 --max-count 1500"
        ),
    )

    # ── Step 1: Landing (PostgreSQL → Parquet @ s3a://landing) ───────────────
    pg_landing = SparkSubmitOperator(
        task_id="pg_landing",
        application=f"{landing_folder}/pg_productcategory_extract.py",
        conn_id="sparkconn",
        verbose=True,
        env_vars=spark_env,
        conf=spark_conf,
    )

    # ── Step 2: Bronze / Raw (Parquet → Delta @ s3a://bronze) ────────────────
    pg_bronze = SparkSubmitOperator(
        task_id="pg_bronze",
        application=f"{raw_folder}/pg_productcategory_extract_raw.py",
        conn_id="sparkconn",
        verbose=True,
        env_vars=spark_env,
        conf=spark_conf,
    )

    # ── Step 3: Silver (Bronze Delta → Silver Delta @ s3a://silver) ──────────
    pg_silver = SparkSubmitOperator(
        task_id="pg_silver",
        application=f"{silver_folder}/pg_dim_productcategory.py",
        conn_id="sparkconn",
        verbose=True,
        env_vars=spark_env,
        conf=spark_conf,
    )

    # ── Step 4: Gold (Silver Delta → Iceberg HadoopCatalog + registro UC → nexo.gold) ──
    pg_gold = SparkSubmitOperator(
        task_id="pg_gold",
        application=f"{gold_folder}/advgold_dim_productcategory_iceberg.py",
        conn_id="sparkconn",
        verbose=True,
        env_vars=spark_env,
        conf=spark_conf,
    )

    # ── Step 5: Trino validation ──────────────────────────────────────────────
    trino_validate = PythonOperator(
        task_id="trino_validate",
        python_callable=validate_trino_gold,
    )

    # ── Step 6: dbt run ───────────────────────────────────────────────────────
    DBT_FLAGS = (
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target-path /tmp/dbt_target_advworks "
        f"--log-path /tmp/dbt_logs_advworks"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run {DBT_FLAGS} --select gold_mart_productcategory"
        ),
    )

    # ── Step 7: dbt test ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test {DBT_FLAGS} --select gold_mart_productcategory"
        ),
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_success")

    (
        start
        >> seed_postgres
        >> pg_landing
        >> pg_bronze
        >> pg_silver
        >> pg_gold
        >> trino_validate
        >> dbt_run
        >> dbt_test
        >> end
    )


dag = integration_e2e_test()
