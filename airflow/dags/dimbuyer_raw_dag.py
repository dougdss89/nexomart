import redis
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from modules.configconnections_loader import load_config, validate_required_keys
import logging

# Configuração de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurações padrão do DAG
default_args = {
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# Carregar as pastas do fluxo de trabalho
def load_folders_job():
    
    config = load_config()
    landing_folder = config.get("job_dag_folder", {}).get("airflow_landing_folder", '')
    raw_layer_folder = config.get("job_dag_folder", {}).get("airflow_rawlayer_folder", '')

    validate_required_keys(config=config.get("job_dag_folder", {}),
                           required_keys=["airflow_landing_folder", "airflow_rawlayer_folder"],
                           config_name="job_dag_folder")

    return landing_folder, raw_layer_folder

# DAG principal
@dag(
    dag_id="buyer_dimension_orchestrator",
    default_args=default_args,
    description="DAG para orquestrar a dimensão de comprador",
    schedule="@daily",
    catchup=False,
    start_date=datetime(2024, 12, 26),
    tags=["spark", "dimension"]
)

def extract_dimbuyer_source():
    # Inicialização de variáveis
    landing_folder, raw_layer_folder = load_folders_job()

    start = EmptyOperator(task_id="start_load")

    extract_buyer_data = SparkSubmitOperator(
        task_id="extract_buyer_to_parquet",
        application=f"{landing_folder}/advbuyer_extract_raw.py",
        conn_id="sparkconn",
        verbose=False,
    )

    write_to_buyer_iceberg_table = SparkSubmitOperator(
        task_id="write_buyer_iceberg_table",
        application=f"{raw_layer_folder}/advbuyer_extract_raw.py",
        conn_id="sparkconn",
        verbose=False,
    )

    end = EmptyOperator(task_id="end_load", trigger_rule="all_success")

    start >> extract_buyer_data >> write_to_buyer_iceberg_table >> end


# Instanciar o DAG
dag = extract_dimbuyer_source()