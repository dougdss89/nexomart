from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from modules import load_configurations, validate_required_keys # type: ignore
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    
    "owner": "spark_airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0
}

#loading config variables folder

def load_folders_job():
    
    try:
        config = load_config() # type: ignore
        landing_folder = config.get("job_dag_folder", {}).get("airflow_landing_folder", "")
        raw_layer_folder = config.get("job_dag_folder", {}).get("airflow_rawlayer_folder", "")
        
        validate_required_keys(config = config.get("job_dag_folder", {}),
                            required_keys = ["airflow_landing_folder", "airflow_rawlayer_folder"],
                            config_name = "job_dag_folder")
        
        return landing_folder, raw_layer_folder
    
    except ModuleNotFoundError as e:
        return f"Impossible to load module. Please verify: {e}"
    
    except Exception as e:
        return f"An exception error has occur. Please verify: {e}"
    
# starting to write dag

@dag(
    dag_id= "country_dimension_orchestrator",
    default_args= default_args,
    description= "orchestrating country dimension",
    schedule = "@daily",
    catchup = False,
    start_date= datetime(2025, 1, 9),
    tags = ["spark", "dimension"]
)

def extract_dimcountry_source():
    
    landing_folder, raw_layer_folder = load_folders_job()
    
    start = EmptyOperator(task_id= "Start")
    
    extract_country_data = SparkSubmitOperator(
        task_id = "extract_country_to_parquet",
        application= f"{landing_folder}/advcountryregion_extract_raw.py",
        conn_id = "sparkconn",
        verbose= False
    )
    
    write_to_country_iceberg_table = SparkSubmitOperator(
        task_id = "write_country_to_table",
        application= f"{raw_layer_folder}/advcountryregion_extract_raw.py",
        conn_id = "sparkconn",
        verbose= False
    )
    
    end = EmptyOperator(task_id= "End")
    
    start >> extract_country_data >> write_to_country_iceberg_table >> end
    
dag = extract_dimcountry_source()