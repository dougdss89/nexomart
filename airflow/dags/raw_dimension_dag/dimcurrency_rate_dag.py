from sched import scheduler
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import logging
from raw_dimension_dag.configconnections_loader import load_config, validate_required_keys
from traceback import print_exc

logging.basicConfig(level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


default_args = {
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def load_folder_jobs():
    
    try:
        config = load_config()

        landing_folder = config.get("job_dag_folder", {}).get("airflow_landing_folder", "")
        raw_layer_folder = config.get("job_dag_folder", {}).get("airflow_rawlayer_folder", "")

        validate_required_keys(config= config.get("job_dag_folder", {}),
                           required_keys= ["airflow_landing_folder", "airflow_rawlayer_folder"],
                           config_name="job_dag_folder"
                        )
    
        logging.info("Variables Imported Sucessfully")
    
        return landing_folder, raw_layer_folder
    
    except ImportError as e:
        logging.error(f"Error to import function load_config: {e}")
        print_exc()
        
    except ValueError as e:
        logging.error(f"Missing some variables: {e}")
        print_exc()
        
    except Exception as e:
        logging.error(f"Unexpected error. Please, verify: {e}")
        print_exc()

@dag(
    
    dag_id = "currency_rate_orchestrator",
    default_args= default_args,
    description= "Orchestrating Currency Rate Dimension",
    schedule= "@daily",
    catchup=False,
    start_date= datetime(2025, 1, 1),
    tags=["spark", "dimension"]
)

def extract_currency_rate():
    
    landing_folder, raw_layer_folder = load_folder_jobs()
    
    """
    Dag to extract currency rate dimension
    
    """
    
    start = EmptyOperator(task_id = "start_load")
    
    extract_currency_rate_data = SparkSubmitOperator(
        
        task_id = "extract_currency_rate_data",
        application= f"{landing_folder}/advcurrency_rate_extract_raw.py",
        conn_id = "sparkconn",
        verbose = True
    )
    
    write_to_currency_rate_iceberg_table = SparkSubmitOperator(
        
        task_id = "write_to_currency_rate_table",
        application=f"{raw_layer_folder}/advcurrency_rate_extract_raw.py",
        conn_id= "sparkconn",
        verbose = True
    )
    
    end = EmptyOperator(task_id = "end", trigger_rule="all_success")
    
    start >> extract_currency_rate_data >> write_to_currency_rate_iceberg_table >> end
    
dag = extract_currency_rate()

logging.info("Dag completed sucessfully")