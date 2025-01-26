from sched import scheduler
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import logging
from configconnections_loader import load_config, validate_required_keys
from traceback import print_exc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
        landing_folder = config.get("job_dag_folder", {}).get("airflow_landing_folder","")
        raw_layer_folder = config.get("job_dag_folder", {}).get("airflow_rawlayer_folder","")
        
        validate_required_keys(config= config.get("job_dag_folder", {}),
                               required_keys=["airflow_landing_folder", "airflow_rawlayer_folder"],
                               config_name="job_dag_foler")
        
        logging.info("Variables imported sucessfully")
        
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
    
    dag_id = "currency_code_orchestrator",
    default_args= default_args,
    description= "Orquestrating currency code dimension",
    schedule="@daily",
    catchup = False,
    start_date=datetime(2025, 1, 25),
    tags=["spark", "dimension"]
)

def extract_currency_code():
    
    landing_folder, raw_layer_folder = load_folder_jobs()
    
    start = EmptyOperator(task_id="start_load")
    
    extract_currency_data = SparkSubmitOperator(
        task_id = "extract_currency_code_data",
        application = f"{landing_folder}/advcurrency_code_extract_raw.py",
        conn_id = "sparkconn",
        verbose = True
    )
    
    write_to_currency_code_iceberg_table = SparkSubmitOperator(
        task_id = "write_to_currencycode_table",
        application = f"{raw_layer_folder}/advcurrency_code_extract_raw.py",
        conn_id= "sparkconn",
        verbose = True
    )
    
    end = EmptyOperator(task_id="end", trigger_rule= "all_success")
    
    start >> extract_currency_data >> write_to_currency_code_iceberg_table >> end
    
    
dag = extract_currency_code()
logging.info("Dag completed sucessfully")