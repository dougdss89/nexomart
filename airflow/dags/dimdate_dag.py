from sched import scheduler
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import logging
from configconnections_loader import load_config, validate_required_keys
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
        
        validate_required_keys(
            
            config = config.get("job_dag_folder", {}),
            required_keys=["airflow_landing_folder", "airflow_landing_folder"],
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
    
    dag_id = "date_dimension_orchestrator",
    default_args= default_args,
    schedule = "@daily",
    start_date= datetime(2025, 1, 1),
    catchup = False,
    tags = ["spark", "dimension"],
    description= "Orchestrating date dimension"
)

def extract_date():
    
    landing_folder, raw_layer_folder = load_folder_jobs()
    
    """ 
        Dag to extract Date Dimension
    """
    
    start = EmptyOperator(task_id = "start_load")
    
    extract_date_data = SparkSubmitOperator(
        
        task_id = "extract_date_data",
        application = f"{landing_folder}/advdate_extract_raw.py",
        conn_id = "sparkconn",
        verbose= True
    )
    
    write_to_date_iceberg_table = SparkSubmitOperator(
        
        task_id = "write_to_date_table",
        application= f"{raw_layer_folder}/advdate_extract_raw.py",
        conn_id = "sparkconn",
        verbose = True
    )
    
    end = EmptyOperator(task_id = "end", trigger_rule= "all_success")
    
    start >> extract_date_data >> write_to_date_iceberg_table >> end
    
dag = extract_date()
logging.info("Dag completed sucessfully")