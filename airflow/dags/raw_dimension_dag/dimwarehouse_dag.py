from sched import scheduler
from airflow.exceptions import AirflowException, AirflowFailException, AirflowNotFoundException
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

#extract .env config varibles
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
    
    dag_id = "warehouse_data_orchestrator",
    default_args= default_args,
    description= "Orchestratin warehouse dimension",
    schedule= "@daily",
    catchup=False,
    start_date= datetime(2025, 1, 1),
    tags = ["spark", "dimension"]
)

#create dag function
def extract_warehouse_data():
    
    try:
    
        landing_folder, raw_layer_folder = load_folder_jobs()
        
        """ 
            Orchestrating warehouse pipeline
        """
        
        start = EmptyOperator(task_id = "start_load")
        
        extract_warehouse_data = SparkSubmitOperator(
            
            task_id = "extract_warehouse_data",
            application = f"{landing_folder}/advwarehouse_extract_raw.py",
            conn_id= "sparkconn",
            verbose= True
        )
        
        write_to_warehouse_iceberg_table = SparkSubmitOperator(
            
            task_id = "write_to_warehouse_table",
            application = f"{raw_layer_folder}/advwarehouse_extract_raw.py",
            conn_id = "sparkconn",
            verbose= True
        )
        
        end = EmptyOperator(task_id= "end", trigger_rule="all_success")
        
    except AirflowNotFoundException as e:
        logging.error(f"Airflow found Exception error: {e}")
        
    except AirflowException as e:
        logging.error(f"Airflow found an error exception while running dag: {e}")
        
    except AirflowNotFoundException as e:
        logging.error(f"File or configurations not found. Verify: {e}")
        
    except Exception as e:
        logging.error(f"Unnexpected error occurred. Verify: {e}")
    
    start >> extract_warehouse_data >> write_to_warehouse_iceberg_table >> end
 
#declaring dag   
dag = extract_warehouse_data()

#logging sucessfull execution
logging.info("Dag completed sucessfully")