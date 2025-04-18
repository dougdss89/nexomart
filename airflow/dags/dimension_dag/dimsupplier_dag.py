from airflow.decorators import dag
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import  AirflowException
import logging

# Agora podemos importar nosso módulo
from dimension_dag.configconnections_loader import load_config, validate_required_keys


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger=logging.getLogger(__name__)

default_args={
    
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    
}

#extract .env config varibles
def load_folders_job():
    
    try:
        config=load_config()
        
        job_dag_folder=config.get("job_dag_folder", {})
        
        validate_required_keys(config=config.get("job_dag_folder", {}),
                            required_keys=["airflow_landing_folder", 
                                           "airflow_rawlayer_folder", 
                                           "airflow_silverlayer_folder"],                            config_name="job_dag_folder")
                
        landing_folder=job_dag_folder.get("airflow_landing_folder", "")
        raw_layer_folder=job_dag_folder.get("airflow_rawlayer_folder", "")
        silver_layer_folder=job_dag_folder.get("airflow_silverlayer_folder", "")
        
        logging.info("Variables imported sucessfully")
        
        return landing_folder, raw_layer_folder, silver_layer_folder
    
    except ModuleNotFoundError as e:
        logger.error(f"Impossible to load module. Please verify: {str(e)}")
        raise AirflowException(f"Impossible to load module. Please verify: {str(e)}")
    
    except Exception as e:
        logger.error(f"An exception error has occur. Please verify: {str(e)}")
        raise AirflowException(f"An exception error has occur. Please verify: {str(e)}")
        
@dag(
    
    dag_id="supplier_dimension_orchestrator",
    default_args= default_args,
    description= "Orchestratin supplier dimension",
    schedule=None,
    catchup=False,
    start_date= datetime(2025, 1, 1),
    tags=["spark", "dimension"]
)

#create dag function
def extract_supplier_data():
    
    try:
    
        landing_folder, raw_layer_folder=load_folders_job()
        
        """ 
            Orchestrating supplier pipeline
        """
        
        Start_load=EmptyOperator(task_id="Start_load")
        
        Extract_supplier_data=SparkSubmitOperator(
            
            task_id="Extract_supplier_data",
            application=f"{landing_folder}/advsupplier_extract_raw.py",
            conn_id="sparkconn",
            verbose=True
        )
        
        Write_to_supplier_bronze_layer=SparkSubmitOperator(
            
            task_id="Write_to_supplier_table",
            application=f"{raw_layer_folder}/advsupplier_extract_raw.py",
            conn_id="sparkconn",
            verbose=True
        )
        
        End=EmptyOperator(task_id= "end", trigger_rule="all_success")
        
    except AirflowNotFoundException as e:
        logging.error(f"Airflow found Exception error: {e}")
        
    except AirflowException as e:
        logging.error(f"Airflow found an error exception while dag run: {e}")
        
    except AirflowNotFoundException as e:
        logging.error(f"File or configurations not found. Verify: {e}")
        
    except Exception as e:
        logging.error(f"Unnexpected error occurred. Verify: {e}")
    
    Start_load >> Extract_supplier_data >> Write_to_supplier_bronze_layer >> End
 
#declaring dag   
dag=extract_supplier_data()

#logging sucessfull execution
logging.info("Dag completed sucessfully")