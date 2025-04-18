from airflow.decorators import dag
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import  AirflowException
import logging

# Agora podemos importar nosso módulo
from dimension_dag.configconnections_loader import load_config, validate_required_keys

# Configuração de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger=logging.getLogger(__name__)

# Configurações padrão do DAG
default_args={
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Carregar as pastas do fluxo de trabalho
def load_folders_job():
    
    try:
        config=load_config()
        
        job_dag_folder=config.get("job_dag_folder", {})
        
        validate_required_keys(config=config.get("job_dag_folder", {}),
                            required_keys=["airflow_landing_folder", 
                                           "airflow_rawlayer_folder", 
                                           "airflow_silverlayer_folder"],
                            config_name="job_dag_folder")
                
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
    
def handle_spark_failure(context):
    
    task_instance=context["task_instance"]
    error_message=f"Task {task_instance.task_id} has failed to execute: {context['exception']}"
    
    logger.error(f"{str(error_message)}")
    raise AirflowException(error_message)

# DAG principal
@dag(
    dag_id="buyer_dimension_orchestrator",
    default_args=default_args,
    description="DAG para orquestrar a dimensão de comprador",
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 12, 26),
    tags=["spark", "dimension"]
)
def extract_dimbuyer_data():
    try:
        # Inicialização de variáveis
        landing_folder, raw_layer_folder, silver_layer_folder=load_folders_job()

        Start_load=EmptyOperator(task_id="Start_load")
        logger.info("Dag has been Start_loaded successfully")

        Extract_buyer_data=SparkSubmitOperator(
            task_id='Extract_buyer_data',
            application=f"{landing_folder}/advbuyer_extract_raw.py",
            conn_id='sparkconn',
            verbose=True,
            on_failure_callback=handle_spark_failure
        )
        logger.info("Data has been extracted from database sucessfully")
            
        Write_to_buyer_bronze_layer=SparkSubmitOperator(
            task_id="write_buyer_bronze_layer",
            application=f"{raw_layer_folder}/advbuyer_extract_raw.py",
            conn_id="sparkconn",
            verbose=True,
            on_failure_callback=handle_spark_failure
        )
        logger.info("Data has been writted on bronze layer sucessfully")
        
        Write_to_buyer_silver_layer=SparkSubmitOperator(
            task_id="write_buyer_silver_layer",
            application=f"{silver_layer_folder}/advdim_buyer.py",
            conn_id="sparkconn",
            verbose=True,
            on_failure_callback=handle_spark_failure
        )
        logger.info("Data has been writted on silver layer sucessfully")

        End=EmptyOperator(task_id="End_load", trigger_rule="all_success")
        logger.info("Dag has been finished successfully")

        Start_load >> Extract_buyer_data >> Write_to_buyer_bronze_layer >> Write_to_buyer_silver_layer >> End
                
    except FileNotFoundError as e:
        logger.error(f"File error: {str(e)}")
        raise AirflowException(f"File not found: {str(e)}")
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise AirflowException(f"DAG failed: {str(e)}")     

# Instanciar o DAG
dag=extract_dimbuyer_data()
logging.info("Dag completed sucessfully")