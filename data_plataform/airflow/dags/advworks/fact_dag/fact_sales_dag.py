import logging
from datetime import datetime

from airflow.decorators import dag
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Now we can import our module
from configconnections_loader import load_config, validate_required_keys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Default DAG configuration
default_args = {
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def load_folders_job():
    try:
        config = load_config()

        job_dag_folder = config.get("job_dag_folder", {})

        validate_required_keys(
            config=config.get("job_dag_folder", {}),
            required_keys=[
                "airflow_landing_folder",
                "airflow_rawlayer_folder",
                "airflow_silverlayer_folder",
            ],
            config_name="job_dag_folder",
        )

        landing_folder = job_dag_folder.get("airflow_landing_folder", "")
        raw_layer_folder = job_dag_folder.get("airflow_rawlayer_folder", "")
        silver_layer_folder = job_dag_folder.get("airflow_silverlayer_folder", "")

        logging.info("Variables imported sucessfully")

        return landing_folder, raw_layer_folder, silver_layer_folder

    except ModuleNotFoundError as e:
        logger.error(f"Impossible to load module. Please verify: {str(e)}")
        raise AirflowException(f"Impossible to load module. Please verify: {str(e)}")

    except Exception as e:
        logger.error(f"An exception error has occur. Please verify: {str(e)}")
        raise AirflowException(f"An exception error has occur. Please verify: {str(e)}")


def spark_handle_failure(context):
    task_instance = context["task_instance"]
    error_message = f"Task {task_instance.task_id} has failed to execute. {context['exception']}"

    logger.error(f"{str(error_message)}")
    raise AirflowException(f"{error_message}")


@dag(
    dag_id="fact_sales_orchestrator",
    description="Orchestrating Fact purchasing table",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    tags=["spark", "fact"],
)
def Extract_fact_sales():
    try:
        landing_folder, raw_layer_folder, silver_layer_folder = load_folders_job()

        """_summary_ dag to extract fact sales data
        """

        Start = EmptyOperator(task_id="Start_load")

        Extract_fact_sales_data = SparkSubmitOperator(
            task_id="Extract_fact_sales_data",
            application=f"{landing_folder}/advfact_sales_extract_raw.py",
            conn_id="sparkconn",
            verbose=True,
        )

        Write_to_fact_sales_bronze_layer = SparkSubmitOperator(
            task_id="Write_to_fact_sales_bronze_layer",
            application=f"{raw_layer_folder}/advfact_sales_extract_raw.py",
            conn_id="sparkconn",
            verbose=True,
            on_failure_callback=spark_handle_failure,
        )

        Write_to_fact_sales_silver_layer = SparkSubmitOperator(
            task_id="Write_to_fact_sales_silver_layer",
            application=f"{silver_layer_folder}/advfact_sales.py",
            conn_id="sparkconn",
            verbose=True,
            on_failure_callback=spark_handle_failure,
        )

        End = EmptyOperator(task_id="End", trigger_rule="all_success")

        (
            Start
            >> Extract_fact_sales_data
            >> Write_to_fact_sales_bronze_layer
            >> Write_to_fact_sales_silver_layer
            >> End
        )

    except FileNotFoundError as e:
        logger.error(f"File error: {str(e)}")
        raise AirflowException(f"File not found: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise AirflowException(f"DAG failed: {str(e)}")


dag = Extract_fact_sales()
logging.info("Dag completed sucessfully")
