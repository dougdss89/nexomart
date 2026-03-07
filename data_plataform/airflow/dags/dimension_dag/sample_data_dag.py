from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from configconnections_loader import load_config, validate_required_keys


default_args = {    
    "owner": "spark",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    
}


def load_folders_job():
    
    config = load_config()
    
    job_dag_folder = config.get("job_dag_folder", {}).get("airflow_landing_folder", '')
    
    
    validate_required_keys(
        config = config.get("job_dag_folder", {}),
        required_keys = ["airflow_landing_folder"],
        config_name = "job_dag_folder"    
    )
    
    return job_dag_folder


@dag(
    dag_id = "sample_data_tpch_load",
    default_args = default_args,
    description = "Dag criado para carga de dados",
    # schedule = "interval", #change to daily
    schedule_interval = timedelta(minutes =6),
    catchup = False,
    start_date = datetime(2025, 4, 17),
    tags =  ["spark", "tpch", "sample"]
)

def load_sample_data():
    
    landing_folder = load_folders_job()
    
    Start_load =  EmptyOperator(task_id = "Start_load")
    
    load_tpch_sample_data = SparkSubmitOperator(
        task_id = "load_tpch_sample_data",
        application = f"{landing_folder}/sampledata.py",
        conn_id = "sparkconn",
        verbose = True)
        
    load_tpch_sample_mkt = SparkSubmitOperator(
        task_id = "load_tpch_sample_mkt",
        application = f"{landing_folder}/sampledata_mkt.py",
        conn_id = "sparkconn",
        verbose = True
    )
        
    End = EmptyOperator(task_id = "End_load", trigger_rule = "all_success")
    
    Start_load >> load_tpch_sample_data >> load_tpch_sample_mkt >> End
    
    
dag = load_sample_data()