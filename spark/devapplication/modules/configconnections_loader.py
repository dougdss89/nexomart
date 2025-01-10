import os
from typing import Dict, Any
from dotenv import dotenv_values

""" this function imports .env file to load jobs and dags configurations """

def find_env_value(start_path: str = os.getcwd()) -> str:
    
    """
        search for .env file in current and system dir
        
        :param start_path: initial path for search file .env
        :return full .env path 
        :raises filenotfounderror: if .env file not founded

    """
    
    current_dir = start_path
    
    while current_dir != os.path.dirname(current_dir): #while .env file not found
        potential_path = os.path.join(current_dir, '.env')
        
        if os.path.exists(potential_path): #check if .env file exists
            return potential_path
        
        current_dir = os.path.dirname(current_dir) 
        
    raise FileNotFoundError(".env file was not founded in any directory")
    
    
def load_config(env_path: str = None) -> Dict[str, Any]:
    
    """
        load and prepare .env variables, substituting dynamic placeholders for string
        values
        
        :param: env_path: path for .env file
        :return Dict with environment variables
        :raises filenotfounderror: if .env file was not founded
    
    """
    #if env_path is none, search for .env file
    if not env_path:
        env_path = find_env_value()        
    
    config = dotenv_values(env_path)
    
    if not config:
        raise FileNotFoundError(f"Fail to load .env file from {env_path}. Please, verify if file exists.")        
    

    def replace_env_vars(value: str)-> str:
        """_summary_  replace environment variables on $VAR or ${VAR} format, with correct values.
                    prioritize .env variables than system environment variables  

        Args:
            value (str): _description_  

        Returns:
            str: Strings configurations
        """

        if not isinstance(value, str):
            return value
        
        for var_name, var_value in config.items():
            if var_value:
                
                value = value.replace(f"${{{var_name}}}", var_value)
                value = value.replace(f"${var_name}", var_value)
                
        return value

    processed_config = {
        key: replace_env_vars(value)
        for key, value in config.items()
    }
    
    configurations = {
        "mssql_sources": {
            "adventureworks": {
                "url": processed_config.get("MSSQLURLADVENTUREWORKS"),
                "database": processed_config.get("MSSQLDATABASEADVENTUREWORKS"),
                "properties": {
                    "user": processed_config.get("SPARKUSER"),
                    "password": processed_config.get("SPARKPWD"),
                    "driver": processed_config.get("MSSQLDRIVER"),
                },
            },
            "wideworldimporters": {
                "url": processed_config.get("MSSQLURLWIDEWORLD"),
                "database": processed_config.get("MSSQLWIDEWORLD_DB"),
                "properties": {
                    "user": processed_config.get("SPARKUSER"),
                    "password": processed_config.get("SPARKPWD"),
                    "driver": processed_config.get("MSSQLDRIVER"),
                },
            },
        },
        "postgresql_destination": {
            "default": {
                "url": processed_config.get("POSTGRECONNURL"),
                "database": processed_config.get("PGDW"),
                "properties": {
                    "user": processed_config.get("SPARKUSER"),
                    "password": processed_config.get("SPARKPWD"),
                    "driver": processed_config.get("PGDRIVER"),
                },
            }
        },
        "hadoop_paths": {
            "landing": processed_config.get("HADOOP_CLUSTER_LANDING"),
            "raw_icebergdb": processed_config.get("ICEBERG_RAW_DB"),
            "silver_icebergdb": processed_config.get("ICEBERG_SILVER_DB"),
            "goldtable": processed_config.get("HADOOP_CLUSTER_GOLD"),
            "spark_warehouse": processed_config.get("HADOOP_WAREHOUSE"),
        },
        "spark_jars": {
            "mssql_spark_jars": processed_config.get("MSSQLSPARKJAR"),
            "postgresql_jar": processed_config.get("POSTGREJAR")
        },
        "job_dag_folder": {
            "airflow_landing_folder": processed_config.get("AIRFLOW_SPARK_JOBS_FOLDER_LANDING_SCRIPT"),
            "airflow_rawlayer_folder": processed_config.get("AIRFLOW_SPARK_JOBS_FOLDER_RAW_SCRIPT"),
            "spark_landing_folder": processed_config.get("SPARK_LANDING_FOLDER"),
            "spark_rawlayer_folder": processed_config.get("SPARK_RAW_LAYER_SCRIPT"),
            "airflow_dag_folder": processed_config.get("DAGSFOLDER")
        },
        "redis": {
            "redis_host": processed_config.get("REDIS_HOST"),
            "redis_port": processed_config.get("REDIS_PORT"),
            "redis_db": processed_config.get("REDIS_DB"),
            "redis_pwd": processed_config.get("REDIS_PASSWORD"),
            "redis_key": processed_config.get("REDIS_KEY")
        },
        "raw_config": processed_config
    }

    return configurations

