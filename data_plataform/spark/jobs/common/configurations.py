import os
from typing import Dict, Any
from dotenv import dotenv_values

def find_env_file(start_path: str = None) -> str:
    """
    Looks for the .env file considering environment variables and Docker volumes.

    :param start_path: Optional starting path to look for the .env file
    :return: Full path to the .env file
    :raises FileNotFoundError: If the .env file is not found
    """
    base_dir = os.getenv('PROJECT_BASE_DIR')
    module_dir = os.getenv('MODULE_DIR')
    
    search_paths = []
    
    if start_path:
        search_paths.append(start_path)
    if module_dir:
        search_paths.append(module_dir)
    if base_dir:
        search_paths.append(base_dir)
    
    # Adds the current module's directory
    current_module_dir = os.path.dirname(os.path.abspath(__file__))
    search_paths.append(current_module_dir)

    # Adds the current working directory
    search_paths.append(os.getcwd())

    # === THE OFFICIAL VOLUME PATH (Added only once) ===
    search_paths.append('/dockervol/configs')

    print(f"Current directory: {os.getcwd()}")
    print(f"Module directory: {current_module_dir}")
    print("Configured search paths:")

    for path in search_paths:
        print(f"- {path}")

    # Looks for the .env file in each path and its parent directories
    for base_path in search_paths:
        current_dir = base_path
        while current_dir != os.path.dirname(current_dir):  # While it hasn't reached the root
            potential_path = os.path.join(current_dir, '.env')
            print(f"Checking: {potential_path}")

            if os.path.exists(potential_path):
                print(f"Found .env file at: {potential_path}")
                return potential_path

            current_dir = os.path.dirname(current_dir)

    raise FileNotFoundError("The .env file was not found in any of the search paths.")

def load_config(env_path: str = None) -> Dict[str, Any]:
    """
    Loads and prepares the configuration from the .env file, substituting dynamic placeholders.

    :param env_path: Path to the .env file. If None, the file will be looked up automatically.
    :return: A dictionary containing the processed configuration
    :raises FileNotFoundError: If the .env file is not found or is empty
    """
    # If the path isn't provided, look for the .env file in the current or parent directory
    if not env_path:
        env_path = find_env_file()

    # Loads the variables from the .env file
    config = dotenv_values(env_path)
    if not config:
        raise FileNotFoundError(f"Error loading the .env file from {env_path}. Check the path and format.")

    def replace_env_vars(value: str) -> str:
        """
        Replaces variables in the $VAR or ${VAR} format with their corresponding values.
        Prioritizes variables loaded from .env over system environment variables.
        """
        if not isinstance(value, str):
            return value

        for var_name, var_value in config.items():
            if var_value:  # Avoids replacing with null values
                value = value.replace(f"${{{var_name}}}", var_value)
                value = value.replace(f"${var_name}", var_value)
        return value

    # Processes all the .env variables
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
            "raw_deltadb": processed_config.get("DELTA_RAW_DB"),
            "silver_deltadb": processed_config.get("DELTA_SILVER_DB"),
            "goldtable": processed_config.get("HADOOP_CLUSTER_GOLD"),
            "spark_warehouse": processed_config.get("HADOOP_WAREHOUSE"),
            "minio_landing": processed_config.get("MINIO_LANDING"),
            "delta_catalog": processed_config.get("DELTA_MAIN_CATALOG"),
        },

        "delta_path": {
            "raw_layer": processed_config.get("DELTA_RAW_DB"),
            "silver_layer": processed_config.get("SILVER_LAYER"),
            "gold_layer": processed_config.get("GOLD_LAYER"),
        },
        
        "spark_jars": {
            "mssql_spark_jars": processed_config.get("MSSQLSPARKJAR"),
            "postgresql_jar": processed_config.get("POSTGREJAR")
        },
        "job_dag_folder": {
            "airflow_landing_folder": processed_config.get("AIRFLOW_SPARK_JOBS_FOLDER_LANDING_SCRIPT"),
            "airflow_rawlayer_folder": processed_config.get("AIRFLOW_SPARK_JOBS_FOLDER_RAW_SCRIPT"),
            # ADD THE LINE BELOW:
            "airflow_silverlayer_folder": processed_config.get("AIRFLOW_SPARK_JOBS_FOLDER_SILVER_SCRIPT"),
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


def validate_required_keys(config: Dict[str, str], required_keys: list, config_name: str):
    """
    Validates that the required keys are present in the configuration dictionary.

    :param config: Configuration dictionary
    :param required_keys: List of required keys
    :param config_name: Name of the component being validated
    :raises RuntimeError: If any key is missing
    """
    if not config or not all(key in config and config[key] for key in required_keys):
        missing_keys = [key for key in required_keys if not config.get(key)]
        raise RuntimeError(f"Missing required configuration in {config_name}: {', '.join(missing_keys)}")


def print_pretty_config(config: Dict[str, Any]) -> None:
    """
    Prints the configuration in a readable way.

    :param config: Configuration dictionary returned by load_config
    """
    import json
    print(json.dumps(config, indent=2))


if __name__ == "__main__":
    try:
        config = load_config()
        print("Configuration loaded successfully:")
        print("End of import")
        print_pretty_config(config)

        # Section validations
        validate_required_keys(config["hadoop_paths"], ["landing", "raw_deltadb", "silver_deltadb", "goldtable", "spark_warehouse"], "Hadoop Paths")
        validate_required_keys(config["spark_jars"], ["mssql_spark_jars", "postgresql_jar"], "Spark JARs")

    except Exception as e:
        print(f"Error loading configuration: {str(e)}")
        import traceback
        traceback.print_exc()

