import os
from typing import Dict, Any
from dotenv import dotenv_values

def find_env_file(start_path: str = None) -> str:
    """
    Procura pelo arquivo .env considerando variáveis de ambiente e volumes Docker.

    :param start_path: Caminho inicial opcional para procurar o arquivo .env
    :return: Caminho completo para o arquivo .env
    :raises FileNotFoundError: Se o arquivo .env não for encontrado
    """
    # Primeiro, verifica se existe uma variável de ambiente apontando para o diretório base
    base_dir = os.getenv('PROJECT_BASE_DIR')
    module_dir = os.getenv('MODULE_DIR')
    
    search_paths = []
    
    # Adiciona caminhos de busca em ordem de prioridade
    if start_path:
        search_paths.append(start_path)
    if module_dir:
        search_paths.append(module_dir)
    if base_dir:
        search_paths.append(base_dir)
    
    # Adiciona o diretório do módulo atual
    current_module_dir = os.path.dirname(os.path.abspath(__file__))
    search_paths.append(current_module_dir)
    
    # Adiciona o diretório atual de trabalho
    search_paths.append(os.getcwd())
    
    print(f"Diretório atual: {os.getcwd()}")
    print(f"Diretório do módulo: {current_module_dir}")
    print("Caminhos de busca configurados:")
    for path in search_paths:
        print(f"- {path}")

    # Procura o .env em cada caminho e seus diretórios superiores
    for base_path in search_paths:
        current_dir = base_path
        while current_dir != os.path.dirname(current_dir):  # Enquanto não chegar na raiz
            potential_path = os.path.join(current_dir, '.env')
            print(f"Verificando: {potential_path}")
            
            if os.path.exists(potential_path):
                print(f"Arquivo .env encontrado em: {potential_path}")
                return potential_path
                
            current_dir = os.path.dirname(current_dir)

    raise FileNotFoundError("Arquivo .env não encontrado em nenhum dos caminhos de busca.")

def load_config(env_path: str = None) -> Dict[str, Any]:
    """
    Carrega e prepara as configurações do arquivo .env, substituindo placeholders dinâmicos.

    :param env_path: Caminho para o arquivo .env. Se None, o arquivo será buscado automaticamente.
    :return: Um dicionário contendo as configurações processadas
    :raises FileNotFoundError: Se o arquivo .env não for encontrado ou vazio
    """
    # Caso o caminho não seja passado, procura pelo arquivo .env no diretório atual ou superior
    if not env_path:
        env_path = find_env_file()

    # Carrega as variáveis do arquivo .env
    config = dotenv_values(env_path)
    if not config:
        raise FileNotFoundError(f"Erro ao carregar o arquivo .env de {env_path}. Verifique o caminho e o formato.")

    def replace_env_vars(value: str) -> str:
        """
        Substitui variáveis no formato $VAR ou ${VAR} com seus valores correspondentes.
        Prioriza variáveis carregadas do .env sobre variáveis de ambiente do sistema.
        """
        if not isinstance(value, str):
            return value

        for var_name, var_value in config.items():
            if var_value:  # Evita substituir com valores nulos
                value = value.replace(f"${{{var_name}}}", var_value)
                value = value.replace(f"${var_name}", var_value)
        return value

    # Processa todas as variáveis do .env
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
            "minio_landing":processed_config.get("MINIO_LANDING"),
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


def validate_required_keys(config: Dict[str, str], required_keys: list, config_name: str):
    """
    Valida se as chaves obrigatórias estão presentes no dicionário de configuração.

    :param config: Dicionário de configuração
    :param required_keys: Lista de chaves obrigatórias
    :param config_name: Nome do componente que está sendo validado
    :raises RuntimeError: Se alguma chave estiver ausente
    """
    if not config or not all(key in config and config[key] for key in required_keys):
        missing_keys = [key for key in required_keys if not config.get(key)]
        raise RuntimeError(f"Missing required configuration in {config_name}: {', '.join(missing_keys)}")


def print_pretty_config(config: Dict[str, Any]) -> None:
    """
    Imprime as configurações de forma legível.
    
    :param config: Dicionário de configurações retornado por load_config
    """
    import json
    print(json.dumps(config, indent=2))


if __name__ == "__main__":
    try:
        config = load_config()
        print("Configurações carregadas com sucesso:")
        print("Fim da importação")
        print_pretty_config(config)

        # Validações das seções
        validate_required_keys(config["hadoop_paths"], ["landing", "raw", "iceberg", "gold", "warehouse"], "Hadoop Paths")
        validate_required_keys(config["spark_jars"], ["mssql_spark_jars", "postgresql_jar"], "Spark JARs")

    except Exception as e:
        print(f"Erro ao carregar configurações: {e}")
        import traceback
        traceback.print_exc()
