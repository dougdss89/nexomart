import os
from typing import Dict, Any
from dotenv import dotenv_values

def load_config(env_path: str = '/opt/spark/apps/shared/.env') -> Dict[str, Any]:
    """
    Carrega e prepara as configurações do arquivo .env, substituindo placeholders dinâmicos.

    :param env_path: Caminho para o arquivo .env
    :return: Um dicionário contendo as configurações processadas
    :raises FileNotFoundError: Se o arquivo .env não for encontrado ou vazio
    :raises KeyError: Se variáveis obrigatórias estiverem ausentes
    """
    
    # Carrega as variáveis do arquivo .env
    config = dotenv_values(env_path)

    # Valida se o arquivo .env foi carregado corretamente
    if not config:
        raise FileNotFoundError(f"Erro ao configurar o arquivo .env em {env_path}. Verifique o caminho e o formato do arquivo.")

    # Função para substituir variáveis de ambiente em strings
    def replace_env_vars(value: str) -> str:
        """
        Substitui variáveis no formato $VAR ou ${VAR} com seus valores correspondentes.
        Prioriza variáveis carregadas do .env sobre variáveis de ambiente do sistema.
        """
        if not isinstance(value, str):
            return value
        
        # Primeiro, substitui variáveis no formato ${VAR}
        for var_name in config.keys():
            value = value.replace(f"${{{var_name}}}", config.get(var_name, ''))
        
        # Depois, substitui variáveis no formato $VAR
        for var_name in config.keys():
            value = value.replace(f"${var_name}", config.get(var_name, ''))
        
        return value

    # Processa todas as variáveis, substituindo placeholders
    processed_config = {
        key: replace_env_vars(value) 
        for key, value in config.items()
    }

    # Constrói o dicionário de configurações com os valores processados
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
        "spark_jars": {
            "mssql_spark_jars": processed_config.get("MSSQLSPARKJAR"),
            "postgresql_jar": processed_config.get("POSTGREJAR")
        },
        "raw_config": processed_config  # Adiciona configuração bruta para referência
    }

    return configurations

def print_pretty_config(config: Dict[str, Any]) -> None:
    """
    Imprime as configurações de forma legível.
    
    :param config: Dicionário de configurações retornado por load_config
    """
    import json
    print(json.dumps(config, indent=2))
       
    
def validate_required_keys(config, required_keys: list, config_name: str):
    """
    Valida se as chaves obrigatórias estão presentes no dicionário de configuração.

    :param config: Dicionário de configuração
    :param required_keys: Lista de chaves obrigatórias
    :param config_name: Nome do componente que está sendo validado
    :raises RuntimeError: Se alguma chave estiver ausente
    """
    if not config or not all(key in config for key in required_keys):
        raise RuntimeError(f"Missing required configuration in {config_name}: {', '.join(required_keys)}")

if __name__ == "__main__":
    try:
        config = load_config()
        print("Configurações carregadas com sucesso:")
        print_pretty_config(config)
        
    except Exception as e:
        print(f"Erro ao carregar configurações: {e}")
        import traceback
        traceback.print_exc()


# Uso
#  validate_required_keys(spark_jars, ['mssql_spark_jars', 'postgresql_jar'], 'spark_jars')
