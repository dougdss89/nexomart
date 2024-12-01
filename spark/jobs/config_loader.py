from dotenv import dotenv_values

def load_config(env_path="/opt/spark/apps/shared/.env"):
    """
    Carrega e prepara as configurações do arquivo .env, substituindo placeholders dinâmicos.

    :param env_path: Caminho para o arquivo .env
    :return: Um dicionário contendo as configurações processadas
    :raises FileNotFoundError: Se o arquivo .env não for encontrado ou vazio
    :raises KeyError: Se variáveis obrigatórias estiverem ausentes
    
    """
    # Carregar variáveis do arquivo .env
    config = dotenv_values(env_path)
    
    # Validar se o .env foi carregado corretamente
    if not config:
        raise FileNotFoundError(f"Erro ao carregar o arquivo .env em {env_path}. Verifique o caminho e o formato do arquivo.")

    # Variáveis obrigatórias
    required_keys = [
        "MSSQLIP", "MSSQLUSER", "MSSQLPASSWORD", 
        "MSSQLDRIVER", "MSSQLURLADVENTUREWORKS", 
        "MSSQLSPARKJAR"
    ]
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        raise KeyError(f"As seguintes variáveis obrigatórias estão ausentes no arquivo .env: {', '.join(missing_keys)}")
    
    # Substituir placeholders dinamicamente
    for key, value in config.items():
        if isinstance(value, str):  # Apenas strings podem conter placeholders
            for placeholder, replacement in config.items():
                if isinstance(replacement, str):  # Garantir que o valor substituto é string
                    value = value.replace(f"${placeholder}", replacement)
            config[key] = value

    # Retornar configurações preparadas
    return {
        "mssql_url_adventureworks": config["MSSQLURLADVENTUREWORKS"],
        "mssql_properties": {
            "user": config["MSSQLUSER"],
            "password": config["MSSQLPASSWORD"],
            "driver": config["MSSQLDRIVER"],
        },
        "mssql_spark_jar": config["MSSQLSPARKJAR"],  # Adicionado o caminho do JAR
    } 