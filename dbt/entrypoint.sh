#!/bin/bash

# Ativa o ambiente virtual
source /opt/venv/bin/activate

# Inicia o serviço dbt (você pode personalizar este comando conforme necessário)
# Por exemplo, para executar um projeto específico:
# dbt run --project-dir /usr/app/dbt

# Mantém o container rodando
exec "$@"
