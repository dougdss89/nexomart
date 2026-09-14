#!/bin/bash
# Cria todos os bancos necessários para a stack de dados.
# Executado automaticamente pelo postgres na primeira inicialização.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflowdb;
    CREATE DATABASE unitycatalogdb;
    CREATE DATABASE metastore;
    CREATE DATABASE nexomarket_db;
EOSQL
