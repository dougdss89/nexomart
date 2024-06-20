#!/bin/bash
AIRFLOW_HOME="/usr/local/airflow"

#inicializa o db
airflow db init

airflow users create \
    --username admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com \
    --password admin

# Inicie o webserver, scheduler e worker em background
airflow webserver -p 8081 &
airflow scheduler &
airflow celery worker &

# Mantenha o script em execução
tail -f /dev/null
