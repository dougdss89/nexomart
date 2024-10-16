from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 14),
    'retries': 0,
}

with DAG(
    dag_id='teste_hadoop_load',
    default_args=default_args,
    description='Um exemplo de DAG para rodar jobs Spark',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Início do pipeline
    start_load = EmptyOperator(
        task_id='start_load'
    )

    # Execução do job Spark
    spark_hadoop_load = SparkSubmitOperator(
        task_id='spark_dataframe_hadoop',
        application='/opt/airflow/sparkapps/testehadoop.py',
        conn_id='sparkconn',
        conf={
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': '/opt/spark/spark-events',  # Diretório de eventos
            'spark.history.fs.logDirectory': '/opt/spark/spark-events'  # Certifique-se que o History Server usa o mesmo diretório
        }
    )

    # Fim do pipeline
    end_load = EmptyOperator(
        task_id='end_load'
    )

# Definindo a ordem das tarefas
start_load >> spark_hadoop_load >> end_load
