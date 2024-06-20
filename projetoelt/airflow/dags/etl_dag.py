from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False
    
}

with DAG('spark_etl_dag', default_args = default_args, schedule_interval='@daily') as dag:
    
    start = EmptyOperator(task_id= 'start')
    
    spark_etl_task = SparkSubmitOperator(
        
        task_id = 'spark_etl_task',
        application='/usr/local/airflow/scripts/etl_script.py',
        conn_id = '00_spark_conn',
        dag = dag
    )
    
    load_to_postgres = PostgresOperator(
        
        task_id = 'load_to_postgres',
        postgres_conn_id = '00_postgre_test',
        sql= """
        
                COPY teste_airlfow from '/usr/local/airflow/dados/steam-games_final.csv' DELIMITER ',' CSV;
        
            """
    )
    
    
    end = EmptyOperator(task_id = 'end')
    
    
    start >> spark_etl_task >> load_to_postgres >> end