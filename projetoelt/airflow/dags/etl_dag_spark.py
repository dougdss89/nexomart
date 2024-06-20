from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


default_args = {
    
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False
    
}

with DAG('spark_dag_bash', default_args = default_args, schedule_interval='@daily') as dag:
    
    start = EmptyOperator(task_id= 'start')
    
    spark_etl_task = BashOperator(
        
        task_id = 'spark_etl_task_bash',
        bash_command= 'spark-submit --master spark://spark:7077 /opt/spark/apps/etl_script.py'
    )
    
    load_to_postgres = PostgresOperator(
        
        task_id = 'load_to_postgre_bash',
        postgres_conn_id = '00_postgre_test',
        sql= """
        
                COPY teste_airlfow from '/opt/spark/data/steam-games_final.csv' DELIMITER ',' CSV;
        
            """
    )
    
    
    end = EmptyOperator(task_id = 'end')
    
    
    start >> spark_etl_task >> load_to_postgres >> end