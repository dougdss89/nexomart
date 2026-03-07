from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand
from configconnections_loader import load_config
import time
import traceback


def load_source_connections():
    
    """ load source connections from .env file"""
    
    try:
        print('Trying to load configuration')
        config = load_config()
        print(f"Configuration loaded: {config}")
        
    except FileNotFoundError as e:
        print(f'File .env not founded: {str(e)}')
        traceback.print_exc()
        
        return None, None, None, None
    
    except KeyError as e:
        print(f"Missing some key configs: {str(e)}")
        traceback.print_exc()
        
        return None, None, None, None
    
    except Exception as e:
        print(f"unexpected error while load configuration: {str(e)}")
        traceback.print_exc()
        
        return None, None, None, None
    
    #load properties, configurations and jars file
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        minio_landing = config.get("hadoop_paths", {})
    
        #check if variable was loaded
        if not spark_jars or 'mssql_spark_jars' not in spark_jars:
            print("Missing spark jars configuration file and path")
            return None, None, None, None
        
        if not mssql_adventureworks:
            print(f"Adventureworks sources and properties wasn't loaded. Missing properties {mssql_adventureworks}")
            return None, None, None, None
        
        if not mssql_properties:
            print(f"We detected certain dependencies on {mssql_properties} that was not loaded. Reload {mssql_adventureworks} and retry it")
            return None, None, None, None
    #create spark session
        spark = (SparkSession
                    .builder
                    .appName('stress_teste_spark')
                    .enableHiveSupport()
                    .getOrCreate()
                )
        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")

    except Exception as e:
        print(f"Fail to create SparkSession {spark}: {str(e)}")
        traceback.print_exc()
        return None, None, None, None
    
spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

def run_stress_test(spark = spark, minio_landing = minio_landing):
    print("Iniciando o Teste de Estresse de Rede (MinIO/S3A)...")
    
    # 1. Carrega as conexões usando o seu módulo arrumado (4 variáveis)
    # spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()
    
    if not spark or not minio_landing:
        print("Erro ao carregar conexões. Abortando teste.")
        return

    try:
        start_time = time.time()
        
        # 2. Gera 50 Milhões de linhas (Aproximadamente 10GB de dados em memória)
        print("Gerando 50 milhões de linhas de dados sintéticos pesados...")
        
        df_heavy = spark.range(0, 50000000) \
            .withColumn("uuid_aleatorio", expr("uuid()")) \
            .withColumn("metrica_teste", rand() * 1000) \
            .withColumn("texto_pesado", expr("repeat('EngenhariaDeDados_ApacheSpark_', 20)"))

        # 3. Força 100 partições para criar 100 conexões simultâneas no MinIO
        df_repartitioned = df_heavy.repartition(100)

        # 4. Grava no MinIO 
        destino = f"{minio_landing}/stress_test/heavy_payload"
        print(f"Iniciando gravação paralela em: {destino}")
        
        df_repartitioned.write \
            .mode("overwrite") \
            .parquet(destino)

        end_time = time.time()
        
        print("SUCESSO ABSOLUTO! Gravou 50 milhões de linhas sem IncompleteBody.")
        print(f"Tempo total: {round((end_time - start_time) / 60, 2)} minutos.")

    except Exception as e:
        print(f"O teste falhou! A rede não aguentou: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    run_stress_test()