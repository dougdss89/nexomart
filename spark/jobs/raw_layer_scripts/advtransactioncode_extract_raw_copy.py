from pyspark.sql import SparkSession
from spark.jobs.configconnections_loader import load_config
import sys
import traceback

def load_source_connections():
    """ load source connections from .env file """
    
    try:
        # Adicione impressão para depuração
        print("Tentando carregar configuração...")
        config = load_config()
        print("Configuração carregada:", config)
        
    except FileNotFoundError as e:
        print(f"Erro: Arquivo de configuração não encontrado: {e}")
        traceback.print_exc()
        return None, None, None
    except KeyError as e:
        print(f"Erro: Chave de configuração ausente: {e}")
        traceback.print_exc()
        return None, None, None
    except Exception as e:
        print(f"Erro inesperado ao carregar configuração: {e}")
        traceback.print_exc()
        return None, None, None
    
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        # mssql_url = mssql_adventureworks["url"]
        
        # Adicione verificações de chaves específicas
        if not spark_jars or 'mssql_spark_jars' not in spark_jars or 'postgresql_jar' not in spark_jars:
            print("Erro: Configurações de spark_jars incompletas")
            return None, None, None
        
        if not mssql_adventureworks:
            print("Erro: Configurações de mssql_sources incompletas")
            return None, None, None
        
        if not mssql_properties:
            print("Erro: Configurações de mssql_properties ausentes")
            return None, None, None
        
        
        # mssql_adventureworks = mssql_sources['adventureworks']
        # Criando sparkSession com mais configurações de depuração
        spark = (SparkSession
            .builder
            .appName('dd_salestransaction_code_raw')
            .config('spark.jars', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
            .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
            .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')},{spark_jars.get('postgresql_jar', '')}")
            .enableHiveSupport()
            .getOrCreate()
        )
        
        return spark, mssql_properties, mssql_adventureworks
    
    except Exception as e:
        print(f"Erro ao criar sessão Spark: {e}")
        traceback.print_exc()
        return None, None, None
        
def extract_raw_transactioncode(spark, mssql_properties, mssql_adventureworks):
    # Verifique se todos os parâmetros são válidos
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Parâmetros inválidos para extração de dados")
        return None
    
    try:
        df_transactioncode_raw = (spark.read
                                  .format('jdbc')
                                  .option('url', mssql_adventureworks['url'])
                                  .option('user', mssql_properties['user'])
                                  .option('password', mssql_properties['password'])
                                  .option('driver', mssql_properties['driver'])
                                  .option('query', 
                                          """ 
                                            select
                                                distinct
                                                soh.SalesOrderID,
                                                purchaseordernumber,
                                                salesordernumber,
                                                carriertrackingnumber,
                                                accountnumber,
                                                CreditCardApprovalCode,
                                                soh.OnlineOrderFlag
                                            from sales.salesorderheader as soh
                                                left join
                                                sales.salesorderdetail as sod
                                            on soh.SalesOrderID = sod.SalesOrderID
                                          """)).load()
        
        return df_transactioncode_raw
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        traceback.print_exc()
        return None

def stop_spark(spark):
    if spark:
        spark.stop()

def main():
    try:
        # Carregar conexões de origem
        spark, mssql_properties, mssql_adventureworks = load_source_connections()
        
        # Verificar se a conexão foi estabelecida com sucesso
        if not spark:
            print("Falha ao estabelecer conexão Spark")
            sys.exit(1)
        
        # Extrair dados
        transactioncode_write = extract_raw_transactioncode(spark, mssql_properties, mssql_adventureworks)
        
        # Verificar se a extração foi bem-sucedida
        if transactioncode_write:
            transactioncode_write.repartition(5).write.mode('overwrite').parquet("hdfs://ha-cluster/raw/transactioncodecopy/")
        else:
            print("Falha na extração de dados")
        
    except Exception as e:
        print(f"Erro no script principal: {e}")
        traceback.print_exc()
    finally:
        # Parar a sessão Spark
        stop_spark(spark)

if __name__=='__main__':
    main()