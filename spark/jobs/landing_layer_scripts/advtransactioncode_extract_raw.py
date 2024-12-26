
from pyspark.sql import SparkSession
from modules.configconnections_loader import load_config


def load_source_connections():
    
    """ load source connections from .env file """
    
    try:
        config = load_config()
        
    except(FileNotFoundError, KeyError) as e:
        return(f"Could not load connections from source: {e}")
        # return None, None, None
    
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        hadoop_landing = config.get("hadoop_paths", {})
        
    except KeyError as e:
        return(f'Missing required configuration: {e}')
        # return None, None, None
    
    #creating sparkSession
    
    spark = (SparkSession
                .builder
                .appName('dd_salestransaction_code_raw')
                    .config('spark.jars', config['spark_jars']['mssql_spark_jars'] + "," + config['spark_jars']['postgresql_jar'])
                    .config('spark.driver.extraClassPath', config['spark_jars']['mssql_spark_jars'] + "," + config['spark_jars']['postgresql_jar'])
                    .config('spark.executor.extraClassPath', config['spark_jars']['mssql_spark_jars'] + "," + config['spark_jars']['postgresql_jar'])
                .enableHiveSupport()
                .getOrCreate()
            )
    
    return spark, mssql_properties, mssql_adventureworks, hadoop_landing
        
spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()

def extract_raw_transactioncode():
    
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

transactioncode_write = extract_raw_transactioncode()

transactioncode_write.repartition(5).write.mode('overwrite').parquet(f"{hadoop_landing}/transactioncode/")

def stop_spark(spark):
    spark.stop()

if __name__=='__main__':
    
    load_source_connections()
    extract_raw_transactioncode()
    stop_spark(spark)