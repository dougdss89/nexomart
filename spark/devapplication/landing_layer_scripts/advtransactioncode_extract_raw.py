
from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from traceback import print_exc


def load_source_connections():
    
    print("Import .env file and creating Spark Session")
    
    #loading configurations and validated it
    try:
        config = load_config()
        print("File .env loaded")
        
        
    except FileExistsError as e:
        print("File .env not founded: {e}")
        
    except KeyError as e:
        print("Missing some key-values pair: {e}")
        
    #importing key-values from .env
    #validating data imported
    #validating imported keys
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        minio_landing = config.get("hadoop_paths", {})

        validate_required_keys(config=spark_jars, 
                               required_keys=["mssql_spark_jars"],
                               config_name="spark_jars")
        
        #validade properties key
        validate_required_keys(config= mssql_adventureworks,
                               required_keys=["properties"],
                               config_name="mssql_adventureworks")
        
        validate_required_keys(
            config=minio_landing,  # validating hadoop_path key
            required_keys=["minio_landing"],  # check if key exists
            config_name="hadoop_paths"
        )
        
        spark = (SparkSession
                    .builder
                    .appName("extract_transaction_code_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")
    
    except RuntimeError as e:
        print(f"Failed to create configuration: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error occur: {e}")
        print_exc()
        return None, None, None
        
spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

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
                                            soh.OnlineOrderFlag,
									        getdate() as inserted_date,
	                                        '9999-31-12 00:00:00.000' as modified_date,
                                            '1' as eltversion
                                            
                                        from sales.salesorderheader as soh
                                            left join
                                            sales.salesorderdetail as sod
                                        on soh.SalesOrderID = sod.SalesOrderID
                                      """)).load()
    
    return df_transactioncode_raw

transactioncode_write = extract_raw_transactioncode()

transactioncode_write.repartition(3).write.mode('overwrite').parquet(f"{minio_landing}/transactioncode")

def stop_spark(spark):
    spark.stop()

if __name__=='__main__':
    
    load_source_connections()
    extract_raw_transactioncode()
    stop_spark(spark)