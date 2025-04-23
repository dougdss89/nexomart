from configconnections_loader import load_config, validate_required_keys
from pyspark.sql  import SparkSession
from traceback import print_exc

def load_source_connection():
    
    """ load from file .env
    """
    config = load_config()
    
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
                    .appName("extract_sales_reason_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")
    
    
    except RuntimeError as e:
        print(f"Failed to create configuration: {str(e)}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error occur: {str(e)}")
        print_exc()
        return None, None, None

spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connection()

def extract_raw_salesreason():
    
    df_salesreason_raw = (spark
                       .read
                        .format("jdbc")
                        .option("url", mssql_adventureworks["url"])
                        .option("driver", mssql_properties["driver"])
                        .option("user", mssql_properties["user"])
                        .option("password", mssql_properties["password"])
                        .option("query",
                               
                            """

                                select 
                                    coalesce(jsr.SalesReasonID, 0) as salesreasonid,
                                    [Name] as salesreason,
                                    soh.salesorderid,
                                    ReasonType,
                                    getdate() as inserted_date,
                                    '9999-31-12 00:00:00.000' as modified_date,
                                    '1' as eltversion

                                from sales.SalesReason as jsr

                                    left join

                                    sales.SalesOrderHeaderSalesReason as soh

                                    on jsr.salesreasonid = soh.salesreasonid
                            """)).load()
                    
    
    return df_salesreason_raw

salesreason_df_write = extract_raw_salesreason()

salesreason_df_write.repartition(5).write.mode("overwrite").parquet(f"{minio_landing}/salesreason")

def stop_spark(spark):
    spark.stop()
    
if __name__ == "__main__":
    
    load_source_connection()
    extract_raw_salesreason()
    stop_spark(spark)