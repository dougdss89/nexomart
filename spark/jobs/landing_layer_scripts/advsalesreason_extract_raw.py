from modules.configconnections_loader import load_config
from pyspark.sql  import SparkSession

def load_source_connection():
    
    """ load from file .env
    """
    
    try:
        config = load_config()
        
    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load source connections from file: {e}")
        return None, None, None
    
    try:
        spark_jars = config["spark_jars"]
        mssql_adventureworks = config["mssql_sources"]["adventureworks"]
        mssql_properties = mssql_adventureworks["properties"]
        mssql_url = mssql_adventureworks["url"]
        hadoop_landing = config.get("hadoop_paths", {})
        
    except KeyError as e:
        print(f"Could not find necessary keys in the config: {e}")
        return None, None, None
    
    spark = (SparkSession
                .builder
                .appName("extract_salesreason_raw")
                    .config("spark.jars", config["spark_jars"]["mssql_spark_jars"] + "," + config["spark_jars"]["postgresql_jar"])
                    .config("spark.driver.extraClassPath", config["spark_jars"]["mssql_spark_jars"] + "," + config["spark_jars"]["postgresql_jar"])
                    .config("spark.executor.extraClassPath", config["spark_jars"]["mssql_spark_jars"] + "," +config["spark_jars"]["postgresql_jar"])
                .enableHiveSupport()
                .getOrCreate()
            )
    
    return spark, mssql_properties, mssql_adventureworks, hadoop_landing

spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connection()

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
                                    ReasonType

                                from sales.SalesReason as jsr

                                    left join

                                    sales.SalesOrderHeaderSalesReason as soh

                                    on jsr.salesreasonid = soh.salesreasonid
                            """)).load()
                    
    
    return df_salesreason_raw

salesreason_df_write = extract_raw_salesreason()

salesreason_df_write.repartition(5).write.mode("overwrite").parquet(f"{hadoop_landing}/salesreason")

def stop_spark(spark):
    spark.stop()
    
if __name__ == "__main__":
    
    load_source_connection()
    extract_raw_salesreason()
    stop_spark()