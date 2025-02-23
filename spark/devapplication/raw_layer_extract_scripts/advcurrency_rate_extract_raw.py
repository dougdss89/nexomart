import logging
from configconnections_loader import load_config, validate_required_keys
from startSparkSession import SparkSessionManager
from pyspark.sql.functions import col, cast
from sys import exit
from traceback import print_exc

#log config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    
    """ 
        Load key-values from .env file to create Spark Session
    """
    
    try:
        config = load_config()
        
        minio_landing = config.get("hadoop_paths", {}).get("minio_landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        # minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")
        
        #validated imported keys
        
        validate_required_keys(
            config = config.get("hadoop_paths", {}),
            required_keys= ["landing", "raw_icebergdb", "minio_landing"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_currency_rate_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing, iceberg_rawdb#, minio_storage
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {e}")
        print_exc()
        exit(1)
        
    except RuntimeError as e:
        logger.error(f"Failed to create Spark Session. Runtime Error: {e}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. {e}")
        print_exc()
        exit(1)
        
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        currency_rate_df = (spark
                                .read
                                .parquet(f"{minio_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return currency_rate_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_currencyrate_schema(dataframe):
    
    try:
        
        dataframe.select(
                    col("currencyrateid").cast("int").alias("currencyrateid"),
                    col("fromcurrencycode").cast("string").alias("fromcurrencycode"),
                    col("tocurrencycode").cast("string").alias("tocurrencycode"),
                    col("averagerate").cast("float").alias("averagerate"),
                    col("endofdayrate").cast("float").alias("endodayrate"),
                    col("currencyratedate").cast("date").alias("currencyratedate"),
                    col("inserted_date").cast("timestamp").alias("extracted_date"),
                    col("modified_date").cast("timestamp").alias("modified_date"),
                    col("eltversion").cast("int").alias("eltversion")
        
                    )
        logger.info("Schema conversion finished successfully")
        
        return dataframe
    except Exception as e:
        logger.info(f"Fail to convert schema. Verify: {e}")
        print_exc()
        exit(1)
        
# def write_currency_rate_minio_storage(data, minio_storage, table):
    
#     try:
#         ((
#             data
#                 .write
#                 .mode("append")
#                 .parquet(f"{minio_storage}/{table}")
            
#         ))
#         return logger.info(f"Data successfully written to: {minio_storage}/{table}")

#     except Exception as e:
#         logger.info(f"Fail to persist data on {minio_storage}. Verify: {e}")
#         print_exc()
#         exit(1)
        
def write_to_iceberg(currency_rate_df, iceberg_rawdb, table_name):
    
    try:
        (
            currency_rate_df
                .write
                .format("Iceberg")
                .mode("overwrite")
                .saveAsTable(f"{iceberg_rawdb}.{table_name}")
        )
        logger.info(f"Table {table_name} written successfully to Iceberg")
        
    except Exception as e:
        logger.error(f"Failed to write table {table_name} to Iceberg table: {e}")
        print_exc()
        exit(1)
        
def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc()
        
        
def main():
    
    spark, minio_landing, iceberg_rawdb = load_source_connections()
    
    currency_rate_df = extract_landing_data(spark, minio_landing, "currency_rate")
    
    currency_rate = convert_currencyrate_schema(currency_rate_df)
    
    # write_currency_rate_minio_storage(currency_rate, miniopath, "currency_rate" )
        
    write_to_iceberg(currency_rate, f"{iceberg_rawdb}", "raw_dimcurrency_rate")
    
    spark_stop(spark)

    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")