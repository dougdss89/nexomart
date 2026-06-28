import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cast
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#log config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    """ Load source connections from .env file and create a SparkSession. """
    try:
        config = load_config()

        # Validating imported keys
        hadoop_paths = config.get("hadoop_paths", {})
        delta_paths = config.get("delta_path", {})

        validate_required_keys(
            config=hadoop_paths,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )
        validate_required_keys(
            config=delta_paths,
            required_keys=["raw_icebergdb"],
            config_name="delta_path"
        )
        #create spark session
        
        spark = (
            SparkSession
                .builder
                .appName("extract_currency_code_landing")
                
                .getOrCreate()
        )
        logger.info("Spark Session created")
        return spark, hadoop_paths.get("minio_landing", ""), delta_paths.get("raw_icebergdb", "")
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        logger.error(f"Failed to create SparkSession. Runtime error: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occurred. Please verify: {str(e)}")
        print_exc()
        exit(1)
    
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        dataframe = (spark
                                .read
                                .parquet(f"{minio_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {str(e)}")
        print_exc()
        exit(1)
        
def convert_currencycode_schema(dataframe):
    
    try:
        dataframe.select(
                    col("currencycode").cast("string").alias("currencycode"),
                    col("currencyname").cast("string").alias("currencyname"),
                    col("countryregioncode").cast("string").alias("countryregioncode"),
                    col("countryname").cast("string").alias("countryname"),
                    col("inserted_date").cast("timestamp").alias("extracted_data"),
                    col("modified_date").cast("timestamp").alias("modified_date"),
                    col("eltversion").cast("int").alias("eltversion")
        )
    
        return dataframe
    
    except Exception as e:
        logger.info(f"Fail to convert schema. Verify: {str(e)}")
        print_exc()
        exit(1)
        
# def write_currency_code_minio_storage(data, minio_storage, table):
    
#     try:
#         ((
#             data
#                 .write
#                 .mode("Append")
#                 .parquet(f"{minio_storage}/{table}")
#         ))
        
#         return logger.info(f"Data successfully written to: {minio_storage}/{table}")

#     except Exception as e:
#         logger.error(f"Fail to persist data on {minio_storage}. Verify: {str(e)}")
#         print_exc()
#         exit(1)
        
def write_to_delta(dataframe, bronze_base: str, table_name: str):
    """ Write DataFrame to Delta Lake at the bronze S3A path. """
    try:
        path = f"{bronze_base}/advworks_rawdb/{table_name}"
        (
            dataframe
            .write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(path)
        )
        print(f"Table {table_name} written to Delta at {path}")
        logger.info(f"Table {table_name} written to Delta at {path}")
    except Exception as e:
        print(f"Failed to write table {table_name} to Delta: {str(e)}")
        print_exc()
        exit(1)


def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stoped successfully")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()
        
        
def main():
    
    spark, minio_landing, bronze_base = load_source_connections()
    raw_table = "raw_dimcurrencycode"
        
    dataframe = extract_landing_data(spark, minio_landing, "currencycode")
    
    dataframe = convert_currencycode_schema(dataframe)
        
    write_to_delta(dataframe=dataframe, bronze_base=bronze_base, table_name=raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")