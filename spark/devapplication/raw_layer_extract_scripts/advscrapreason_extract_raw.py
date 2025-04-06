import logging
from configconnections_loader import load_config, validate_required_keys
from startSparkSession import SparkSessionManager
from pyspark.sql.functions import col
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
        minio_landing = config.get("hadoop_paths", {})
        iceberg = config.get("hadoop_paths", {})

        validate_required_keys(
            config=minio_landing,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )
        
        validate_required_keys(
            config=iceberg,
            required_keys=["raw_icebergdb"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_scrapreason_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", "") #, minio_storage
    
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
        dataframe = (spark
                        .read
                        .parquet(f"{minio_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_scrapreason_schema(tmp_scrapreason):
    
    try:
        tmp_scrapreason.select(
            
                        col("scrapreasonid").cast("int").alias("scrapreason_id"),
                        col("scrapname").cast("string").alias("scrapname"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
        )
        
        return tmp_scrapreason
    
    except Exception as e:
        logger.error(f"Error to convert schema from dataframe {tmp_scrapreason}. Please verify: {e}")
        print_exc()
        exit(1)
        
def write_to_iceberg(dataframe, raw_icebergdb:str,  table_name:str):
    """ Write DataFrame to iceberg table. """
    try:
        raw_lakehouse = f"{raw_icebergdb}.{table_name}"

        (
            dataframe
            .write.format("iceberg")
            .mode("append")  # Use "append" to add data instead
            .saveAsTable(f"{raw_lakehouse}")
        )
        return(f"Table {table_name} written successfully to iceberg.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to iceberg: {e}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Failed to write table {table_name} to iceberg table: {e}")
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
    
    spark, minio_landing, iceberg = load_source_connections()
    
    raw_table = "raw_dimscrapreason"
    
    dataframe = extract_landing_data(spark, minio_landing, "scrapreason")
    
    dataframe = convert_scrapreason_schema(dataframe)
    
    # write_scrapreason_minio_storage(scrapreason, miniopath, "scrapreason")
    
    write_to_iceberg(dataframe, iceberg,  raw_table)
    
    spark_stop(spark)
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")