import logging
from modules.configconnections_loader import load_config, validate_required_keys
from modules.startSparkSession import SparkSessionManager
from pyspark.sql.functions import col
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
        
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        
        #validated imported keys
        
        validate_required_keys(
            config = config.get("hadoop_paths", {}),
            required_keys= ["landing", "raw_icebergdb"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_scrapreason_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, hadoop_landing, iceberg_rawdb
    
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
        
def extract_landing_data(spark, hadoop_landing, raw_table):
    try:
        scrapreason_df = (spark
                        .read
                        .parquet(f"{hadoop_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return scrapreason_df
    
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
        
def write_to_iceberg(scrapreason_df, iceberg_rawdb, table_name):
    
    try:
        (
        scrapreason_df
            .write
            .format("Iceberg")
            .mode("overwrite")
            .saveAsTable(f"iceberg_table.{iceberg_rawdb}.{table_name}")
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
    
    spark, hadoop_landing, iceberg_rawdb = load_source_connections()
    
    scrapreason_df = extract_landing_data(spark, hadoop_landing, "scrapreason")
    
    scrapreason = convert_scrapreason_schema(scrapreason_df)
    
    write_to_iceberg(scrapreason, f"{iceberg_rawdb}", "raw_dimscrapreason")
    
    spark_stop(spark)
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")