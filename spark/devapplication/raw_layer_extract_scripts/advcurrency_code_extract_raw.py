import logging
from pyspark.sql import SparkSession
from modules.configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#log config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    
    """ Load source from .env file and create Spark Session """
    try:
        config = load_config()
        
        hadoop_landing = config.get("hadoop_paths", {}).get("landing")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb")
        
        validate_required_keys(
            config=config.get("hadoop_paths",{}),
            required_keys=["landing", "raw_icebergdb"],
            config_name="hadoop_paths"
        )
        #create spark session
        
        spark = (
            SparkSession
                .builder
                .appName("extract_currency_code_landing")
                .enableHiveSupport()
                .getOrCreate()
        )
        return spark, hadoop_landing, iceberg_rawdb
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {e}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        logger.error(f"Failed to create SparkSession. Runtime error: {e}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occurred. Please verify: {e}")
        print_exc()
        exit(1)
    
def extract_landing_data(spark, hadoop_landing, raw_table):
    try:
        currency_code_df = (spark
                                .read
                                .parquet(f"{hadoop_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return currency_code_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def write_to_iceberg(currency_code_df, iceberg_rawdb, table_name):
    
    try:
        (
            currency_code_df
                .write
                .format("Iceberg")
                .mode("overwrite")
                .saveAstable(f"iceberg_table.{iceberg_rawdb}.{table_name}")
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
    
    currency_code_dataframe = extract_landing_data(spark, hadoop_landing, "currencycode")
    
    write_to_iceberg(currency_code_dataframe, f"{iceberg_rawdb}", "raw_dimcurrencycode")
    
    spark_stop(spark)
    
if __name__ == "__main__":
    main()