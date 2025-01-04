import logging
from pyspark.sql import SparkSession
from modules.configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

# Configuração de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    """ Load source connections from .env file and create a SparkSession. """
    try:
        config = load_config()

        # Validating imported keys
        hadoop_landing = config.get("hadoop_paths", {}).get("landing")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb")

        validate_required_keys(
            config=config.get("hadoop_paths", {}),
            required_keys=["landing", "raw_icebergdb"],
            config_name="hadoop_paths"
        )

        # Creating SparkSession
        spark = (
            SparkSession.builder
            .appName('extract_buyer_landing')
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
        df = spark.read.parquet(f"{hadoop_landing}/{raw_table}")
        logger.info(f"Data extracted successfully for table: {raw_table}")
        return df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)

def write_to_iceberg(df, iceberg_rawdb, table_name):
    
    try:
        df.repartition(5).write.format("iceberg").mode("overwrite").saveAsTable(f"iceberg_table.{iceberg_rawdb}.{table_name}")

        logger.info(f"Table {table_name} written successfully to Iceberg.")
        
    except Exception as e:
        logger.error(f"Failed to write table {table_name} to Iceberg: {e}")
        print_exc()
        exit(1)

def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped successfully.")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {e}")
        print_exc()

def main():
    spark, hadoop_landing, hadoop_raw_table = load_source_connections()
    
    buyer_df = extract_landing_data(spark, hadoop_landing, "countryregion")
    
    write_to_iceberg(buyer_df, f"{hadoop_raw_table}", "raw_dimcountryregion")
    
    spark_stop(spark)

if __name__ == '__main__':
    main()
