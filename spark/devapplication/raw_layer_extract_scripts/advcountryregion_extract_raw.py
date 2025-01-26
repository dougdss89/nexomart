import logging
from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
import pyspark.sql.functions as f
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
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")

        validate_required_keys(
            config=config.get("hadoop_paths", {}),
            required_keys=["landing", "raw_icebergdb", "minio_landing"],
            config_name="hadoop_paths"
        )

        # Creating SparkSession
        spark = (
            SparkSession.builder
            .appName('extract_country_region_landing')
            .enableHiveSupport()
            .getOrCreate()
        )

        return spark, hadoop_landing, iceberg_rawdb, minio_storage

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
    
def extract_landing_data(spark, hadoop_landing, raw_table:str):
    try:
        df = spark.read.parquet(f"{hadoop_landing}/{raw_table}")
        logger.info(f"Data extracted successfully for table: {raw_table}")
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_country_schema(dataframe):
    
    country_region = dataframe.selectExpr(

                                    "cast(territoryid as int) as territoryid",
                                    "cast(name as string) as name",
                                    "cast(group as string) as group",
                                    "cast(countryregioncode as string) as countryregioncode",
                                    "cast(inserted_date as timestamp) as extracted_date",
                                    "cast(modified_date as timestamp) as modified_date",
                                    "cast(eltversion as int) as eltversion")
    
    return country_region

def write_minio_country_rg_storage(data, minio_storage, table):
    
    try:
        
        data.write.mode("overwrite").parquet(f"{minio_storage}/{table}")
        
        return logger.info(f"Data successfully written to: {minio_storage}/{table}")
        
    except Exception as e:
        logger.error(f"Fail to persist data on {minio_storage}. Verify: {e}")
        print_exc()
        exit(1)

def write_to_iceberg(df, iceberg_rawdb, table_name):
    
    try: 
        ((df.repartition(5)
            .write
            .format("iceberg")
            .mode("append")
            .saveAsTable(f"{iceberg_rawdb}.{table_name}")
        ))


        return logger.info(f"Table {table_name} written successfully to Iceberg.")
        
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
    spark, hadoop_landing, hadoop_raw_table, minio_path = load_source_connections()
    
    country_df = extract_landing_data(spark, hadoop_landing, "countryregion")
    
    country_region = convert_country_schema(country_df)
    
    write_minio_country_rg_storage(country_region, minio_path, "country_region")
    
    write_to_iceberg(country_region, f"{hadoop_raw_table}", "raw_dimcountryregion")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == '__main__':
    main()
    logger.info("Sparkjob completed successfully")