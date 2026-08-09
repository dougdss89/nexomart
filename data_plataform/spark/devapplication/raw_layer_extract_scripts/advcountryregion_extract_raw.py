import logging
from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
import pyspark.sql.functions as f
from sys import exit
from traceback import print_exc

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_source_connections():
    """ Load source connections from .env file and create a SparkSession. """
    try:
        config = load_config()

        # Validating imported keys
        minio_landing = config.get("hadoop_paths", {})
        iceberg = config.get("hadoop_paths", {})
        iceberg_catalog = config.get("hadoop_paths", {})

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
        
        validate_required_keys(
            config=iceberg_catalog,
            required_keys=["iceberg_catalog"],
            config_name="hadoop_paths"
        )

        # Creating SparkSession
        spark = (
            SparkSession.builder
            .appName('extract_countryregion_landing')
            
            .getOrCreate()
        )

        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", ""),  iceberg_catalog.get("iceberg_catalog", "")

    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        print(f"Failed to create SparkSession. Runtime error: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        print(f"Unexpected error occurred. Please verify: {str(e)}")
        print_exc()
        exit(1)
    
def extract_landing_data(spark, minio_landing, raw_table:str):
    try:
        dataframe = spark.read.parquet(f"{minio_landing}/{raw_table}")
        logger.info(f"Data extracted successfully for table: {raw_table}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {str(e)}")
        print_exc()
        exit(1)
        
def convert_country_schema(dataframe):
    
    dataframe = dataframe.selectExpr(

                                    "cast(territoryid as int) as territoryid",
                                    "cast(name as string) as name",
                                    "cast(group as string) as group",
                                    "cast(countryregioncode as string) as countryregioncode",
                                    "cast(inserted_date as timestamp) as extracted_date",
                                    "cast(modified_date as timestamp) as modified_date",
                                    "cast(eltversion as int) as eltversion")
    
    return dataframe


 
def write_to_iceberg(dataframe, catalog:str, raw_icebergdb:str, table_name:str):
    """ Write DataFrame to iceberg table. """
    try:
        raw_lakehouse = f"{catalog}.{raw_icebergdb}.{table_name}"

        (
            dataframe
            .write.format("iceberg")
            .mode("append")  # Use "append" to add data instead
            .saveAsTable(f"{raw_lakehouse}")
        )
        print(f"Table {table_name} written successfully to iceberg.")
        
        return logger.info(f"Table {table_name} written successfully to iceberg.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to iceberg: {str(e)}")
        print_exc()
        exit(1)

def spark_stop(spark):
    try:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped successfully.")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()

def main():
    spark, minio_landing, iceberg, iceberg_catalog = load_source_connections()
    raw_table = "raw_dimcountryregion"
        
    dataframe = extract_landing_data(spark, minio_landing, "countryregion")
    
    dataframe = convert_country_schema(dataframe)
        
    write_to_iceberg(dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name= raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == '__main__':
    main()
    logger.info("Sparkjob completed successfully")