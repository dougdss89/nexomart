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
        minio_landing = config.get("hadoop_paths", {})
        bronze_delta = config.get("delta_path", {})

        validate_required_keys(
            config=minio_landing,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )
        
        validate_required_keys(
            config=bronze_delta,
            required_keys=["bronze_layer"],
            config_name="delta_path"
        )

        # Creating SparkSession
        spark = (
            SparkSession.builder
            .appName('extract_country_region_landing')
            .enableHiveSupport()
            .getOrCreate()
        )

        return spark, minio_landing.get("minio_landing", ""), bronze_delta.get("bronze_layer", "") #, minio_storage

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
    
def extract_landing_data(spark, minio_landing, raw_table:str):
    try:
        df = spark.read.parquet(f"{minio_landing}/{raw_table}")
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

# def write_minio_country_rg_storage(data, minio_storage, table):
    
#     try:
        
#         data.write.mode("overwrite").parquet(f"{minio_storage}/{table}")
        
#         return logger.info(f"Data successfully written to: {minio_storage}/{table}")
        
#     except Exception as e:
#         logger.error(f"Fail to persist data on {minio_storage}. Verify: {e}")
#         print_exc()
#         exit(1)

 
def write_to_delta(df, bronze_layer:str, file_path:str, table_name:str):
    """ Write DataFrame to delta table. """
    try:
        minio_path = f"{bronze_layer}/{file_path}/{table_name}"

        (
            df
            .write.format("delta")
            .mode("append")  # Use "append" to add data instead
            .save(f"{minio_path}")
        )
        print(f"Table {table_name} written successfully to Delta.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to Delta: {e}")
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
    spark, minio_landing, bronze_delta = load_source_connections()
    filepath = "advworks"
    raw_table = "raw_dimcountryregion"
    
    country_df = extract_landing_data(spark, minio_landing, "countryregion")
    
    dataframe = convert_country_schema(country_df)
    
    # write_minio_country_rg_storage(country_region, minio_path, "country_region")
    
    write_to_delta(dataframe, bronze_delta, filepath,  raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == '__main__':
    main()
    logger.info("Sparkjob completed successfully")