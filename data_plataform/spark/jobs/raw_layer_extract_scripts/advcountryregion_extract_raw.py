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

        # Creating SparkSession
        spark = (
            SparkSession.builder
            .appName('extract_countryregion_landing')
            
            .getOrCreate()
        )

        return spark, hadoop_paths.get("minio_landing", ""), delta_paths.get("raw_icebergdb", "")

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
            logger.info("SparkSession stopped successfully.")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()

def main():
    spark, minio_landing, bronze_base = load_source_connections()
    raw_table = "raw_dimcountryregion"
        
    dataframe = extract_landing_data(spark, minio_landing, "countryregion")
    
    dataframe = convert_country_schema(dataframe)
        
    write_to_delta(dataframe=dataframe, bronze_base=bronze_base, table_name=raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == '__main__':
    main()
    logger.info("Sparkjob completed successfully")