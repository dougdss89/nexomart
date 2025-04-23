import logging
from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, cast
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
            .appName('extract_buyer_landing')
            
            .getOrCreate()
        )

        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", ""), iceberg_catalog.get("iceberg_catalog", "")

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
        
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        currency_rate_dataframe = (spark
                                .read
                                .parquet(f"{minio_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return currency_rate_dataframe
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {str(e)}")
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
        logger.info(f"Fail to convert schema. Verify: {str(e)}")
        print_exc()
        exit(1)
        
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
            logger.info("SparkSession stoped successfully")
            
    except Exception as e:
        logger.error(f"Failed to stop SparkSession: {str(e)}")
        print_exc()
        
        
def main():
    
    spark, minio_landing, iceberg, iceberg_catalog = load_source_connections()
    raw_table = "raw_dimcurrencyrate"
    
    dataframe = extract_landing_data(spark, minio_landing, "currency_rate")
    
    dataframe = convert_currencyrate_schema(dataframe)
    
    # write_currency_rate_minio_storage(currency_rate, miniopath, "currency_rate" )
        
    write_to_iceberg(dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name= raw_table)
    
    spark_stop(spark)

    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")