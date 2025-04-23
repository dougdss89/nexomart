from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.functions import col
from sys import exit
from traceback import print_exc
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#teste de sincronização
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
        
def extract_landing_buyer(spark, minio_landing, raw_table_mame:str):
    """ Extract buyer data from the landing zone. """
    try:

        
        dataframe = spark.read.parquet(f"{minio_landing}/{raw_table_mame}")
        
        return dataframe
    
    except Exception as e:
        print(f"Failed to extract buyer data: {str(e)}")
        print_exc()
        exit(1)

def buyer_replace_schema(dataframe):
    
# Aplicar as transformações de tipo nas colunas
    dataframe = dataframe.select(
                            col("businessentityid").cast("int").alias("businessentityid"),
                            col("firstname").cast("string").alias("firstname"),
                            col("lastname").cast("string").alias("lastname"),
                            col("loginid").cast("string").alias("loginid"),
                            col("birthdate").cast("date").alias("birthdate"),
                            col("maritalstatus").cast("string").alias("maritalstatus"),
                            col("gender").cast("string").alias("gender"),
                            col("hiredate").cast("date").alias("hiredate"),
                            col("jobtitle").cast("string").alias("jobtitle"),
                            col("departmentid").cast("int").alias("departmentid"),
                            col("name").cast("string").alias("name"),
                            col("groupname").cast("string").alias("groupname"),
                            col("payfrequency").cast("int").alias("payfrequency"),
                            col("rate").cast("decimal(10,2)").alias("rate"),
                            col("ratechangedate").cast("date").alias("salarychange_date"),
                            col("startdate").cast("date").alias("startdate"),
                            col("enddate").cast("date").alias("enddate"),
                            col("salariedflag").cast("int").alias("salariedflag"),
                            col("currentflag").cast("int").alias("currentflag"),
                            col("inserted_date").cast("timestamp").alias("extracted_date"),
                            col("modified_date").cast("timestamp").alias("modified_date"),
                            col("eltversion").cast("int").alias("eltversion")
                        )
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
    """ Stop the SparkSession. """
    try:
        if spark:
            spark.stop()
            print("SparkSession stopped successfully.")
            
    except Exception as e:
        print(f"Failed to stop SparkSession: {str(e)}")
        print_exc()

def main():
    spark, minio_landing, iceberg, iceberg_catalog = load_source_connections()
    raw_table = "raw_dimbuyer"
    
    
    # Extract data from landing zone
    dataframe = extract_landing_buyer(spark, minio_landing, "buyer")
    
    dataframe = buyer_replace_schema(dataframe)
        
    # Write data to Iceberg
    write_to_iceberg(dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name= raw_table)
    
    # Stop SparkSession
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")