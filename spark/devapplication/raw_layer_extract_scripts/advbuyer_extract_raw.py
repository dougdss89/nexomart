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
            .appName('extract_buyer_landing')
            .enableHiveSupport()
            .getOrCreate()
        )

        return spark, minio_landing.get("minio_landing", ""), bronze_delta.get("bronze_layer", "") #, minio_storage

    except (FileNotFoundError, KeyError) as e:
        print(f"Could not load connections from source: {e}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        print(f"Failed to create SparkSession. Runtime error: {e}")
        print_exc()
        exit(1)

    except Exception as e:
        print(f"Unexpected error occurred. Please verify: {e}")
        print_exc()
        exit(1)
        

def extract_landing_buyer(spark, minio_landing, raw_table_mame:str):
    """ Extract buyer data from the landing zone. """
    try:
        df_buyer_raw = spark.read.parquet(f"{minio_landing}/{raw_table_mame}")
        
        return df_buyer_raw
    
    except Exception as e:
        print(f"Failed to extract buyer data: {e}")
        print_exc()
        exit(1)

def buyer_replace_schema(dataframe):
    
# Aplicar as transformações de tipo nas colunas
    buyer_dataframe = dataframe.select(
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
    return buyer_dataframe

# def write_minio_buyer_storage(data, minio_storage, table):
    
#     try:
#         data.write.mode("overwrite").parquet(f"{minio_storage}/{table}")
        
#         print(f"{table} written successfully on {minio_storage}")
        
#     except Exception as e:
#         print(f"Fail to create file on {minio_storage}. Verify: {e}")
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
    """ Stop the SparkSession. """
    try:
        if spark:
            spark.stop()
            print("SparkSession stopped successfully.")
            
    except Exception as e:
        print(f"Failed to stop SparkSession: {e}")
        print_exc()

def main():
    spark, minio_landing, bronze_delta = load_source_connections()
    filepath = "advworks"

    # Extract data from landing zone
    buyer_df = extract_landing_buyer(spark, minio_landing, "buyer")
    
    buyer_dataframe = buyer_replace_schema(buyer_df)
        
    # write_minio_buyer_storage(buyer_dataframe, minio_path, "buyer")
    
    # Write data to Iceberg
    write_to_delta(buyer_dataframe, bronze_delta, filepath, "raw_dimbuyer")
    
    # Stop SparkSession
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")