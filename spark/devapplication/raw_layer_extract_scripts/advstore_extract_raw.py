import logging
from configconnections_loader import load_config, validate_required_keys
from pyspark.sql.functions import col, cast
from startSparkSession import SparkSessionManager
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
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_store_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing.get("minio_landing", ""), bronze_delta.get("bronze_layer", "") #, minio_storage
    
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
        
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        store_df = (spark
                        .read
                        .parquet(f"{minio_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return store_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_store_schema(tmp_store):
    try:
        tmp_store.select(
                        col("storeid").cast("int").alias("store_id"),
                        col("personid").cast("int").alias("person_id"),
                        col("customerid").cast("int").alias("customer_id"),
                        col("accountnumber").cast("string").alias("accountnumber"),
                        col("storename").cast("string").alias("storename"),
                        col("firstname").cast("string").alias("firstname"),
                        col("lastname").cast("string").alias("lastname"),
                        col("annualsales").cast("float").alias("annualsales"),
                        col("annualrevenue").cast("float").alias("annualrevenue"),
                        col("bankname").cast("string").alias("bankname"),   
                        col("businesstype").cast("string").alias("businesstype"),
                        col("yearopened").cast("int").alias("yearopened"),
                        col("Specialty").cast("string").alias("Specialty"),
                        col("squarefeet").cast("float").alias("squarefeet"),
                        col("brands").cast("string").alias("brands"),
                        col("internet").cast("string").alias("internet"),
                        col("numberemployees").cast("int").alias("numberemployees"),
                        col("salespersonid").cast("int").alias("salesperson_id"),
                        col("territoryid").cast("int").alias("territory_id"),
                        col("continentname").cast("string").alias("continentname"),
                        col("regionname").cast("string").alias("region_name"),
                        col("countryregioncode").cast("string").alias("countryregioncode"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
        )
        
        return tmp_store
    
    except Exception as e:
        logger.error(f"Error to convert dataframe {tmp_store} schema. Please verify: {e}")
        print_exc()
        exit(1)
        
# def write_store_minio_storage(data, minio_storage, table):
    
#     try:
#         (
#             data
#             .write
#             .mode("append")
#             .parquet(f"{minio_storage}/{table}")
#         )
        

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
        return(f"Table {table_name} written successfully to Delta.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to Delta: {e}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Failed to write table {table_name} to Delta table: {e}")
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
    
    spark, minio_landing, bronze_delta = load_source_connections()
    filepath = "advworks"
    raw_table = "raw_dimstore"
    
    store_df = extract_landing_data(spark, minio_landing, "store")
    
    dataframe = convert_store_schema(store_df)
    
    # write_store_minio_storage(store, miniopath, "store")
    
    write_to_delta(dataframe, bronze_delta, filepath, raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")