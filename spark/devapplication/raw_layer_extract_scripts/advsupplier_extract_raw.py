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
    
    """ 
        Load key-values from .env file to create Spark Session
    """
    
    try:
        config = load_config()
        
        minio_landing = config.get("hadoop_paths", {}).get("minio_landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        # minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")
        
        #validated imported keys
        
        validate_required_keys(
            config = config.get("hadoop_paths", {}),
            required_keys= ["landing", "raw_icebergdb", "minio_landing"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_supplier_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing, iceberg_rawdb #, minio_storage
    
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
        supplier_df = (spark
                        .read
                        .parquet(f"{minio_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return supplier_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_supplier_schema(tmp_supplier):
    
    try:
        tmp_supplier.select(
            
                            col("productid").cast("int").alias("product_id"),
                            col("businessentityid").cast("int").alias("businessentity_id"),
                            col("accountnumber").cast("string").alias("accountnumber"),
                            col("name").cast("string").alias("name"),
                            col("contacttype").cast("string").alias("contacttype"),
                            col("firstname").cast("string").alias("firstname"),
                            col("lastname").cast("string").alias("lastname"),
                            col("phonenumber").cast("string").alias("phonenumber"),
                            col("phonenumbertype").cast("string").alias("phonetype"),
                            col("creditrating").cast("int").alias("creditrating"),
                            col("preferredvendorstatus").cast("int").alias("principal_vendor"),
                            col("averageleadtime").cast("int").alias("avgleadtime"),
                            col("standardprice").cast("float").alias("standardprice"),
                            col("lastreceiptdate").cast("timestamp").alias("lastreceipted_date"),
                            col("minorderqty").cast("int").alias("minororderqty"),
                            col("maxorderqty").cast("int").alias("maxorderqty"),
                            col("onorderqty").cast("int").alias("on_orderqty"),
                            col("unitmeasurecode").cast("string").alias("measurecode"),
                            col("stateprovincename").cast("string").alias("stateprovince"),
                            col("countryregionname").cast("string").alias("countryregionname"),
                            col("addressline1").cast("string").alias("addressline1"),
                            col("addressline2").cast("string").alias("addressline2"),
                            col("city").cast("string").alias("city"),
                            col("postalcode").cast("string").alias("postalcode"),
                            col("inserted_date").cast("timestamp").alias("extracted_date"),
                            col("modified_date").cast("timestamp").alias("modified_date"),
                            col("eltversion").cast("int").alias("eltversion")
        )
        
        return tmp_supplier
    
    except Exception as e:
        logger.error(f"Error to convert dataframe {tmp_supplier} schema. Please verify: {e}")
        print_exc()
        exit(1)
        
# def write_supplier_minio_storage(data, minio_storage, table):
    
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
        
def write_to_iceberg(supplier_df, iceberg_rawdb, table_name):
    
    try:
        (
            supplier_df
                .write
                .format("Iceberg")
                .mode("overwrite")
                .saveAsTable(f"{iceberg_rawdb}.{table_name}")
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
    
    spark, minio_landing, iceberg_rawdb#, miniopath = load_source_connections()
    
    supplier_df = extract_landing_data(spark, minio_landing, "supplier")
    
    supplier = convert_supplier_schema(supplier_df)
    
    # write_supplier_minio_storage(supplier, miniopath, "supplier")
    
    write_to_iceberg(supplier, f"{iceberg_rawdb}", "raw_dimsupplier")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")