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
                            app_name="extract_product_landing"
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
        
def convert_product_schema(tmp_product):
    
    try:
        tmp_product.select(
            
                        col("productid").cast("int").alias("product_id"),
                        col("productname").cast("string").alias("productname"),
                        col("productnumber").cast("string").alias("productname"),
                        col("productsubcategoryid").cast("int").alias("productsubcategory_id"),
                        col("subcategoryname").cast("string").alias("subcategoryname"),
                        col("productcategoryid").cast("int").alias("productcategory_id"),
                        col("categoryname").cast("string").alias("categoryname"),
                        col("color").cast("string").alias("color"),
                        col("class").cast("string").alias("class"),
                        col("size").cast("string").alias("size"),
                        col("productline").cast("string").alias("procutline"),
                        col("productmodelid").cast("int").alias("productmodel_id"),
                        col("modelname").cast("string").alias("modelname"),
                        col("style").cast("string").alias("style"),
                        col("listprice").cast("float").alias("listprice"),
                        col("standardcost").cast("float").alias("standardcost"),
                        col("sellstartdate").cast("timestamp").alias("sellstartdate"),
                        col("sellenddate").cast("timestamp").alias("sellenddate"),
                        col("safetystocklevel").cast("int").alias("safetystocklevel"),
                        col("reorderpoint").cast("int").alias("reorderpoint"),
                        col("sizeunitmeasurecode").cast("string").alias("sizeunitcode"),
                        col("weightunitmeasurecode").cast("string").alias("weightunitcode"),
                        col("weight").cast("float").alias("weight"),
                        col("daystomanufacture").cast("int").alias("daystomanufacture"),
                        col("discontinueddate").cast("timestamp").alias("discontinuedadate"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
        )
        return tmp_product
    
    except Exception as e:
        logger.error(f"Error to convert dataframe schema {tmp_product}. Please verify: {e}")
        print_exc()
        exit(1)
        
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        product_df = (spark
                        .read
                        .parquet(f"{minio_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return product_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
# def write_product_minio_storage(data, minio_storage, table):
    
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
        
def write_to_iceberg(product_df, iceberg_rawdb, table_name):
    
    try:
        (
            product_df
                .write
                .format("Iceberg")
                .mode("append")
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
    
    spark, minio_landing, iceberg_rawdb = load_source_connections()
    
    product_df = extract_landing_data(spark, minio_landing, "product")
    
    product = convert_product_schema(product_df)
    
    # write_product_minio_storage(product, miniopath, "product")
    
    write_to_iceberg(product, f"{iceberg_rawdb}", "raw_dimproduct")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")