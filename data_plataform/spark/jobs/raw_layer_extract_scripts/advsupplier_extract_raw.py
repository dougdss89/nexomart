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
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_supplier_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", ""), iceberg_catalog.get("iceberg_catalog", "") #, minio_storage
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)
        
    except RuntimeError as e:
        logger.error(f"Failed to create Spark Session. Runtime Error: {str(e)}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error occur. {str(e)}")
        print_exc()
        exit(1)
        
def extract_landing_data(spark, minio_landing, raw_table):
    try:
        dataframe = (spark
                        .read
                        .parquet(f"{minio_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {str(e)}")
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
        logger.error(f"Error to convert dataframe {tmp_supplier} schema. Please verify: {str(e)}")
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
    raw_table = "raw_dimsupplier"
    
    dataframe = extract_landing_data(spark, minio_landing, "supplier")
    
    dataframe = convert_supplier_schema(dataframe)
    
    # write_supplier_minio_storage(supplier, miniopath, "supplier")
    
    write_to_iceberg(dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name= raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")