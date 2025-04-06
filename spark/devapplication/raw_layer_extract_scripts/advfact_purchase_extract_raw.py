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
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_fact_purchase_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", "") #, minio_storage
    
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
        dataframe = (spark
                                .read
                                .parquet(f"{minio_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return dataframe
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_fctpurchase_schema(dataframe):
    
    try:
        dataframe.select(
                        
                        col("purchaseorderid").cast("int").alias("purchaseorder_id"),
                        col("purchaseorderdetailid").cast("int").alias("purchaseorderdetail_id"),
                        col("revisionnumber").cast("int").alias("revisionnumber"),
                        col("status").cast("int").alias("status"),
                        col("employeeid").cast("int").alias("employee_id"),
                        col("vendorid").cast("int").alias("vendor_id"),
                        col("orderdate").cast("date").alias("orderdate"),
                        col("shipdate").cast("date").alias("shipdate"),
                        col("duedate").cast("date").alias("duedate"),
                        col("shipmethodid").cast("int").alias("shipmethod_id"),
                        col("productid").cast("int").alias("producti_id"),
                        col("receivedqty").cast("int").alias("received_qty"),
                        col("rejectedqty").cast("int").alias("rejected_qty"),
                        col("stockedqty").cast("int").alias("stocked_qty"),
                        col("unitprice").cast("float").alias("unit_price"),
                        col("freight").cast("float").alias("freight"),
                        col("taxamt").cast("float").alias("taxamt"),
                        col("linetotal").cast("float").alias("linetotal"),
                        col("subtotal").cast("float").alias("subtotal"),
                        col("totaldue").cast("float").alias("totaldue"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
                    )
                        
        return dataframe
        
    except Exception as e:
        logger.error(f"Error to convert schema from dataframe: {dataframe}. Please verify: {e}")
        print_exc()
                
def write_to_iceberg(dataframe, raw_icebergdb:str,  table_name:str):
    """ Write DataFrame to iceberg table. """
    try:
        raw_lakehouse = f"{raw_icebergdb}.{table_name}"

        (
            dataframe
            .write.format("iceberg")
            .mode("append")  # Use "append" to add data instead
            .saveAsTable(f"{raw_lakehouse}")
        )
        return(f"Table {table_name} written successfully to iceberg.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to iceberg: {e}")
        print_exc()
        exit(1)
        
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
    
    spark, minio_landing, iceberg = load_source_connections()
    
    raw_table = "raw_fact_purchase"
    
    dataframe = extract_landing_data(spark, minio_landing, "fact_purchase")
    
    dataframe = convert_fctpurchase_schema(dataframe)
    
    # write_fct_purchase_minio_storage(fact_purchase, miniopath, "fact_purchase")
    
    write_to_iceberg(dataframe, iceberg,   raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")