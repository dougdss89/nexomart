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
        
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")
        
        #validated imported keys
        
        validate_required_keys(
            config = config.get("hadoop_paths", {}),
            required_keys= ["landing", "raw_icebergdb", "minio_landing"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_fact_purchase_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, hadoop_landing, iceberg_rawdb, minio_storage
    
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
        
def extract_landing_data(spark, hadoop_landing, raw_table):
    try:
        fact_purchase_df = (spark
                                .read
                                .parquet(f"{hadoop_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return fact_purchase_df
    
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
        
def write_fct_purchase_minio_storage(data, minio_storage, table):
    
    try:
        (
            data
            .write
            .mode("append")
            .parquet(f"{minio_storage}/{table}")
        )
        

        return logger.info(f"Data successfully written to: {minio_storage}/{table}")


    except Exception as e:
        logger.error(f"Fail to persist data on {minio_storage}. Verify: {e}")
        print_exc()
        exit(1)
        
def write_to_iceberg(fact_purchase_df, iceberg_rawdb, table_name):
    
    try:
        (
            fact_purchase_df
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
    
    spark, hadoop_landing, iceberg_rawdb, miniopath = load_source_connections()
    
    fact_purchase_df = extract_landing_data(spark, hadoop_landing, "fact_purchase")
    
    fact_purchase = convert_fctpurchase_schema(fact_purchase_df)
    
    write_fct_purchase_minio_storage(fact_purchase, miniopath, "fact_purchase")
    
    write_to_iceberg(fact_purchase, f"{iceberg_rawdb}", "raw_fact_purchase")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")