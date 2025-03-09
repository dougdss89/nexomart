import logging
from configconnections_loader import load_config, validate_required_keys
from startSparkSession import SparkSessionManager
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
                            app_name="extract_fact_sales_landing"
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
        fact_sales_df = (spark
                            .read
                            .parquet(f"{minio_landing}/{raw_table}")
                                
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return fact_sales_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_fctsales_schema(tmp_salesdf):
    try:
        tmp_salesdf.select(

                            col("salesorderid").cast("int").alias("salesorder_id"),
                            col("SalesOrderDetailID").cast("int").alias("salesorderdetail_id"),
                            col("accountnumber").cast("string").alias("accountnumber"),
                            col("carriertrackingnumber").cast("string").alias("carriertrackingnumber"),
                            col("purchaseordernumber").cast("string").alias("purchaseordernumber"),
                            col("salesordernumber").cast("string").alias("salesordernumber"),
                            col("orderdate").cast("date").alias("orderdate"),
                            col("shipdate").cast("date").alias("shipdate"),
                            col("duedate").cast("date").alias("duedate"),
                            col("salesreasonid").cast("int").alias("salesreason_id"),
                            col("salespersonid").cast("int").alias("salesperson_id"),
                            col("productid").cast("int").alias("product_id"),
                            col("customerid").cast("int").alias("customer_id"),
                            col("shipmethodid").cast("int").alias("shipmethod_id"),
                            col("territoryid").cast("int").alias("territory_id"),
                            col("specialofferid").cast("int").alias("specialoffer_id"),
                            col("currencyrateid").cast("int").alias("currencyrate_id"),
                            col("orderqty").cast("int").alias("orderqty"),
                            col("unitprice").cast("float").alias("unitprice"),
                            col("unitpricediscount").cast("float").alias("unitpricediscount"),
                            col("taxamt").cast("float").alias("taxamt"),
                            col("freight").cast("float").alias("freight"),
                            col("onlineorderflag").cast("int").alias("onlineorderflag"),
                            col("linetotal").cast("float").alias("linetotal"),
                            col("inserted_date").cast("timestamp").alias("extracted_date"),
                            col("modified_date").cast("timestamp").alias("modified_date"),
                            col("eltversion").cast("int").alias("eltversion")
            
        )
        return tmp_salesdf
    
    except Exception as e:
        logger.error(f"Error to convert dataframe {tmp_salesdf} schema. Please verify: {e}")
        print_exc()
        exit(1)
        
# def write_fct_sales_minio_storage(data, minio_storage, table):
    
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
    
    spark, minio_landing, bronze_delta = load_source_connections()
    filepath = "advworks"
    raw_table = "raw_fact_sales"
    
    fact_sales_df = extract_landing_data(spark, minio_landing, "fact_sales")
    
    dataframe = convert_fctsales_schema(fact_sales_df)
    
    # write_fct_sales_minio_storage(fact_sales, miniopath, "fact_sales")
    
    write_to_delta(dataframe, bronze_delta, filepath, raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")