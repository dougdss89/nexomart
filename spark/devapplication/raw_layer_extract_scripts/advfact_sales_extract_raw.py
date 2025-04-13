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
                            app_name="extract_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", ""), iceberg_catalog.get("iceberg_catalog", "") #, minio_storage
    
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
        
def convert_fctsales_schema(dataframe):
    try:
        dataframe.select(

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
        return dataframe
    
    except Exception as e:
        logger.error(f"Error to convert dataframe {dataframe} schema. Please verify: {e}")
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
        print(f"Failed to write table {table_name} to iceberg: {e}")
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
    
    spark, minio_landing, iceberg, iceberg_catalog = load_source_connections() 
    raw_table = "raw_fact_sales"
    
    dataframe = extract_landing_data(spark, minio_landing, "fact_sales")
    
    dataframe = convert_fctsales_schema(dataframe)
    
    # write_fct_sales_minio_storage(fact_sales, miniopath, "fact_sales")
    
    write_to_iceberg(dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name= raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")