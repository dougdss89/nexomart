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
                            app_name="extract_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", "")
    
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
        
def convert_schema(dataframe):
    
    try:
        dataframe.select(
            
                        col("productid").cast("int").alias("id"),
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
        return dataframe
    
    except Exception as e:
        logger.error(f"Error to convert dataframe schema {dataframe}. Please verify: {e}")
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
                
def write_to_iceberg(dataframe, raw_icebergdb:str,  table_name:str):
    """ Write DataFrame to iceberg table. """
    try:
        raw_lakehouse = f"{raw_icebergdb}.{table_name}"

        (
            dataframe
            .write
            .format("iceberg")
            .mode("append")  # Use "append" to add data instead
            .saveAsTable(f"{raw_lakehouse}")
        )
        return(f"Table {table_name} written successfully to iceberg.")
        
    except Exception as e:
        print(f"Failed to write table {table_name} to iceberg: {e}")
        print_exc()
        exit(1)
        
    except Exception as e:
        logger.error(f"Failed to write table {table_name} to iceberg table: {e}")
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
    
    raw_table = "raw_dimproduct"
    
    dataframe = extract_landing_data(spark, minio_landing, "product")
    
    dataframe = convert_schema(dataframe)
            
    write_to_iceberg(dataframe, iceberg,  raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")