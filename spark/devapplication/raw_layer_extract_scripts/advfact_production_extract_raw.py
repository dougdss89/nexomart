import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cast
from configconnections_loader import load_config, validate_required_keys
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
        
        spark = (
            SparkSession
                .builder
                .appName("extract_fact_production_landing")
                
                .getOrCreate()
        )
        return spark, minio_landing.get("minio_landing", ""), iceberg.get("raw_icebergdb", ""), iceberg_catalog.get("iceberg_catalog", "") #, minio_storage
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {e}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        logger.error(f"Failed to create SparkSession. Runtime error: {e}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occurred. Please verify: {e}")
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
        
def convert_factproduction_schema(dataframe):
    
    try:
    
        dataframe.select(
                        col("workorderid").cast("int").alias("workorderid"),
                        col("productid").cast("int").alias("productid"),
                        col("orderqty").cast("int").alias("orderqty"),
                        col("stockedqty").cast("int").alias("stockedqty"),
                        col("scrappedqty").cast("int").alias("scrappedqty"),
                        col("scrapreasonid").cast("int").alias("scrapreasonid"),
                        col("locationid").cast("int").alias("locationid"),
                        col("actualresourcehrs").cast("float").alias("actualresourcehrs"),
                        col("actualcost").cast("float").alias("actualcost"),
                        col("scheduledstartdate").cast("date").alias("scheduledstartdate"),
                        col("scheduledenddate").cast("date").alias("scheduledenddate"),
                        col("actualstartdate").cast("date").alias("actualstartdate"),
                        col("actualenddate").cast("date").alias("actualenddate"),
                        col("startdate").cast("date").alias("startdate"),
                        col("enddate").cast("date").alias("enddate"),
                        col("duedate").cast("date").alias("duedate"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
            
        )
        return dataframe
    
    except Exception as e:
        logger.error(f"Error to convert dataframe schema: {dataframe}. Please verify: {e}")
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
    raw_table = "raw_fact_production"
    
    fct_production = extract_landing_data(spark, minio_landing, "fact_production")
    
    dataframe = convert_factproduction_schema(fct_production)
    
    # write_fact_production_minio_storage(fact_production, miniopath, "fact_production")
    
    write_to_iceberg(dataframe=dataframe, catalog=iceberg_catalog, raw_icebergdb=iceberg, table_name= raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")