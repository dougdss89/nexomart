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
    
    """ Load source from .env file and create Spark Session """
    try:
        config = load_config()
        
        hadoop_landing = config.get("hadoop_paths", {}).get("landing", "")
        iceberg_rawdb = config.get("hadoop_paths", {}).get("raw_icebergdb", "")
        minio_storage = config.get("hadoop_paths", {}).get("minio_landing", "")
        
        validate_required_keys(
            config=config.get("hadoop_paths",{}),
            required_keys=["landing", "raw_icebergdb", "minio_landing"],
            config_name="hadoop_paths"
        )
        #create spark session
        
        spark = (
            SparkSession
                .builder
                .appName("extract_fact_production_landing")
                .enableHiveSupport()
                .getOrCreate()
        )
        return spark, hadoop_landing, iceberg_rawdb, minio_storage
    
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
    
def extract_landing_data(spark, hadoop_landing, raw_table):
    try:
        fact_production_df = (spark
                                .read
                                .parquet(f"{hadoop_landing}/{raw_table}")
                            )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return fact_production_df
    
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
        
def write_fact_production_minio_storage(data, minio_storage, table):
    
    try:
        ((
            data
                .write
                .mode("append")
                .parquet(f"{minio_storage}/{table}")
        ))
        return logger.info(f"Data successfully written to: {minio_storage}/{table}")
    
    except Exception as e:
        logger.error(f"Fail to persist data on {minio_storage}. Verify: {e}")
        print_exc()
        exit(1)       
        
        
def write_to_iceberg(fact_production_df, iceberg_rawdb, table_name):
    
    try:
        (
            fact_production_df
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
    
    fct_production = extract_landing_data(spark, hadoop_landing, "fact_production")
    
    fact_production = convert_factproduction_schema(fct_production)
    
    write_fact_production_minio_storage(fact_production, miniopath, "fact_production")
    
    write_to_iceberg(fact_production, f"{iceberg_rawdb}", "raw_fact_production")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")