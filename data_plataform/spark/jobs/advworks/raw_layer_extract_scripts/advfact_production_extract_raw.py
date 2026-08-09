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
        hadoop_paths = config.get("hadoop_paths", {})

        validate_required_keys(
            config=hadoop_paths,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )
        #create spark session
        
        spark = (
            SparkSession
                .builder
                .appName("extract_fact_production_landing")
                
                .getOrCreate()
        )
        return spark, hadoop_paths.get("minio_landing", "")
    
    except (FileNotFoundError, KeyError) as e:
        logger.error(f"Could not load connections from source: {str(e)}")
        print_exc()
        exit(1)

    except RuntimeError as e:
        logger.error(f"Failed to create SparkSession. Runtime error: {str(e)}")
        print_exc()
        exit(1)

    except Exception as e:
        logger.error(f"Unexpected error occurred. Please verify: {str(e)}")
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
        logger.error(f"Error to convert dataframe schema: {dataframe}. Please verify: {str(e)}")
        print_exc()
        exit(1)
        
        
def write_to_iceberg(spark, dataframe, table_name: str):
    """ Write DataFrame to Iceberg at iceberglake.bronze.<table_name>. """
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberglake.bronze")
        (
            dataframe
            .writeTo(f"iceberglake.bronze.{table_name}")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        print(f"Table {table_name} written to Iceberg at iceberglake.bronze.{table_name}")
        logger.info(f"Table {table_name} written to Iceberg at iceberglake.bronze.{table_name}")
    except Exception as e:
        print(f"Failed to write table {table_name} to Iceberg: {str(e)}")
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
    
    spark, minio_landing = load_source_connections()
    raw_table = "raw_fact_production"
    
    fct_production = extract_landing_data(spark, minio_landing, "fact_production")
    
    dataframe = convert_factproduction_schema(fct_production)
    
    # write_fact_production_minio_storage(fact_production, miniopath, "fact_production")
    
    write_to_iceberg(spark=spark, dataframe=dataframe, table_name=raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")

if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")