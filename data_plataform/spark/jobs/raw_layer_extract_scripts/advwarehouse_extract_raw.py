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
        hadoop_paths = config.get("hadoop_paths", {})
        delta_paths = config.get("delta_path", {})

        validate_required_keys(
            config=hadoop_paths,
            required_keys=["minio_landing"],
            config_name="hadoop_paths"
        )
        validate_required_keys(
            config=delta_paths,
            required_keys=["raw_icebergdb"],
            config_name="delta_path"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_warehouse_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, hadoop_paths.get("minio_landing", ""), delta_paths.get("raw_icebergdb", "")
    
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

def convert_warehouse_schema(dataframe):
   
    try: 
        dataframe.select(

                        col("locationid").cast("int").alias("location_id"),
                        col("shelf").cast("string").alias("shelf"),
                        col("bin").cast("int").alias("bin"),
                        col("quantity").cast("int").alias("quantity"),
                        col("productid").cast("int").alias("product_id"),
                        col("availability").cast("float").alias("availability"),
                        col("costrate").cast("float").alias("costrate"),
                        col("name").cast("string").alias("name"),
                        col("billofmaterialsid").cast("string").alias("billofmaterials_id"),
                        col("componentid").cast("int").alias("component_id"),
                        col("bomlevel").cast("int").alias("bomlevel"),
                        col("startdate").cast("date").alias("startdate"),
                        col("enddate").cast("date").alias("end_date"),
                        col("perassemblyqty").cast("float").alias("perassemblyqty"),
                        col("unitmeasurecode").cast("string").alias("measurecode"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")                    
        )
        return dataframe

    except Exception as e:
        logger.error(f"Error to convert dataframe {dataframe} schema. Please verify: {str(e)}")
        print_exc()
        exit(1)
        
def write_to_delta(dataframe, bronze_base: str, table_name: str):
    """ Write DataFrame to Delta Lake at the bronze S3A path. """
    try:
        path = f"{bronze_base}/advworks_rawdb/{table_name}"
        (
            dataframe
            .write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(path)
        )
        print(f"Table {table_name} written to Delta at {path}")
        logger.info(f"Table {table_name} written to Delta at {path}")
    except Exception as e:
        print(f"Failed to write table {table_name} to Delta: {str(e)}")
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
    
    spark, minio_landing, bronze_base = load_source_connections()
    raw_table = "raw_dimwarehouse"
    
    dataframe = extract_landing_data(spark, minio_landing, "warehouse")
    
    dataframe = convert_warehouse_schema(dataframe)
    
    # write_warehouse_minio_storage(warehouse, miniopath, "warehouse")
    
    write_to_delta(dataframe=dataframe, bronze_base=bronze_base, table_name=raw_table)
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")