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
                            app_name="extract_salesperson_landing"
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
        salesperson_df = (spark
                        .read
                        .parquet(f"{hadoop_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return salesperson_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_salesperson_schema(tmp_salesperson):
    
    try:
        tmp_salesperson.select(
                        col("businessentityid").cast("int").alias("businessentity_id"),
                        col("loginid").cast("string").alias("logind_id"),
                        col("firstname").cast("string").alias("firstname"),
                        col("lastname").cast("string").alias("lastname"),
                        col("gender").cast("string").alias("gender"),
                        col("birthdate").cast("string").alias("birthdate"),
                        col("jobtitle").cast("string").alias("jobtitle"),
                        col("maritalstatus").cast("string").alias("maritalstatus"),
                        col("organizationlevel").cast("int").alias("organizationlevel"),
                        col("departmentid").cast("int").alias("department_id"),
                        col("startdate").cast("date").alias("start_date"),
                        col("enddate").cast("date").alias("end_date"),
                        col("groupname").cast("string").alias("groupname"),
                        col("departmentname").cast("string").alias("departmentname"),
                        col("payfrequency").cast("int").alias("payfrequency"),
                        col("rate").cast("float").alias("salary"),
                        col("ratechangedate").cast("float").alias("ratechangedate"),
                        col("commissionpct").cast("float").alias("commission_pct"),
                        col("salesquota").cast("float").alias("salesquota"),
                        col("bonus").cast("float").alias("bonus"),
                        col("regionname").cast("string").alias("regioname"),
                        col("continentname").cast("string").alias("continentname"),
                        col("countryregioncode").cast("string").alias("countrycode"),
                        col("territoryid").cast("int").alias("territoryid"),
                        col("salariedflag").cast("int").alias("salariedflag"),
                        col("currentflag").cast("int").alias("currentflag"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
        )
        
        return tmp_salesperson
    
    except Exception as e:
        logger.error(f"Error to convert dataframe {tmp_salesperson} schema. Please verify: {e}")
        print_exc()
        exit(1)
        

def write_salesperson_minio_storage(data, minio_storage, table):
    
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
        
def write_to_iceberg(salesperson_df, iceberg_rawdb, table_name):
    
    try:
        (
            salesperson_df
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
    
    salesperson_df = extract_landing_data(spark, hadoop_landing, "salesperson")
    
    salesperson = convert_salesperson_schema(salesperson_df)
    
    write_salesperson_minio_storage(salesperson, miniopath, "salesperson")
    
    write_to_iceberg(salesperson, f"{iceberg_rawdb}", "raw_dimsalesperson")
    
    spark_stop(spark)
    
    return logger.info("All functions have been executed successfully ")
    
if __name__ == "__main__":
    main()
    logger.info("Sparkjob completed successfully")