import logging
from modules.configconnections_loader import load_config, validate_required_keys
from pyspark.sql.functions import col, cast
from modules.startSparkSession import SparkSessionManager
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
        
        #validated imported keys
        
        validate_required_keys(
            config = config.get("hadoop_paths", {}),
            required_keys= ["landing", "raw_icebergdb"],
            config_name="hadoop_paths"
        )
        
        #create spark session
        
        session_manager = SparkSessionManager(
                            app_name="extract_currency_rate_landing"
        )
        
        spark = session_manager.create_session()
        
        return spark, hadoop_landing, iceberg_rawdb
    
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
        store_df = (spark
                        .read
                        .parquet(f"{hadoop_landing}/{raw_table}")
                    )
        logger.info(f"Data extracted sucessfully for table: {raw_table}")
        
        return store_df
    
    except Exception as e:
        logger.error(f"Failed to extract data from {raw_table}: {e}")
        print_exc()
        exit(1)
        
def convert_store_schema(tmp_store):
    try:
        tmp_store.select(
                        col("storeid").cast("int").alias("store_id"),
                        col("personid").cast("int").alias("person_id"),
                        col("customerid").cast("int").alias("customer_id"),
                        col("accountnumber").cast("string").alias("accountnumber"),
                        col("storename").cast("string").alias("storename"),
                        col("firstname").cast("string").alias("firstname"),
                        col("lastname").cast("string").alias("lastname"),
                        col("annualsales").cast("float").alias("annualsales"),
                        col("annualrevenue").cast("float").alias("annualrevenue"),
                        col("bankname").cast("string").alias("bankname"),   
                        col("businesstype").cast("string").alias("businesstype"),
                        col("yearopened").cast("int").alias("yearopened"),
                        col("speciality").cast("string").alias("speciality"),
                        col("squarefeet").cast("float").alias("squarefeet"),
                        col("brands").cast("string").alias("brands"),
                        col("internet").cast("string").alias("internet"),
                        col("numberemployees").cast("int").alias("numberemployees"),
                        col("salespersonid").cast("int").alias("salesperson_id"),
                        col("territoryid").cast("int").alias("territory_id"),
                        col("continentname").cast("string").alias("continentname"),
                        col("regionname").cast("string").alias("region_name"),
                        col("countryregioncode").cast("string").alias("countryregioncode"),
                        col("inserted_date").cast("timestamp").alias("extracted_date"),
                        col("modified_date").cast("timestamp").alias("modified_date"),
                        col("eltversion").cast("int").alias("eltversion")
        )
        
        return tmp_store
    
    except Exception as e:
        logger.error(f"Error to convert dataframe {tmp_store} schema. Please verify: {e}")
        print_exc()
        exit(1)
        
def write_to_iceberg(store_df, iceberg_rawdb, table_name):
    
    try:
        (
            store_df
                .write
                .format("Iceberg")
                .mode("overwrite")
                .saveAsTable(f"iceberg_table.{iceberg_rawdb}.{table_name}")
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
    
    spark, hadoop_landing, iceberg_rawdb = load_source_connections()
    
    store_df = extract_landing_data(spark, hadoop_landing, "store")
    
    store = convert_store_schema(store_df)
    
    write_to_iceberg(store, f"{iceberg_rawdb}", "raw_dimstore")
    
    spark_stop(spark)
    
if __name__ == "__main__":
    main()