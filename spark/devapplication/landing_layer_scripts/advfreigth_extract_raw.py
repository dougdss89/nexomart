from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

def load_source_connections():
    """This function loads the .env file, creates a SparkSession, and validates key configurations."""
    try:
        print("Loading .env file...")
        config = load_config()
        print("Configuration loaded.")
        
    except FileNotFoundError as e:
        print(f"File .env was not found: {e}")
        return None, None, None
    except KeyError as e:
        print(f"Missing key-value pairs: {e}")
        return None, None, None

    #validating imported keys
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        minio_landing = config.get("hadoop_paths", {})

        validate_required_keys(config=spark_jars, 
                               required_keys=["mssql_spark_jars"],
                               config_name="spark_jars")
        
        #validade properties key
        validate_required_keys(config= mssql_adventureworks,
                               required_keys=["properties"],
                               config_name="mssql_adventureworks")
        
        validate_required_keys(
            config=minio_landing,  # validating hadoop_path key
            required_keys=["minio_landing"],  # check if key exists
            config_name="hadoop_paths"
        )
        
        spark = (SparkSession
                    .builder
                    .appName("extract_freight_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")
    
    except RuntimeError as e:
        print(f"Configuration validation failed: {e}")
        return None, None, None
    
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        print_exc()
        return None, None, None

def extract_freight_raw(spark, mssql_properties, mssql_adventureworks):
    
    """Extract freight data as a DataFrame."""
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing connection or session variables. Invalid parameters.")
        return None

    try:
        df_freight_raw = (spark
                          .read
                          .format("jdbc")
                          .option("url", mssql_adventureworks["url"])
                          .option("driver", mssql_properties["driver"])
                          .option("user", mssql_properties["user"])
                          .option("password", mssql_properties["password"])
                          .option("query",
                                  """ 
                                  SELECT 
                                      COALESCE(shipmethodid, 0) AS shipmethodid,
                                      [Name],
                                      shipbase,
                                      shiprate,
                                      getdate() as inserted_date,
									  '9999-31-12 00:00:00.000' as modified_date,
									  '1' as eltversion
                                  FROM purchasing.shipmethod
                                  """)
                          .load())
        return df_freight_raw
    
    except Exception as e:
        print(f"Error extracting freight data: {e}")
        print_exc()
        return None

def stop_spark(spark):
    """Stop the SparkSession."""
    if spark:
        spark.stop()

def main():
    """Main entry point for the script."""
    spark = None
    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()

        if not spark:
            print("Failed to create Spark Session.")
            exit(1)

        freight_df = extract_freight_raw(spark, mssql_properties, mssql_adventureworks)

        if freight_df:
            freight_df.write.parquet(f"{minio_landing}/freigth", mode="overwrite")
            print("Dataframe loaded successfully to HDFS.")

    except Exception as e:
        print("Main script encountered an error.")
        print_exc()
        
    finally:
        stop_spark(spark)
        print("Spark Session successfully closed.")

if __name__ == "__main__":
    main()
