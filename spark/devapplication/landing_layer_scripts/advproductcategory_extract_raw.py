from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

print("Start script")

def load_source_connections():
    
    """ this function loads .env file """
    try:
        print("Load .env file")
        config = load_config()  
    
    except FileNotFoundError as e:
        print(f"File .env not founded: {e}")
        return None
    
    except KeyError as e:
        print("Missing some key-values pair: {e}")
        return None
    
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
                    .appName("extract_product_category_raw")
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
    

def extract_product_category_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing some configurations")
        return None
    
    try:
        df_product_category_raw = (spark
                                    .read
                                    .format("jdbc")
                                    .option("url", mssql_adventureworks["url"])
                                    .option("driver", mssql_properties["driver"])
                                    .option("user", mssql_properties["user"])
                                    .option("password", mssql_properties["password"])
                                    .option("query", 

                                            """ 
                                            
                                                select 

                                                    coalesce(ProductCategoryID, 0) as productcategoryid,
                                                    [name] as categoryname,
                                                    getdate() as inserted_date,
									                '9999-31-12 00:00:00.000' as modified_date,
									                '1' as eltversion

                                                from production.productcategory
                                            
                                            """
                                        ).load())
        
        return df_product_category_raw

    except Exception as e:
        print(f"Error extracting freight data: {e}")
        print_exc()
        return None
    
def stop_spark(spark):
    """ stop Spark Session """
    if spark:
        spark.stop()
        
def main():
    """ Main script point """
    
    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()
        
        if not spark:
            print(" Missing Spark Session.")
            exit(1)
            
            
        product_category_df = extract_product_category_raw(spark, mssql_properties, mssql_adventureworks)
        
        if product_category_df:
            product_category_df.repartition(2).write.parquet(f"{minio_landing}/productcategory", mode="overwrite")
            print(f"Dataframe loaded successfully on destination: {product_category_df}")
            
    except Exception as e:
        print("Main script encontered error: {e}")
        print_exc()
        
    finally:
        print("Stop Spark session...")
        stop_spark(spark)
        
        print("Spark session closed.")
        
if __name__ == "__main__":
    main()