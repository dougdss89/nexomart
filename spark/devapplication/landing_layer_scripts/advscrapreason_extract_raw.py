from pyspark.sql.session import SparkSession
from spark.devapplication.modules.configconnections_loader_old import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#loading .env and key-values configurations

def load_source_connections():
    
    #extract values from .env file
    
    try:
        print("Loading .env")
    
        config = load_config()
        
    except FileExistsError as e:
        print(f"File .env was not founded and loaded: {e}")
        print_exc()
        return None
    
    except KeyError as e:
        print(f"Missing some key-values pair: {e}")
        print_exc()
        return None
    
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        hadoop_landing = config.get("hadoop_paths", {})

        validate_required_keys(config=spark_jars, 
                               required_keys=["mssql_spark_jars", "postgresql_jar"],
                               config_name="spark_jars")
        
        #validade properties key
        validate_required_keys(config= mssql_adventureworks,
                               required_keys=["properties"],
                               config_name="mssql_adventureworks")
        
        validate_required_keys(config=hadoop_landing,
                               required_keys=["landing"],
                               config_name="hadoop_paths")
        
        spark = (SparkSession
                    .builder
                    .appName("extract_scrapreason_raw")
                        .config("spark.jars", f"{spark_jars.get("mssql_spark_jars", "")}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get("mssql_spark_jars", "")}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get("mssql_spark_jars", "")}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, hadoop_landing

    except RuntimeError as e:
        print(f"Fail to create spark session: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"unexpected error ocurred: {e}")
        print_exc()
        
        return None, None, None
    
def extract_scrapreason_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing configurations")
            
    try:        
        df_scrapreason_raw = (spark
                                .read
                                .format("jdbc")
                                .option("url", mssql_adventureworks["url"])
                                .option("driver", mssql_properties["driver"])
                                .option("user", mssql_properties["user"])
                                .option("password", mssql_properties["password"])
                                .option("query",
                                        
                                        """ 
                                            select

                                                coalesce(scrapreasonid,0) as scrapreasonid,
                                                [name] as scrapname
                                                
                                            from production.scrapreason
                                        """                                        
                                        ).load())
        
        return df_scrapreason_raw
    
    except RuntimeError as e:
        print(f"Fail to extract data from source: {e} ")
        print_exc()
        return None
    
    except Exception as e:
        print(f"unexpected error encountered: {e}")
        print_exc()
        return None, None, None

#stop spark session 
def spark_stop(spark):
    if spark:
        spark.stop()
        
#creating main function
def main():
    
    spark = None
    
    try:
        spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()
        
        if not spark:
            print("Spark Session failed. Exiting application")
            print_exc()
            exit(1)
            
        df_scrapreason = extract_scrapreason_raw(spark, mssql_properties, mssql_adventureworks)
        
        if df_scrapreason:
            df_scrapreason.write.parquet(f"{hadoop_landing}/scrapreason", mode="overwrite")
            
            print(f"""Dataframe loaded successfully on destination:    
            {df_scrapreason}""")
            
    except Exception as e:
        print(f"Main script encontered error: {e}")
        print_exc()
        
    finally:
        print("Stop spark session...")
        spark_stop(spark)
        
        print("Spark Session closed")
        
if __name__ == "__main__":
    main()