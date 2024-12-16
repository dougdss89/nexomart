from pyspark.sql.session import SparkSession
from spark.jobs.configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#loading source configurations

def load_source_connections():
    
    print("Importing .env file")
    
    config = load_config()
    
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        
        
        #creating spark session
        
        spark = (SparkSession
                 .builder
                    .appName("extract_stateprovince_raw")
                        .config('spark.jars', f"{spark_jars.get('mssql_spark_jars', '')}, {spark_jars.get('postgresql_jar', '')}")
                        .config('spark.driver.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')}, {spark_jars.get('postgresql_jar', '')}")
                        .config('spark.executor.extraClassPath', f"{spark_jars.get('mssql_spark_jars', '')}, {spark_jars.get('postgresql_jar', '')}")
                 .enableHiveSupport()
                 .getOrCreate()
                )
        
        return spark, mssql_properties, mssql_adventureworks
    
    except RuntimeError as e:
        print(f"Fail to create Spark Session: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error ocurred: {e}")
        print_exc()
        
        return None, None, None
    
def extract_stateprovince_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing some connections configurations")
        
        return None
    try:
        df_stateprovince_raw = (spark
                                    .read
                                    .format("jdbc")
                                    .option("url", mssql_adventureworks["url"])
                                    .option("driver", mssql_properties["driver"])
                                    .option("user", mssql_properties["user"])
                                    .option("password", mssql_properties["password"])
                                    .option("query", 
                                           
                                            """ 
                                            select

                                                coalesce (psp.TerritoryID, 0) territoryid,
                                                psp.CountryRegionCode,
                                                coalesce(psp.StateProvinceID, 0) as stateprovinceid,
                                                psp.StateProvinceCode,
                                                psp.name

                                            from person.stateprovince as psp
                                                                                        
                                            """           
                                ).load())
        
        return df_stateprovince_raw
    
    except RuntimeError as e:
        print(f"Fail to extract data from source: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error ocurred: {e}")
        print_exc()
        
        return None, None, None
    
def spark_stop(spark):
    if spark:
        spark.stop()
        
    else:
        print(f"Spark Session not founded: {spark}")
        
    return ("Spark session closed successfully")
        
#main point to execute script

def main():
    spark = None
    
    try:
        spark, mssql_properties, mssql_adventureworks = load_source_connections()
        
        if not spark:
            print("Spark Session not founded. Recreate")
            exit(1)
        
        df_stateprovince = extract_stateprovince_raw(spark, mssql_properties, mssql_adventureworks)
            
        if df_stateprovince:
            df_stateprovince.write.parquet("hdfs://ha-cluster/raw/stateprovince", mode='overwrite')
            
            print(f"""Dataframe loaded successfully on destination:    
            
                {df_stateprovince}""")
            
    except Exception as e:
        print(f"Main script encontered error: {e}")
        print_exc()
        
    finally:
        print("Stop spark session...")
        spark_stop(spark)
        
        print("Spark Session closed")
        
if __name__ == "__main__":
    main()