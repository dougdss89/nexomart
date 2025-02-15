from pyspark.sql.session import SparkSession
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#loading source configurations

def load_source_connections():
    
    print("Import .env file and creating Spark Session")
    
    #loading configurations and validated it
    try:
        config = load_config()
        print("File .env loaded")
        
        
    except FileExistsError as e:
        print("File .env not founded: {e}")
        
    except KeyError as e:
        print("Missing some key-values pair: {e}")
        
    #importing key-values from .env
    #validating data imported
    #validating imported keys
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        minio_landing = config.get("hadoop_paths", {}).get("minio_landing", "")

        validate_required_keys(config=spark_jars, 
                               required_keys=["mssql_spark_jars"],
                               config_name="spark_jars")
        
        #validade properties key
        validate_required_keys(config= mssql_adventureworks,
                               required_keys=["properties"],
                               config_name="mssql_adventureworks")
        
        validate_required_keys(config=minio_landing,
                               required_keys=["landing"],
                               config_name="minio_landing")
        
        spark = (SparkSession
                    .builder
                    .appName("extract_state_province_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, minio_landing
    
    except RuntimeError as e:
        print(f"Failed to create configuration: {e}")
        print_exc()
        
        return None
    
    except Exception as e:
        print(f"Unexpected error occur: {e}")
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
                                                psp.name,
									            getdate() as inserted_date,
	                                            '9999-31-12 00:00:00.000' as modified_date,
                                                '1' as eltversion
                                                

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
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()
        
        if not spark:
            print("Spark Session not founded. Recreate")
            exit(1)
        
        df_stateprovince = extract_stateprovince_raw(spark, mssql_properties, mssql_adventureworks)
            
        if df_stateprovince:
            df_stateprovince.write.parquet(f"{minio_landing}/stateprovince", mode="overwrite")
            
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