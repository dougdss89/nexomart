from pyspark.sql import SparkSession
from spark.devapplication.modules.configconnections_loader_old import load_config, validate_required_keys
from sys import exit
from traceback import print_exc


def load_source_connections():
    
    print("Starting load .env")
    
    try:
        config = load_config()
        
    except FileNotFoundError as e:
        print(f"Config file .env was not load: {e}")
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
                    .appName("extract_promotion_raw")
                        .config("spark.jars", f"{spark_jars.get("mssql_spark_jars", "")}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get("mssql_spark_jars", "")}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get("mssql_spark_jars", "")}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, hadoop_landing
    
    except RuntimeError as e:
        print(f"Fail to validate configurations: {e}")
        print_exc()
        return None
    
    except Exception as e:
        print(f"Unexpected error occur: {e}")
        print_exc()
        return None, None, None
    
#load raw data to dataframe
def extract_promotion_raw(spark, mssql_properties, mssql_adventureworks):
    
    #check if config variables was loaded
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing configurations")
        return None
    try: 
        df_promotion_raw = (spark
                                .read
                                .format("jdbc")
                                .option("url", mssql_adventureworks["url"])
                                .option("driver", mssql_properties["driver"])
                                .option("user", mssql_properties["user"])
                                .option("password", mssql_properties["password"])
                                .option("query", 
                                        
                                        """ 
                                        select 

                                            coalesce(specialofferid, 0) as specialofferid,
                                            [description],
                                            discountpct,
                                            [type],
                                            category,
                                            minqty,
                                            maxqty,
                                            startdate,
                                            enddate,
                                            getdate() as inserted_date,
                                            '9999-31-12 00:00:00.000' as modified_date,
                                            '1' as eltversion

                                        from sales.specialoffer
                                        
                                        """
                                ).load())
        return df_promotion_raw
    
    except Exception as e:
        print(f"Error to extract and create {df_promotion_raw} dataframe: {e}")
        print_exc()
        return None, None, None
    
def spark_stop(spark):
    if spark:
        spark.stop()
        
def main():
    
    #main script point
    spark = None
    
    try:
        spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()
        
        if not spark:
            print("Missing Spark Session")
            print_exc()
            exit(1)
            
        promotion_df = extract_promotion_raw(spark, mssql_properties, mssql_adventureworks)
            
        if promotion_df:
            promotion_df.write.parquet(f"{hadoop_landing}/promotion", mode="overwrite")
                        
            print(f"""Dataframe loaded successfully on destination:
                  
            {promotion_df}""")
            
                        
    except Exception as e:
        print(f"Main script encontered error: {e}")
        print_exc()
        
    finally:
        print("Stop spark session...")
        spark_stop(spark)
        
        print("Spark Session closed")
        
if __name__ == "__main__":
    main()