from pyspark.sql.session import SparkSession
from spark.jobs.configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

#starting load script
def load_source_connections():
    
    #loading key-values pair from env variables
    
    print("Loading env variables from .env file")
    
    try:
        config = load_config()
        print(".env file loaded successfully")
        
    except FileNotFoundError as e:
        print(f"File not founded: {e}")
        print_exc()
        
    except KeyError as e:
        print(f"Error to load some key-values pair from env variables: {e}")
        print_exc()
    
    #validade keys and create spark session
    try:
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        
        #validade imported keys
        validate_required_keys(config=spark_jars,
                               required_keys=["mssql_spark_jars", "postgresql_jar"],
                               config_name="spark_jars"
                            )
        
        validate_required_keys(config=mssql_adventureworks, #var name that received config from get method
                               required_keys=["properties"], #check if properties was imported 
                               config_name="mssql_adventureworks") #configuration source from properties
        
        spark = (SparkSession
                    .builder
                        .appName("extract_fact_production_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate()
                )
        
        return spark, mssql_properties, mssql_adventureworks
    
    except RuntimeError as e:
        print(f"Runtime error to validade keys or create Spark Session: {e}")
        print_exc()
        
    except Exception as e:
        print(f"Unexpected error encountered: {e}")
        print_exc()
        exit(1)
        
def extract_store_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print(f"Error to load variables from environment")
        
    try:
        df_fact_production_raw = (spark
                            .read
                            .format("jdbc")
                            .option("url", mssql_adventureworks["url"])
                            .option("driver", mssql_properties["driver"])
                            .option("user", mssql_properties["user"])
                            .option("password", mssql_properties["password"])
                            .option("query",
                                
                            """ 
                                select

                                    pw.workorderid,
                                    pw.productid,
                                    pw.orderqty,
                                    pw.stockedqty,
                                    pw.scrappedqty,
                                    pw.scrapreasonid,
                                    pwr.locationid,
                                    pwr.actualresourcehrs,
                                    pwr.plannedcost,
                                    pwr.actualcost,
                                    pwr.scheduledstartdate,
                                    pwr.scheduledenddate,
                                    pwr.actualstartdate,
                                    pwr.actualenddate,
                                    pw.startdate,
                                    pw.enddate,
                                    pw.duedate
                                    
                                from production.workorder as pw
                                left join
                                    production.workorderrouting as pwr
                                on pw.workorderid = pwr.workorderid 
                            """
                            ).load())
        
        return df_fact_production_raw
    
    except ConnectionRefusedError as e:
        print(f"Error to connect on database source: {e}")
        print_exc()
        
    except TimeoutError as e:
        print(f"Timeout error. Verify connection network: {e}")
        print_exc()
        
    except Exception as e:
        print(f"Some unexpected error ocurred: {e}")
        print_exc()
        exit(1)

#stop spark session
def spark_stop(spark):
    if spark:
        spark.stop()
        

#main script function
def main():
    
    spark = None
    
    try:
        spark, mssql_properties, mssql_adventureworks = load_source_connections()
        
        if not spark:
            print(f"Fail to create Spark Session: {spark}")
            print_exc()
            exit(1)
            
        #create and write dataframe 
        df_fact_production = extract_store_raw(spark, mssql_properties, mssql_adventureworks)
        
        if df_fact_production:
            df_fact_production.write.parquet("hdfs://ha-cluster/raw/fact_production", mode="overwrite")
            print(f"""Dataframe loaded successfully on destination:    
            {df_fact_production}""")
            
    except RuntimeError as e:
        print(f"Error to execute script: {e}")
        
    except ConnectionError as e:
        print(f"Error to connect on Data Lake Hadoop: {e}")
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        print_exc
        exit(1)
        
    finally:
        spark_stop(spark)
        print("Spark Session closed")
        
if __name__ == "__main__":
    main()
    print(f"Script successfully executed")
