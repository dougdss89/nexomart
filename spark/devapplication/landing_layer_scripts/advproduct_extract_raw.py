from pyspark.sql import SparkSession
from configconnections_loader import load_config, validate_required_keys
from sys import exit
from traceback import print_exc

print("Start script")

def load_source_connections():
    
    """ this function loads .env file """
    
    try:
        print("loading .env file source")
        config = load_config()
        
        print("Configuration loaded")
        
    except FileNotFoundError as e:
        print(f"File .env was not founded. Verifiy file path: {e}")
        return None
    
    except KeyError as e:
        print("Some key-values pair was not loaded: {e}")
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
                    .appName("extract_product_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, minio_landing.get("minio_landing", "")
    
    except RuntimeError as e:
        print(f"Fail to validate configuration: {e}")
        return None, None, None
    
    except Exception as e:
        print(f"Unexpected error ocurred: {e}")
        print_exc()
        return None, None, None
    
    
def extract_product_raw(spark, mssql_properties, mssql_adventureworks):
    
    """ extraction product table from source """
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print(f"Error to load variables from environment")
        
        return None
    
    try:
        df_product_raw = (spark
                            .read
                            .format("jdbc")
                            .option("driver", mssql_properties["driver"])
                            .option("url", mssql_adventureworks["url"])
                            .option("user", mssql_properties["user"])
                            .option("password", mssql_properties["password"])
                            .option("query", """
                                    
                                        select

                                            pp.ProductID,
                                            pp.name as productname,
                                            pp.productnumber,
                                            pps.productsubcategoryid,
                                            pps.name subcategoryname,
                                            ppc.productcategoryid,
                                            ppc.name as categoryname,
                                            pp.color,
                                            pp.class,
                                            pp.size,
                                            pp.productline,
                                            pp.productmodelid,
                                            ppm.name as modelname,
                                            pp.style,
                                            pp.listprice,
                                            pp.standardcost,
                                            sellstartdate,
                                            pp.sellenddate,
                                            cast(pp.safetystocklevel as int) as safetystocklevel,
                                            cast(pp.reorderpoint as int) as reorderpoint,
                                            pp.sizeunitmeasurecode,
                                            pp.weightunitmeasurecode,
                                            pp.[weight],
                                            pp.daystomanufacture,
                                            pp.discontinueddate,
                                            getdate() as inserted_date,
									        '9999-31-12 00:00:00.000' as modified_date,
									        '1' as eltversion

                                        from production.product as pp
                                        left join
                                            production.productsubcategory as pps
                                        on pp.productsubcategoryid = pps.productsubcategoryid
                                        left join
                                            production.productcategory as ppc
                                        on pps.productcategoryid = ppc.productcategoryid
                                        left join
                                            production.productmodel as ppm
                                        on pp.productmodelid = ppm.productmodelid"""
                        ).load())
        
        return df_product_raw
    
    except Exception as e:
        print(f"Error extraction product data: {e}")
        print_exc()
        return None
    
def stop_spark(spark):
    if spark:
        
        spark.stop()
        
def main():
    """ main entry point for script"""
    
    spark = None
    try:
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()
        
        if not spark:
            print("Failed to create Spark Session")
            exit(1)
            
        product_df = extract_product_raw(spark, mssql_properties, mssql_adventureworks)
        
        if product_df:
            product_df.repartition(2).write.parquet(f"{minio_landing}/product", mode="overwrite")
            print("Dataframe loaded successfully to HDFS")
            
    except Exception as e:
        print("Main script error: {e}")
        print_exc()
        return None
    
    finally:
        stop_spark(spark)
        print("Spark Session was closed successfully")
        
if __name__ == "__main__":
    main()