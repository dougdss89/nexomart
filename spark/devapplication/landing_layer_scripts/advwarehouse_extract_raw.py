from pyspark.sql.session import SparkSession
from configconnections_loader import load_config, validate_required_keys
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
        #get configurations to variables
        spark_jars = config.get("spark_jars", {})
        mssql_adventureworks = config.get("mssql_sources", {}).get("adventureworks", {})
        mssql_properties = mssql_adventureworks.get("properties", {})
        minio_landing = config.get("hadoop_paths", {}).get("minio_landing", "")
        
        validate_required_keys(
            config=spark_jars,
            required_keys=["mssql_spark_jars", "postgresql_jar"],
            config_name="spark_jars"
        )
        validate_required_keys(
            config=mssql_adventureworks,
            required_keys=["properties"],
            config_name="mssql_adventureworks"
        )
        
        validate_required_keys(
            config=minio_landing,
            required_keys=["landing"],
            config_name="minio_landing"
        )

        #creating Spark Session

        spark = (SparkSession
                    .builder
                        .appName("extract_warehouse_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                    .enableHiveSupport()
                    .getOrCreate()
                )

        return spark, mssql_properties, mssql_adventureworks, minio_landing

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
        df_warehouse_extract_raw = (spark
                                .read
                                .format("jdbc")
                                .option("url", mssql_adventureworks["url"])
                                .option("driver", mssql_properties["driver"])
                                .option("user", mssql_properties["user"])
                                .option("password", mssql_properties["password"])
                                .option("query",
                                        
                                        """ 
                                        select

                                            cast(ppi.locationID as int) as locationid,
                                            ppi.shelf,
                                            cast(ppi.bin as int) as bin,
                                            cast(ppi.quantity as int) as quantity,
                                            ppi.productid,
                                            pl.[availability],
                                            cast(pl.costrate as decimal(5,2)) as costrate,
                                            pl.[name],
                                            pbm.billofmaterialsid,
                                            pbm.componentid,
                                            pbm.startdate,
                                            cast(pbm.bomlevel as decimal(5,2)) as bomlevel,
                                            pbm.enddate,
                                            pbm.perassemblyqty,
                                            pbm.unitmeasurecode,
									        getdate() as inserted_date,
	                                        '9999-31-12 00:00:00.000' as modified_date,
                                            '1' as eltversion

                                        from production.productinventory as ppi
                                            left join
                                            production.[location] as pl
                                        on ppi.locationid = pl.locationid
                                            left join
                                            production.billofmaterials as pbm
                                        on ppi.productid = pbm.productassemblyid
                                        """                                        
                                        ).load())
        
        return df_warehouse_extract_raw
    
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
        spark, mssql_properties, mssql_adventureworks, minio_landing = load_source_connections()
        
        if not spark:
            print("Spark Session failed. Exiting application")
            print_exc()
            exit(1)
            
        df_warehouse = extract_scrapreason_raw(spark, mssql_properties, mssql_adventureworks)
        
        if df_warehouse:
            df_warehouse.write.parquet(f"{minio_landing}/warehouse", mode="overwrite")
            
            print(f"""Dataframe loaded successfully on destination:    
            {df_warehouse}""")
            
    except Exception as e:
        print(f"Main script encontered error: {e}")
        print_exc()
        
    finally:
        print("Stop spark session...")
        spark_stop(spark)
        
        print("Spark Session closed")
        
if __name__ == "__main__":
    main()