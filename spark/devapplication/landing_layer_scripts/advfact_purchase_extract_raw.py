from pyspark.sql.session import SparkSession
from spark.devapplication.modules.configconnections_loader_old import load_config, validate_required_keys
from sys import exit
from traceback import print_exc


#load connections from env variables

def load_source_connections():
    
    try:
        config = load_config()
        print("Configurations loaded successfully")
        
    except FileNotFoundError as e:
        print(f"File .env not found. Impossible to load environment variables: {e}")
        print_exc() 
        return None
    
    except KeyError as e:
        print(f"Missing key-values pair: {e}")
        print_exc()
        return None
    
    #validating imported keys
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
                    .appName("extract_fact_purchase_raw")
                        .config("spark.jars", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.executor.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .config("spark.driver.extraClassPath", f"{spark_jars.get('mssql_spark_jars', '')}")
                        .enableHiveSupport()
                    .getOrCreate())
        
        return spark, mssql_properties, mssql_adventureworks, hadoop_landing
    
    except RuntimeError as e:
        print(f"Fail to create Spark Session. Run time error: {e}")
        print_exc()
        return None
    
    except Exception as e:
        print(f"Unexpected error ocurred. Please verify: {e}")
        print_exc()
        exit(1)
        return None, None, None
    
#loading data from sources
def extract_fact_purchase_raw(spark, mssql_properties, mssql_adventureworks):
    
    if not all([spark, mssql_properties, mssql_adventureworks]):
        print("Missing key configurations or Spark Session.")
        exit(1)
        
    try:
        df_fact_purchase_raw = (spark
                                    .read
                                    .format('jdbc')
                                    .option("url", mssql_adventureworks["url"])
                                    .option("driver", mssql_properties["driver"])
                                    .option("user", mssql_properties["user"])
                                    .option("password", mssql_properties["password"])
                                    .option("query", 
                                            
                                    """ 
                                        select 

                                            pod.PurchaseOrderID,
                                            pod.PurchaseOrderDetailID,
                                            poh.revisionnumber,
                                            poh.[status],
                                            poh.employeeid,
                                            poh.vendorid,
                                            poh.orderdate,
                                            poh.shipdate,
                                            pod.duedate,
                                            shipmethodid,
                                            pod.productid,
                                            pod.receivedqty,
                                            pod.rejectedqty,
                                            pod.stockedqty,
                                            pod.unitprice,
                                            poh.freight,
                                            poh.taxamt,
                                            pod.linetotal,
                                            poh.subtotal,
                                            poh.totaldue,
                                            getdate() as inserted_date,
											'9999-31-12 00:00:00.000' as modified_date,
											'1' as eltversion
                                            
                                        from purchasing.PurchaseOrderDetail as pod

                                        left join

                                            purchasing.PurchaseOrderHeader as poh
                                        on pod.PurchaseOrderDetailID = poh.PurchaseOrderID
                                    """).load())
        
        return df_fact_purchase_raw
    
    except ConnectionError as e:
        print(f"Error to connect on database. Fail to extract data: {e}")
        print_exc()
        return None
    
    except TimeoutError as e:
        print(f"Timeout error to connect on database: {e}")
        print_exc()
        return None
    except Exception as e:
        print(f"Unexpected error ocurred: {e}")
        print_exc()
        exit(1)
        return None, None, None
    
#stop spark_session
def spark_stop(spark):
    
    try:
        spark.stop()
        print(f"Spark Session closed")
        
    except TimeoutError as e:
        print(f"Spark Session not founded: {e}")
        print_exc()
        return None
    
    except Exception as e:
        print(f"Unexpected error ocurred: {e}")
        print_exc()
        return None, None, None
    
#main script
def main():
    
    spark = None
    
    try:
        spark, mssql_properties, mssql_adventureworks, hadoop_landing = load_source_connections()
        
        if not spark:
            print("Spark Session not founded.")
            print_exc()
            exit(1)
            
        df_purchase = extract_fact_purchase_raw(spark, mssql_properties, mssql_adventureworks)
        
        if df_purchase:
            df_purchase.write.parquet(f"{hadoop_landing}/fact_purchase", mode="overwrite")
            
            print(f"""Dataframe loaded successfully on destination:    
            {df_purchase}""")
            
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